import express from "express";
import pool from "../db.js";
import { autoDistributeTargetsOldBranch } from "./allocations.js";


export const transferRouter = express.Router();

function updateEmployeeTransferFromAllocations(conn, period, branchId, userId) {
  return new Promise((resolve, reject) => {
    conn.query(
      `SELECT kpi, amount FROM allocations
       WHERE period=? AND branch_id=? AND user_id=? AND state='Transfered'`,
      [period, branchId, userId],
      (err, rows) => {
        if (err) return reject(err);
        if (!rows.length)
          return reject(new Error("No transfer allocations found"));

        const mapping = {
          deposit: "deposit_target",
          loan_gen: "loan_gen_target",
          loan_amulya: "loan_amulya_target",
          recovery: "recovery_target",
          audit: "audit_target",
        };

        const updateData = {};
        rows.forEach((r) => {
          if (mapping[r.kpi]) updateData[mapping[r.kpi]] = r.amount;
        });

        conn.query(
          "UPDATE employee_transfer SET ? WHERE period=? AND old_branch_id=? AND staff_id=?",
          [updateData, period, branchId, userId],
          (err) => {
            if (err) return reject(err);
            resolve(updateData);
          },
        );
      },
    );
  });
}
function updateProratedTargetsFn(req, res) {
  const { staff_id, period, old_branchId, new_branchId } = req.body;

  if (!staff_id || !period || !old_branchId || !new_branchId) {
    return res.status(400).json({
      error: "staff_id, period, old_branchId, new_branchId are required",
    });
  }

  function getFY(period) {
    const [startStr, endStr] = period.split("-");
    const startYear = parseInt(startStr);
    const endYear = startYear - (startYear % 100) + parseInt(endStr);
    return {
      start: new Date(Date.UTC(startYear, 3, 1)),
      end: new Date(Date.UTC(endYear, 2, 31)),
    };
  }

  function monthDiffstart(d1, d2) {
    return Math.max(
      0,
      (d2.getFullYear() - d1.getFullYear()) * 12 +
        (d2.getMonth() - d1.getMonth()),
    );
  }

  function monthDiffend(d1, d2) {
    return Math.max(
      0,
      (d2.getFullYear() - d1.getFullYear()) * 12 +
        (d2.getMonth() - d1.getMonth()) +
        1,
    );
  }

  const fy = getFY(period);

  pool.getConnection((err, conn) => {
    if (err) return res.status(500).json({ error: "DB Connection error" });

    conn.beginTransaction((err) => {
      if (err) return rollback("Transaction start failed");

      conn.query(
        "SELECT transfer_date FROM users WHERE id=? AND period = ?",
        [staff_id ,period],
        (err, staffRows) => {
          if (err) return rollback(err);
          if (!staffRows.length)
            return rollback("No staff found with given staff_id");

          const userTd = new Date(staffRows[0].transfer_date);

          conn.query(
            "SELECT transfer_date FROM bm_transfer_target WHERE staff_id=? AND period=? ORDER BY id DESC LIMIT 1",
            [staff_id, period],
            (err, bmRows) => {
              if (err) return rollback(err);

              if (!bmRows.length) {
                return handleCase_UpdateAndInsert("A_NoUserTransfer", userTd);
              }

              const bmTd = new Date(bmRows[0].transfer_date);

              if (userTd >= fy.start && userTd <= fy.end) {
                return handleCase_InsideFY(userTd, bmTd);
              }

              return handleCase_UpdateAndInsert("B2_OutsideFY", userTd);
            },
          );
        },
      );

      function handleCase_UpdateAndInsert(caseType, userTd) {
        const empSql = `
          SELECT * FROM employee_transfer 
          WHERE staff_id=? AND period=? 
          ORDER BY transfer_date DESC LIMIT 1
        `;

        conn.query(empSql, [staff_id, period], (err, empRows) => {
          if (err) return rollback(err);
          if (!empRows.length) return rollback("No employee_transfer found");

          const emp = empRows[0];
          const empTd = new Date(emp.transfer_date);

          const empMonths = monthDiffstart(fy.start, empTd);

          const updatedEmp = {
            deposit_target: (emp.deposit_target / 12) * empMonths,
            loan_gen_target: (emp.loan_gen_target / 12) * empMonths,
            loan_amulya_target: (emp.loan_amulya_target / 12) * empMonths,
            audit_target: (emp.audit_target / 12) * empMonths,
            recovery_target: (emp.recovery_target / 12) * empMonths,
            insurance_target: (emp.insurance_target / 12) * empMonths,
          };

          conn.query(
            "UPDATE employee_transfer SET ? WHERE id=?",
            [updatedEmp, emp.id],
            (err) => {
              if (err) return rollback(err);

              conn.query(
                "SELECT * FROM targets WHERE period=? AND branch_id=?",
                [period, new_branchId],
                (err, targets) => {
                  if (err) return rollback(err);
                  if (!targets.length)
                    return rollback("No target master found");

                  const t = targets.reduce((acc, curr) => {
                    acc[curr.kpi] = curr.amount;
                    return acc;
                  }, {});

                  const bmMonths = monthDiffend(empTd, fy.end);
                  const bmRatio = bmMonths / 12;

                  const insertBm = `
                    INSERT INTO bm_transfer_target
                    (staff_id, branch_id, transfer_date, deposit_target, loan_gen_target, loan_amulya_target,
                     audit_target, recovery_target, insurance_target, period)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                  `;

                  const bmValues = [
                    staff_id,
                    new_branchId,
                    userTd,
                    (t.deposit || 0) * bmRatio,
                    (t.loan_gen || 0) * bmRatio,
                    (t.loan_amulya || 0) * bmRatio,
                    (t.audit || 0) * bmRatio,
                    (t.recovery || 0) * bmRatio,
                    (t.insurance || 0) * bmRatio,
                    period,
                  ];

                  conn.query(insertBm, bmValues, (err, result) => {
                    if (err) return rollback(err);
                    commit({
                      case: caseType,
                      inserted_id: result.insertId,
                    });
                  });
                },
              );
            },
          );
        });
      }

      function handleCase_InsideFY(userTd, bmTd) {
        const bmSql =
          "SELECT * FROM bm_transfer_target WHERE staff_id=? AND period=? ORDER BY id DESC LIMIT 1";

        conn.query(bmSql, [staff_id, period], (err, bmRows) => {
          if (err) return rollback(err);
          if (!bmRows.length) return rollback("No BM record found");

          const bm = bmRows[0];

          // Correct months = user.transfer_date → bm.transfer_date
          const months = monthDiffstart(userTd, bmTd);

          //  Fetch branch entries
          const entriesSql = `
            SELECT * FROM entries 
            WHERE branch_id=? AND period=? AND status='Verified'
            AND date >= ? AND date < ?
          `;

          conn.query(
            entriesSql,
            [old_branchId, period, bmTd, userTd],
            (err, entryRows) => {
              if (err) return rollback(err);

              //  Fetch insurance
              const insSql = `
                SELECT * FROM entries 
                WHERE employee_id=? AND period=? AND status='Verified'
                AND kpi='insurance'
              `;

              conn.query(
                insSql,
                [staff_id, period, bmTd, userTd],
                (err, insRows) => {
                  if (err) return rollback(err);

                  const updatedEmp = {
                    deposit_target: (bm.deposit_target / 12) * months,
                    loan_gen_target: (bm.loan_gen_target / 12) * months,
                    loan_amulya_target: (bm.loan_amulya_target / 12) * months,
                    audit_target: (bm.audit_target / 12) * months,
                    recovery_target: (bm.recovery_target / 12) * months,
                    insurance_target: (bm.insurance_target / 12) * months,

                    deposit_achieved: entryRows
                      .filter((e) => e.kpi === "deposit")
                      .reduce((sum, e) => sum + e.value, 0),

                    loan_gen_achieved: entryRows
                      .filter((e) => e.kpi === "loan_gen")
                      .reduce((sum, e) => sum + e.value, 0),

                    loan_amulya_achieved: entryRows
                      .filter((e) => e.kpi === "loan_amulya")
                      .reduce((sum, e) => sum + e.value, 0),

                    audit_achieved: entryRows
                      .filter((e) => e.kpi === "audit")
                      .reduce((sum, e) => sum + e.value, 0),

                    recovery_achieved: entryRows
                      .filter((e) => e.kpi === "recovery")
                      .reduce((sum, e) => sum + e.value, 0),

                    insurance_achieved: insRows.reduce(
                      (sum, e) => sum + e.value,
                      0,
                    ),
                  };

                  // Update employee_transfer
                  const updateSql = `
                    UPDATE employee_transfer SET
                    deposit_target=?, loan_gen_target=?, loan_amulya_target=?,
                    audit_target=?, recovery_target=?, insurance_target=?,
                    deposit_achieved=?, loan_gen_achieved=?, loan_amulya_achieved=?,
                    audit_achieved=?, recovery_achieved=?, insurance_achieved=?
                    WHERE staff_id=? AND period=?
                  `;

                  conn.query(
                    updateSql,
                    [
                      updatedEmp.deposit_target,
                      updatedEmp.loan_gen_target,
                      updatedEmp.loan_amulya_target,
                      updatedEmp.audit_target,
                      updatedEmp.recovery_target,
                      updatedEmp.insurance_target,
                      updatedEmp.deposit_achieved,
                      updatedEmp.loan_gen_achieved,
                      updatedEmp.loan_amulya_achieved,
                      updatedEmp.audit_achieved,
                      updatedEmp.recovery_achieved,
                      updatedEmp.insurance_achieved,
                      staff_id,
                      period,
                    ],
                    (err) => {
                      if (err) return rollback(err);
                      const Months = monthDiffend(bmTd, fy.end);
                      const Ratio = Months / 12;
                      // Insert BM transfer
                      const insertBm = `
                        INSERT INTO bm_transfer_target
                        (staff_id, branch_id, transfer_date, deposit_target, loan_gen_target, loan_amulya_target,
                         audit_target, recovery_target, insurance_target, period)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                      `;

                      const bmVals = [
                        staff_id,
                        new_branchId,
                        userTd,
                        updatedEmp.deposit_target || 0 * Ratio,
                        updatedEmp.loan_gen_target || 0 * Ratio,
                        updatedEmp.loan_amulya_target || 0 * Ratio,
                        updatedEmp.audit_target || 0 * Ratio,
                        updatedEmp.recovery_target || 0 * Ratio,
                        updatedEmp.insurance_target || 0 * Ratio,
                        period,
                      ];

                      conn.query(insertBm, bmVals, (err, result) => {
                        if (err) return rollback(err);

                        commit({
                          case: "B1_InsideFY",
                          message:
                            "employee_transfer updated + bm_transfer_target inserted",
                          monthsBetween: months,
                          inserted_id: result.insertId,
                        });
                      });
                    },
                  );
                },
              );
            },
          );
        });
        return rollback("Inside FY logic unchanged");
      }

      function rollback(error) {
        conn.rollback(() => {
          conn.release();
          res.status(500).json({ error });
        });
      }

      function commit(response) {
        conn.commit(() => {
          conn.release();
          res.json(response);
        });
      }
    });
  });
}

export const getFinancialYearRange = (period) => {
  const [startStr, endStr] = period.split("-");

  const startYear = parseInt(startStr);
  const endYear = startYear - (startYear % 100) + parseInt(endStr);

  const start = new Date(Date.UTC(startYear, 3, 1));
  const end = new Date(Date.UTC(endYear, 2, 31));

  return { start, end };
};

//trasfer logic for create transfer
transferRouter.post("/transfer-staff-master", (req, res) => {
  const {
    staff_id,
    period,
    old_branchId,
    new_branchId,
    role,
    selectedRole,
    transferData,
  } = req.body;

  if (
    !staff_id ||
    !period ||
    (selectedRole === "Clerk" && !new_branchId) ||
    (selectedRole !== "HO_STAFF" &&
      selectedRole !== "Clerk" &&
      (!old_branchId || !new_branchId)) ||
    (selectedRole !== "Attender" &&
      selectedRole !== "Clerk" &&
      (!old_branchId || !new_branchId))
  ) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  if (role === "HO_STAFF" || role === "Attender") {
    pool.query(
      "UPDATE users SET transfer_date = NOW() WHERE id=? AND period = ?",
      [staff_id ,period],
      (err) => {
        if (err) {
          return res.status(500).json({
            error: "Failed to update transfer date",
          });
        }

        if (role === "Attender") {
          saveAttenderTransfer(pool, transferData, (err) => {
            if (err) {
              console.error(err);
              return res.status(500).json({
                error: "Attender transfer failed",
              });
            }

            return res.json({
              success: true,
              message: "Attender transfer saved successfully",
            });
          });
        }
        if (role === "HO_STAFF") {
          saveHoTransfer(pool, transferData, (err) => {
            if (err) {
              console.error(err);
              return res.status(500).json({
                error: "HO transfer failed",
              });
            }

            return res.json({
              success: true,
              message: "HO staff transfer saved successfully",
            });
          });
        }
      },
    );

    return; // stop here
  }

  pool.query(
    "UPDATE users SET transfer_date = NOW() WHERE id=? AND period = ?",
    [staff_id,period],
    (err) => {
      if (err) {
        return res
          .status(500)
          .json({ error: "Failed to update transfer date" });
      }

      autoDistributeTargetsOldBranch(period, old_branchId, role, (err) => {
        if (err) {
          return res
            .status(500)
            .json({ error: "Old branch distribution failed" });
        }

        const {
          staff_id,
          old_branch_id,
          new_branch_id,
          kpi_total,
          period,
          deposit_target,
          deposit_achieved,
          loan_gen_target,
          loan_gen_achieved,
          loan_amulya_target,
          loan_amulya_achieved,
          audit_target,
          audit_achieved,
          recovery_target,
          recovery_achieved,
          insurance_target,
          insurance_achieved,
          old_designation,
          new_designation,
        } = transferData;

        const transfer = {
          staff_id,
          old_branch_id,
          new_branch_id,
          kpi_total,
          period,
          deposit_target,
          deposit_achieved,
          loan_gen_target,
          loan_gen_achieved,
          loan_amulya_target,
          loan_amulya_achieved,
          audit_target,
          audit_achieved,
          recovery_target,
          recovery_achieved,
          insurance_target,
          insurance_achieved,
          old_designation,
          new_designation,
        };

        pool.query("INSERT INTO employee_transfer SET ?", transfer, (err) => {
          if (err) {
            return res
              .status(500)
              .json({ error: "Employee transfer insert failed" });
          }

          if (role === "BM") {
            updateProratedTargetsFn(
              { body: { staff_id, period, old_branchId, new_branchId } },
              res,
            );
          } else {
            updateEmployeeTransferFromAllocations(
              pool,
              period,
              old_branchId,
              staff_id,
            )
              .then(() => {
                res.json({
                  success: true,
                  message: "Employee transfer updated successfully",
                });
              })
              .catch((err) => {
                console.error(err);
                res.status(500).json({
                  error: "Employee transfer allocation update failed",
                });
              });
          }
        });
      });
    },
  );
});

// transfer logic for update transfer
transferRouter.post("/transfer-staff-master-update", (req, res) => {
  const {
    staff_id,
    period,
    old_branchId,
    new_branchId,
    role,
    selectedRole,
    transferData,
  } = req.body;

  if (
    !staff_id ||
    !period ||
    (selectedRole === "Clerk" && !new_branchId) ||
    (selectedRole !== "HO_STAFF" && (!old_branchId || !new_branchId)) ||
    (selectedRole !== "Attender" && (!old_branchId || !new_branchId))
  ) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  if (role === "HO_STAFF" || role === "Attender") {
    pool.query(
      "UPDATE users SET transfer_date = NOW() WHERE id=? AND period = ?",
      [staff_id ,period],
      (err) => {
        if (err) {
          return res.status(500).json({
            error: "Failed to update transfer date",
          });
        }

        if (role === "Attender") {
          saveAttenderTransfer(pool, transferData, (err) => {
            if (err) {
              console.error(err);
              return res.status(500).json({
                error: "Attender transfer failed",
              });
            }

            return res.json({
              success: true,
              message: "Attender transfer saved successfully",
            });
          });
        }
        if (role === "HO_STAFF") {
          saveHoTransfer(pool, transferData, (err) => {
            if (err) {
              console.error(err);
              return res.status(500).json({
                error: "HO transfer failed",
              });
            }

            return res.json({
              success: true,
              message: "HO staff transfer saved successfully",
            });
          });
        }
      },
    );

    return; // stop here
  }

  pool.query(
    "UPDATE users SET transfer_date = NOW() WHERE id=? AND period = ?",
    [staff_id , period],
    (err) => {
      if (err) {
        return res
          .status(500)
          .json({ error: "Failed to update transfer date" });
      }

      autoDistributeTargetsOldBranch(period, old_branchId, (err) => {
        if (err) {
          return res
            .status(500)
            .json({ error: "Old branch distribution failed" });
        }

        const {
          id,
          staff_id,
          old_branch_id,
          new_branch_id,
          kpi_total,
          period,
          deposit_target,
          deposit_achieved,
          loan_gen_target,
          loan_gen_achieved,
          loan_amulya_target,
          loan_amulya_achieved,
          audit_target,
          audit_achieved,
          recovery_target,
          recovery_achieved,
          insurance_target,
          insurance_achieved,
          old_designation,
          new_designation,
        } = transferData;

        const transfer = {
          staff_id,
          old_branch_id,
          new_branch_id,
          kpi_total,
          period,
          deposit_target,
          deposit_achieved,
          loan_gen_target,
          loan_gen_achieved,
          loan_amulya_target,
          loan_amulya_achieved,
          audit_target,
          audit_achieved,
          recovery_target,
          recovery_achieved,
          insurance_target,
          insurance_achieved,
          old_designation,
          new_designation,
        };

        pool.query(
          "UPDATE employee_transfer SET ? WHERE id = ?",
          [transfer, id],
          (err) => {
            if (err) {
              return res
                .status(500)
                .json({ error: "Employee transfer update failed" });
            }

            if (role === "BM") {
              updateProratedTargetsFn(
                { body: { staff_id, period, old_branchId, new_branchId } },
                res,
              );
            } else {
              updateEmployeeTransferFromAllocations(
                pool,
                period,
                old_branchId,
                staff_id,
              )
                .then(() => {
                  res.json({
                    success: true,
                    message: "Employee transfer updated successfully",
                  });
                })
                .catch((err) => {
                  console.error(err);
                  res.status(500).json({
                    error: "Employee transfer allocation update failed",
                  });
                });
            }
          },
        );
      });
    },
  );
});

function saveHoTransfer(pool, body, callback) {
  const { staff_id, hod_id, old_hod_id, period } = body;

  if (!staff_id || !period) {
    return callback(new Error("Missing required HO transfer fields"));
  }

  const values = [
    staff_id,
    body.kpi_total || 0,
    body.deposit_achieved || 0,
    body.loan_gen_achieved || 0,
    body.loan_amulya_achieved || 0,
    body.audit_achieved || 0,
    hod_id || null,
    old_hod_id || null,
    body.old_designation || null,
    body.new_designation || null,
    period,
    body.resiged || 0
  ];

  const sql = `
    INSERT INTO ho_staff_transfer (
      staff_id,
      transfer_date,
      kpi_total,
      \`Alloted_Work\`,
      \`Discipline_&_Time_Management\`,
      \`General_Work_Performance\`,
      \`Branch_Communication\`,
      hod_id,
      old_hod_id,
      old_designation,
      new_designation,
      period,
      resiged
    )
    VALUES (?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  pool.query(sql, values, callback);
}

transferRouter.post("/ho_transfer", (req, res) => {
  const { transferData } = req.body;

  if (!transferData) {
    return res.status(400).json({
      error: "Missing transferData payload",
    });
  }

  saveHoTransfer(pool, transferData, (err) => {
    if (err) {
      console.error("HO transfer error:", err);
      return res.status(500).json({
        error: err.message || "HO transfer failed",
      });
    }

    return res.json({
      success: true,
      message: "HO staff transfer saved successfully",
    });
  });
});

function saveAttenderTransfer(pool, body, callback) {
  const { staff_id, hod_id, old_hod_id, new_branch_id, old_branch_id, period } =
    body;

  if (!staff_id || !period) {
    return callback(new Error("Missing required Attender transfer fields"));
  }

  const values = [
    staff_id,
    body.kpi_total || 0,
    body.deposit_achieved || 0,
    body.loan_gen_achieved || 0,
    hod_id || null,
    old_hod_id || null,
    new_branch_id || null,
    old_branch_id || null,
    body.old_designation || null,
    body.new_designation || null,
    period,
    body.resiged || 0
  ];

  const sql = `
    INSERT INTO attender_transfer (
      staff_id,
      transfer_date,
      kpi_total,
      \`Cleanliness\`,
      \`Attitude_Behavior_&_Discipline\`,
      hod_id,
      old_hod_id,
      branch_id,
      old_branch_id,
      old_designation,
      new_designation,
      period,
      resiged
    )
    VALUES (?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  pool.query(sql, values, callback);
}
transferRouter.post("/attender_transfer", (req, res) => {
  const { transferData } = req.body;

  if (!transferData) {
    return res.status(400).json({
      error: "Missing transferData payload",
    });
  }

  saveAttenderTransfer(pool, transferData, (err) => {
    if (err) {
      console.error("Attender transfer error:", err);
      return res.status(500).json({
        error: err.message || "Attender transfer failed",
      });
    }

    return res.json({
      success: true,
      message: "Attender staff transfer saved successfully",
    });
  });
});
