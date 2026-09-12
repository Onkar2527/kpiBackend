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
      (err, targetRows) => {
        if (err) return reject(err);
        if (!targetRows.length)
          return reject(new Error("No transfer allocations found"));

        const mapping = {
          deposit: "deposit_target",
          loan_gen: "loan_gen_target",
          loan_amulya: "loan_amulya_target",
          recovery: "recovery_target",
          audit: "audit_target",
        };

        const updateData = {};
        targetRows.forEach((r) => {
          if (mapping[r.kpi]) updateData[mapping[r.kpi]] = r.amount;
        });

        // Query the latest staffwise baselines for this user
        const baselineSql = `
          SELECT p1.kpi, p1.amount
          FROM previous_period_data_staffwise p1
          INNER JOIN (
              SELECT kpi, MAX(id) as max_id
              FROM previous_period_data_staffwise
              WHERE period=? AND branch_id=? AND employee_id=? AND deleted_at IS NULL
              GROUP BY kpi
          ) p2 ON p1.id = p2.max_id
        `;

        conn.query(baselineSql, [period, branchId, userId], (err, baselineRows) => {
          if (err) return reject(err);

          const baselineMapping = {
            deposit: "deposit_baseline",
            loan_gen: "loan_gen_baseline",
            loan_amulya: "loan_amulya_baseline",
            recovery: "recovery_baseline",
            audit: "audit_baseline",
            insurance: "insurance_baseline",
          };

          baselineRows.forEach((r) => {
            if (baselineMapping[r.kpi]) {
              updateData[baselineMapping[r.kpi]] = r.amount;
            }
          });

          conn.query(
            "UPDATE employee_transfer SET ? WHERE period=? AND old_branch_id=? AND staff_id=?",
            [updateData, period, branchId, userId],
            (err) => {
              if (err) return reject(err);
              resolve(updateData);
            },
          );
        });
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
                // CASE A: No BM transfer found -> tenure started at FY start
                const empMonths = Math.max(1, monthDiffstart(fy.start, userTd));
                const bmMonths = Math.max(0, monthDiffend(userTd, fy.end));
                const bmRatio = bmMonths / 12;
                return handleOldBranchAndNewBranch("A_NoUserTransfer", userTd, fy.start, empMonths, bmRatio);
              }

              const bmTd = new Date(bmRows[0].transfer_date);

              // CASE B1: Inside FY -> tenure started at previous transfer bmTd
              if (userTd >= fy.start && userTd <= fy.end) {
                const empMonths = Math.max(1, monthDiffstart(bmTd, userTd));
                const bmMonths = Math.max(0, monthDiffend(userTd, fy.end));
                const bmRatio = bmMonths / 12;
                return handleOldBranchAndNewBranch("B1_InsideFY", userTd, bmTd, empMonths, bmRatio);
              }

              // CASE B2: Outside FY
              const empMonths = Math.max(1, monthDiffstart(fy.start, userTd));
              const bmMonths = Math.max(0, monthDiffend(userTd, fy.end));
              const bmRatio = bmMonths / 12;
              return handleOldBranchAndNewBranch("B2_OutsideFY", userTd, fy.start, empMonths, bmRatio);
            },
          );
        },
      );

      function handleOldBranchAndNewBranch(caseType, userTd, startDate, empMonths, bmRatio) {
        // 1. Fetch employee_transfer record for this staff and old branch
        const empSql = `
          SELECT * FROM employee_transfer 
          WHERE staff_id=? AND period=? AND old_branch_id=?
          ORDER BY id DESC LIMIT 1
        `;

        conn.query(empSql, [staff_id, period, old_branchId], (err, empRows) => {
          if (err) return rollback(err);

          const proceedWithEmp = (emp) => {
            // 2. Fetch targets for old branch
            conn.query(
              "SELECT * FROM targets WHERE period=? AND branch_id=?",
              [period, old_branchId],
              (err, oldTargetRows) => {
                if (err) return rollback(err);

                const oldT = (oldTargetRows || []).reduce((acc, curr) => {
                  acc[curr.kpi] = Number(curr.amount || 0);
                  return acc;
                }, {});

                // 3. Fetch baselines for old branch
                conn.query(
                  "SELECT kpi, amount FROM previous_period_data WHERE period=? AND branch_id=?",
                  [period, old_branchId],
                  (err, oldPrevRows) => {
                    if (err) return rollback(err);

                    const oldB = {};
                    (oldPrevRows || []).forEach((p) => {
                      oldB[p.kpi] = Number(p.amount || 0);
                    });

                    // 4. Fetch branch entries during old tenure [startDate, userTd]
                    const entriesSql = `
                      SELECT kpi, SUM(value) AS achieved 
                      FROM entries 
                      WHERE branch_id=? AND period=? AND status='Verified'
                      AND date >= ? AND date < ?
                      GROUP BY kpi
                    `;

                    conn.query(
                      entriesSql,
                      [old_branchId, period, startDate, userTd],
                      (err, entryRows) => {
                        if (err) return rollback(err);

                        const achievedMap = {};
                        (entryRows || []).forEach((e) => {
                          achievedMap[e.kpi] = Number(e.achieved || 0);
                        });

                        // 5. Fetch insurance for staff during old tenure
                        const insSql = `
                          SELECT SUM(value) AS achieved 
                          FROM entries 
                          WHERE employee_id=? AND period=? AND status='Verified'
                          AND kpi='insurance'
                          AND date >= ? AND date < ?
                        `;

                        conn.query(
                          insSql,
                          [staff_id, period, startDate, userTd],
                          (err, insRows) => {
                            if (err) return rollback(err);

                            achievedMap["insurance"] = Number(insRows?.[0]?.achieved || 0);

                            // Calculate prorated targets & baselines for old branch
                            const oldDepositTarget = oldT.deposit !== undefined ? (oldT.deposit / 12) * empMonths : ((emp?.deposit_target || 0) / 12) * empMonths;
                            const oldLoanGenTarget = oldT.loan_gen !== undefined ? (oldT.loan_gen / 12) * empMonths : ((emp?.loan_gen_target || 0) / 12) * empMonths;
                            const oldLoanAmulyaTarget = oldT.loan_amulya !== undefined ? (oldT.loan_amulya / 12) * empMonths : ((emp?.loan_amulya_target || 0) / 12) * empMonths;
                            const oldAuditTarget = oldT.audit !== undefined ? (oldT.audit / 12) * empMonths : ((emp?.audit_target || 0) / 12) * empMonths;
                            const oldRecoveryTarget = oldT.recovery !== undefined ? (oldT.recovery / 12) * empMonths : ((emp?.recovery_target || 0) / 12) * empMonths;
                            const oldInsuranceTarget = oldT.insurance !== undefined ? (oldT.insurance / 12) * empMonths : ((emp?.insurance_target || 0) / 12) * empMonths;

                            const oldDepositBaseline = (Number(oldB.deposit || emp?.deposit_baseline || 0) / 12) * empMonths;
                            const oldLoanGenBaseline = (Number(oldB.loan_gen || emp?.loan_gen_baseline || 0) / 12) * empMonths;
                            const oldLoanAmulyaBaseline = (Number(oldB.loan_amulya || emp?.loan_amulya_baseline || 0) / 12) * empMonths;
                            const oldAuditBaseline = (Number(oldB.audit || emp?.audit_baseline || 0) / 12) * empMonths;
                            const oldRecoveryBaseline = (Number(oldB.recovery || emp?.recovery_baseline || 0) / 12) * empMonths;

                            const updateEmpSql = `
                              UPDATE employee_transfer SET
                                deposit_target=?, loan_gen_target=?, loan_amulya_target=?,
                                audit_target=?, recovery_target=?, insurance_target=?,
                                deposit_baseline=?, loan_gen_baseline=?, loan_amulya_baseline=?,
                                audit_baseline=?, recovery_baseline=?,
                                deposit_achieved=?, loan_gen_achieved=?, loan_amulya_achieved=?,
                                audit_achieved=?, recovery_achieved=?, insurance_achieved=?,
                                transfer_date=?
                              WHERE id=?
                            `;

                            const updateEmpVals = [
                              oldDepositTarget,
                              oldLoanGenTarget,
                              oldLoanAmulyaTarget,
                              oldAuditTarget,
                              oldRecoveryTarget,
                              oldInsuranceTarget,
                              oldDepositBaseline,
                              oldLoanGenBaseline,
                              oldLoanAmulyaBaseline,
                              oldAuditBaseline,
                              oldRecoveryBaseline,
                              achievedMap["deposit"] || 0,
                              achievedMap["loan_gen"] || 0,
                              achievedMap["loan_amulya"] || 0,
                              achievedMap["audit"] || 0,
                              achievedMap["recovery"] || 0,
                              achievedMap["insurance"] || 0,
                              userTd,
                              emp?.id,
                            ];

                            const executeEmpUpdate = (cb) => {
                              if (emp?.id) {
                                conn.query(updateEmpSql, updateEmpVals, (err) => {
                                  if (err) return rollback(err);
                                  cb();
                                });
                              } else {
                                cb();
                              }
                            };

                            executeEmpUpdate(() => {
                              // 6. Process New Branch targets & baselines
                              conn.query(
                                "SELECT * FROM targets WHERE period=? AND branch_id=?",
                                [period, new_branchId],
                                (err, newTargets) => {
                                  if (err) return rollback(err);
                                  if (!newTargets.length)
                                    return rollback("No target master found for new branch");

                                  const t = newTargets.reduce((acc, curr) => {
                                    acc[curr.kpi] = Number(curr.amount || 0);
                                    return acc;
                                  }, {});

                                  // Query baseline from previous_period_data for new branch
                                  const prevSql = `SELECT kpi, amount FROM previous_period_data WHERE period=? AND branch_id=?`;
                                  conn.query(prevSql, [period, new_branchId], (err, prevRows) => {
                                    if (err) return rollback(err);

                                    let deposit_baseline = 0;
                                    let loan_gen_baseline = 0;
                                    let loan_amulya_baseline = 0;
                                    let audit_baseline = 0;
                                    let recovery_baseline = 0;

                                    if (prevRows && prevRows.length > 0) {
                                      prevRows.forEach((p) => {
                                        if (p.kpi === "deposit") deposit_baseline = Number(p.amount || 0);
                                        if (p.kpi === "loan_gen") loan_gen_baseline = Number(p.amount || 0);
                                        if (p.kpi === "loan_amulya") loan_amulya_baseline = Number(p.amount || 0);
                                        if (p.kpi === "audit") audit_baseline = Number(p.amount || 0);
                                        if (p.kpi === "recovery") recovery_baseline = Number(p.amount || 0);
                                      });
                                    }

                                    const doInsertBm = (dBase, lgBase, laBase, auBase, recBase) => {
                                      const insertBm = `
                                        INSERT INTO bm_transfer_target
                                        (staff_id, branch_id, transfer_date, deposit_target, loan_gen_target, loan_amulya_target,
                                         audit_target, recovery_target, insurance_target, period,
                                         deposit_baseline, loan_gen_baseline, loan_amulya_baseline, audit_baseline, recovery_baseline)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                                        (dBase || 0) * bmRatio,
                                        (lgBase || 0) * bmRatio,
                                        (laBase || 0) * bmRatio,
                                        (auBase || 0) * bmRatio,
                                        (recBase || 0) * bmRatio,
                                      ];

                                      conn.query(insertBm, bmValues, (err, result) => {
                                        if (err) return rollback(err);

                                        commit({
                                          case: caseType,
                                          message:
                                            "employee_transfer updated + bm_transfer_target inserted",
                                          oldBranchMonths: empMonths,
                                          inserted_id: result.insertId,
                                        });
                                      });
                                    };

                                    if (!deposit_baseline && !loan_gen_baseline && !loan_amulya_baseline) {
                                      const empSql2 = `SELECT deposit_baseline, loan_gen_baseline, loan_amulya_baseline, audit_baseline, recovery_baseline FROM employee_transfer WHERE staff_id=? AND period=? ORDER BY id DESC LIMIT 1`;
                                      conn.query(empSql2, [staff_id, period], (err, empBRows) => {
                                        if (err) return rollback(err);
                                        if (empBRows && empBRows.length > 0) {
                                          deposit_baseline = Number(empBRows[0].deposit_baseline || 0);
                                          loan_gen_baseline = Number(empBRows[0].loan_gen_baseline || 0);
                                          loan_amulya_baseline = Number(empBRows[0].loan_amulya_baseline || 0);
                                          audit_baseline = Number(empBRows[0].audit_baseline || 0);
                                          recovery_baseline = Number(empBRows[0].recovery_baseline || 0);
                                        }
                                        doInsertBm(deposit_baseline, loan_gen_baseline, loan_amulya_baseline, audit_baseline, recovery_baseline);
                                      });
                                    } else {
                                      doInsertBm(deposit_baseline, loan_gen_baseline, loan_amulya_baseline, audit_baseline, recovery_baseline);
                                    }
                                  });
                                },
                              );
                            });
                          },
                        );
                      },
                    );
                  },
                );
              },
            );
          };

          if (empRows && empRows.length > 0) {
            proceedWithEmp(empRows[0]);
          } else {
            conn.query(
              "SELECT * FROM employee_transfer WHERE staff_id=? AND period=? ORDER BY id DESC LIMIT 1",
              [staff_id, period],
              (err, fallbackRows) => {
                if (err) return rollback(err);
                proceedWithEmp(fallbackRows?.[0] || null);
              },
            );
          }
        });
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
          deposit_baseline,
          loan_gen_target,
          loan_gen_achieved,
          loan_gen_baseline,
          loan_amulya_target,
          loan_amulya_achieved,
          loan_amulya_baseline,
          audit_target,
          audit_achieved,
          audit_baseline,
          recovery_target,
          recovery_achieved,
          recovery_baseline,
          insurance_target,
          insurance_achieved,
          insurance_baseline,
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
          deposit_baseline: deposit_baseline || 0,
          loan_gen_target,
          loan_gen_achieved,
          loan_gen_baseline: loan_gen_baseline || 0,
          loan_amulya_target,
          loan_amulya_achieved,
          loan_amulya_baseline: loan_amulya_baseline || 0,
          audit_target,
          audit_achieved,
          audit_baseline: audit_baseline || 0,
          recovery_target,
          recovery_achieved,
          recovery_baseline: recovery_baseline || 0,
          insurance_target,
          insurance_achieved,
          insurance_baseline: insurance_baseline || 0,
          old_designation,
          new_designation,
          transfer_date: new Date(),
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
          deposit_baseline,
          loan_gen_target,
          loan_gen_achieved,
          loan_gen_baseline,
          loan_amulya_target,
          loan_amulya_achieved,
          loan_amulya_baseline,
          audit_target,
          audit_achieved,
          audit_baseline,
          recovery_target,
          recovery_achieved,
          recovery_baseline,
          insurance_target,
          insurance_achieved,
          insurance_baseline,
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
          deposit_baseline: deposit_baseline || 0,
          loan_gen_target,
          loan_gen_achieved,
          loan_gen_baseline: loan_gen_baseline || 0,
          loan_amulya_target,
          loan_amulya_achieved,
          loan_amulya_baseline: loan_amulya_baseline || 0,
          audit_target,
          audit_achieved,
          audit_baseline: audit_baseline || 0,
          recovery_target,
          recovery_achieved,
          recovery_baseline: recovery_baseline || 0,
          insurance_target,
          insurance_achieved,
          insurance_baseline: insurance_baseline || 0,
          old_designation,
          new_designation,
          transfer_date: new Date(),
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
