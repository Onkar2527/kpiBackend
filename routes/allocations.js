import express from "express";
import pool from "../db.js";

// Router handling staff allocations: auto‑distribution and publish.
export const allocationsRouter = express.Router();

export const autoDistributeTargets = (period, branchId, callback) => {
  console.log(period, branchId);

  pool.query(
    "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (error, targets) => {
      if (error) return callback(error);
      if (targets.length === 0)
        return callback(new Error("No branch targets found"));

      pool.query(
        "SELECT * FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (error, staff) => {
          if (error) return callback(error);
          if (staff.length === 0)
            return callback(new Error("No active staff in branch"));

          const kpisToSplit = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "audit",
            "insurance",
            "recovery",
          ];
          const allocations = [];

          pool.query(
            "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
            [period, branchId, kpisToSplit],
            (error) => {
              if (error) return callback(error);

              kpisToSplit.forEach((kpi) => {
                const target = targets.find((t) => t.kpi === kpi);
                if (!target || !target.amount || target.amount <= 0) return;

                if (kpi === "audit") {
                  staff.forEach((user) => {
                    allocations.push([
                      period,
                      branchId,
                      user.id,
                      "audit",
                      target.amount,
                      "published",
                    ]);
                  });
                } else if (kpi === "insurance") {
                  staff.forEach((user) => {
                    allocations.push([
                      period,
                      branchId,
                      user.id,
                      kpi,
                      target.amount,
                      "published",
                    ]);
                  });
                } else {
                  const amount = target.amount;
                  const base = Math.floor(amount / staff.length);
                  const rem = amount % staff.length;

                  staff.forEach((user, idx) => {
                    allocations.push([
                      period,
                      branchId,
                      user.id,
                      kpi,
                      base + (idx < rem ? 1 : 0),
                      "published",
                    ]);
                  });
                }
              });

              pool.query(
                "INSERT INTO allocations (period, branch_id, user_id, kpi, amount, state) VALUES  ?",
                [allocations],
                (error) => {
                  if (error) return callback(error);
                  callback(null);
                }
              );
            }
          );
        }
      );
    }
  );
};

export const autoDistributeTargetsInTransfer = (period, branchId, callback) => {
  pool.query(
    "DELETE FROM allocations WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (error) => {
      if (error) return callback(error);
    }
  );

  pool.query(
    "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (error, targets) => {
      if (error) return callback(error);
      if (targets.length === 0)
        return callback(new Error("No branch targets found"));

      pool.query(
        "SELECT * FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (error, staff) => {
          if (error) return callback(error);
          if (staff.length === 0)
            return callback(new Error("No active staff in branch"));

          const kpisToSplit = ["deposit", "loan_gen", "loan_amulya"];
          const allocations = [];

          kpisToSplit.forEach((kpi) => {
            const target = targets.find((t) => t.kpi === kpi);
            const amount = target ? target.amount : 0;
            const base = Math.floor(amount / staff.length);
            const rem = amount % staff.length;
            staff.forEach((user, idx) => {
              allocations.push([
                period,
                branchId,
                user.id,
                kpi,
                base + (idx < rem ? 1 : 0),
                "published",
              ]);
            });
          });

          const auditTarget = targets.find((t) => t.kpi === "audit");
          if (auditTarget) {
            staff.forEach((user) => {
              allocations.push([
                period,
                branchId,
                user.id,
                "audit",
                auditTarget.amount,
                "published",
              ]);
            });
          }
          pool.query(
            "INSERT INTO allocations (period, branch_id, user_id, kpi, amount, state) VALUES ?",
            [allocations],
            (error) => {
              if (error) return callback(error);
              callback(null);
            }
          );
        }
      );
    }
  );
};

function getFinancialYearEnd(period) {
  const [startYr, endYr] = period.split("-");
  return new Date(`20${endYr}-03-31`); // Example: 2025-26 → 2026-03-31
}

function getMonthsWorked(resignedDate, periodEndDate) {
  const start = new Date(resignedDate);
  const end = new Date(periodEndDate);

  let months =
    (end.getFullYear() - start.getFullYear()) * 12 +
    (end.getMonth() - start.getMonth()) +
    1;

  return months < 0 ? 0 : months;
}

function calculateTargets(
  totalTarget,
  totalStaff,
  resignedStaffCount,
  monthsWorked
) {
  const perMonthTarget = totalTarget / 12;
  const perStaffPerMonth = perMonthTarget / totalStaff;

  const resignedTarget = perStaffPerMonth * monthsWorked;
  const remainingTarget = totalTarget - resignedTarget;

  const remainingStaff = totalStaff - resignedStaffCount;

  const newIndividualTarget =
    remainingStaff > 0 ? remainingTarget / remainingStaff : 0;

  return { resignedTarget, newIndividualTarget };
}
//resign user logic
export const autoDistributeTargetsResign = (period, branchId, callback) => {
  const periodEnd = getFinancialYearEnd(period);

  // 1. Fetch targets
  pool.query(
    "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (err, targets) => {
      if (err) return callback(err);
      if (!targets.length) return callback(new Error("No targets found"));

      // 2. Fetch staff
      pool.query(
        "SELECT id, name, resign, transfer_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));

          const activeStaff = staff.filter((s) => s.resign === 0);
          const resignedStaff = staff.filter((s) => s.resign === 1);
          const totalStaff = staff.length;

          const kpis = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "insurance",
            "audit",
          ];

          // 3. DELETE old allocations (we will only UPDATE, not insert fresh rows)
          pool.query(
            "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
            [period, branchId, kpis],
            (err) => {
              if (err) return callback(err);

              let updates = [];

              kpis.forEach((kpi) => {
                const t = targets.find((x) => x.kpi === kpi);
                if (!t) return;

                const totalTarget = t.amount;
                let totalResignedWorkedTarget = 0;

                
                resignedStaff.forEach((r) => {
                  if (!r.resign_date) return;

                  const monthsWorked = getMonthsWorked(
                    r.resign_date,
                    periodEnd
                  );

                  if (kpi === "audit") {
                    // AUDIT resigned staff calculation
                    const perMonth = totalTarget / 12;
                    const resignedAuditTarget = perMonth * monthsWorked;

                    updates.push([
                      Math.round(resignedAuditTarget),
                      "resigned",
                      period,
                      branchId,
                      r.id,
                      kpi,
                    ]);
                  } else {
                    // OTHER KPI resigned target
                    const { resignedTarget } = calculateTargets(
                      totalTarget,
                      totalStaff,
                      1,
                      monthsWorked
                    );

                    totalResignedWorkedTarget += resignedTarget;

                    updates.push([
                      Math.round(resignedTarget),
                      "resigned",
                      period,
                      branchId,
                      r.id,
                      kpi,
                    ]);
                  }
                });

               

                if (kpi === "audit") {
                  // AUDIT → give full target directly to each active staff
                  activeStaff.forEach((st) => {
                    updates.push([
                      Math.round(totalTarget),
                      "published",
                      period,
                      branchId,
                      st.id,
                      kpi,
                    ]);
                  });

                  return; // skip equal distribution
                }

                // OTHER KPIs → distribute remaining target equally
                const remainingTarget = totalTarget - totalResignedWorkedTarget;

                const perActive = Math.floor(
                  activeStaff.length ? remainingTarget / activeStaff.length : 0
                );

                activeStaff.forEach((st) => {
                  updates.push([
                    perActive,
                    "published",
                    period,
                    branchId,
                    st.id,
                    kpi,
                  ]);
                });
              });

              // 6. UPDATE QUERY (NOT INSERT)
              pool.query(
                `
                INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi)
                VALUES ?
                ON DUPLICATE KEY UPDATE 
                  amount = VALUES(amount),
                  state = VALUES(state)
                `,
                [updates],
                (err) => {
                  if (err) return callback(err);

                  callback(null, { message: "Target Update Successful" });
                }
              );
            }
          );
        }
      );
    }
  );
};
//new user add logic
export const autoDistributeTargetsNewUsers = (period, branchId, callback) => {
  const fy = getFinancialYearRange(period);
  console.log(fy, fy.start, fy.end);

  pool.query(
    "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (err, targets) => {
      if (err) return callback(err);
      if (!targets.length) return callback(new Error("No targets found"));

      pool.query(
        "SELECT id, name, user_add_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));

          const activeStaff = [];
          const newStaff = [];

          staff.forEach((s) => {
            if (!s.user_add_date) {
              activeStaff.push(s);
            } else {
              const joinDate = new Date(s.user_add_date);

              if (joinDate > fy.start && joinDate <= fy.end) {
                newStaff.push(s);
              } else {
                activeStaff.push(s);
              }
            }
          });

          const totalStaff = activeStaff.length + newStaff.length;
          const kpis = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "insurance",
            "audit",
          ];

          pool.query(
            "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
            [period, branchId, kpis],
            (err) => {
              if (err) return callback(err);

              let updates = [];

              kpis.forEach((kpi) => {
                const t = targets.find((x) => x.kpi === kpi);
                if (!t) return;

                const totalTarget = t.amount;
                let totalResignedWorkedTarget = 0;

                if (kpi === "audit") {
                  newStaff.forEach((ns) => {
                    const months = getMonthsWorked(ns.user_add_date, fy.end);
                    const newAudit = Math.floor((totalTarget / 12) * months);

                    updates.push([
                      newAudit,
                      "published",
                      period,
                      branchId,
                      ns.id,
                      kpi,
                    ]);
                  });

                  activeStaff.forEach((os) => {
                    updates.push([
                      Math.floor(totalTarget),
                      "published",
                      period,
                      branchId,
                      os.id,
                      kpi,
                    ]);
                  });

                  return;
                }

                let newStaffTotalGiven = 0;

                newStaff.forEach((ns) => {
                  const monthsWorked = getMonthsWorked(
                    ns.user_add_date,
                    fy.end
                  );

                  const newTarget = Math.floor(
                    (totalTarget / 12 / totalStaff) * monthsWorked
                  );

                  newStaffTotalGiven += newTarget;

                  updates.push([
                    newTarget,
                    "published",
                    period,
                    branchId,
                    ns.id,
                    kpi,
                  ]);
                });

                const remainingTarget =
                  totalTarget - totalResignedWorkedTarget - newStaffTotalGiven;

                const perOld = Math.floor(
                  activeStaff.length ? remainingTarget / activeStaff.length : 0
                );

                activeStaff.forEach((os) => {
                  updates.push([
                    perOld,
                    "published",
                    period,
                    branchId,
                    os.id,
                    kpi,
                  ]);
                });
              });

              pool.query(
                `
                INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi)
                VALUES ?
                ON DUPLICATE KEY UPDATE 
                  amount = VALUES(amount),
                  state = VALUES(state)
                `,
                [updates],
                (err) => {
                  if (err) return callback(err);
                  callback(null, { message: "Target Update Successful" });
                }
              );
            }
          );
        }
      );
    }
  );
};
//   FINANCIAL YEAR RANGE
export const getFinancialYearRange = (period) => {
  const [startStr, endStr] = period.split("-");

  const startYear = parseInt(startStr);
  const endYear = startYear - (startYear % 100) + parseInt(endStr);

  const start = new Date(Date.UTC(startYear, 3, 1));
  const end = new Date(Date.UTC(endYear, 2, 31));

  return { start, end };
};

//old Branch trasfer target distribution
export const autoDistributeTargetsOldBranch = (period, branchId, callback) => {
  const periodEnd = getFinancialYearEnd(period);
   const fy = getFinancialYearRange(period);

  // 1. Fetch targets
  pool.query(
    "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (err, targets) => {
      if (err) return callback(err);
      if (!targets.length) return callback(new Error("No targets found"));

      // 2. Fetch staff
      pool.query(
        "SELECT id, name, transfer_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));

          const activeStaff = [];
          const resignPrevoius = [];
          const resignedStaff = [];

          const currentDate = new Date();

          staff.forEach((s) => {
            if (!s.transfer_date) {
              activeStaff.push(s);
              return;
            }

            const joinDate = new Date(s.transfer_date);

            const isTodayTransfer =
              joinDate.toISOString().split("T")[0] ===
              currentDate.toISOString().split("T")[0];

            if (!isTodayTransfer) {
              resignPrevoius.push(s);
              return;
            }

            if (joinDate > fy.start && joinDate <= fy.end &&  joinDate.toISOString().split("T")[0] ===
              currentDate.toISOString().split("T")[0] ) {
              resignedStaff.push(s);
              return;
            }

            activeStaff.push(s);
          });
  
          const totalStaff =
            activeStaff.length + resignedStaff.length + resignPrevoius.length;

          const kpis = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "insurance",
            "audit",
          ];

          // 3. DELETE old allocations (we will only UPDATE, not insert fresh rows)
          pool.query(
            "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
            [period, branchId, kpis],
            (err) => {
              if (err) return callback(err);

              let updates = [];

              kpis.forEach((kpi) => {
                const t = targets.find((x) => x.kpi === kpi);
                if (!t) return;

                const totalTarget = t.amount;
                let totalResignedWorkedTarget = 0;

               
                resignedStaff.forEach((r) => {
                  if (!r.transfer_date) return;

                  const monthsWorked = getMonthsWorked(
                    r.transfer_date,
                    periodEnd
                  );

                  if (kpi === "audit") {
                    // AUDIT resigned staff calculation
                    const perMonth = totalTarget / 12;
                    const resignedAuditTarget = perMonth * monthsWorked;

                    updates.push([
                      Math.round(resignedAuditTarget),
                      "Transfered",
                      period,
                      branchId,
                      r.id,
                      kpi,
                    ]);
                  } else {
                    // OTHER KPI resigned target
                    const { resignedTarget } = calculateTargets(
                      totalTarget,
                      totalStaff,
                      1,
                      monthsWorked
                    );

                    totalResignedWorkedTarget += resignedTarget;

                    updates.push([
                      Math.round(resignedTarget),
                      "Transfered",
                      period,
                      branchId,
                      r.id,
                      kpi,
                    ]);
                  }
                });
                let totalResignedWorkedTargetPrevious = 0;
                resignPrevoius.forEach((r) => {
                  if (!r.transfer_date) return;

                  const monthsWorked = getMonthsWorked(
                    r.transfer_date,
                    periodEnd
                  );

                  if (kpi === "audit") {
                    // AUDIT resigned staff calculation
                    const perMonth = totalTarget / 12;
                    const resignedAuditTarget = perMonth * monthsWorked;

                    updates.push([
                      Math.round(resignedAuditTarget),
                      "published",
                      period,
                      branchId,
                      r.id,
                      kpi,
                    ]);
                  } else {
                    // OTHER KPI resigned target
                    const { resignedTarget } = calculateTargets(
                      totalTarget,
                      totalStaff,
                      1,
                      monthsWorked
                    );

                    totalResignedWorkedTargetPrevious += resignedTarget;

                    updates.push([
                      Math.round(resignedTarget),
                      "published",
                      period,
                      branchId,
                      r.id,
                      kpi,
                    ]);
                  }
                });

               
                if (kpi === "audit") {
                  activeStaff.forEach((st) => {
                    updates.push([
                      Math.round(totalTarget),
                      "published",
                      period,
                      branchId,
                      st.id,
                      kpi,
                    ]);
                  });

                  return; 
                }

                // OTHER KPIs → distribute remaining target equally
                const remainingTarget = totalTarget - totalResignedWorkedTarget;

                const perActive = Math.floor(
                  activeStaff.length ? remainingTarget / activeStaff.length : 0
                );

                activeStaff.forEach((st) => {
                  updates.push([
                    perActive,
                    "published",
                    period,
                    branchId,
                    st.id,
                    kpi,
                  ]);
                });
              });

              // 6. UPDATE QUERY (NOT INSERT)
              pool.query(
                `
                INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi)
                VALUES ?
                ON DUPLICATE KEY UPDATE 
                  amount = VALUES(amount),
                  state = VALUES(state)
                `,
                [updates],
                (err) => {
                  if (err) return callback(err);

                  callback(null, { message: "Target Update Successful" });
                }
              );
            }
          );
        }
      );
    }
  );
};
//new Branch trasfer target distribution
export const autoDistributeTargetsNewBranch = (period, branchId, callback) => {
  const fy = getFinancialYearRange(period);
  console.log(fy, fy.start, fy.end);

  pool.query(
    "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (err, targets) => {
      if (err) return callback(err);
      if (!targets.length) return callback(new Error("No targets found"));

      pool.query(
        "SELECT id, name, transfer_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));

          const activeStaff = [];
          const newStaff = [];

          staff.forEach((s) => {
            if (!s.transfer_date) {
              activeStaff.push(s);
            } else {
              const joinDate = new Date(s.transfer_date);

              if (joinDate > fy.start && joinDate <= fy.end) {
                newStaff.push(s);
              } else {
                activeStaff.push(s);
              }
            }
          });

          const totalStaff = activeStaff.length + newStaff.length;
          const kpis = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "insurance",
            "audit",
          ];

          pool.query(
            "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
            [period, branchId, kpis],
            (err) => {
              if (err) return callback(err);

              let updates = [];

              kpis.forEach((kpi) => {
                const t = targets.find((x) => x.kpi === kpi);
                if (!t) return;

                const totalTarget = t.amount;
                let totalResignedWorkedTarget = 0;

                if (kpi === "audit") {
                  newStaff.forEach((ns) => {
                    const months = getMonthsWorked(ns.transfer_date, fy.end);
                    const newAudit = Math.floor((totalTarget / 12) * months);

                    updates.push([
                      newAudit,
                      "published",
                      period,
                      branchId,
                      ns.id,
                      kpi,
                    ]);
                  });

                  activeStaff.forEach((os) => {
                    updates.push([
                      Math.floor(totalTarget),
                      "published",
                      period,
                      branchId,
                      os.id,
                      kpi,
                    ]);
                  });

                  return;
                }

                let newStaffTotalGiven = 0;

                newStaff.forEach((ns) => {
                  const monthsWorked = getMonthsWorked(
                    ns.transfer_date,
                    fy.end
                  );

                  const newTarget = Math.floor(
                    (totalTarget / 12 / totalStaff) * monthsWorked
                  );

                  newStaffTotalGiven += newTarget;

                  updates.push([
                    newTarget,
                    "published",
                    period,
                    branchId,
                    ns.id,
                    kpi,
                  ]);
                });

                const remainingTarget =
                  totalTarget - totalResignedWorkedTarget - newStaffTotalGiven;

                const perOld = Math.floor(
                  activeStaff.length ? remainingTarget / activeStaff.length : 0
                );

                activeStaff.forEach((os) => {
                  updates.push([
                    perOld,
                    "published",
                    period,
                    branchId,
                    os.id,
                    kpi,
                  ]);
                });
              });

              pool.query(
                `
                INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi)
                VALUES ?
                ON DUPLICATE KEY UPDATE 
                  amount = VALUES(amount),
                  state = VALUES(state)
                `,
                [updates],
                (err) => {
                  if (err) return callback(err);
                  callback(null, { message: "Target Update Successful" });
                }
              );
            }
          );
        }
      );
    }
  );
};

// across active staff (roles STAFF, ATTENDER, CLERK) in the given branch.
allocationsRouter.post("/auto-distribute", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargets(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

allocationsRouter.post("/auto-distribute-transfer", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsInTransfer(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

allocationsRouter.post("/auto-distribute-resign", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsResign(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

allocationsRouter.post("/auto-distribute-new-user", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsNewUsers(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});
allocationsRouter.post("/auto-distribute-old-branch", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsOldBranch(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});
allocationsRouter.post("/auto-distribute-new-branch", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsNewBranch(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

// Publish all allocations for a branch/period: sets state to published.
allocationsRouter.post("/publish", (req, res) => {
  const { period, branchId } = req.body || {};
  pool.query(
    "UPDATE allocations SET state = ? WHERE period = ? AND branch_id = ?",
    ["published", period, branchId],
    (error) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      res.json({ ok: true });
    }
  );
});

// Query allocations by period/branch or employee.
allocationsRouter.get("/", (req, res) => {
  const { period, branchId, employeeId } = req.query;
  if (!period) return res.status(400).json({ error: "period required" });

  const weightageQuery = "SELECT kpi, weightage FROM weightage";

  pool.query(weightageQuery, (err, weightRows) => {
    if (err) return res.status(500).json({ error: "Internal server error" });

    const weightMap = {};
    weightRows.forEach((w) => (weightMap[w.kpi] = w.weightage));

    if (employeeId) {
      const query = `
        SELECT a.*, w.weightage, e.achieved
        FROM allocations a 
        LEFT JOIN weightage w ON a.kpi = w.kpi 
        LEFT JOIN (
          SELECT kpi, SUM(value) as achieved 
          FROM entries 
          WHERE period = ? AND employee_id = ? AND status = 'Verified' 
          GROUP BY kpi
        ) e ON a.kpi = e.kpi
        WHERE a.period = ? AND a.user_id = ?
      `;

      pool.query(
        query,
        [period, employeeId, period, employeeId],
        (error, personalTargets) => {
          if (error)
            return res.status(500).json({ error: "Internal server error" });

          const branchQuery = `
          SELECT t.*, w.weightage, e.achieved
          FROM targets t 
          LEFT JOIN weightage w ON t.kpi = w.kpi 
          LEFT JOIN (
            SELECT kpi, SUM(value) as achieved 
            FROM entries 
            WHERE period = ? AND branch_id = ? AND status = 'Verified' 
            GROUP BY kpi
          ) e ON t.kpi = e.kpi
          WHERE t.period = ? AND t.branch_id = ?
        `;

          pool.query(
            branchQuery,
            [period, branchId, period, branchId],
            (error, branchTargets) => {
              if (error)
                return res.status(500).json({ error: "Internal server error" });

              const auditQuery = `
            SELECT 
              '${period}' AS period, 
              '${branchId}' AS branch_id, 
              'audit' AS kpi, 
              100 AS amount, 
              'published' AS state, 
              w.weightage, 
              e.achieved
            FROM (SELECT 1) AS dummy
            LEFT JOIN weightage w ON w.kpi = 'audit'
            LEFT JOIN (
              SELECT SUM(value) AS achieved 
              FROM entries 
              WHERE period = ? AND branch_id = ? AND status = 'Verified' AND kpi = 'audit'
            ) e ON 1=1
          `;

              pool.query(auditQuery, [period, branchId], (err, auditResult) => {
                if (err)
                  return res
                    .status(500)
                    .json({ error: "Internal server error" });

                // Insert audit if missing
                const hasAudit = branchTargets.some((t) => t.kpi === "audit");
                if (!hasAudit && auditResult.length > 0) {
                  branchTargets.push(auditResult[0]);
                }

                const personalKpiSet = new Set(
                  personalTargets.map((t) => t.kpi)
                );

                let finalBranchTargets = branchTargets.filter(
                  (t) => !personalKpiSet.has(t.kpi)
                );

                // Remove duplicates INSIDE branchTargets
                const uniqueBranch = {};
                finalBranchTargets = finalBranchTargets.filter((t) => {
                  if (!uniqueBranch[t.kpi]) {
                    uniqueBranch[t.kpi] = true;
                    return true;
                  }
                  return false;
                });

                const mustHavePersonal = ["deposit", "loan_gen", "audit"];

                mustHavePersonal.forEach((kpi) => {
                  if (!personalKpiSet.has(kpi)) {
                    personalTargets.push({
                      period,
                      user_id: employeeId,
                      branch_id: branchId,
                      kpi,
                      amount: 0,
                      achieved: 0,
                      weightage: weightMap[kpi] || 0,
                    });
                  }
                });

                const branchRequired = ["loan_amulya", "insurance", "recovery"];

                branchRequired.forEach((kpi) => {
                  if (!finalBranchTargets.some((t) => t.kpi === kpi)) {
                    finalBranchTargets.push({
                      period,
                      branch_id: branchId,
                      kpi,
                      amount: 0,
                      achieved: 0,
                      weightage: weightMap[kpi] || 0,
                    });
                  }
                });
                const personalKPIs = new Set(personalTargets.map((p) => p.kpi));

                finalBranchTargets = finalBranchTargets.filter(
                  (b) => !personalKPIs.has(b.kpi)
                );

                return res.json({
                  personal: personalTargets,
                  branch: finalBranchTargets,
                });
              });
            }
          );
        }
      );
    } else {
      let query = `
        SELECT a.*, u.name as staffName, w.weightage 
        FROM allocations a 
        JOIN users u ON a.user_id = u.id 
        LEFT JOIN weightage w ON a.kpi = w.kpi 
        WHERE a.period = ?
      `;
      const params = [period];

      if (branchId) {
        query += " AND a.branch_id = ?";
        params.push(branchId);
      }

      pool.query(query, params, (error, results) => {
        if (error)
          return res.status(500).json({ error: "Internal server error" });

        res.json(results);
      });
    }
  });
});
allocationsRouter.post("/update-prorated-targets", (req, res) => {
  const { staff_id, period } = req.body;

  pool.getConnection((err, conn) => {
    if (err) return res.status(500).json({ error: "Connection error", details: err });

    conn.beginTransaction((err) => {
      if (err) {
        conn.release();
        return res.status(500).json({ error: "Transaction start error" });
      }

      // 1. Fetch latest transfer record
      const selectQuery = `
        SELECT id, transfer_date,
          deposit_target, loan_gen_target, loan_amulya_target,
          audit_target, recovery_target, insurance_target
        FROM employee_transfer
        WHERE staff_id = ? AND period = ?
        ORDER BY transfer_date DESC
        LIMIT 1
      `;

      conn.query(selectQuery, [staff_id, period], (err, rows) => {
        if (err) {
          return conn.rollback(() => {
            conn.release();
            res.status(500).json({ error: "DB select error", details: err });
          });
        }

        if (rows.length === 0) {
          conn.release();
          return res.status(404).json({ message: "No transfer record found" });
        }

        const row = rows[0];

      

        const getFYEndDate = (period) => {
          const [startYearStr, endYearStr] = period.split("-");
          const endYear = Number("20" + endYearStr);
          return new Date(`${endYear}-03-31`);
        };

        const getMonthsWorked = (start, end) => {
          return (
            (end.getFullYear() - start.getFullYear()) * 12 +
            (end.getMonth() - start.getMonth()) +
            1
          );
        };

      

        const transferDate = new Date(row.transfer_date);
        const periodEndDate = getFYEndDate(period);

        const monthsWorked = getMonthsWorked(transferDate, periodEndDate);
        console.log(monthsWorked);
        
        const updatedTargets = {
          deposit_target: (row.deposit_target / 12) * monthsWorked,
          loan_gen_target: (row.loan_gen_target / 12) * monthsWorked,
          loan_amulya_target: (row.loan_amulya_target / 12) * monthsWorked,
          audit_target: (row.audit_target / 12) * monthsWorked,
          recovery_target: (row.recovery_target / 12) * monthsWorked,
          insurance_target: (row.insurance_target / 12) * monthsWorked,
        };

       
        const updateQuery = `UPDATE employee_transfer SET ? WHERE id = ?`;

        conn.query(updateQuery, [updatedTargets, row.id], (err) => {
          if (err) {
            return conn.rollback(() => {
              conn.release();
              res.status(500).json({ error: "DB update error", details: err });
            });
          }

          
          conn.commit((err) => {
            if (err) {
              return conn.rollback(() => {
                conn.release();
                res.status(500).json({ error: "Commit failed", details: err });
              });
            }

            conn.release();
            res.json({
              success: true,
              monthsWorked,
              updatedTargets,
            });
          });
        });
      });
    });
  });
});


