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
function getFinancialYearStart(period) {
  const [startYr, endYr] = period.split("-");
  return new Date(`20${startYr}-04-01`); // Example: 2025-26 → 2026-03-31
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
        "SELECT id, name, resign, resign_date, transfer_date, user_add_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));
          const transferTargetMap = {
            deposit: 0,
            loan_gen: 0,
            loan_amulya: 0,
            recovery: 0,
          };

          pool.query(
            `
           SELECT 
            SUM(deposit_target)     AS deposit,
            SUM(loan_gen_target)    AS loan_gen,
            SUM(loan_amulya_target) AS loan_amulya,
            SUM(recovery_target)    AS recovery
          FROM employee_transfer
          WHERE old_branch_id = ?
            AND period = ?
            AND old_designation <> 'BM';

            `,
            [branchId, period],
            (err4, rows) => {
              if (err4) return callback(err4);

              const r = rows[0] || {};

              transferTargetMap.deposit = Number(r.deposit || 0);
              transferTargetMap.loan_gen = Number(r.loan_gen || 0);
              transferTargetMap.loan_amulya = Number(r.loan_amulya || 0);
              transferTargetMap.recovery = Number(r.recovery || 0);
              console.log(transferTargetMap);
              

              // NEW ARRAYS
              const transferStaff = [];
              const newJoinStaff = [];
              const oldStaff = [];
              const resignedStaff = staff.filter((s) => s.resign === 1);
              staff.forEach((s) => {
                if (s.resign === 1) return; // Skip resigned staff

                const td = s.transfer_date ? new Date(s.transfer_date) : null;
                const jd = s.user_add_date ? new Date(s.user_add_date) : null;

                if (td) {
                  if (td >= fy.start && td <= fy.end) {
                    transferStaff.push(s);
                  } else {
                    oldStaff.push(s); // Old staff
                  }
                  return;
                }

                if (jd && jd >= fy.start && jd <= fy.end) {
                  newJoinStaff.push(s);
                  return;
                }

                oldStaff.push(s);
              });

              // ACTIVE STAFF = OLD STAFF ONLY
              const activeStaff = oldStaff;

              const totalStaff =
                activeStaff.length +
                transferStaff.length +
                newJoinStaff.length +
                resignedStaff.length;

              const kpis = [
                "deposit",
                "loan_gen",
                "loan_amulya",
                "recovery",
                "insurance",
                "audit",
              ];

              // DELETE OLD ALLOCATIONS
              pool.query(
                "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
                [period, branchId, kpis],
                (err) => {
                  if (err) return callback(err);

                  let updates = [];

                  kpis.forEach((kpi) => {
                    const t = targets.find((x) => x.kpi === kpi);
                    if (!t) return;

                   const totalTarget = Math.max(
                      0,
                      t.amount - (transferTargetMap[kpi] || 0)
                    );
                   
                    

                    let totalResignedWorkedTarget = 0;
                    let totalTransferGiven = 0;
                    let totalNewJoinGiven = 0;

                    resignedStaff.forEach((r) => {
                      if (!r.resign_date) return;

                      const monthsWorked = getRemaingMonthsWorked(
                        r.resign_date,
                        periodEnd
                      );

                      if (kpi === "audit" || kpi === "insurance") {
                        const auditTarget = (totalTarget / 12) * monthsWorked;

                        updates.push([
                          Math.round(auditTarget),
                          "resigned",
                          period,
                          branchId,
                          r.id,
                          kpi,
                        ]);
                      } else {
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

                    transferStaff.forEach((ts) => {
                      const months = getMonthsWorked(
                        ts.transfer_date,
                        periodEnd
                      );

                      if (kpi === "audit" || kpi === "insurance") {
                        const auditTarget = (totalTarget / 12) * months;

                        updates.push([
                          Math.round(auditTarget),
                          "transfer",
                          period,
                          branchId,
                          ts.id,
                          kpi,
                        ]);
                      } else {
                        const perStaffAnnual = totalTarget / totalStaff;
                        const target = (perStaffAnnual * months) / 12;

                        totalTransferGiven += target;

                        updates.push([
                          Math.round(target),
                          "transfer",
                          period,
                          branchId,
                          ts.id,
                          kpi,
                        ]);
                      }
                    });

                    newJoinStaff.forEach((nj) => {
                      const months = getMonthsWorked(
                        nj.user_add_date,
                        periodEnd
                      );

                      if (kpi === "audit" || kpi === "insurance") {
                        const auditTarget = (totalTarget / 12) * months;

                        updates.push([
                          Math.round(auditTarget),
                          "new_join",
                          period,
                          branchId,
                          nj.id,
                          kpi,
                        ]);
                      } else {
                        const perStaffAnnual = totalTarget / totalStaff;
                        const target = (perStaffAnnual * months) / 12;

                        totalNewJoinGiven += target;

                        updates.push([
                          Math.round(target),
                          "new_join",
                          period,
                          branchId,
                          nj.id,
                          kpi,
                        ]);
                      }
                    });

                    if (kpi === "audit" || kpi === "insurance") {
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

                    const remainingTarget =
                      totalTarget -
                      totalResignedWorkedTarget -
                      totalTransferGiven -
                      totalNewJoinGiven;

                    const perActive = Math.floor(
                      activeStaff.length
                        ? remainingTarget / activeStaff.length
                        : 0
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

                  // UPSERT FINAL ALLOCATIONS
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

                      callback(null, {
                        message: "Target Update Successful",
                        summary: {
                          activeStaff: activeStaff.length,
                          transferStaff: transferStaff.length,
                          newJoinStaff: newJoinStaff.length,
                          resignedStaff: resignedStaff.length,
                        },
                      });
                    }
                  );
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

  pool.query(
    "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
    [period, branchId],
    (err, targets) => {
      if (err) return callback(err);
      if (!targets.length) return callback(new Error("No targets found"));

      pool.query(
        "SELECT id, name, user_add_date, transfer_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));

            const transferTargetMap = {
            deposit: 0,
            loan_gen: 0,
            loan_amulya: 0,
            recovery: 0,
          };

          pool.query(
            `
           SELECT 
            SUM(deposit_target)     AS deposit,
            SUM(loan_gen_target)    AS loan_gen,
            SUM(loan_amulya_target) AS loan_amulya,
            SUM(recovery_target)    AS recovery
          FROM employee_transfer
          WHERE old_branch_id = ?
            AND period = ?
            AND old_designation <> 'BM';

            `,
            [branchId, period],
            (err4, rows) => {
              if (err4) return callback(err4);

              const r = rows[0] || {};

              transferTargetMap.deposit = Number(r.deposit || 0);
              transferTargetMap.loan_gen = Number(r.loan_gen || 0);
              transferTargetMap.loan_amulya = Number(r.loan_amulya || 0);
              transferTargetMap.recovery = Number(r.recovery || 0);
              console.log(transferTargetMap);
              

          // NEW SEPARATE ARRAYS
          const transferStaff = [];
          const newJoinStaff = [];
          const activeStaff = [];

          staff.forEach((s) => {
            const td = s.transfer_date ? new Date(s.transfer_date) : null;
            const jd = s.user_add_date ? new Date(s.user_add_date) : null;

            if (td) {
              if (td >= fy.start && td <= fy.end) {
                transferStaff.push(s);
              } else {
                activeStaff.push(s);
              }
              return;
            }

            if (jd && jd >= fy.start && jd <= fy.end) {
              newJoinStaff.push(s);
              return;
            }

            activeStaff.push(s);
          });

          // TOTAL STAFF
          const totalStaff =
            activeStaff.length + transferStaff.length + newJoinStaff.length;

          // KPIs
          const kpis = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "insurance",
            "audit",
          ];

          // DELETE OLD ALLOCATIONS
          pool.query(
            "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
            [period, branchId, kpis],
            (err) => {
              if (err) return callback(err);

              let updates = [];

              kpis.forEach((kpi) => {
                const t = targets.find((x) => x.kpi === kpi);
                if (!t) return;

                const totalTarget = Math.max(
                0,
                t.amount - (transferTargetMap[kpi] || 0)
              );

                let totalTransferGiven = 0;
                let totalNewJoinGiven = 0;

                if (kpi === "audit" || kpi === "insurance") {
                  // Transfer staff audit target
                  transferStaff.forEach((ts) => {
                    const months = getMonthsWorked(ts.transfer_date, fy.end);
                    const auditTarget = Math.floor((totalTarget / 12) * months);

                    updates.push([
                      auditTarget,
                      "transfer",
                      period,
                      branchId,
                      ts.id,
                      kpi,
                    ]);
                  });

                  // New joined staff audit target
                  newJoinStaff.forEach((nj) => {
                    const months = getMonthsWorked(nj.user_add_date, fy.end);
                    const auditTarget = Math.floor((totalTarget / 12) * months);

                    updates.push([
                      auditTarget,
                      "new_join",
                      period,
                      branchId,
                      nj.id,
                      kpi,
                    ]);
                  });

                  // Active staff full target
                  activeStaff.forEach((as) => {
                    updates.push([
                      Math.floor(totalTarget),
                      "published",
                      period,
                      branchId,
                      as.id,
                      kpi,
                    ]);
                  });

                  return;
                }

                transferStaff.forEach((ts) => {
                  const monthsWorked = getMonthsWorked(
                    ts.transfer_date,
                    fy.end
                  );

                  const perStaffAnnual = totalTarget / totalStaff;
                  const target = (perStaffAnnual * monthsWorked) / 12;

                  totalTransferGiven += target;

                  updates.push([
                    Math.round(target),
                    "transfer",
                    period,
                    branchId,
                    ts.id,
                    kpi,
                  ]);
                });

                newJoinStaff.forEach((nj) => {
                  const monthsWorked = getMonthsWorked(
                    nj.user_add_date,
                    fy.end
                  );

                  const perStaffAnnual = totalTarget / totalStaff;
                  const target = (perStaffAnnual * monthsWorked) / 12;

                  totalNewJoinGiven += target;

                  updates.push([
                    Math.round(target),
                    "new_join",
                    period,
                    branchId,
                    nj.id,
                    kpi,
                  ]);
                });

                const remainingTarget =
                  totalTarget - totalTransferGiven - totalNewJoinGiven;

                const perActive = Math.floor(
                  activeStaff.length ? remainingTarget / activeStaff.length : 0
                );

                activeStaff.forEach((as) => {
                  updates.push([
                    perActive,
                    "published",
                    period,
                    branchId,
                    as.id,
                    kpi,
                  ]);
                });
              });

              // UPSERT DATA
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

                  callback(null, {
                    message: "Target Update Successful",
                    summary: {
                      activeStaff: activeStaff.length,
                      newJoinStaff: newJoinStaff.length,
                      transferStaff: transferStaff.length,
                    },
                  });
                }
              );
            }
          );
          });
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
function getRemaingMonthsWorked(resignedDate, periodEndDate) {
  const start = new Date(resignedDate);
  const end = new Date(periodEndDate);

  let months =
    (end.getFullYear() - start.getFullYear()) * 12 +
    (end.getMonth() - start.getMonth()) +
    1;
  months = 12 - months;
  return months < 0 ? 0 : months;
}
//old Branch trasfer target distribution
function monthDiff(d1, d2) {
  let months =
    (d2.getFullYear() - d1.getFullYear()) * 12 +
    (d2.getMonth() - d1.getMonth());

  return months < 0 ? 0 : months;
}

async function getActualMonthsWorked(pool, staffId, userTd, fyStart) {
  // 1. Get TWO latest transfer dates
  const dbDates = await new Promise((resolve) => {
    pool.query(
      "SELECT transfer_date FROM employee_transfer WHERE staff_id = ? ORDER BY id DESC LIMIT 2",
      [staffId],
      (err, rows) => {
        if (err || !rows.length) return resolve([]);
        resolve(rows);
      }
    );
  });

  let dbTd = null;

  if (dbDates.length === 2) {
    dbTd = dbDates[1].transfer_date; // SECOND latest 2025-12-10 00:00:00
  } else if (dbDates.length === 1) {
    dbTd = dbDates[0].transfer_date;
  }

  const dDb = dbTd ? new Date(dbTd) : null;
  const dUser = userTd ? new Date(userTd) : null;
  const fy = new Date(fyStart);
  const effectiveDb = dDb || dUser || new Date();


  if (dDb && dUser) {
    const earlier = dDb < dUser ? dDb : dUser;
    const later = dDb > dUser ? dDb : dUser;
    return monthDiff(earlier, later);
  }


  if (dDb) return monthDiff(fy, dDb);

  if (dUser) return monthDiff(fy, dUser);

  return monthDiff(fy, effectiveDb);
}

export const autoDistributeTargetsOldBranch = (period, branchId,role, callback) => {
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
        "SELECT id, name, transfer_date, user_add_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));

          const transferTargetMap = {
            deposit: 0,
            loan_gen: 0,
            loan_amulya: 0,
            recovery: 0,
          };

          pool.query(
            `
           SELECT 
            SUM(deposit_target)     AS deposit,
            SUM(loan_gen_target)    AS loan_gen,
            SUM(loan_amulya_target) AS loan_amulya,
            SUM(recovery_target)    AS recovery
          FROM employee_transfer
          WHERE old_branch_id = ?
            AND period = ?
            AND old_designation <> 'BM';

            `,
            [branchId, period],
            (err4, rows) => {
              if (err4) return callback(err4);

              const r = rows[0] || {};

              transferTargetMap.deposit = Number(r.deposit || 0);
              transferTargetMap.loan_gen = Number(r.loan_gen || 0);
              transferTargetMap.loan_amulya = Number(r.loan_amulya || 0);
              transferTargetMap.recovery = Number(r.recovery || 0);
              console.log(transferTargetMap);
              
          const activeStaff = [];
          const resignPrevoius = [];
          const resignedStaff = [];
          const newjoinerStaff = [];
          const currentDate = new Date();

          staff.forEach((s) => {
            const td = s.transfer_date ? new Date(s.transfer_date) : null;
            const jd = s.user_add_date ? new Date(s.user_add_date) : null;

            const today = currentDate.toISOString().split("T")[0];
            const transferDay = td ? td.toISOString().split("T")[0] : null;
           
            

            if (td) {
              if (transferDay === today) {
                resignedStaff.push(s);
              } else if (td < currentDate) {
                resignPrevoius.push(s);
              } else {
                activeStaff.push(s);
              }
              return;
            }

            if (jd && jd >= fy.start && jd <= fy.end) {
              newjoinerStaff.push(s);
              return;
            }

            activeStaff.push(s);
          });
          
          
          const totalStaff =
            activeStaff.length +
            resignedStaff.length +
            resignPrevoius.length +
            newjoinerStaff.length;

          const kpis = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "insurance",
            "audit",
          ];

          // 3. DELETE old allocations
          pool.query(
            "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
            [period, branchId, kpis],
            async (err) => {
              if (err) return callback(err);

              let updates = [];

              for (const kpi of kpis) {
                const t = targets.find((x) => x.kpi === kpi);
                if (!t) continue;
                let totalTarget
                if(role !== 'BM'){
                totalTarget = Math.max(
                0,
                t.amount - (transferTargetMap[kpi] || 0)
              );
            }else{
              totalTarget=t.amount;
            }
              console.log(totalTarget);
              
                let totalResignedWorkedTarget = 0;

                for (const r of resignedStaff) {
                  if (!r.transfer_date) continue;

                  const monthsWorked = await getActualMonthsWorked(
                    pool,
                    r.id,
                    r.user_add_date,
                    fy.start
                  );
                 
                  
                  if (kpi === "audit" || kpi === 'insurance') {
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
                }

                let totalResignedWorkedTargetPrevious = 0;

                for (const r of resignPrevoius) {
                  if (!r.transfer_date) continue;

                  const monthsWorked = getMonthsWorked(
                    r.transfer_date,
                    periodEnd
                  );

                  if (kpi === "audit" || kpi === 'insurance') {
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
                }

                if (kpi === "audit" || kpi === 'insurance') {
                  for (const st of activeStaff) {
                    updates.push([
                      Math.round(totalTarget),
                      "published",
                      period,
                      branchId,
                      st.id,
                      kpi,
                    ]);
                  }
                  continue;
                }

                let totalNewJoinerWorkedTargetPrevious = 0;

                for (const r of newjoinerStaff) {
                  if (!r.user_add_date) continue;

                  const monthsWorked = getMonthsWorked(
                    r.user_add_date,
                    periodEnd
                  );

                  if (kpi === "audit" || kpi === 'insurance') {
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
                    const { resignedTarget } = calculateTargets(
                      totalTarget,
                      totalStaff,
                      1,
                      monthsWorked
                    );

                    totalNewJoinerWorkedTargetPrevious += resignedTarget;

                    updates.push([
                      Math.round(resignedTarget),
                      "published",
                      period,
                      branchId,
                      r.id,
                      kpi,
                    ]);
                  }
                }

                const remainingTarget =
                  totalTarget -
                  totalResignedWorkedTarget -
                  totalResignedWorkedTargetPrevious -
                  totalNewJoinerWorkedTargetPrevious;

                const perActive = Math.floor(
                  activeStaff.length ? remainingTarget / activeStaff.length : 0
                );

                for (const st of activeStaff) {
                  updates.push([
                    perActive,
                    "published",
                    period,
                    branchId,
                    st.id,
                    kpi,
                  ]);
                }
              }

              // 6. UPSERT allocations
              pool.query(
                `
                INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi)
                VALUES ?
                ON DUPLICATE KEY UPDATE amount = VALUES(amount), state = VALUES(state)
                `,
                [updates],
                (err) => {
                  if (err) return callback(err);

                  callback(null, { message: "Target Update Successful" });
                }
              );
            }
          );
          });
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
        "SELECT id, name, transfer_date,user_add_date FROM users WHERE branch_id = ? AND role IN (?)",
        [branchId, ["CLERK"]],
        (err, staff) => {
          if (err) return callback(err);
          if (!staff.length) return callback(new Error("No staff found"));

            const transferTargetMap = {
            deposit: 0,
            loan_gen: 0,
            loan_amulya: 0,
            recovery: 0,
          };

          pool.query(
            `
           SELECT 
            SUM(deposit_target)     AS deposit,
            SUM(loan_gen_target)    AS loan_gen,
            SUM(loan_amulya_target) AS loan_amulya,
            SUM(recovery_target)    AS recovery
          FROM employee_transfer
          WHERE old_branch_id = ?
            AND period = ?
            AND old_designation <> 'BM';
            `,
            [branchId, period],
            (err4, rows) => {
              if (err4) return callback(err4);

              const r = rows[0] || {};

              transferTargetMap.deposit = Number(r.deposit || 0);
              transferTargetMap.loan_gen = Number(r.loan_gen || 0);
              transferTargetMap.loan_amulya = Number(r.loan_amulya || 0);
              transferTargetMap.recovery = Number(r.recovery || 0);
              console.log(transferTargetMap);
              
          const activeStaff = [];
          const newStaff = [];
          const newStaffOnDate = [];

          staff.forEach((s) => {
            const td = s.transfer_date ? new Date(s.transfer_date) : null;
            const jd = s.user_add_date ? new Date(s.user_add_date) : null;

            if (td) {
              if (td >= fy.start && td <= fy.end) {
                newStaff.push(s);
              } else {
                activeStaff.push(s);
              }
              return;
            }

            if (jd && jd >= fy.start && jd <= fy.end) {
              newStaffOnDate.push(s);
              return;
            }

            activeStaff.push(s);
          });
          const finalNewStaff = [...newStaff, ...newStaffOnDate];
          const totalStaff = activeStaff.length + finalNewStaff.length;
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

               const totalTarget = Math.max(
                0,
                t.amount - (transferTargetMap[kpi] || 0)
              );
                let totalResignedWorkedTarget = 0;
                console.log(totalTarget);
                

                if (kpi === "audit" || kpi === 'insurance') {
                  finalNewStaff.forEach((ns) => {
                    const months = getMonthsWorked(
                      ns.transfer_date || ns.user_add_date,
                      fy.end
                    );
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

                finalNewStaff.forEach((ns) => {
                  const monthsWorked = getMonthsWorked(
                    ns.transfer_date || ns.user_add_date,
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
        });
        }
      );
    }
  );
};
// export const autoDistributeTargetToBM = (period, branchId, callback) => {
//   const fy = getFinancialYearRange(period);
//   console.log(fy, fy.start, fy.end);

//   pool.query(
//     "SELECT * FROM targets WHERE period = ? AND branch_id = ?",
//     [period, branchId],
//     (err, targets) => {
//       if (err) return callback(err);
//       if (!targets.length) return callback(new Error("No targets found"));

//       pool.query(
//         "SELECT id, name, transfer_date FROM users WHERE branch_id = ? AND role=BM",
//         [branchId],
//         (err, staff) => {
//           if (err) return callback(err);
//           if (!staff.length) return callback(new Error("No staff found"));

//           const activeStaff = [];
//           const newStaff = [];
//           const newStaffOnDate = [];

//           staff.forEach((s) => {
//             const td = s.transfer_date ? new Date(s.transfer_date) : null;

//             if (td) {
//               if (td >= fy.start && td <= fy.end) {
//                 activeStaff.push(s);
//               } else {
//                 newStaff.push(s);
//               }
//               return;
//             }
//             activeStaff.push(s);
//           });
//           const totalStaff = activeStaff.length + newStaff.length;
//           const kpis = [
//             "deposit",
//             "loan_gen",
//             "loan_amulya",
//             "recovery",
//             "insurance",
//             "audit",
//           ];

//           pool.query(
//             "DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)",
//             [period, branchId, kpis],
//             (err) => {
//               if (err) return callback(err);

//               let updates = [];

//               kpis.forEach((kpi) => {
//                 const t = targets.find((x) => x.kpi === kpi);
//                 if (!t) return;

//                 const totalTarget = t.amount;
//                 let totalResignedWorkedTarget = 0;

//                 if (kpi === "audit") {
//                   activeStaff.forEach((ns) => {
//                     const months = getMonthsWorked(
//                       ns.transfer_date || ns.user_add_date,
//                       fy.end
//                     );
//                     const newAudit = Math.floor((totalTarget / 12) * months);

//                     updates.push([
//                       newAudit,
//                       "published",
//                       period,
//                       branchId,
//                       ns.id,
//                       kpi,
//                     ]);
//                   });

//                   return;
//                 }

//                 let newStaffTotalGiven = 0;

//                 finalNewStaff.forEach((ns) => {
//                   const monthsWorked = getMonthsWorked(
//                     ns.transfer_date || ns.user_add_date,
//                     fy.end
//                   );

//                   const newTarget = Math.floor(
//                     (totalTarget / 12 / totalStaff) * monthsWorked
//                   );

//                   newStaffTotalGiven += newTarget;

//                   updates.push([
//                     newTarget,
//                     "published",
//                     period,
//                     branchId,
//                     ns.id,
//                     kpi,
//                   ]);
//                 });

//                 const remainingTarget =
//                   totalTarget - totalResignedWorkedTarget - newStaffTotalGiven;

//                 const perOld = Math.floor(
//                   activeStaff.length ? remainingTarget / activeStaff.length : 0
//                 );

//                 activeStaff.forEach((os) => {
//                   updates.push([
//                     perOld,
//                     "published",
//                     period,
//                     branchId,
//                     os.id,
//                     kpi,
//                   ]);
//                 });
//               });

//               pool.query(
//                 `
//                 INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi)
//                 VALUES ?
//                 ON DUPLICATE KEY UPDATE
//                   amount = VALUES(amount),
//                   state = VALUES(state)
//                 `,
//                 [updates],
//                 (err) => {
//                   if (err) return callback(err);
//                   callback(null, { message: "Target Update Successful" });
//                 }
//               );
//             }
//           );
//         }
//       );
//     }
//   );
// };

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
// GET /allocations?period=...&branchId=...&employeeId=...

allocationsRouter.get("/", (req, res) => {
  const { period, branchId, employeeId } = req.query;
  if (!period) return res.status(400).json({ error: "period required" });


  const PERSONAL_KPIS = ["deposit", "loan_gen", "loan_amulya"];
  const BRANCH_KPIS = ["insurance", "recovery", "audit"];

 
  pool.query("SELECT kpi, weightage FROM weightage", (err, weightRows) => {
    if (err) return res.status(500).json({ error: "Internal server error" });

    const weightMap = {};
    weightRows.forEach(w => weightMap[w.kpi] = Number(w.weightage) || 0);

 
    const branchAggQuery = `
      SELECT kpi, SUM(value) AS total_achieved
      FROM entries
      WHERE period = ? AND branch_id = ? AND status='Verified'
      GROUP BY kpi
    `;

    pool.query(branchAggQuery, [period, branchId], (errAgg, aggRows) => {
      if (errAgg) return res.status(500).json({ error: "Internal server error" });

      const branchAch = {};
      aggRows.forEach(x => branchAch[x.kpi] = Number(x.total_achieved) || 0);

      
      if (employeeId) {
        const personalQuery = `
          SELECT 
            a.kpi,
            MAX(a.amount) AS amount,
            MAX(a.state) AS state,
            COALESCE(w.weightage, 0) AS weightage,
            COALESCE(e.achieved, 0) AS achieved
          FROM allocations a
          JOIN users u ON u.id = a.user_id
          LEFT JOIN weightage w ON a.kpi = w.kpi
          LEFT JOIN (
            SELECT kpi, SUM(value) AS achieved
            FROM entries
            WHERE period = ? AND employee_id = ? AND branch_id = ? AND status='Verified'
            GROUP BY kpi
          ) e ON a.kpi = e.kpi
          WHERE a.period = ? AND a.user_id = ? AND u.branch_id = ? AND a.state='published'
          GROUP BY a.kpi
        `;

        pool.query(
          personalQuery,
          [period, employeeId, branchId, period, employeeId, branchId],
          (errP, personalTargets) => {
            if (errP) return res.status(500).json({ error: "Internal server error" });

           
            const branchTargetsQuery = `
              SELECT 
                t.kpi,
                COALESCE(t.amount,0) AS amount,
                COALESCE(w.weightage,0) AS weightage
              FROM targets t
              LEFT JOIN weightage w ON t.kpi = w.kpi
              WHERE t.period=? AND t.branch_id=?
            `;

            pool.query(branchTargetsQuery, [period, branchId], (errB, branchTargets) => {
              if (errB) return res.status(500).json({ error: "Internal server error" });

              // Map branch targets
              const branchMap = {};
              branchTargets.forEach(bt => {
                branchMap[bt.kpi] = {
                  kpi: bt.kpi,
                  amount: Number(bt.amount) || 0,
                  weightage: Number(bt.weightage) || 0,
                };
              });

              
              let finalPersonal = personalTargets
                .filter(p => PERSONAL_KPIS.includes(p.kpi))
                .map(p => ({
                  kpi: p.kpi,
                  amount: Number(p.amount) || 0,
                  achieved: Number(p.achieved) || 0,
                  weightage: Number(p.weightage) || weightMap[p.kpi] || 0,
                  state: p.state || "published",
                }));

             
              PERSONAL_KPIS.forEach(k => {
                if (!finalPersonal.find(p => p.kpi === k)) {
                  finalPersonal.push({
                    kpi: k,
                    amount: 0,
                    achieved: Number(branchAch[k]) || 0,
                    weightage: weightMap[k] || 0,
                    state: "published",
                  });
                }
              });

            
              let finalBranch = BRANCH_KPIS.map(kpi => {
                const bt = branchMap[kpi];

                return {
                  kpi,
                  amount: bt ? Number(bt.amount) : 0,
                  achieved: Number(branchAch[kpi]) || 0,
                  weightage: bt ? Number(bt.weightage) : (weightMap[kpi] || 0),
                };
              });

              return res.json({
                personal: finalPersonal,
                branch: finalBranch,
              });
            });
          }
        );
        return;
      }

  
      const query = `
        SELECT a.*, u.name AS staffName, COALESCE(w.weightage,0) AS weightage
        FROM (
          SELECT user_id, kpi, MAX(amount) AS amount, MAX(state) AS state, period, branch_id
          FROM allocations
          WHERE period = ?
          GROUP BY user_id, kpi
        ) a
        JOIN users u ON u.id = a.user_id
        LEFT JOIN weightage w ON w.kpi = a.kpi
        ${branchId ? "WHERE a.branch_id = ?" : ""}
      `;

      const params = branchId ? [period, branchId] : [period];

      pool.query(query, params, (errAll, rows) => {
        if (errAll) return res.status(500).json({ error: "Internal server error" });

        rows = rows.map(r => ({
          ...r,
          amount: Number(r.amount) || 0,
          weightage: Number(r.weightage) || 0,
        }));

        return res.json(rows);
      });
    });
  });
});

allocationsRouter.post("/update-prorated-targets", (req, res) => {
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
        (d2.getMonth() - d1.getMonth())
    );
  }

  function monthDiffend(d1, d2) {
    return Math.max(
      0,
      (d2.getFullYear() - d1.getFullYear()) * 12 +
        (d2.getMonth() - d1.getMonth()) +
        1
    );
  }

  const fy = getFY(period);

  pool.getConnection((err, conn) => {
    if (err) return res.status(500).json({ error: "DB Connection error" });

    conn.beginTransaction((err) => {
      if (err) return rollback("Transaction start failed");

      //  Get USER transfer date
      conn.query(
        "SELECT transfer_date FROM users WHERE id=?",
        [staff_id],
        (err, staffRows) => {
          if (err) return rollback(err);
          if (staffRows.length === 0)
            return rollback("No staff found with given staff_id");

          const userTd = new Date(staffRows[0].transfer_date);

          //  Get BM transfer record
          conn.query(
            "SELECT transfer_date FROM bm_transfer_target WHERE staff_id=? AND period=? ORDER BY id DESC LIMIT 1",
            [staff_id, period],
            (err, bmRows) => {
              if (err) return rollback(err);

              if (bmRows.length === 0) {
                // CASE A: No BM transfer found
                return handleCase_UpdateAndInsert("A_NoUserTransfer", userTd);
              }

              const bmTd = new Date(bmRows[0].transfer_date);

              // CASE B1: Inside FY
              if (userTd >= fy.start && userTd <= fy.end) {
                return handleCase_InsideFY(userTd, bmTd);
              }

              // CASE B2: Outside FY
              return handleCase_UpdateAndInsert("B2_OutsideFY", userTd);
            }
          );
        }
      );

      // update employee_transfer + Insert BM target

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

          // Correct months = user.transfer_date → fy.end
          const empMonths = monthDiffstart(fy.start, empTd);

          const updatedEmp = {
            deposit_target: (emp.deposit_target / 12) * empMonths,
            loan_gen_target: (emp.loan_gen_target / 12) * empMonths,
            loan_amulya_target: (emp.loan_amulya_target / 12) * empMonths,
            audit_target: (emp.audit_target / 12) * empMonths,
            recovery_target: (emp.recovery_target / 12) * empMonths,
            insurance_target: (emp.insurance_target / 12) * empMonths,
          };
          console.log(updatedEmp);

          conn.query(
            "UPDATE employee_transfer SET ? WHERE id=?",
            [updatedEmp, emp.id],
            (err) => {
              if (err) return rollback(err);

              // Get branch targets
              conn.query(
                "SELECT * FROM targets WHERE period=? AND branch_id=?",
                [period, new_branchId],
                (err, targets) => {
                  if (err) return rollback(err);
                  if (!targets.length)
                    return rollback("No target master found");

                  // Convert rows to object
                  const t = targets.reduce((acc, curr) => {
                    acc[curr.kpi] = curr.amount;
                    return acc;
                  }, {});

                  // BM months = FY START to FY END
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
                      message:
                        "employee_transfer updated + bm_transfer_target inserted",
                      empMonths,
                      inserted_id: result.insertId,
                    });
                  });
                }
              );
            }
          );
        });
      }

      // update employee_transfer + Insert BM target

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
                      0
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
                  console.log(
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
                    staff_id
                  );

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
                    }
                  );
                }
              );
            }
          );
        });
      }

      // Helper: rollback

      function rollback(error) {
        conn.rollback(() => {
          conn.release();
          res.status(500).json({ error });
        });
      }

      // Helper: commit

      function commit(response) {
        conn.commit(() => {
          conn.release();
          res.json(response);
        });
      }
    });
  });
});

allocationsRouter.post("/CLEARK-TO-BM-Target", (req, res) => {
  const { period, branchId, staff_id } = req.body || {};

  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  function getFY(period) {
    const [startStr, endStr] = period.split("-");
    const startYear = parseInt(startStr);
    const endYear = startYear - (startYear % 100) + parseInt(endStr);
    return {
      start: new Date(Date.UTC(startYear, 3, 1)),
      end: new Date(Date.UTC(endYear, 2, 31)),
    };
  }
  function monthDiffend(d1, d2) {
    return Math.max(
      0,
      (d2.getFullYear() - d1.getFullYear()) * 12 +
        (d2.getMonth() - d1.getMonth()) +
        1
    );
  }
  const fy = getFY(period);
  const totalTarget = `select * from targets where period=? and branch_id=?`;
  pool.query(totalTarget, [period, branchId], (err, targetRows) => {
    if (err) return res.status(500).json({ error: "Internal server error" });
    if (!targetRows.length)
      return res
        .status(400)
        .json({ error: "No targets found for the branch and period" });
    const t = targetRows.reduce((acc, curr) => {
      acc[curr.kpi] = curr.amount;
      return acc;
    }, {});

    // Get USER transfer date
    const userTdQuery = `SELECT transfer_date FROM users WHERE id=?`;
    pool.query(userTdQuery, [staff_id], (err, staffRows) => {
      if (err) return res.status(500).json({ error: "Internal server error" });
      if (staffRows.length === 0)
        return res
          .status(400)
          .json({ error: "No staff found with given staff_id" });
      const userTd = new Date(staffRows[0].transfer_date);

      // BM months = FY START to FY END
      const bmMonths = monthDiffend(userTd, fy.end);
      const bmRatio = bmMonths / 12;

      const insertBm = `
                    INSERT INTO bm_transfer_target
                    (staff_id, branch_id, transfer_date, deposit_target, loan_gen_target, loan_amulya_target,
                     audit_target, recovery_target, insurance_target, period)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                  `;

      const bmValues = [
        staff_id,
        branchId,
        userTd,
        (t.deposit || 0) * bmRatio,
        (t.loan_gen || 0) * bmRatio,
        (t.loan_amulya || 0) * bmRatio,
        (t.audit || 0) * bmRatio,
        (t.recovery || 0) * bmRatio,
        (t.insurance || 0) * bmRatio,
        period,
      ];
      pool.query(insertBm, bmValues, (err, result) => {
        if (err)
          return res.status(500).json({ error: "Internal server error" });
        res.json({ ok: true, inserted_id: result.insertId });
      });
    });
  });
});
