import express from "express";
import pool from "../db.js";

// Router handling staff allocations: auto‑distribution and publish.
export const allocationsRouter = express.Router();

//This function  help to auto distribute logic
export const autoDistributeTargets = async (period, branchId, callback) => {
  try {
    const targets = await new Promise((res, rej) => pool.query("SELECT * FROM targets WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    if (targets.length === 0) return callback(new Error("No branch targets found"));

    const staff = await new Promise((res, rej) => pool.query("SELECT * FROM users WHERE branch_id = ? AND period = ? AND role IN (?) ", [branchId, period, ["CLERK"]], (e, r) => e ? rej(e) : res(r)));
    if (staff.length === 0) return callback(new Error("No active staff in branch"));

    const previousData = await new Promise((res, rej) => pool.query("SELECT * FROM previous_period_data WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const kpisToSplit = ["deposit", "loan_gen", "loan_amulya", "audit", "insurance", "recovery"];
    
    await new Promise((res, rej) => pool.query("DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)", [period, branchId, kpisToSplit], (e, r) => e ? rej(e) : res(r)));
    await new Promise((res, rej) => pool.query("UPDATE previous_period_data_staffwise SET deleted_at = NOW() WHERE period = ? AND branch_id = ? AND kpi IN (?) AND deleted_at IS NULL", [period, branchId, kpisToSplit], (e, r) => e ? rej(e) : res(r)));

    const allocations = [];
    const staffwiseBaselines = [];

    kpisToSplit.forEach((kpi) => {
      const target = targets.find((t) => t.kpi === kpi);
      if (target && target.amount && target.amount > 0) {
        if (kpi === "audit" || kpi === "insurance") {
          staff.forEach((user) => allocations.push([period, branchId, user.id, kpi, target.amount, "published"]));
        } else {
          const base = Math.floor(target.amount / staff.length);
          const rem = target.amount % staff.length;
          staff.forEach((user, idx) => allocations.push([period, branchId, user.id, kpi, base + (idx < rem ? 1 : 0), "published"]));
        }
      }

      const prev = previousData.find(p => p.kpi === kpi);
      if (prev && prev.amount && prev.amount > 0) {
        const base = Math.floor(prev.amount / staff.length);
        const rem = prev.amount % staff.length;
        staff.forEach((user, idx) => staffwiseBaselines.push([user.id, period, branchId, kpi, base + (idx < rem ? 1 : 0)]));
      }
    });

    if (allocations.length > 0) {
      await new Promise((res, rej) => pool.query("INSERT INTO allocations (period, branch_id, user_id, kpi, amount, state) VALUES ?", [allocations], (e, r) => e ? rej(e) : res(r)));
    }
    
    if (staffwiseBaselines.length > 0) {
      await new Promise((res, rej) => pool.query("INSERT INTO previous_period_data_staffwise (employee_id, period, branch_id, kpi, amount) VALUES ?", [staffwiseBaselines], (e, r) => e ? rej(e) : res(r)));
    }

    callback(null);
  } catch (err) {
    callback(err);
  }
};

//This function  help to auto distribute logic for only the transfer staff
export const autoDistributeTargetsInTransfer = async (period, branchId, callback) => {
  try {
    await new Promise((res, rej) => pool.query("DELETE FROM allocations WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    await new Promise((res, rej) => pool.query("UPDATE previous_period_data_staffwise SET deleted_at = NOW() WHERE period = ? AND branch_id = ? AND deleted_at IS NULL", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const targets = await new Promise((res, rej) => pool.query("SELECT * FROM targets WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    if (targets.length === 0) return callback(new Error("No branch targets found"));

    const staff = await new Promise((res, rej) => pool.query("SELECT * FROM users WHERE branch_id = ? AND period = ? AND role IN (?)", [branchId, period, ["CLERK"]], (e, r) => e ? rej(e) : res(r)));
    if (staff.length === 0) return callback(new Error("No active staff in branch"));

    const previousData = await new Promise((res, rej) => pool.query("SELECT * FROM previous_period_data WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const kpisToSplit = ["deposit", "loan_gen", "loan_amulya"];
    const allocations = [];
    const staffwiseBaselines = [];

    kpisToSplit.forEach((kpi) => {
      const target = targets.find((t) => t.kpi === kpi);
      const amount = target ? target.amount : 0;
      if (amount > 0) {
        const base = Math.floor(amount / staff.length);
        const rem = amount % staff.length;
        staff.forEach((user, idx) => allocations.push([period, branchId, user.id, kpi, base + (idx < rem ? 1 : 0), "published"]));
      }

      const prev = previousData.find((p) => p.kpi === kpi);
      const prevAmount = prev ? prev.amount : 0;
      if (prevAmount > 0) {
        const base = Math.floor(prevAmount / staff.length);
        const rem = prevAmount % staff.length;
        staff.forEach((user, idx) => staffwiseBaselines.push([user.id, period, branchId, kpi, base + (idx < rem ? 1 : 0)]));
      }
    });

    const auditTarget = targets.find((t) => t.kpi === "audit");
    if (auditTarget && auditTarget.amount > 0) {
      staff.forEach((user) => allocations.push([period, branchId, user.id, "audit", auditTarget.amount, "published"]));
    }
    
    const auditPrev = previousData.find((p) => p.kpi === "audit");
    if (auditPrev && auditPrev.amount > 0) {
      staff.forEach((user) => staffwiseBaselines.push([user.id, period, branchId, "audit", auditPrev.amount]));
    }

    if (allocations.length > 0) {
      await new Promise((res, rej) => pool.query("INSERT INTO allocations (period, branch_id, user_id, kpi, amount, state) VALUES ?", [allocations], (e, r) => e ? rej(e) : res(r)));
    }
    if (staffwiseBaselines.length > 0) {
      await new Promise((res, rej) => pool.query("INSERT INTO previous_period_data_staffwise (employee_id, period, branch_id, kpi, amount) VALUES ?", [staffwiseBaselines], (e, r) => e ? rej(e) : res(r)));
    }

    callback(null);
  } catch (err) {
    callback(err);
  }
};

//This function  help to get year start and end date
function getFinancialYearEnd(period) {
  const [startYr, endYr] = period.split("-");
  return new Date(`20${endYr}-03-31`);
}
//This function help to calculate totol month work
function getMonthsWorked(resignedDate, periodEndDate) {
  const start = new Date(resignedDate);
  const end = new Date(periodEndDate);

  let months =
    (end.getFullYear() - start.getFullYear()) * 12 +
    (end.getMonth() - start.getMonth()) +
    1;

  return months < 0 ? 0 : months;
}
//This fucction help to calculate total target according totol months of work
// and total target of this branch and this total staff members
function calculateTargets(
  totalTarget,
  totalStaff,
  resignedStaffCount,
  monthsWorked,
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
// This function help us to calculate target distribution in case resign staff
export const autoDistributeTargetsResign = async (
  period,
  branchId,
  callback,
) => {
  const periodEnd = getFinancialYearEnd(period);

  function getMonthsWorked(resignedDate, periodEndDate) {
    const resign = new Date(resignedDate);
    const periodEnd = new Date(periodEndDate);

    const fyStart = new Date(periodEnd.getFullYear() - 1, 3, 1);

    let months =
      (resign.getFullYear() - fyStart.getFullYear()) * 12 +
      (resign.getMonth() - fyStart.getMonth()) +
      1;

    return Math.max(0, Math.min(months, 12));
  }

  try {
    const users = await new Promise((resolve, reject) => {
      pool.query(
        "SELECT id AS user_id, resign, resign_date FROM users WHERE branch_id = ? AND period = ? AND role = 'CLERK'",
        [branchId , period],
        (err, rows) => (err ? reject(err) : resolve(rows)),
      );
    });

    if (!users.length) return callback(new Error("No clerks found"));

    const userMap = {};
    const userIds = [];
    users.forEach((u) => {
      userIds.push(u.user_id);
      userMap[u.user_id] = { user_id: u.user_id, resign: u.resign, resign_date: u.resign_date };
    });

    const allocations = await new Promise((resolve, reject) => {
      pool.query(
        "SELECT user_id, kpi, amount AS annual_target FROM allocations WHERE period = ? AND user_id IN (?) AND branch_id = ? AND kpi ='insurance'",
        [period, userIds, branchId],
        (err, rows) => (err ? reject(err) : resolve(rows)),
      );
    });
    
    const previousData = await new Promise((resolve, reject) => {
      pool.query(
        "SELECT employee_id AS user_id, kpi, amount AS annual_target FROM previous_period_data_staffwise WHERE period = ? AND employee_id IN (?) AND branch_id = ? AND kpi ='insurance' AND deleted_at IS NULL",
        [period, userIds, branchId],
        (err, rows) => (err ? reject(err) : resolve(rows)),
      );
    });

    if (!allocations.length) return callback(new Error("No allocations found"));

    const updates = [];
    const baselineUpdates = [];

    const distributeData = (dataArray, updatesArray, isBaseline) => {
      const kpiMap = {};
      dataArray.forEach((a) => {
        if (!kpiMap[a.kpi]) kpiMap[a.kpi] = [];
        kpiMap[a.kpi].push({ ...a, ...userMap[a.user_id] });
      });

      Object.keys(kpiMap).forEach((kpi) => {
        const clerks = kpiMap[kpi];
        let redistributionPool = 0;
        const active = [];
        const resigned = [];

        clerks.forEach((c) => {
          if (Number(c.resign) === 1 && c.resign_date) {
            const monthsWorked = getMonthsWorked(c.resign_date, periodEnd);
            const worked = (Number(c.annual_target) * monthsWorked) / 12;
            const remaining = Number(c.annual_target) - worked;
            redistributionPool += remaining;
            resigned.push({ user_id: c.user_id, finalAmount: worked });
          } else {
            active.push(c);
          }
        });

        let distributed = 0;
        const extra = active.length > 0 ? redistributionPool / active.length : 0;

        active.forEach((c, index) => {
          let finalAmount = Number(c.annual_target) + extra;
          if (index === active.length - 1) {
            finalAmount = Number(c.annual_target) + (redistributionPool - distributed);
          }
          distributed += finalAmount - Number(c.annual_target);
          updatesArray.push([Math.round(finalAmount), isBaseline ? undefined : "published", period, branchId, c.user_id, kpi]);
        });

        resigned.forEach((r) => {
          updatesArray.push([Math.round(r.finalAmount), isBaseline ? undefined : "resigned", period, branchId, r.user_id, kpi]);
        });
      });
    };

    distributeData(allocations, updates, false);
    if (previousData.length > 0) {
      distributeData(previousData, baselineUpdates, true);
    }

    for (const row of updates) {
      await new Promise((resolve, reject) => {
        pool.query(
          "UPDATE allocations SET amount = ?, state = ? WHERE period = ? AND branch_id = ? AND user_id = ? AND kpi = ?",
          row,
          (err) => (err ? (console.error(err), reject(err)) : resolve())
        );
      });
    }

    for (const row of baselineUpdates) {
      await new Promise((resolve, reject) => {
        // format: [amount, undefined, period, branchId, user_id, kpi]
        const amount = row[0];
        const periodParam = row[2];
        const branchParam = row[3];
        const userParam = row[4];
        const kpiParam = row[5];
        pool.query(
          "UPDATE previous_period_data_staffwise SET amount = ? WHERE period = ? AND branch_id = ? AND employee_id = ? AND kpi = ? AND deleted_at IS NULL",
          [amount, periodParam, branchParam, userParam, kpiParam],
          (err) => (err ? (console.error(err), reject(err)) : resolve())
        );
      });
    }

    callback(null, { message: "Per-KPI redistribution completed" });
  } catch (err) {
    console.error(err);
    callback(err);
  }
};
// This function help us to calculate target distribution in case new users Add
export const autoDistributeTargetsNewUsers = async (period, branchId, callback) => {
  const fy = getFinancialYearRange(period);

  try {
    const targets = await new Promise((res, rej) => pool.query("SELECT * FROM targets WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    if (!targets.length) return callback(new Error("No targets found"));

    const previousData = await new Promise((res, rej) => pool.query("SELECT * FROM previous_period_data WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const staff = await new Promise((res, rej) => pool.query("SELECT id, name, user_add_date, transfer_date FROM users WHERE branch_id = ? AND period = ? AND role IN (?)", [branchId, period, ["CLERK"]], (e, r) => e ? rej(e) : res(r)));
    if (!staff.length) return callback(new Error("No staff found"));

    const transferTargetMap = { deposit: 0, loan_gen: 0, loan_amulya: 0, recovery: 0 };
    const rRows = await new Promise((res, rej) => pool.query(
      `SELECT SUM(deposit_target) AS deposit, SUM(loan_gen_target) AS loan_gen, SUM(loan_amulya_target) AS loan_amulya, SUM(recovery_target) AS recovery FROM employee_transfer WHERE old_branch_id = ? AND period = ? AND old_designation <> 'BM';`,
      [branchId, period],
      (e, r) => e ? rej(e) : res(r)
    ));
    const r = rRows[0] || {};
    transferTargetMap.deposit = Number(r.deposit || 0);
    transferTargetMap.loan_gen = Number(r.loan_gen || 0);
    transferTargetMap.loan_amulya = Number(r.loan_amulya || 0);
    transferTargetMap.recovery = Number(r.recovery || 0);

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

    const totalStaff = activeStaff.length + transferStaff.length + newJoinStaff.length;
    const kpis = ["deposit", "loan_gen", "loan_amulya", "recovery", "insurance", "audit"];

    await new Promise((res, rej) => pool.query("DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)", [period, branchId, kpis], (e, r) => e ? rej(e) : res(r)));
    await new Promise((res, rej) => pool.query("UPDATE previous_period_data_staffwise SET deleted_at = NOW() WHERE period = ? AND branch_id = ? AND kpi IN (?) AND deleted_at IS NULL", [period, branchId, kpis], (e, r) => e ? rej(e) : res(r)));

    let updates = [];
    let baselineUpdates = [];

    kpis.forEach((kpi) => {
      const t = targets.find((x) => x.kpi === kpi);
      const prevT = previousData.find((x) => x.kpi === kpi);

      const processKpi = (sourceObj, updatesArray, isBaseline) => {
        if (!sourceObj) return;
        const totalTarget = sourceObj.amount;
        let totalTransferGiven = 0;
        let totalNewJoinGiven = 0;

        if (kpi === "audit" || kpi === "insurance") {
          transferStaff.forEach((ts) => {
            const months = getMonthsWorked(ts.transfer_date, fy.end);
            const auditTarget = Math.floor((totalTarget / 12) * months);
            updatesArray.push([auditTarget, isBaseline ? undefined : "transfer", period, branchId, ts.id, kpi]);
          });
          newJoinStaff.forEach((nj) => {
            const months = getMonthsWorked(nj.user_add_date, fy.end);
            const auditTarget = Math.floor((totalTarget / 12) * months);
            updatesArray.push([auditTarget, isBaseline ? undefined : "published", period, branchId, nj.id, kpi]);
          });
          activeStaff.forEach((as) => {
            updatesArray.push([Math.floor(totalTarget), isBaseline ? undefined : "published", period, branchId, as.id, kpi]);
          });
          return;
        }

        transferStaff.forEach((ts) => {
          const monthsWorked = getMonthsWorked(ts.transfer_date, fy.end);
          const perStaffAnnual = totalTarget / totalStaff;
          const target = (perStaffAnnual * monthsWorked) / 12;
          totalTransferGiven += target;
          updatesArray.push([Math.round(target), isBaseline ? undefined : "transfer", period, branchId, ts.id, kpi]);
        });

        newJoinStaff.forEach((nj) => {
          const monthsWorked = getMonthsWorked(nj.user_add_date, fy.end);
          const perStaffAnnual = totalTarget / totalStaff;
          const target = (perStaffAnnual * monthsWorked) / 12;
          totalNewJoinGiven += target;
          updatesArray.push([Math.round(target), isBaseline ? undefined : "published", period, branchId, nj.id, kpi]);
        });

        const remainingTarget = totalTarget - totalTransferGiven - totalNewJoinGiven - (isBaseline ? 0 : (transferTargetMap[kpi] || 0));
        const perActive = Math.floor(activeStaff.length ? remainingTarget / activeStaff.length : 0);

        activeStaff.forEach((as) => {
          updatesArray.push([perActive, isBaseline ? undefined : "published", period, branchId, as.id, kpi]);
        });
      };

      processKpi(t, updates, false);
      processKpi(prevT, baselineUpdates, true);
    });

    if (updates.length > 0) {
      await new Promise((res, rej) => {
        pool.query(
          `INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount), state = VALUES(state)`,
          [updates],
          (e) => e ? rej(e) : res()
        );
      });
    }

    if (baselineUpdates.length > 0) {
      // transform baselineUpdates because state is undefined
      const cleanBaselineUpdates = baselineUpdates.map(u => [u[4], u[2], u[3], u[5], u[0]]); // employee_id, period, branch_id, kpi, amount
      await new Promise((res, rej) => {
        pool.query(
          `INSERT INTO previous_period_data_staffwise (employee_id, period, branch_id, kpi, amount) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount)`,
          [cleanBaselineUpdates],
          (e) => e ? rej(e) : res()
        );
      });
    }

    callback(null, {
      message: "Target Update Successful",
      summary: {
        activeStaff: activeStaff.length,
        newJoinStaff: newJoinStaff.length,
        transferStaff: transferStaff.length,
      },
    });
  } catch (err) {
    callback(err);
  }
};
// FINANCIAL YEAR RANGE
export const getFinancialYearRange = (period) => {
  const [startStr, endStr] = period.split("-");

  const startYear = parseInt(startStr);
  const endYear = startYear - (startYear % 100) + parseInt(endStr);

  const start = new Date(Date.UTC(startYear, 3, 1));
  const end = new Date(Date.UTC(endYear, 2, 31));

  return { start, end };
};

//old Branch trasfer target distribution
function monthDiff(d1, d2) {
  let months =
    (d2.getFullYear() - d1.getFullYear()) * 12 +
    (d2.getMonth() - d1.getMonth());

  return months < 0 ? 0 : months;
}

//get Actual Months Worked According this transfer
async function getActualMonthsWorked(pool, staffId, userTd, fyStart) {
  // 1. Get TWO latest transfer dates
  const dbDates = await new Promise((resolve) => {
    pool.query(
      "SELECT transfer_date FROM employee_transfer WHERE staff_id = ? ORDER BY id DESC LIMIT 2",
      [staffId],
      (err, rows) => {
        if (err || !rows.length) return resolve([]);
        resolve(rows);
      },
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

// This function help us to calculate target distribution in case Old branch
export const autoDistributeTargetsOldBranch = async (period, branchId, role, callback) => {
  const periodEnd = getFinancialYearEnd(period);
  const fy = getFinancialYearRange(period);

  try {
    const targets = await new Promise((res, rej) => pool.query("SELECT * FROM targets WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    if (!targets.length) return callback(new Error("No targets found"));

    const previousData = await new Promise((res, rej) => pool.query("SELECT * FROM previous_period_data WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const staff = await new Promise((res, rej) => pool.query("SELECT id, name, transfer_date, user_add_date FROM users WHERE branch_id = ? AND period = ? AND role IN (?)", [branchId, period, ["CLERK"]], (e, r) => e ? rej(e) : res(r)));
    if (!staff.length) return callback(new Error("No staff found"));

    const transferTargetMap = { deposit: 0, loan_gen: 0, loan_amulya: 0, recovery: 0 };
    const rRows = await new Promise((res, rej) => pool.query(
      `SELECT SUM(deposit_target) AS deposit, SUM(loan_gen_target) AS loan_gen, SUM(loan_amulya_target) AS loan_amulya, SUM(recovery_target) AS recovery FROM employee_transfer WHERE old_branch_id = ? AND period = ? AND old_designation <> 'BM';`,
      [branchId, period],
      (e, r) => e ? rej(e) : res(r)
    ));
    const r = rRows[0] || {};
    transferTargetMap.deposit = Number(r.deposit || 0);
    transferTargetMap.loan_gen = Number(r.loan_gen || 0);
    transferTargetMap.loan_amulya = Number(r.loan_amulya || 0);
    transferTargetMap.recovery = Number(r.recovery || 0);

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

    const totalStaff = activeStaff.length + resignedStaff.length + resignPrevoius.length + newjoinerStaff.length;
    const kpis = ["deposit", "loan_gen", "loan_amulya", "recovery", "insurance", "audit"];

    await new Promise((res, rej) => pool.query("DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)", [period, branchId, kpis], (e, r) => e ? rej(e) : res(r)));
    await new Promise((res, rej) => pool.query("UPDATE previous_period_data_staffwise SET deleted_at = NOW() WHERE period = ? AND branch_id = ? AND kpi IN (?) AND deleted_at IS NULL", [period, branchId, kpis], (e, r) => e ? rej(e) : res(r)));

    let updates = [];
    let baselineUpdates = [];

    for (const kpi of kpis) {
      const t = targets.find((x) => x.kpi === kpi);
      const prevT = previousData.find((x) => x.kpi === kpi);

      const processKpi = async (sourceObj, updatesArray, isBaseline) => {
        if (!sourceObj) return;
        const totalTarget = sourceObj.amount;
        let totalResignedWorkedTarget = 0;

        for (const r of resignedStaff) {
          if (!r.transfer_date) continue;
          const monthsWorked = await getActualMonthsWorked(pool, r.id, r.user_add_date, fy.start);
          if (kpi === "audit" || kpi === "insurance") {
            const perMonth = totalTarget / 12;
            const resignedAuditTarget = perMonth * monthsWorked;
            updatesArray.push([Math.round(resignedAuditTarget), isBaseline ? undefined : "Transfered", period, branchId, r.id, kpi]);
          } else {
            const { resignedTarget } = calculateTargets(totalTarget, totalStaff, 1, monthsWorked);
            totalResignedWorkedTarget += resignedTarget;
            updatesArray.push([Math.round(resignedTarget), isBaseline ? undefined : "Transfered", period, branchId, r.id, kpi]);
          }
        }

        let totalResignedWorkedTargetPrevious = 0;
        for (const r of resignPrevoius) {
          if (!r.transfer_date) continue;
          const monthsWorked = getMonthsWorked(r.transfer_date, periodEnd);
          if (kpi === "audit" || kpi === "insurance") {
            const perMonth = totalTarget / 12;
            const resignedAuditTarget = perMonth * monthsWorked;
            updatesArray.push([Math.round(resignedAuditTarget), isBaseline ? undefined : "published", period, branchId, r.id, kpi]);
          } else {
            const { resignedTarget } = calculateTargets(totalTarget, totalStaff, 1, monthsWorked);
            totalResignedWorkedTargetPrevious += resignedTarget;
            updatesArray.push([Math.round(resignedTarget), isBaseline ? undefined : "published", period, branchId, r.id, kpi]);
          }
        }

        if (kpi === "audit" || kpi === "insurance") {
          for (const st of activeStaff) {
            updatesArray.push([Math.round(totalTarget), isBaseline ? undefined : "published", period, branchId, st.id, kpi]);
          }
          return;
        }

        let totalNewJoinerWorkedTargetPrevious = 0;
        for (const r of newjoinerStaff) {
          if (!r.user_add_date) continue;
          const monthsWorked = getMonthsWorked(r.user_add_date, periodEnd);
          if (kpi === "audit" || kpi === "insurance") {
            const perMonth = totalTarget / 12;
            const resignedAuditTarget = perMonth * monthsWorked;
            updatesArray.push([Math.round(resignedAuditTarget), isBaseline ? undefined : "published", period, branchId, r.id, kpi]);
          } else {
            const { resignedTarget } = calculateTargets(totalTarget, totalStaff, 1, monthsWorked);
            totalNewJoinerWorkedTargetPrevious += resignedTarget;
            updatesArray.push([Math.round(resignedTarget), isBaseline ? undefined : "published", period, branchId, r.id, kpi]);
          }
        }

        let remainingTarget = 0;
        if (role === "BM") {
          remainingTarget = totalTarget - totalResignedWorkedTarget - totalResignedWorkedTargetPrevious - totalNewJoinerWorkedTargetPrevious;
        } else {
          remainingTarget = totalTarget - totalResignedWorkedTarget - totalResignedWorkedTargetPrevious - totalNewJoinerWorkedTargetPrevious - (isBaseline ? 0 : (transferTargetMap[kpi] || 0));
        }

        const perActive = Math.floor(activeStaff.length ? remainingTarget / activeStaff.length : 0);
        for (const st of activeStaff) {
          updatesArray.push([perActive, isBaseline ? undefined : "published", period, branchId, st.id, kpi]);
        }
      };

      await processKpi(t, updates, false);
      await processKpi(prevT, baselineUpdates, true);
    }

    if (updates.length > 0) {
      await new Promise((res, rej) => pool.query("INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount), state = VALUES(state)", [updates], (e) => e ? rej(e) : res()));
    }
    
    if (baselineUpdates.length > 0) {
      const cleanBaselines = baselineUpdates.map(u => [u[4], u[2], u[3], u[5], u[0]]); // employee_id, period, branch_id, kpi, amount
      await new Promise((res, rej) => pool.query("INSERT INTO previous_period_data_staffwise (employee_id, period, branch_id, kpi, amount) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount)", [cleanBaselines], (e) => e ? rej(e) : res()));
    }

    callback(null, { message: "Target Update Successful" });
  } catch (err) {
    callback(err);
  }
};
// This function help us to calculate target distribution in case new Branch
export const autoDistributeTargetsNewBranch = async (period, branchId, callback) => {
  const fy = getFinancialYearRange(period);

  try {
    const targets = await new Promise((res, rej) => pool.query("SELECT * FROM targets WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    if (!targets.length) return callback(new Error("No targets found"));

    const previousData = await new Promise((res, rej) => pool.query("SELECT * FROM previous_period_data WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const staff = await new Promise((res, rej) => pool.query("SELECT id, name, transfer_date, user_add_date FROM users WHERE branch_id = ? AND period = ? AND role IN (?)", [branchId, period, ["CLERK"]], (e, r) => e ? rej(e) : res(r)));
    if (!staff.length) return callback(new Error("No staff found"));

    const transferTargetMap = { deposit: 0, loan_gen: 0, loan_amulya: 0, recovery: 0 };
    const rRows = await new Promise((res, rej) => pool.query(
      `SELECT SUM(deposit_target) AS deposit, SUM(loan_gen_target) AS loan_gen, SUM(loan_amulya_target) AS loan_amulya, SUM(recovery_target) AS recovery FROM employee_transfer WHERE old_branch_id = ? AND period = ? AND old_designation <> 'BM';`,
      [branchId, period],
      (e, r) => e ? rej(e) : res(r)
    ));
    const r = rRows[0] || {};
    transferTargetMap.deposit = Number(r.deposit || 0);
    transferTargetMap.loan_gen = Number(r.loan_gen || 0);
    transferTargetMap.loan_amulya = Number(r.loan_amulya || 0);
    transferTargetMap.recovery = Number(r.recovery || 0);

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
    const kpis = ["deposit", "loan_gen", "loan_amulya", "recovery", "insurance", "audit"];

    await new Promise((res, rej) => pool.query("DELETE FROM allocations WHERE period = ? AND branch_id = ? AND kpi IN (?)", [period, branchId, kpis], (e, r) => e ? rej(e) : res(r)));
    await new Promise((res, rej) => pool.query("UPDATE previous_period_data_staffwise SET deleted_at = NOW() WHERE period = ? AND branch_id = ? AND kpi IN (?) AND deleted_at IS NULL", [period, branchId, kpis], (e, r) => e ? rej(e) : res(r)));

    let updates = [];
    let baselineUpdates = [];

    for (const kpi of kpis) {
      const t = targets.find((x) => x.kpi === kpi);
      const prevT = previousData.find((x) => x.kpi === kpi);

      const processKpi = async (sourceObj, updatesArray, isBaseline) => {
        if (!sourceObj) return;
        const totalTarget = sourceObj.amount;
        let totalResignedWorkedTarget = 0;

        if (kpi === "audit" || kpi === "insurance") {
          finalNewStaff.forEach((ns) => {
            const months = getMonthsWorked(ns.transfer_date || ns.user_add_date, fy.end);
            const newAudit = Math.floor((totalTarget / 12) * months);
            updatesArray.push([newAudit, isBaseline ? undefined : "published", period, branchId, ns.id, kpi]);
          });
          activeStaff.forEach((os) => {
            updatesArray.push([Math.floor(totalTarget), isBaseline ? undefined : "published", period, branchId, os.id, kpi]);
          });
          return;
        }

        let newStaffTotalGiven = 0;
        finalNewStaff.forEach((ns) => {
          const monthsWorked = getMonthsWorked(ns.transfer_date || ns.user_add_date, fy.end);
          const newTarget = Math.floor((totalTarget / 12 / totalStaff) * monthsWorked);
          newStaffTotalGiven += newTarget;
          updatesArray.push([newTarget, isBaseline ? undefined : "published", period, branchId, ns.id, kpi]);
        });

        const remainingTarget = totalTarget - totalResignedWorkedTarget - newStaffTotalGiven - (isBaseline ? 0 : (transferTargetMap[kpi] || 0));
        const perOld = Math.floor(activeStaff.length ? remainingTarget / activeStaff.length : 0);

        activeStaff.forEach((os) => {
          updatesArray.push([perOld, isBaseline ? undefined : "published", period, branchId, os.id, kpi]);
        });
      };

      await processKpi(t, updates, false);
      await processKpi(prevT, baselineUpdates, true);
    }

    if (updates.length > 0) {
      await new Promise((res, rej) => pool.query("INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount), state = VALUES(state)", [updates], (e) => e ? rej(e) : res()));
    }
    
    if (baselineUpdates.length > 0) {
      const cleanBaselines = baselineUpdates.map(u => [u[4], u[2], u[3], u[5], u[0]]); // employee_id, period, branch_id, kpi, amount
      await new Promise((res, rej) => pool.query("INSERT INTO previous_period_data_staffwise (employee_id, period, branch_id, kpi, amount) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount)", [cleanBaselines], (e) => e ? rej(e) : res()));
    }

    callback(null, { message: "Target Update Successful" });
  } catch (err) {
    callback(err);
  }
};
// This function help us to calculate target distribution in case resign BM
export const autoAdjustBMTransferTargets = (
  period,
  branchId,
  callback
) => {
  if (!period || !branchId) {
    return callback(new Error("period and branchId required"));
  }

  const periodEnd = getFinancialYearEnd(period);

  function getMonthsWorked(resignedDate, periodEndDate) {
    const resign = new Date(resignedDate);
    const periodEnd = new Date(periodEndDate);

    const fyStart = new Date(periodEnd.getFullYear() - 1, 3, 1);

    let months =
      (resign.getFullYear() - fyStart.getFullYear()) * 12 +
      (resign.getMonth() - fyStart.getMonth()) +
      1;

    return Math.max(0, Math.min(months, 12));
  }

 
  pool.query(
    `SELECT id, resign, resign_date
     FROM users
     WHERE branch_id = ? AND period = ?
  AND role = 'BM'
  AND resign_date IS NOT NULL`,
    [branchId,period],
    (err, userRows) => {
      if (err) return callback(err);
      if (userRows.length === 0)
        return callback(new Error("BM not found"));

      const bm = userRows[0];
      const userId = bm.id;
      
     
      pool.query(
        `SELECT kpi, amount
         FROM targets
         WHERE period = ?
           AND branch_id = ?`,
        [period, branchId, userId],
        (err, rows) => {
          if (err) return callback(err);
          if (rows.length === 0) return callback(new Error("No target found"));

          const mapping = {
            deposit: "deposit_target",
            loan_gen: "loan_gen_target",
            loan_amulya: "loan_amulya_target",
            recovery: "recovery_target",
            audit: "audit_target",
          };

          const updateData = {};

          rows.forEach((row) => {
            if (mapping[row.kpi]) {
              let annualTarget = Number(row.amount);

              const monthsWorked = getMonthsWorked(bm.resign_date, periodEnd);
              
              
              const monthlyTarget = annualTarget / 12;
              let finalAmount = monthlyTarget * monthsWorked;

              finalAmount = Number(finalAmount.toFixed(2));
              
              
              updateData[mapping[row.kpi]] = finalAmount;
            }
          });

          if (Object.keys(updateData).length === 0) {
            return callback(new Error("No valid KPI data to update"));
          }

          pool.query(
            `SELECT id
             FROM employee_transfer
             WHERE period = ?
               AND old_branch_id = ?
               AND staff_id = ?`,
            [period, branchId, userId],
            (err, result) => {
              if (err) return callback(err);
              if (result.length === 0)
                return callback(new Error("No employee_transfer record found"));

              const transferId = result[0].id;

              pool.query(
                "UPDATE employee_transfer SET ? WHERE id = ?",
                [updateData, transferId],

                (err) => {
                  if (err) return callback(err);

                  pool.query(
                    "UPDATE users SET branch_id = '' WHERE id = ? AND period = ?",
                    [userId,period],
                    (err) => {
                      if (err) return callback(err);

                      return callback(null);
                    },
                  );
                },
              );
            },
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

//this api for auto distribute for the transfer staff
allocationsRouter.post("/auto-distribute-transfer", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsInTransfer(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

//this api for auto distribute for the resign staff
allocationsRouter.post("/auto-distribute-resign", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsResign(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

//this api for auto distribute for the resign BM
allocationsRouter.post("/auto-distribute-resign-BM", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoAdjustBMTransferTargets(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});
//this api for auto distribute for the new joining staff
allocationsRouter.post("/auto-distribute-new-user", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsNewUsers(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});
//this api for auto distribute for the old branch in case of trasfer
allocationsRouter.post("/auto-distribute-old-branch", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsOldBranch(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});
//this api for auto distribute for the new branch in case of trasfer
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
    },
  );
});

// Query allocations by period/branch or employee.
allocationsRouter.get("/", (req, res) => {
  const { period, branchId, employeeId } = req.query;

  if (!period) return res.status(400).json({ error: "period required" });

  const PERSONAL_KPIS = ["deposit", "loan_gen", "loan_amulya"];
  const BRANCH_KPIS = ["insurance", "recovery", "audit"];

  pool.query("SELECT kpi, weightage FROM weightage", (err, weightRows) => {
    if (err) return res.status(500).json({ error: err.message });

    const weightMap = {};
    weightRows.forEach((w) => {
      weightMap[w.kpi] = Number(w.weightage) || 0;
    });

    const branchAggQuery = `
      SELECT kpi, SUM(value) AS total_achieved
      FROM entries
      WHERE period = ? AND branch_id = ? AND status='Verified'
      GROUP BY kpi
    `;

    pool.query(branchAggQuery, [period, branchId], (errAgg, aggRows) => {
      if (errAgg) return res.status(500).json({ error: errAgg.message });

      const branchAch = {};
      aggRows.forEach((x) => {
        branchAch[x.kpi] = Number(x.total_achieved) || 0;
      });

      // ================= EMPLOYEE FLOW =================
      if (employeeId) {
        const personalQuery = `
          SELECT 
            a.kpi,
            MAX(a.amount) AS amount,
            MAX(a.state) AS state,
            COALESCE(MAX(w.weightage), 0) AS weightage,
            (COALESCE(MAX(e.achieved), 0) + COALESCE(MAX(prev.amount), 0)) AS achieved,
            COALESCE(MAX(prev.amount), 0) AS baseline
          FROM allocations a
          JOIN users u ON u.id = a.user_id
          LEFT JOIN weightage w ON a.kpi = w.kpi
          LEFT JOIN (
            SELECT kpi, SUM(value) AS achieved
            FROM entries
            WHERE period = ? AND employee_id = ? AND branch_id = ? AND status='Verified'
            GROUP BY kpi
          ) e ON a.kpi = e.kpi
          LEFT JOIN (
            SELECT kpi, SUM(amount) AS amount
            FROM previous_period_data_staffwise
            WHERE period = ? AND employee_id = ? AND branch_id = ? AND deleted_at IS NULL
            GROUP BY kpi
          ) prev ON a.kpi COLLATE utf8mb4_unicode_ci = prev.kpi COLLATE utf8mb4_unicode_ci
          WHERE a.period = ? 
          AND a.user_id = ? 
          AND u.branch_id = ? 
          AND a.state='published' 
          AND u.period = ?
          GROUP BY a.kpi
        `;

        pool.query(
          personalQuery,
          [period, employeeId, branchId, period, employeeId, branchId, period, employeeId, branchId, period],
          (errP, personalTargets) => {
            if (errP) return res.status(500).json({ error: errP.message });

            
          
const branchTargetsQuery = `
 SELECT 
    k.kpi,

    CASE 
        WHEN k.kpi = 'recovery'
        AND emp.transfer_date IS NULL
        AND (emp.user_add_date IS NULL OR emp.user_add_date <= ?)
        THEN COALESCE(MAX(t.amount),0)

        WHEN k.kpi = 'recovery'
        AND (
            emp.transfer_date BETWEEN ? AND ?
            OR
            emp.user_add_date BETWEEN ? AND ?
        )
        THEN COALESCE(MAX(a.amount),0)

        ELSE COALESCE(MAX(a.amount),0)
    END AS amount,

    COALESCE(MAX(w.weightage), 0) AS weightage,
    (COALESCE(MAX(e_sum.achieved), 0) + COALESCE(MAX(prev.amount), 0)) AS achieved,
    COALESCE(MAX(prev.amount), 0) AS baseline

FROM (
    SELECT 'deposit' AS kpi
    UNION SELECT 'loan_gen'
    UNION SELECT 'loan_amulya'
    UNION SELECT 'recovery'
    UNION SELECT 'insurance'
    UNION SELECT 'audit'
) k

JOIN users emp 
ON emp.id = ?
AND emp.period = ?

JOIN users bm 
ON bm.branch_id = emp.branch_id 
AND bm.role = 'BM'
AND bm.period = ?

LEFT JOIN allocations a 
ON a.kpi = k.kpi 
AND a.user_id = emp.id 
AND a.period = ?
AND a.state = 'published'

LEFT JOIN targets t
ON t.kpi = k.kpi
AND t.branch_id = emp.branch_id
AND t.period = ?

LEFT JOIN weightage w 
ON w.kpi = k.kpi

LEFT JOIN (
    SELECT 
        kpi,
        employee_id,
        SUM(value) AS achieved
    FROM entries
    WHERE status = 'Verified' AND period = ? AND branch_id = ?
    GROUP BY kpi, employee_id
) e_sum
ON e_sum.kpi = k.kpi
AND (
    (
        k.kpi IN ('audit','recovery')
        AND (
            (
                emp.transfer_date IS NULL
                AND (emp.user_add_date IS NULL OR emp.user_add_date <= ?)
                AND e_sum.employee_id = bm.id
            )
            OR
            (
                emp.transfer_date BETWEEN ? AND ?
                AND e_sum.employee_id = emp.id
            )
            OR
            (
                emp.user_add_date BETWEEN ? AND ?
                AND e_sum.employee_id = emp.id
            )
        )
    )
    OR
    (
        k.kpi NOT IN ('audit','recovery')
        AND e_sum.employee_id = emp.id
    )
)
LEFT JOIN (
    SELECT employee_id, kpi, SUM(amount) AS amount
    FROM previous_period_data_staffwise
    WHERE period = ? AND branch_id = ? AND deleted_at IS NULL
    GROUP BY employee_id, kpi
) prev
ON prev.employee_id COLLATE utf8mb4_unicode_ci = emp.id COLLATE utf8mb4_unicode_ci
AND prev.kpi COLLATE utf8mb4_unicode_ci = k.kpi COLLATE utf8mb4_unicode_ci

GROUP BY k.kpi
ORDER BY k.kpi;
`;    
const startYear = parseInt(period.split('-')[0]);
const fyStart = `${startYear}-04-01`;
const fyEnd   = `${startYear + 1}-03-31`;
            pool.query(
              branchTargetsQuery,

              [
                fyStart, // 1
                fyStart,
                fyEnd, // 2,3
                fyStart,
                fyEnd, // 4,5

                employeeId, // 6
                period, // 7

                period, // 8 (BM)

                period, // 9 (allocations)
                period, // 10 (targets)
                
                period, // 11 (entries - period)
                branchId, // 12 (entries - branchId)

                fyStart, // 13
                fyStart,
                fyEnd, // 14,15
                fyStart,
                fyEnd, // 16,17
                
                period, // 18 (prev - period)
                branchId, // 19 (prev - branchId)
              ],
              (errB, branchTargets) => {
                if (errB) return res.status(500).json({ error: errB.message });

                if (!Array.isArray(branchTargets)) branchTargets = [];

                const branchMap = {};
                branchTargets.forEach((bt) => {
                  branchMap[bt.kpi] = {
                    kpi: bt.kpi,
                    amount: Number(bt.amount) || 0,
                    weightage: Number(bt.weightage) || 0,
                    achieved: Number(bt.achieved) || 0,
                    baseline: Number(bt.baseline) || 0,
                  };
                });

                let finalPersonal = personalTargets
                  .filter((p) => PERSONAL_KPIS.includes(p.kpi))
                  .map((p) => ({
                    kpi: p.kpi,
                    amount: Number(p.amount) || 0,
                    achieved: Number(p.achieved) || 0,
                    baseline: Number(p.baseline) || 0,
                    weightage: Number(p.weightage) || weightMap[p.kpi] || 0,
                    state: p.state || "published",
                  }));

                PERSONAL_KPIS.forEach((k) => {
                  if (!finalPersonal.find((p) => p.kpi === k)) {
                    finalPersonal.push({
                      kpi: k,
                      amount: 0,
                      achieved: Number(branchAch[k]) || 0,
                      baseline: 0,
                      weightage: weightMap[k] || 0,
                      state: "published",
                    });
                  }
                });

                let finalBranch = BRANCH_KPIS.map((kpi) => {
                  const bt = branchMap[kpi];
                  return {
                    kpi,
                    amount: bt ? Number(bt.amount) : 0,
                    achieved: bt ? Number(bt.achieved) : 0,
                    baseline: bt ? Number(bt.baseline) : 0,
                    weightage: bt ? Number(bt.weightage) : weightMap[kpi] || 0,
                  };
                });

                return res.json({
                  personal: finalPersonal,
                  branch: finalBranch,
                });
              },
            );
          },
        );

        return;
      }

      // ================= ALL USERS =================
      const query = `
        SELECT 
          a.*, 
          u.name AS staffName, 
          COALESCE(w.weightage, 0) AS weightage
        FROM (
          SELECT 
            user_id, 
            kpi, 
            MAX(amount) AS amount, 
            MAX(state) AS state,
            MAX(period) AS period,
            MAX(branch_id) AS branch_id
          FROM allocations
          WHERE period = ? AND state='published'
          GROUP BY user_id, kpi
        ) a
        JOIN users u ON u.id = a.user_id
        LEFT JOIN weightage w ON w.kpi = a.kpi
        ${branchId ? "WHERE a.branch_id = ?" : ""}
      `;

      const params = branchId ? [period, branchId] : [period];

      pool.query(query, params, (errAll, rows) => {
        if (errAll)
          return res.status(500).json({ error: "Internal server error" });

        rows = rows.map((r) => ({
          ...r,
          amount: Number(r.amount) || 0,
          weightage: Number(r.weightage) || 0,
        }));

        return res.json(rows);
      });
    });
  });
});

//BM transfer major part
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

      //  Get USER transfer date
      conn.query(
        "SELECT transfer_date FROM users WHERE id=? AND period = ?",
        [staff_id,period],
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
            },
          );
        },
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
                },
              );
            },
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
// this api for the Clerk to BM Target distributrition
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
        1,
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
    const userTdQuery = `SELECT transfer_date FROM users WHERE id=? AND period = ?`;
    pool.query(userTdQuery, [staff_id,period], (err, staffRows) => {
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
