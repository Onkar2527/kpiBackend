const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, 'routes', 'allocations.js');
let content = fs.readFileSync(filePath, 'utf8');

const regexOldBranch = /export const autoDistributeTargetsOldBranch = \([\s\S]*?(?=\n\/\/ This function help us to calculate target distribution in case new Branch)/;

const newOldBranch = `export const autoDistributeTargetsOldBranch = async (period, branchId, role, callback) => {
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
      \`SELECT SUM(deposit_target) AS deposit, SUM(loan_gen_target) AS loan_gen, SUM(loan_amulya_target) AS loan_amulya, SUM(recovery_target) AS recovery FROM employee_transfer WHERE old_branch_id = ? AND period = ? AND old_designation <> 'BM';\`,
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
};`;

content = content.replace(regexOldBranch, newOldBranch);

const regexNewBranch = /export const autoDistributeTargetsNewBranch = \([\s\S]*?(?=\n\/\/ This function help us to calculate target distribution in case resign BM)/;

const newNewBranch = `export const autoDistributeTargetsNewBranch = async (period, branchId, callback) => {
  const fy = getFinancialYearRange(period);

  try {
    const targets = await new Promise((res, rej) => pool.query("SELECT * FROM targets WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    if (!targets.length) return callback(new Error("No targets found"));

    const previousData = await new Promise((res, rej) => pool.query("SELECT * FROM previous_period_data WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const staff = await new Promise((res, rej) => pool.query("SELECT id, name, transfer_date, user_add_date FROM users WHERE branch_id = ? AND period = ? AND role IN (?)", [branchId, period, ["CLERK"]], (e, r) => e ? rej(e) : res(r)));
    if (!staff.length) return callback(new Error("No staff found"));

    const transferTargetMap = { deposit: 0, loan_gen: 0, loan_amulya: 0, recovery: 0 };
    const rRows = await new Promise((res, rej) => pool.query(
      \`SELECT SUM(deposit_target) AS deposit, SUM(loan_gen_target) AS loan_gen, SUM(loan_amulya_target) AS loan_amulya, SUM(recovery_target) AS recovery FROM employee_transfer WHERE old_branch_id = ? AND period = ? AND old_designation <> 'BM';\`,
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
};`;

content = content.replace(regexNewBranch, newNewBranch);
fs.writeFileSync(filePath, content, 'utf8');
console.log('Successfully replaced autoDistributeTargetsOldBranch and autoDistributeTargetsNewBranch!');
