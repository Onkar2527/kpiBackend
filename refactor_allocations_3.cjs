const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, 'routes', 'allocations.js');
let content = fs.readFileSync(filePath, 'utf8');

const regex = /export const autoDistributeTargetsNewUsers = \(period, branchId, callback\) => \{[\s\S]*?(?=\n\/\/ FINANCIAL YEAR RANGE)/;

const newContent = `export const autoDistributeTargetsNewUsers = async (period, branchId, callback) => {
  const fy = getFinancialYearRange(period);

  try {
    const targets = await new Promise((res, rej) => pool.query("SELECT * FROM targets WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));
    if (!targets.length) return callback(new Error("No targets found"));

    const previousData = await new Promise((res, rej) => pool.query("SELECT * FROM previous_period_data WHERE period = ? AND branch_id = ?", [period, branchId], (e, r) => e ? rej(e) : res(r)));

    const staff = await new Promise((res, rej) => pool.query("SELECT id, name, user_add_date, transfer_date FROM users WHERE branch_id = ? AND period = ? AND role IN (?)", [branchId, period, ["CLERK"]], (e, r) => e ? rej(e) : res(r)));
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
          \`INSERT INTO allocations (amount, state, period, branch_id, user_id, kpi) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount), state = VALUES(state)\`,
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
          \`INSERT INTO previous_period_data_staffwise (employee_id, period, branch_id, kpi, amount) VALUES ? ON DUPLICATE KEY UPDATE amount = VALUES(amount)\`,
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
};`;

content = content.replace(regex, newContent);
fs.writeFileSync(filePath, content, 'utf8');
console.log('Successfully replaced autoDistributeTargetsNewUsers!');
