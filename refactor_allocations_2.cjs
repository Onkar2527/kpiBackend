const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, 'routes', 'allocations.js');
let content = fs.readFileSync(filePath, 'utf8');

const regex = /export const autoDistributeTargetsResign = async \([\s\S]*?(?=\n\/\/ This function help us to calculate target distribution in case new users Add)/;

const newContent = `export const autoDistributeTargetsResign = async (
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
};`;

content = content.replace(regex, newContent);
fs.writeFileSync(filePath, content, 'utf8');
console.log('Successfully replaced autoDistributeTargetsResign!');
