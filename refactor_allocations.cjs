const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, 'routes', 'allocations.js');
let content = fs.readFileSync(filePath, 'utf8');

// We will use string manipulation to replace the functions.

// 1. autoDistributeTargets
const autoDistributeTargetsRegex = /export const autoDistributeTargets = \(period, branchId, callback\) => \{[\s\S]*?(?=\n\/\*|\n\/\/This function  help to auto distribute logic for only the transfer staff)/;
const newAutoDistributeTargets = `export const autoDistributeTargets = async (period, branchId, callback) => {
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
`;
content = content.replace(autoDistributeTargetsRegex, newAutoDistributeTargets);

// 2. autoDistributeTargetsInTransfer
const autoDistributeTargetsInTransferRegex = /export const autoDistributeTargetsInTransfer = \(period, branchId, callback\) => \{[\s\S]*?(?=\n\/\/This function  help to get year start and end date)/;
const newAutoDistributeTargetsInTransfer = `export const autoDistributeTargetsInTransfer = async (period, branchId, callback) => {
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
`;
content = content.replace(autoDistributeTargetsInTransferRegex, newAutoDistributeTargetsInTransfer);

fs.writeFileSync(filePath, content, 'utf8');
console.log('Successfully replaced autoDistributeTargets and autoDistributeTargetsInTransfer!');
