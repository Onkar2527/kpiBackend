import express from "express";
import pool from "../db.js";

export const entriesRouter = express.Router();

entriesRouter.post("/", (req, res) => {
  const {
    period,
    branchId,
    employeeId,
    kpi,
    accountNo,
    value,
    date,
    typeOfDeposit,
    type,
  } = req.body || {};
  if (!period || !branchId || !kpi || !typeOfDeposit || !type)
    return res.status(400).json({ error: "Missing required fields" });

  const entryDate = date || new Date().toISOString().slice(0, 10);

  try {
    if (type === "Remove") {
      if (typeOfDeposit?.toLowerCase() === "individual") {
        const hasAccount = accountNo && accountNo.trim() !== "";
        const selectQuery = hasAccount
          ? `SELECT value FROM entries WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND account_no=?`
          : `SELECT value FROM entries WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND (account_no IS NULL OR account_no='')`;

        const queryParams = hasAccount
          ? [period, branchId, kpi, employeeId, accountNo]
          : [period, branchId, kpi, employeeId];

        pool.query(
          selectQuery,
          queryParams,
          (err, results) => {
            if (err)
              return res
                .status(500)
                .json({ error: "Error fetching existing entry" });
            if (!results.length)
              return res.status(400).json({ error: "Insufficient balance. Current balance is 0." });

            const currentValue = Number(results[0].value || 0);
            const newValue = currentValue - Number(value || 0);
            if (newValue < 0)
              return res
                .status(400)
                .json({ error: `Insufficient balance. Current balance is ${currentValue}.` });

            if (newValue === 0) {
              const delQuery = hasAccount
                ? `DELETE FROM entries WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND account_no=?`
                : `DELETE FROM entries WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND (account_no IS NULL OR account_no='')`;

              const delParams = hasAccount
                ? [period, branchId, kpi, employeeId, accountNo]
                : [period, branchId, kpi, employeeId];

              pool.query(
                delQuery,
                delParams,
                (err) => {
                  if (err)
                    return res
                      .status(500)
                      .json({ error: "Error deleting entry" });
                  return res.json({
                    message: "Entry fully removed (value reached 0)",
                  });
                },
              );
            } else {
              const updateQuery = hasAccount
                ? `UPDATE entries SET value=?, date=? WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND account_no=?`
                : `UPDATE entries SET value=?, date=? WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND (account_no IS NULL OR account_no='')`;

              const updateParams = hasAccount
                ? [newValue, entryDate, period, branchId, kpi, employeeId, accountNo]
                : [newValue, entryDate, period, branchId, kpi, employeeId];

              pool.query(
                updateQuery,
                updateParams,
                (err) => {
                  if (err)
                    return res
                      .status(500)
                      .json({ error: "Error updating entry" });
                  return res.json({
                    message: "Entry value reduced successfully",
                    newValue,
                  });
                },
              );
            }
          },
        );
        return;
      }

      if (typeOfDeposit?.toLowerCase() === "combined") {
        const hasAccount = accountNo && accountNo.trim() !== "";
        const selectQuery = hasAccount
          ? `SELECT SUM(value) AS totalValue FROM entries WHERE period=? AND branch_id=? AND kpi=? AND account_no=?`
          : `SELECT SUM(value) AS totalValue FROM entries WHERE period=? AND branch_id=? AND kpi=? AND (account_no IS NULL OR account_no='') AND type='Combined'`;

        const selectParams = hasAccount
          ? [period, branchId, kpi, accountNo]
          : [period, branchId, kpi];

        pool.query(
          selectQuery,
          selectParams,
          (err, results) => {
            if (err)
              return res
                .status(500)
                .json({ error: "Error fetching combined entries" });

            const existingTotal = Number(results[0]?.totalValue || 0);
            const subtractValue = Number(value || 0);
            const newTotal = existingTotal - subtractValue;

            if (newTotal < 0)
              return res
                .status(400)
                .json({ error: `Insufficient balance. Current balance is ${existingTotal}.` });

            const delQuery = hasAccount
              ? `DELETE FROM entries WHERE period=? AND branch_id=? AND kpi=? AND account_no=?`
              : `DELETE FROM entries WHERE period=? AND branch_id=? AND kpi=? AND (account_no IS NULL OR account_no='') AND type='Combined'`;

            const delParams = hasAccount
              ? [period, branchId, kpi, accountNo]
              : [period, branchId, kpi];

            pool.query(delQuery, delParams, (err) => {
              if (err)
                return res
                  .status(500)
                  .json({ error: "Error deleting old entries" });

              if (newTotal === 0) {
                return res.json({
                  message: "Entries removed (value reached 0)",
                });
              }

              const staffQuery = `SELECT id FROM users WHERE branch_id=? AND period = ? AND role IN ('CLERK')`;
              pool.query(staffQuery, [branchId, period], (error, staffResults) => {
                if (error)
                  return res
                    .status(500)
                    .json({ error: "Error fetching staff" });
                if (!staffResults?.length)
                  return res.status(400).json({ error: "No staff found" });

                const staff = staffResults;
                const baseValue = Math.floor(newTotal / staff.length);
                const remainder = newTotal % staff.length;

                const entryDate = date || new Date().toISOString().slice(0, 10);
                const entries = staff.map((s, i) => [
                  period,
                  branchId,
                  s.id,
                  kpi,
                  hasAccount ? accountNo : null,
                  baseValue + (i < remainder ? 1 : 0),
                  entryDate,
                  typeOfDeposit,
                  "Pending",
                ]);

                const nonZeroEntries = entries.filter(e => e[5] > 0);

                if (nonZeroEntries.length === 0) {
                  return res.json({
                    message: `Combined entries updated (no non-zero entries to insert)`,
                    newTotal,
                    perEmployee: baseValue,
                  });
                }

                const insertQuery = `
          INSERT INTO entries 
          (period, branch_id, employee_id, kpi, account_no, value, date, type, status)
          VALUES ?`;

                pool.query(insertQuery, [nonZeroEntries], (err) => {
                  if (err)
                    return res
                      .status(500)
                      .json({ error: "Error inserting adjusted entries" });

                  return res.json({
                    message: `Combined entries updated`,
                    newTotal,
                    perEmployee: baseValue,
                  });
                });
              });
            });
          },
        );
        return;
      }
    }

    if (typeOfDeposit?.toLowerCase() === "individual")
      return insertIndividual();
    if (typeOfDeposit?.toLowerCase() === "combined") return insertCombined();
    return res.status(400).json({ error: "Invalid or missing typeOfDeposit" });

    function insertIndividual() {
      const entry = {
        period,
        branch_id: branchId,
        employee_id: employeeId,
        kpi,
        account_no: accountNo || null,
        value: Number(value) || 0,
        date: entryDate,
        type: typeOfDeposit,
        status: "Pending",
      };

      pool.query("INSERT INTO entries SET ?", entry, (err) => {
        if (err)
          return res.status(500).json({ error: "Failed to insert entry" });
        return res.json({ message: "Entry added (individual)", entry });
      });
    }

    function insertCombined() {
      const query = `SELECT id FROM users WHERE branch_id=? AND period = ? AND role IN ('CLERK')`;
      pool.query(query, [branchId, period], (error, results) => {
        if (error)
          return res.status(500).json({ error: "Internal server error" });
        if (!results || results.length === 0)
          return res.status(400).json({ error: "No staff found" });

        const staff = results;
        const totalValue = Number(value) || 0;
        const baseValue = Math.floor(totalValue / staff.length);
        const remainder = totalValue % staff.length;

        const entries = staff.map((s, i) => [
          period,
          branchId,
          s.id,
          kpi,
          accountNo || null,
          baseValue + (i < remainder ? 1 : 0),
          entryDate,
          typeOfDeposit,
          "Pending",
        ]);

        const nonZeroEntries = entries.filter(e => e[5] > 0);

        if (nonZeroEntries.length === 0) {
          return res.json({
            message: `Combined entries distributed (no non-zero entries to insert)`,
            distributedValue: totalValue,
            perEmployee: baseValue,
          });
        }

        const insertQuery = `
          INSERT INTO entries 
          (period, branch_id, employee_id, kpi, account_no, value, date, type, status)
          VALUES ?`;

        pool.query(insertQuery, [nonZeroEntries], (err) => {
          if (err)
            return res.status(500).json({ error: "Failed to insert entries" });
          return res.json({
            message: `Entries distributed among ${staff.length} staff`,
            distributedValue: totalValue,
            perEmployee: baseValue,
          });
        });
      });
    }
  } catch (err) {
    console.error("Error inserting entry:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

// GET /entries
// List entries filtered by period, branchId, employeeId and status.
entriesRouter.get("/", (req, res) => {
  const { period, branchId, employeeId, status } = req.query;
  let query = `SELECT e.*, u.name AS staffName 
    FROM entries e 
    JOIN users u ON e.employee_id = u.id  
    WHERE 1 = 1
    `;
  const params = [];

  if (period) {
    query += " AND e.period = ? AND u.period = ?";
    params.push(period);
    params.push(period);
  }
  if (branchId) {
    query += " AND e.branch_id = ?";
    params.push(branchId);
  }
  if (employeeId) {
    query += " AND e.employee_id = ?";
    params.push(employeeId);
  }
  if (status) {
    query += " AND e.status = ?";
    params.push(status);
  }

  pool.query(query, params, (error, results) => {
    if (error) return res.status(500).json({ error: "Internal server error" });
    res.json(results);
  });
});

// POST /entries/:id/verify
// Mark an entry as Verified.
entriesRouter.post("/:id/verify", (req, res) => {
  pool.query(
    "UPDATE entries SET status = ?, verified_at = ? WHERE id = ?",
    ["Verified", new Date(), req.params.id],
    (error) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      res.json({ ok: true });
    },
  );
});

// POST /entries/:id/return
// Mark an entry as Returned.
entriesRouter.post("/:id/return", (req, res) => {
  pool.query(
    "UPDATE entries SET status = ? WHERE id = ?",
    ["Returned", req.params.id],
    (error) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      res.json({ ok: true });
    },
  );
});

//admin route to get entries of a month
entriesRouter.post("/monthEntries", (req, res) => {
  const { period } = req.body;
  if (!period) {
    return res.status(400).json({ error: "Period is required" });
  }

  pool.query("SELECT * FROM entries WHERE period = ?", [period], (error, entries) => {
    if (error) {
      console.error("Error fetching entries:", error);
      return res.status(500).json({ error: "Internal server error" });
    }

    if (entries.length === 0) {
      return res.json([]);
    }

    pool.query("SELECT id, PF_NO FROM users WHERE period = ?", [period], (error, users) => {
      if (error) {
        console.error("Error fetching users:", error);
        return res.status(500).json({ error: "Internal server error" });
      }

      pool.query("SELECT code, name FROM branches WHERE period = ?", [period], (error, branches) => {
        if (error) {
          console.error("Error fetching branches:", error);
          return res.status(500).json({ error: "Internal server error" });
        }

        const userMap = new Map();
        users.forEach(u => {
          userMap.set(String(u.id), u.PF_NO);
        });

        const branchMap = new Map();
        branches.forEach(b => {
          branchMap.set(String(b.code), b.name);
        });

        const results = entries.map(e => ({
          ...e,
          PF_NO: userMap.get(String(e.employee_id)) || null,
          branchName: branchMap.get(String(e.branch_id)) || null
        }));

        res.json(results);
      });
    });
  });
});

//Admin give entries delete option
entriesRouter.delete("/entries/:id", (req, res) => {
  pool.query(
    "DELETE FROM entries WHERE id = ?",
    [req.params.id],
    (error, result) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      if (result.affectedRows === 0) {
        return res.status(404).json({ error: "Entry not found" });
      }
      res.json({ ok: true });
    },
  );
});
