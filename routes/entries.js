import express from "express";
import pool from "../db.js";
import { nanoid } from "nanoid";

// Router implementing KPI entry CRUD and verification.

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
        const selectQuery = `SELECT value FROM entries WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND account_no=?`;
        pool.query(
          selectQuery,
          [period, branchId, kpi, employeeId, accountNo],
          (err, results) => {
            if (err)
              return res
                .status(500)
                .json({ error: "Error fetching existing entry" });
            if (!results.length)
              return res.status(404).json({ error: "Entry not found" });

            const currentValue = Number(results[0].value || 0);
            const newValue = currentValue - Number(value || 0);
            if (newValue < 0)
              return res
                .status(400)
                .json({ error: "Cannot subtract beyond existing amount" });

            if (newValue === 0) {
              const delQuery = `DELETE FROM entries WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND account_no=?`;
              pool.query(
                delQuery,
                [period, branchId, kpi, employeeId, accountNo],
                (err) => {
                  if (err)
                    return res
                      .status(500)
                      .json({ error: "Error deleting entry" });
                  return res.json({
                    message: "Entry fully removed (value reached 0)",
                  });
                }
              );
            } else {
              const updateQuery = `UPDATE entries SET value=?, date=? WHERE period=? AND branch_id=? AND kpi=? AND employee_id=? AND account_no=?`;
              pool.query(
                updateQuery,
                [
                  newValue,
                  entryDate,
                  period,
                  branchId,
                  kpi,
                  employeeId,
                  accountNo,
                ],
                (err) => {
                  if (err)
                    return res
                      .status(500)
                      .json({ error: "Error updating entry" });
                  return res.json({
                    message: "Entry value reduced successfully",
                    newValue,
                  });
                }
              );
            }
          }
        );
        return;
      }

      if (typeOfDeposit?.toLowerCase() === "combined") {
        const selectQuery = `
    SELECT SUM(value) AS totalValue 
    FROM entries 
    WHERE period=? AND branch_id=? AND kpi=? AND account_no=?`;

        pool.query(
          selectQuery,
          [period, branchId, kpi, accountNo],
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
                .json({ error: "Cannot subtract beyond existing total" });

            const delQuery = `
      DELETE FROM entries 
      WHERE period=? AND branch_id=? AND kpi=? AND account_no=?`;

            pool.query(delQuery, [period, branchId, kpi, accountNo], (err) => {
              if (err)
                return res
                  .status(500)
                  .json({ error: "Error deleting old entries" });

              if (newTotal === 0) {
                return res.json({
                  message: "Entries removed for this account (value reached 0)",
                });
              }

              const staffQuery = `SELECT id FROM users WHERE branch_id=? AND role IN ('CLERK')`;
              pool.query(staffQuery, [branchId], (error, staffResults) => {
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
                  accountNo,
                  baseValue + (i < remainder ? 1 : 0),
                  entryDate,
                  "Pending",
                ]);

                const insertQuery = `
          INSERT INTO entries 
          (period, branch_id, employee_id, kpi, account_no, value, date, status)
          VALUES ?`;

                pool.query(insertQuery, [entries], (err) => {
                  if (err)
                    return res
                      .status(500)
                      .json({ error: "Error inserting adjusted entries" });

                  return res.json({
                    message: `Combined entries updated for account ${accountNo}`,
                    newTotal,
                    perEmployee: baseValue,
                  });
                });
              });
            });
          }
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
        status: "Pending",
      };
      pool.query("INSERT INTO entries SET ?", entry, (err) => {
        if (err)
          return res.status(500).json({ error: "Failed to insert entry" });
        return res.json({ message: "Entry added (individual)", entry });
      });
    }

    function insertCombined() {
      const query = `SELECT id FROM users WHERE branch_id=? AND role IN ('CLERK')`;
      pool.query(query, [branchId], (error, results) => {
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
          "Pending",
        ]);

        const insertQuery = `INSERT INTO entries (period, branch_id, employee_id, kpi, account_no, value, date, status) VALUES ?`;
        pool.query(insertQuery, [entries], (err) => {
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
  let query =
    `SELECT e.*, u.name AS staffName 
    FROM entries e 
    JOIN users u ON e.employee_id = u.id 
    WHERE 1 = 1
    `;
  const params = [];

  if (period) {
    query += " AND e.period = ?";
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
    }
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
    }
  );
});

//admin route to get entries of a month
entriesRouter.post("/monthEntries", (req, res) => {
  const { period } = req.body;
  if (!period) {
    return res.status(400).json({ error: "Period is required" });
  }

  const query = `
    SELECT * FROM entries
    WHERE period = ? and status='Verified' AND MONTH(date) = MONTH(CURRENT_DATE())
      AND YEAR(date) = YEAR(CURRENT_DATE())
  `;
  pool.query(query, [period], (error, results) => {
    if (error) {
      return res.status(500).json({ error: "Internal server error" });
    }
    res.json(results);
  });
});

entriesRouter.delete('/entries/:id', (req, res) => {
  console.log(`Deleting Entries with id: ${req.params.id}`);
  pool.query('DELETE FROM entries WHERE id = ?', [req.params.id], (error, result) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Entry not found' });
    }
    res.json({ ok: true });
  });
});