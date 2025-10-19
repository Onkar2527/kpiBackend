import express from 'express';
import pool from '../db.js';
import { nanoid } from 'nanoid';

// Router implementing KPI entry CRUD and verification.

export const entriesRouter = express.Router();

// POST /entries
// Create a new KPI entry in Pending state.
entriesRouter.post('/', (req, res) => {
  const { period, branchId, employeeId, kpi, accountNo, value, date } = req.body || {};
  if (!period || !branchId || !employeeId || !kpi) return res.status(400).json({ error: 'missing fields' });
  const entry = {
    id: nanoid(),
    period,
    branch_id: branchId,
    employee_id: employeeId,
    kpi,
    account_no: accountNo || null,
    value: Number(value) || 0,
    date: date || new Date().toISOString().slice(0, 10),
    status: 'Pending'
  };
  pool.query('INSERT INTO entries SET ?', entry, (error, result) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(entry);
  });
});

// GET /entries
// List entries filtered by period, branchId, employeeId and status.
entriesRouter.get('/', (req, res) => {
  const { period, branchId, employeeId, status } = req.query;
  let query = 'SELECT e.*, u.name as staffName FROM entries e JOIN users u ON e.employee_id = u.id WHERE 1 = 1';
  const params = [];

  if (period) {
    query += ' AND e.period = ?';
    params.push(period);
  }
  if (branchId) {
    query += ' AND e.branch_id = ?';
    params.push(branchId);
  }
  if (employeeId) {
    query += ' AND e.employee_id = ?';
    params.push(employeeId);
  }
  if (status) {
    query += ' AND e.status = ?';
    params.push(status);
  }

  pool.query(query, params, (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

// POST /entries/:id/verify
// Mark an entry as Verified.
entriesRouter.post('/:id/verify', (req, res) => {
  pool.query('UPDATE entries SET status = ?, verified_at = ? WHERE id = ?', ['Verified', new Date(), req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

// POST /entries/:id/return
// Mark an entry as Returned.
entriesRouter.post('/:id/return', (req, res) => {
  pool.query('UPDATE entries SET status = ? WHERE id = ?', ['Returned', req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});
