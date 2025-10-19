import express from 'express';
import db from '../db.js';

const router = express.Router();

router.get('/', (req, res) => {
  db.query('SELECT period FROM periods ORDER BY id DESC LIMIT 1', (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (rows.length > 0) {
      res.json(rows[0]);
    } else {
      res.json({ period: new Date().toISOString().slice(0, 7) });
    }
  });
});

router.post('/', (req, res) => {
  const { period } = req.body;
  db.query('INSERT INTO periods (period) VALUES (?)', [period], (err) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.status(201).json({ message: 'Period updated successfully' });
  });
});

export default router;
