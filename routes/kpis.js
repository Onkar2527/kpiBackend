import express from 'express';
import db from '../db.js';

const router = express.Router();

// Get all KPIs for a specific role
router.get('/roles/:role/kpis', (req, res) => {
  const { role } = req.params;
  db.query('SELECT * FROM role_kpis WHERE role = ?', [role], (error, results) => {
    if (error) {
      console.error(error);
      return res.status(500).json({ error: 'Internal Server Error' });
    }
    res.json(results);
  });
});

// Submit a new evaluation
router.post('/evaluations', (req, res) => {
  const { period, userId, roleKpiId, score, evaluatorId } = req.body;
  db.query(
    'INSERT INTO kpi_evaluations (period, user_id, role_kpi_id, score, evaluator_id) VALUES (?, ?, ?, ?, ?)',
    [period, userId, roleKpiId, score, evaluatorId],
    (error) => {
      if (error) {
        console.error(error);
        return res.status(500).json({ error: 'Internal Server Error' });
      }
      res.status(201).json({ message: 'Evaluation submitted successfully' });
    }
  );
});

// Get evaluations for a user
router.get('/users/:userId/evaluations', (req, res) => {
    const { userId } = req.params;
    const { period } = req.query;
    let query = `
        SELECT e.*, k.kpi_name, k.weightage, k.kpi_type
        FROM kpi_evaluations e
        JOIN role_kpis k ON e.role_kpi_id = k.id
        WHERE e.user_id = ?
    `;
    const params = [userId];
    if (period) {
        query += ' AND e.period = ?';
        params.push(period);
    }
    db.query(query, params, (error, results) => {
        if (error) {
            console.error(error);
            return res.status(500).json({ error: 'Internal Server Error' });
        }
        res.json(results);
    });
});

export { router as kpisRouter };
