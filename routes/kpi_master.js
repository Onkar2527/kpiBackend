import express from "express";
import pool from "../db.js";


export const KpiRouter = express.Router();
//kpi master
KpiRouter.get('/kpiMaster', (req, res) => {
  pool.query('SELECT * FROM kpi_master where deleted_at is null', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

KpiRouter.post('/kpiMaster', (req, res) => {
  const {kpi_name} = req.body;
    const kpis = { kpi_name};

  pool.getConnection((err, connection) => {
    if (err) return res.status(500).json({ error: 'Internal server error' });

    connection.beginTransaction(err => {
      if (err) {
        connection.release();
        return res.status(500).json({ error: 'Internal server error' });
      }

      const query = 'INSERT INTO kpi_master SET ?';

      connection.query(query, kpis, (error) => {
        if (error) {
          console.error('Insert error:', error);
          return connection.rollback(() => {
            connection.release();
            res.status(500).json({ error: 'Database insert failed' });
          });
        }

        connection.commit(err => {
          if (err) {
            return connection.rollback(() => {
              connection.release();
              res.status(500).json({ error: 'Transaction commit failed' });
            });
          }

          connection.release();
          res.json({
            message: 'kpi added successfully',
            data: kpis
          });
        });
      });
    });
  });
});


KpiRouter.put('/kpiMaster/:id', (req, res) => {
  const { kpi_name } = req.body;
  pool.query('UPDATE kpi_master SET kpi_name = ? WHERE id = ?', [ kpi_name, req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

KpiRouter.delete('/kpiMaster/:id', (req, res) => {
  console.log(`Deleting KPI with id: ${req.params.id}`);

  const query = 'UPDATE kpi_master SET deleted_at = NOW() WHERE id = ?';
  pool.query(query, [req.params.id], (error, result) => {
    if (error) {
      console.error('Error deleting KPI:', error);
      return res.status(500).json({ error: 'Internal server error' });
    }

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'KPI not found' });
    }

    res.json({ ok: true, message: 'KPI deleted successfully' });
  });
});
//kpi mapping 
KpiRouter.get('/kpiMapping', (req, res) => {
  pool.query('SELECT * FROM role_kpi_mapping where deleted_at is null', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

KpiRouter.post('/kpiMapping', (req, res) => {
  const {role,kpi_id,weightage} = req.body;
    const kpis = { role,kpi_id,weightage};

  pool.getConnection((err, connection) => {
    if (err) return res.status(500).json({ error: 'Internal server error' });

    connection.beginTransaction(err => {
      if (err) {
        connection.release();
        return res.status(500).json({ error: 'Internal server error' });
      }

      const query = 'INSERT INTO role_kpi_mapping SET ?';

      connection.query(query, kpis, (error) => {
        if (error) {
          console.error('Insert error:', error);
          return connection.rollback(() => {
            connection.release();
            res.status(500).json({ error: 'Database insert failed' });
          });
        }

        connection.commit(err => {
          if (err) {
            return connection.rollback(() => {
              connection.release();
              res.status(500).json({ error: 'Transaction commit failed' });
            });
          }

          connection.release();
          res.json({
            message: 'kpi-mapping added successfully',
            data: kpis
          });
        });
      });
    });
  });
});


KpiRouter.put('/kpiMapping/:id', (req, res) => {
  const { role,kpi_id,weightage} = req.body;
  pool.query('UPDATE role_kpi_mapping SET role = ?,kpi_id = ?,weightage = ? WHERE id = ?', [ role,kpi_id,weightage, req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

KpiRouter.delete('/kpiMapping/:id', (req, res) => {
  console.log(`Deleting KPI with id: ${req.params.id}`);

  const query = 'UPDATE role_kpi_mapping SET deleted_at = NOW() WHERE id = ?';
  pool.query(query, [req.params.id], (error, result) => {
    if (error) {
      console.error('Error deleting KPI:', error);
      return res.status(500).json({ error: 'Internal server error' });
    }

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'KPI-mapping not found' });
    }

    res.json({ ok: true, message: 'KPI-mapping deleted successfully' });
  });
});

