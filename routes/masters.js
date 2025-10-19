import express from 'express';
import pool from '../db.js';
import bcrypt from 'bcryptjs';

export const mastersRouter = express.Router();

// Departments
mastersRouter.get('/departments', (req, res) => {
  pool.query('SELECT * FROM departments', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.post('/departments', (req, res) => {
  const { name } = req.body;
  pool.getConnection((err, connection) => {
    if (err) return res.status(500).json({ error: 'Internal server error' });
    connection.beginTransaction(err => {
      if (err) {
        connection.release();
        return res.status(500).json({ error: 'Internal server error' });
      }
      connection.query('INSERT INTO departments (name) VALUES (?)', [name], (error, result) => {
        if (error) {
          return connection.rollback(() => {
            connection.release();
            res.status(500).json({ error: 'Internal server error' });
          });
        }
        connection.commit(err => {
          if (err) {
            return connection.rollback(() => {
              connection.release();
              res.status(500).json({ error: 'Internal server error' });
            });
          }
          connection.release();
          res.json({ id: result.insertId, name });
        });
      });
    });
  });
});

mastersRouter.put('/departments/:id', (req, res) => {
  const { name } = req.body;
  pool.query('UPDATE departments SET name = ? WHERE id = ?', [name, req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

// Weightages
mastersRouter.get('/weightages', (req, res) => {
  pool.query('SELECT * FROM weightage', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.put('/weightages', (req, res) => {
  const { kpi, weightage } = req.body;
  pool.query('UPDATE weightage SET weightage = ? WHERE kpi = ?', [weightage, kpi], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

mastersRouter.delete('/departments/:id', (req, res) => {
  console.log(`Deleting department with id: ${req.params.id}`);
  pool.query('DELETE FROM departments WHERE id = ?', [req.params.id], (error, result) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Department not found' });
    }
    res.json({ ok: true });
  });
});

// Users
mastersRouter.get('/users', (req, res) => {
  pool.query('SELECT id, username, name, role, branch_id, department_id FROM users', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.post('/users', (req, res) => {
  const {  username, name, password, role, branch_id, department_id } = req.body;
  const password_hash = bcrypt.hashSync(password, 10);
  console.log('Adding user:', { username, name, role, branch_id, department_id,password_hash });
  const user = {  username, name, password_hash, role, branch_id, department_id };
  pool.getConnection((err, connection) => {
    if (err) return res.status(500).json({ error: 'Internal server error' });
    connection.beginTransaction(err => {
      if (err) {
        connection.release();
        return res.status(500).json({ error: 'Internal server error' });
      }
      connection.query('INSERT INTO users SET ?', user, (error) => {
        if (error) {
          return connection.rollback(() => {
            connection.release();
            res.status(500).json({ error: 'Internal server error' });
          });
        }
        connection.commit(err => {
          if (err) {
            return connection.rollback(() => {
              connection.release();
              res.status(500).json({ error: 'Internal server error' });
            });
          }
          connection.release();
          res.json(user);
        });
      });
    });
  });
});

mastersRouter.put('/users/:id', (req, res) => {
  const { username, name, role, branch_id, department_id } = req.body;
  const user = { username, name, role, branch_id, department_id };
  pool.query('UPDATE users SET ? WHERE id = ?', [user, req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

mastersRouter.delete('/users/:id', (req, res) => {
  console.log(`Deleting user with id: ${req.params.id}`);
  pool.query('DELETE FROM users WHERE id = ?', [req.params.id], (error, result) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    res.json({ ok: true });
  });
});

mastersRouter.get('/users/branch/:branchId/role/:role', (req, res) => {
  const { branchId, role } = req.params;
  pool.query('SELECT id, name, role, branch_id FROM users WHERE branch_id = ? AND role = ?', [branchId, role], (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

// Branches
mastersRouter.get('/branches', (req, res) => {
  pool.query('SELECT * FROM branches', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.post('/branches', (req, res) => {
  const {  code, name } = req.body;
  const branch = { code, name };
  console.log('Adding branch:', branch);
  pool.getConnection((err, connection) => {
    if (err) return res.status(500).json({ error: 'Internal server error' });
    connection.beginTransaction(err => {
      if (err) {
        connection.release();
        return res.status(500).json({ error: 'Internal server error' });
      }
      connection.query('INSERT INTO branches SET ?', branch, (error) => {
        if (error) {
          return connection.rollback(() => {
            connection.release();
            res.status(500).json({ error: 'Internal server error' });
          });
        }
        connection.commit(err => {
          if (err) {
            return connection.rollback(() => {
              connection.release();
              res.status(500).json({ error: 'Internal server error' });
            });
          }
          connection.release();
          res.json(branch);
        });
      });
    });
  });
});

mastersRouter.put('/branches/:id', (req, res) => {
  const { code, name } = req.body;
  pool.query('UPDATE branches SET code = ?, name = ? WHERE id = ?', [code, name, req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

mastersRouter.delete('/branches/:id', (req, res) => {
  console.log(`Deleting branch with id: ${req.params.id}`);
  pool.query('DELETE FROM branches WHERE id = ?', [req.params.id], (error, result) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Branch not found' });
    }
    res.json({ ok: true });
  });
});
 
//staff Transfers

mastersRouter.get('/transfers', (req, res) => {
  pool.query('SELECT * FROM employee_transfer', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.post('/transfers', (req, res) => {
  const { staff_id, old_branch_id, new_branch_id, kpi_total } = req.body;
  const transfer = { staff_id, old_branch_id, new_branch_id, kpi_total };
  console.log('Adding transfer:', transfer);
  pool.getConnection((err, connection) => {
    if (err) return res.status(500).json({ error: 'Internal server error' });
    connection.beginTransaction(err => {
      if (err) {
        connection.release();
        return res.status(500).json({ error: 'Internal server error' });
      }
      connection.query('INSERT INTO employee_transfer SET ?', transfer, (error) => {
        if (error) {
          return connection.rollback(() => {
            connection.release();
            res.status(500).json({ error: 'Internal server error' });
          });
        }
        connection.commit(err => {
          if (err) {
            return connection.rollback(() => {
              connection.release();
              res.status(500).json({ error: 'Internal server error' });
            });
          }
          connection.release();
          res.json(transfer);
        });
      });
    });
  });
});

mastersRouter.put('/transfers/:id', (req, res) => {
  const { staff_id, old_branch_id, new_branch_id, kpi_total } = req.body;
  pool.query('UPDATE employee_transfer SET staff_id = ?, old_branch_id = ?, new_branch_id = ?, kpi_total = ? WHERE id = ?', [staff_id, old_branch_id, new_branch_id, kpi_total, req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

mastersRouter.delete('/transfers/:id', (req, res) => {
  console.log(`Deleting transfer with id: ${req.params.id}`);
  pool.query('DELETE FROM employee_transfer WHERE id = ?', [req.params.id], (error, result) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Transfer not found' });
    }
    res.json({ ok: true });
  });
});

mastersRouter.put('/Transfers_user/:id', (req, res) => {
  const { branch_id } = req.body;
  const user = { branch_id ,transfered:1};
  pool.query('UPDATE users SET ? WHERE id = ?', [user, req.params.id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});
mastersRouter.delete('/Transfer_for_delete_allocation', (req, res) => {
  const { user_id } = req.body;
  const user = { user_id};
  pool.query('DELETE FROM allocations WHERE user_id = ?', [user_id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

mastersRouter.delete('/Transfer_for_delete_ho_staff', (req, res) => {
  const { ho_staff_id, branch_id } = req.body;
  const user = { ho_staff_id, branch_id };
  pool.query('DELETE FROM ho_staff_kpi WHERE ho_staff_id = ? AND branch_id = ?', [ho_staff_id, branch_id], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});
