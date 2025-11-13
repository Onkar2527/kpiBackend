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
  pool.query('SELECT u.id, u.username, u.name, u.role, b.name as branch_name, u.PF_NO, d.name as department_name FROM users u left join branches b on u.branch_id=b.id left join departments d on d.id=u.department_id', (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.post('/users', (req, res) => {
  const {  username, name, password, role, branch_id, department_id,PF_NO } = req.body;
  const password_hash = bcrypt.hashSync(password, 10);
  console.log('Adding user:', { username, name, role, branch_id, department_id,password_hash });
  const user = {  username, name, password_hash, role, branch_id, department_id ,PF_NO};
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
  const { username, name, role, branch_id, department_id,PF_NO } = req.body;
  const user = { username, name, role, branch_id, department_id,PF_NO };
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
  pool.query(`SELECT e.id, u.name,b1.name as old_branch,b2.name as new_branch  FROM employee_transfer e 
              join users u on e.staff_id=u.id join branches b1 on b1.id=e.old_branch_id 
              join branches b2 on b2.id=e.new_branch_id`, (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.post('/transfers', (req, res) => {
  const {
    staff_id,
    old_branch_id,
    new_branch_id,
    kpi_total,
    period,
    deposit_target,
    deposit_achieved,
    loan_gen_target,
    loan_gen_achieved,
    loan_amulya_target,
    loan_amulya_achieved,
    audit_target,
    audit_achieved,
    recovery_target,
    recovery_achieved,
    insurance_target,
    insurance_achieved,
    old_designation,
    new_designation
  } = req.body;

  
  const transfer = {
    staff_id,
    old_branch_id,
    new_branch_id,
    kpi_total,
    period,
    deposit_target,
    deposit_achieved,
    loan_gen_target,
    loan_gen_achieved,
    loan_amulya_target,
    loan_amulya_achieved,
    audit_target,
    audit_achieved,
    recovery_target,
    recovery_achieved,
    insurance_target,
    insurance_achieved,
    old_designation,
    new_designation
  };

  console.log('Adding transfer:', transfer);

  pool.getConnection((err, connection) => {
    if (err) return res.status(500).json({ error: 'Internal server error' });

    connection.beginTransaction(err => {
      if (err) {
        connection.release();
        return res.status(500).json({ error: 'Internal server error' });
      }

      const query = 'INSERT INTO employee_transfer SET ?';

      connection.query(query, transfer, (error) => {
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
            message: 'Transfer added successfully',
            data: transfer
          });
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
  const { branch_id ,role } = req.body;
  const user = { branch_id,role ,transfered:1};
  
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

// trasfer-history
mastersRouter.post('/trasfer-history', (req, res) => {
  const {period}=req.body;
  const query=`SELECT e.staff_id, s.name, DATE(MIN(e.transfer_date)) AS transfer_date FROM employee_transfer e JOIN  users s ON s.id = e.staff_id WHERE e.period= ?  GROUP BY e.staff_id, DATE(e.transfer_date) ORDER BY DATE(MIN(e.transfer_date))`;
  pool.query(query,[period], (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});

mastersRouter.post("/transfer-Kpi-history", (req, res) => {
  const { period, staff_id } = req.body;
  console.log(req.body);

  // --- Step 1: Fetch transfer data ---
  const query = `
    SELECT 
      e.*, 
      u.name,
      b.name AS branch_name
    FROM 
      employee_transfer e
      JOIN users u ON u.id = e.staff_id
      JOIN branches b ON b.id = e.old_branch_id
    WHERE 
      e.period = ? AND e.staff_id = ?
    ORDER BY 
      e.staff_id, e.transfer_date ASC
  `;

  pool.query(query, [period, staff_id], (err, transfers) => {
    if (err) return res.status(500).json({ error: "Internal server error" });
    if (transfers.length === 0) return res.json([]);

    // --- Step 2: Fetch weightage values ---
    const weightageQuery = `SELECT kpi, weightage FROM weightage`;

    pool.query(weightageQuery, (err, weightages) => {
      if (err) return res.status(500).json({ error: "Error fetching weightage" });

      // Convert to map for easy lookup
      const weightageMap = {};
      weightages.forEach((w) => {
        weightageMap[w.kpi] = w.weightage;
      });

      const calculateScore = (kpi, achieved, target) => {
        let outOf10;
        const ratio = achieved / target;
        const auditRatio = kpi === "audit" ? kpi.achieved / kpi.target : 0;
        const recoveryRatio = kpi === "recovery" ? kpi.achieved / kpi.target : 0;

        switch (kpi) {
          case "deposit":
          case "loan_gen":
            if (ratio < 1) {
              outOf10 = ratio * 10;
            } else if (ratio < 1.25) {
              outOf10 = 10;
            } else if (auditRatio >= 0.75 && recoveryRatio >= 0.75) {
              outOf10 = 12.5;
            } else {
              outOf10 = 10;
            }
            break;

          case "loan_amulya":
            if (ratio < 1) {
              outOf10 = ratio * 10;
            } else if (ratio < 1.25) {
              outOf10 = 10;
            } else {
              outOf10 = 12.5;
            }
            break;

          case "insurance":
            if (ratio === 0) {
              outOf10 = -2;
            } else if (ratio < 1) {
              outOf10 = ratio * 10;
            } else if (ratio < 1.25) {
              outOf10 = 10;
            } else {
              outOf10 = 12.5;
            }
            break;

          case "recovery":
          case "audit":
            if (ratio < 1) {
              outOf10 = ratio * 10;
            } else {
              outOf10 = 12.5;
            }
            break;

          default:
            outOf10 = 0;
        }

        return Math.max(0, Math.min(12.5, isNaN(outOf10) ? 0 : outOf10));
      };

      const staffResult = {
        staff_id: transfers[0].staff_id,
        name: transfers[0].name,
        period: transfers[0].period,
        transfers: [],
      };

      // --- Step 3: Apply SAME SCORE LOGIC per transfer record ---
      transfers.forEach((t) => {
        const branchScores = {};
        let totalWeightageScore = 0;

        const kpis = [
          { key: "deposit", achieved: t.deposit_achieved, target: t.deposit_target },
          { key: "loan_gen", achieved: t.loan_gen_achieved, target: t.loan_gen_target },
          { key: "loan_amulya", achieved: t.loan_amulya_achieved, target: t.loan_amulya_target },
          { key: "recovery", achieved: t.recovery_achieved, target: t.recovery_target },
          { key: "audit", achieved: t.audit_achieved, target: t.audit_target },
          { key: "insurance", achieved: t.insurance_achieved, target: t.insurance_target },
        ];

        kpis.forEach((row) => {
          if (row.target == null) return;

          const score = calculateScore(row.key, row.achieved, row.target);
          const weightage = weightageMap[row.key] || 0;
          const weightageScore = (score * weightage) / 100;

          branchScores[row.key] = {
            achieved: row.achieved || 0,
            target: row.target || 0,
            score,
            weightage,
            weightageScore:
              row.key === "insurance" && score === 0
                ? -2
                : isNaN(weightageScore)
                ? 0
                : weightageScore,
          };

          totalWeightageScore += branchScores[row.key].weightageScore;
        });

        // --- Step 4: Add all results per transfer ---
        staffResult.transfers.push({
          transfer_date: t.transfer_date,
          old_designation: t.old_designation,
          new_designation: t.new_designation,
          branch_name: t.branch_name,
          total_weightage_score: totalWeightageScore,
          ...branchScores,
        });
      });

      res.json([staffResult]);
    });
  });
});


