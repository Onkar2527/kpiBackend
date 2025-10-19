import express from "express";
import pool from "../db.js";

// Router handling staff allocations: auto‑distribution and publish.
export const allocationsRouter = express.Router();

export const autoDistributeTargets = (period, branchId, callback) => {
  pool.query('SELECT * FROM targets WHERE period = ? AND branch_id = ?', [period, branchId], (error, targets) => {
    if (error) return callback(error);
    if (targets.length === 0) return callback(new Error('No branch targets found'));

    pool.query('SELECT * FROM users WHERE branch_id = ? AND role IN (?)', [branchId, ['ATTENDER', 'CLERK']], (error, staff) => {
      if (error) return callback(error);
      if (staff.length === 0) return callback(new Error('No active staff in branch'));

      const kpisToSplit = ['deposit', 'loan_gen', 'loan_amulya'];
      const allocations = [];

      pool.query('DELETE FROM allocations WHERE period = ? AND branch_id = ?', [period, branchId], (error) => {
        if (error) return callback(error);

        kpisToSplit.forEach(kpi => {
          const target = targets.find(t => t.kpi === kpi);
          const amount = target ? target.amount : 0;
          const base = Math.floor(amount / staff.length);
          const rem = amount % staff.length;
          staff.forEach((user, idx) => {
            allocations.push([period, branchId, user.id, kpi, base + (idx < rem ? 1 : 0), 'published']);
          });
        });

        const auditTarget = targets.find(t => t.kpi === 'audit');
        if (auditTarget) {
          staff.forEach(user => {
            allocations.push([period, branchId, user.id, 'audit', auditTarget.amount, 'published']);
          });
        }

        pool.query('INSERT INTO allocations (period, branch_id, user_id, kpi, amount, state) VALUES ?', [allocations], (error) => {
          if (error) return callback(error);
          callback(null);
        });
      });
    });
  });
};

export const autoDistributeTargetsInTransfer = (period, branchId, callback) => {

pool.query('DELETE FROM allocations WHERE period = ? AND branch_id = ?', [period, branchId], (error) => {
  if (error) return callback(error);
  });
  
 pool.query('SELECT * FROM targets WHERE period = ? AND branch_id = ?', [period, branchId], (error, targets) => {
    if (error) return callback(error);
    if (targets.length === 0) return callback(new Error('No branch targets found'));

    pool.query('SELECT * FROM users WHERE branch_id = ? AND role IN (?)', [branchId, ['ATTENDER', 'CLERK']], (error, staff) => {
      if (error) return callback(error);
      if (staff.length === 0) return callback(new Error('No active staff in branch'));

      const kpisToSplit = ['deposit', 'loan_gen', 'loan_amulya'];
      const allocations = [];

      
        kpisToSplit.forEach(kpi => {
          const target = targets.find(t => t.kpi === kpi);
          const amount = target ? target.amount : 0;
          const base = Math.floor(amount / staff.length);
          const rem = amount % staff.length;
          staff.forEach((user, idx) => {
            allocations.push([period, branchId, user.id, kpi, base + (idx < rem ? 1 : 0), 'published']);
          });
        });

        const auditTarget = targets.find(t => t.kpi === 'audit');
        if (auditTarget) {
          staff.forEach(user => {
            allocations.push([period, branchId, user.id, 'audit', auditTarget.amount, 'published']);
          });
        }

        pool.query('INSERT INTO allocations (period, branch_id, user_id, kpi, amount, state) VALUES ?', [allocations], (error) => {
          if (error) return callback(error);
          callback(null);
        });
      });
    });
  
};


// POST /allocations/auto-distribute?period=YYYY-MM&branchId=ID
// Equal split of branch targets (deposit, loan_gen, loan_amulya)
// across active staff (roles STAFF, ATTENDER, CLERK) in the given branch.
allocationsRouter.post("/auto-distribute", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargets(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

allocationsRouter.post("/auto-distribute-transfer", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  autoDistributeTargetsInTransfer(period, branchId, (error) => {
    if (error) return res.status(500).json({ error: error.message });
    res.json({ ok: true });
  });
});

// POST /allocations/publish
// Publish all allocations for a branch/period: sets state to published.
allocationsRouter.post("/publish", (req, res) => {
  const { period, branchId } = req.body || {};
  pool.query(
    "UPDATE allocations SET state = ? WHERE period = ? AND branch_id = ?",
    ["published", period, branchId],
    (error) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      res.json({ ok: true });
    }
  );
});

// GET /allocations
// Query allocations by period/branch or employee.
allocationsRouter.get("/", (req, res) => {
  const { period, branchId, employeeId } = req.query;
  if (!period) return res.status(400).json({ error: "period required" });

  if (employeeId) {
    const query = `
      SELECT a.*, w.weightage, e.achieved
      FROM allocations a 
      LEFT JOIN weightage w ON a.kpi = w.kpi 
      LEFT JOIN (
        SELECT kpi, SUM(value) as achieved 
        FROM entries 
        WHERE period = ? AND employee_id = ? AND status = 'Verified' 
        GROUP BY kpi
      ) e ON a.kpi = e.kpi
      WHERE a.period = ? AND a.user_id = ?
    `;
    pool.query(
      query,
      [period, employeeId, period, employeeId],
      (error, personalTargets) => {
        if (error)
          return res.status(500).json({ error: "Internal server error" });

        const branchQuery = `
        SELECT t.*, w.weightage, e.achieved
        FROM targets t 
        LEFT JOIN weightage w ON t.kpi = w.kpi 
        LEFT JOIN (
          SELECT kpi, SUM(value) as achieved 
          FROM entries 
          WHERE period = ? AND branch_id = ? AND status = 'Verified' 
          GROUP BY kpi
        ) e ON t.kpi = e.kpi
        WHERE t.period = ? AND t.branch_id = ?
      `;
        pool.query(
          branchQuery,
          [period, branchId, period, branchId],
          (error, branchTargets) => {
            if (error)
              return res.status(500).json({ error: "Internal server error" });

            const auditQuery = `
          SELECT 
            '${period}' AS period, 
            '${branchId}' AS branch_id, 
            'audit' AS kpi, 
            100 AS amount, 
            'published' AS state, 
            w.weightage, 
            e.achieved
          FROM (SELECT 1) AS dummy
          LEFT JOIN weightage w ON w.kpi = 'audit'
          LEFT JOIN (
            SELECT SUM(value) AS achieved 
            FROM entries 
            WHERE period = ? AND branch_id = ? AND status = 'Verified' AND kpi = 'audit'
          ) e ON 1=1
        `;
            pool.query(auditQuery, [period, branchId], (err, auditResult) => {
              if (err)
                return res.status(500).json({ error: "Internal server error" });

              const hasAudit = branchTargets.some((t) => t.kpi === "audit");
              if (!hasAudit && auditResult.length > 0) {
                branchTargets.push(auditResult[0]);
              }

              const personalKpis = personalTargets.map((t) => t.kpi);
              const finalBranchTargets = branchTargets.filter(
                (t) => !personalKpis.includes(t.kpi)
              );

              res.json({
                personal: personalTargets,
                branch: finalBranchTargets,
              });
            });
          }
        );
      }
    );
  } else {
    let query =
      "SELECT a.*, u.name as staffName, w.weightage FROM allocations a JOIN users u ON a.user_id = u.id LEFT JOIN weightage w ON a.kpi = w.kpi WHERE a.period = ?";
    const params = [period];

    if (branchId) {
      query += " AND a.branch_id = ?";
      params.push(branchId);
    }

    pool.query(query, params, (error, results) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      res.json(results);
    });
  }
});
