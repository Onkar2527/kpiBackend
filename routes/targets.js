import express from 'express';
import multer from 'multer';
import csv from 'csv-parser';
import { Readable } from 'stream';
import pool from '../db.js';
import { autoDistributeTargets } from './allocations.js';
import { log } from 'console';

// Create the insurance_targets table if it doesn't exist
pool.query(`
  CREATE TABLE IF NOT EXISTS insurance_targets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    period VARCHAR(7) NOT NULL,
    kpi VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    state VARCHAR(50) NOT NULL
  )
`, (error) => {
  if (error) {
    console.error('Error creating insurance_targets table:', error);
  }
});

// Router implementing branch and insurance target endpoints.

export const targetsRouter = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

// POST /targets/upload
// Upload a CSV file with targets for a given period.
// targetsRouter.post('/upload', upload.single('targetFile'), (req, res) => {
//   if (!req.file) return res.status(400).json({ error: 'targetFile required' });

//   const results = [];
//   Readable.from(req.file.buffer)
//     .pipe(csv())
//     .on('data', (data) => results.push(data))
//     .on('end', () => {
//       const values = results.flatMap(row =>
//         Object.keys(row)
//           .filter(key => key !== 'branch_id' && key !== 'period')
//           .map(key => [row.period, row.branch_id, key, row[key], 'published'])
//       );
//       console.log(results);
      
//       const hasAudit = results.some(row => row.hasOwnProperty('audit'));
//       if (!hasAudit && results.length > 0) {
//         values.push([results[0].period, results[0].branch_id, 'audit', 100, 'published']);
//       }

//       pool.query('DELETE FROM targets WHERE period = ? AND branch_id = ? AND kpi IN (?, ?, ?)', [results[0].period, results[0].branch_id, 'audit', 'loan_amulya', 'loan_gen'], (error) => {
//         if (error) return res.status(500).json({ error: 'Internal server error' });

//         pool.query('INSERT INTO periods (period) VALUES (?)', [results[0].period], (error) => {
//           if (error) console.error('Error inserting period:', error);
//         });
//         pool.query('INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES ?', [values], (error) => {
//           if (error) return res.status(500).json({ error: 'Internal server error' });
          
//           const uniqueBranchIds = [...new Set(results.map(row => row.branch_id))];
//           console.log("gwvfs",uniqueBranchIds);
          
//           uniqueBranchIds.forEach(branchId => {
//             autoDistributeTargets(results[0].period, branchId, (err) => {
//               if (err) {
//                 console.error(`Error auto-distributing targets for branch ${branchId}:`, err);
//               }
//             });
//           });

//           res.json({ ok: true });
//         });
//       });
//     });
// });
targetsRouter.post('/upload', upload.single('targetFile'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'targetFile required' });

  const results = [];
  Readable.from(req.file.buffer)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {

      let values = [];

   
      results.forEach(row => {
        Object.keys(row).forEach(key => {
          const cleanKey = key.trim(); 
          if (cleanKey && cleanKey !== 'branch_id' && cleanKey !== 'period') {
            const amount = row[key] === '' || row[key] == null ? 0 : row[key]; 
            values.push([row.period, row.branch_id, cleanKey, amount, 'published']);
          }
        });
      });

     
      results.forEach(row => {
        if (!Object.keys(row).some(k => k.trim() === 'audit')) {
          values.push([row.period, row.branch_id, 'audit', 100, 'published']);
        }
      });

      const branchIds = [...new Set(results.map(r => r.branch_id))];
      const placeholders = branchIds.map(() => '?').join(',');

        console.log(results);
      
      pool.query(
        `DELETE FROM targets 
         WHERE period = ? 
         AND branch_id IN (${placeholders}) 
         AND kpi IN (?, ?, ?)`,
        [results[0].period, ...branchIds, 'audit', 'loan_amulya', 'loan_gen'],
        (error) => {
          if (error) {
            console.error('Delete error:', error);
            return res.status(500).json({ error: 'Internal server error' });
          }

        
          pool.query('INSERT IGNORE INTO periods (period) VALUES (?)', [results[0].period], (error) => {
            if (error) console.error('Error inserting period:', error);
          });

        
          pool.query(
            'INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES ?',
            [values],
            (error) => {
              if (error) {
                console.error('Insert error:', error);
                return res.status(500).json({ error: 'Internal server error' });
              }

             
              branchIds.forEach(branchId => {
                autoDistributeTargets(results[0].period, branchId, (err) => {
                  if (err) console.error(`Error auto-distributing targets for branch ${branchId}:`, err);
                });
              });

              res.json({ ok: true });
            }
          );
        }
      );
    });
});



targetsRouter.post('/upload-branch-specific', upload.single('targetFile'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'targetFile required' });

  const results = [];
  Readable.from(req.file.buffer)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {
      const values = results.flatMap(row =>
        Object.keys(row)
          .filter(key => key !== 'branch_id' && key !== 'period')
          .map(key => [row.period, row.branch_id, key, row[key], 'published'])
      );
      console.log(results);
      

      // const hasAudit = results.some(row => row.hasOwnProperty('audit'));
      // if (!hasAudit && results.length > 0) {
      //   values.push([results[0].period, results[0].branch_id, 'audit', 100, 'published']);
      // }

      pool.query('DELETE FROM targets WHERE period = ? AND branch_id = ? AND kpi IN (?, ?)', [results[0].period, results[0].branch_id, 'insurance', 'recovery'], (error) => {
        if (error) return res.status(500).json({ error: 'Internal server error' });

        pool.query('INSERT INTO periods (period) VALUES (?)', [results[0].period], (error) => {
          if (error) console.error('Error inserting period:', error);
        });
        pool.query('INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES ?', [values], (error) => {
          if (error) return res.status(500).json({ error: 'Internal server error' });
          
          const uniqueBranchIds = [...new Set(results.map(row => row.branch_id))];
          uniqueBranchIds.forEach(branchId => {
            autoDistributeTargets(results[0].period, branchId, (err) => {
              if (err) {
                console.error(`Error auto-distributing targets for branch ${branchId}:`, err);
              }
            });
          });

          res.json({ ok: true });
        });
      });
    });
});

targetsRouter.post('/upload-insurance-global', (req, res) => {
  const { period, csvData } = req.body;
  if (!period || !csvData) return res.status(400).json({ error: 'period and csvData required' });

  const lines = csvData.split('\n').slice(1); // Skip header
  const targets = lines.map(line => {
    const [amount] = line.split(',');
    return [period, 'insurance', amount, 'published'];
  });

  pool.query('INSERT INTO periods (period) VALUES (?)', [period], (error) => {
    if (error) console.error('Error inserting period:', error);
  });
  pool.query('INSERT INTO insurance_targets (period, kpi, amount, state) VALUES ?', [targets], (error) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json({ ok: true });
  });
});

// GET /targets
// Return targets for a branch and period.
targetsRouter.get('/', (req, res) => {
  const { period, branchId } = req.query;
  let query = 'SELECT * FROM targets WHERE 1 = 1';
  const params = [];

  if (period) {
    query += ' AND period = ?';
    params.push(period);
  }
  if (branchId) {
    query += ' AND branch_id = ?';
    params.push(branchId);
  }

  pool.query(query, params, (error, results) => {
    if (error) return res.status(500).json({ error: 'Internal server error' });
    res.json(results);
  });
});
