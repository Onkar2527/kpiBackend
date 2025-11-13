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


//upload the loan_amulya and deposit and loan gen  and audit file
targetsRouter.post('/upload', upload.single('targetFile'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'targetFile required' });

  const results = [];
  Readable.from(req.file.buffer)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {

      let values = [];

      results.forEach(row => {
        let periodKey = Object.keys(row).find(k => k.trim().toLowerCase() === 'period');
        const period = (row[periodKey] || '').trim();

        Object.keys(row).forEach(key => {
          const cleanKey = key.trim(); 
          if (cleanKey && cleanKey !== 'branch_id' && cleanKey !== 'period') {
            const amount = row[key] === '' || row[key] == null ? 0 : row[key]; 
            values.push([period, row.branch_id, cleanKey, amount, 'published']);
          }
        });
      });

      
      results.forEach(row => {
        let periodKey = Object.keys(row).find(k => k.trim().toLowerCase() === 'period');
        const period = (row[periodKey] || '').trim();
        if (!Object.keys(row).some(k => k.trim() === 'audit')) {
          values.push([period, row.branch_id, 'audit', 100, 'published']);
        }
      });

      const branchIds = [...new Set(results.map(r => r.branch_id))];
      const placeholders = branchIds.map(() => '?').join(',');

      console.log('Parsed data preview:', results.slice(0, 3));

     
      const allKpis = [...new Set(values.map(v => v[2]))];
      const kpiPlaceholders = allKpis.map(() => '?').join(',');

      console.log('Deleting only KPIs:', allKpis);

     
      const deleteQuery = `
        DELETE FROM targets
        WHERE period = ? 
        AND branch_id IN (${placeholders}) 
        AND kpi IN (${kpiPlaceholders})
      `;

      pool.query(deleteQuery, [results[0].period, ...branchIds, ...allKpis], (error) => {
        if (error) {
          console.error('Delete error:', error);
          return res.status(500).json({ error: 'Internal server error' });
        }

       
        pool.query(
          'INSERT IGNORE INTO periods (period) VALUES (?)',
          [results[0].period],
          (error) => {
            if (error) console.error('Error inserting period:', error);
          }
        );

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
      });
    });
});


//upload the loan_amulya and deposit and loan gen  file
targetsRouter.post('/upload1', upload.single('targetFile'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'targetFile required' });

  const results = [];

  Readable.from(req.file.buffer)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {
      if (results.length === 0) {
        return res.status(400).json({ error: 'No data found in CSV' });
      }

      let values = [];

      results.forEach(row => {
        const normalized = {};
        for (const key in row) {
          if (!key) continue;
          const cleanKey = key.trim().toLowerCase();
          normalized[cleanKey] = (row[key] || '').toString().trim();
        }

        const period = normalized['period'];
        const branchId = normalized['branch_id'];
        if (!period || !branchId) return;

        Object.keys(normalized).forEach(kpi => {
          if (kpi !== 'period' && kpi !== 'branch_id' && kpi !== '') {
            const raw = normalized[kpi];
            const amount = raw === '' || raw == null ? 0 : parseFloat(raw) || 0;
            values.push([period, branchId, kpi, amount, 'published']);
          }
        });
      });

      console.log('Parsed normalized data (first 3 rows):');
      console.log(results.slice(0, 3));

      const firstPeriod = results[0]['period'] || results[0].period;
      const branchIds = [...new Set(results.map(r => r['branch_id'] || r.branch_id))];
      const placeholders = branchIds.map(() => '?').join(',');

    
      const allKpis = [
        ...new Set(
          values.map(v => v[2]) 
        )
      ];
      const kpiPlaceholders = allKpis.map(() => '?').join(',');

      console.log('Deleting only KPIs:', allKpis);

    
      const deleteQuery = `
        DELETE FROM targets 
        WHERE period = ? 
        AND branch_id IN (${placeholders}) 
        AND kpi IN (${kpiPlaceholders})
      `;

      pool.query(deleteQuery, [firstPeriod, ...branchIds, ...allKpis], (error) => {
        if (error) {
          console.error('Delete error:', error);
          return res.status(500).json({ error: 'Internal server error' });
        }

   
        pool.query(
          'INSERT IGNORE INTO periods (period) VALUES (?)',
          [firstPeriod],
          (error) => {
            if (error) console.error('Error inserting period:', error);
          }
        );

        pool.query(
          'INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES ?',
          [values],
          (error) => {
            if (error) {
              console.error('Insert error:', error);
              return res.status(500).json({ error: 'Internal server error' });
            }

            branchIds.forEach(branchId => {
              autoDistributeTargets(firstPeriod, branchId, (err) => {
                if (err)
                  console.error(`Error auto-distributing targets for branch ${branchId}:`, err);
              });
            });

            res.json({ ok: true, inserted: values.length, branches: branchIds });
          }
        );
      });
    });
});



//upload insurance file
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
      const allKpis = [...new Set(values.map(v => v[2]))];
      const kpiPlaceholders = allKpis.map(() => '?').join(',');
      const allBranchIds = [...new Set(values.map(v => v[1]))];
      const branchPlaceholders = allBranchIds.map(() => '?').join(',');
      pool.query(`DELETE FROM targets WHERE period = ? AND branch_id IN (${branchPlaceholders}) AND kpi IN (${kpiPlaceholders})`, [results[0].period, ...allBranchIds, ...allKpis], (error) => {
        if (error) return res.status(500).json({ error: 'Internal server error' });

        pool.query('INSERT IGNORE INTO periods (period) VALUES (?)', [results[0].period], (error) => {
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

//dashborad traget upload 
targetsRouter.post('/previousData', upload.single('prevoiustargetFile'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'prevoiustargetFile required' });

  const results = [];

  Readable.from(req.file.buffer)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {
      if (!results.length) {
        return res.status(400).json({ error: 'CSV file is empty or invalid.' });
      }


      const values = [];
      const branchIds = new Set();

      results.forEach(row => {
        let periodKey = Object.keys(row).find(k => k.trim().toLowerCase() === 'period');
        const period = (row[periodKey] || '').trim();

        const branch_id = (row.branch_id || '').trim();

        if (!period || !branch_id) return; 

        branchIds.add(branch_id);

        Object.keys(row).forEach(key => {
          const cleanKey = key.trim();
          if (cleanKey !== 'branch_id' && cleanKey.toLowerCase() !== 'period') {
            const amount = row[key] === '' || row[key] == null ? 0 : Number(row[key]);
            values.push([period, branch_id, cleanKey, amount]);
          }
        });
      });
     
      
      const branchIdArray = Array.from(branchIds);
      if (!branchIdArray.length) {
        return res.status(400).json({ error: 'No valid branch_id values found in CSV.' });
      }

      const branchPlaceholders = branchIdArray.map(() => '?').join(',');

  
      const deleteQuery = `
        DELETE FROM dashboard_table
        WHERE period = ?
        AND branch_id IN (${branchPlaceholders})
        AND kpi IN (?, ?)
      `;

      pool.query(deleteQuery, [results[0].period, ...branchIdArray, 'balance_deposit', 'loan_gen'], (error) => {
        if (error) {
          console.error(' Delete error:', error);
          return res.status(500).json({ error: 'Internal server error (delete)' });
        }

     
      

      
        pool.query(
          'INSERT INTO dashboard_table (period, branch_id, kpi, amount) VALUES ?',
          [values],
          (error) => {
            if (error) {
              console.error(' Insert error:', error);
              return res.status(500).json({ error: 'Internal server error (insert)' });
            }

            res.json({
              ok: true,
              inserted: values.length,
              deletedBranches: branchIdArray.length
            });
          }
        );
      });
    });
  });

  //dashborad total Achived upload 
  targetsRouter.post('/totalAchieved', upload.single('totalAchievedFile'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'totalAchievedFile required' });

  const results = [];

  Readable.from(req.file.buffer)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {
      if (!results.length) {
        return res.status(400).json({ error: 'CSV file is empty or invalid.' });
      }


      const values = [];
      const branchIds = new Set();

      results.forEach(row => {
        let periodKey = Object.keys(row).find(k => k.trim().toLowerCase() === 'period');
        const period = (row[periodKey] || '').trim();

        const branch_id = (row.branch_id || '').trim();

        if (!period || !branch_id) return; 

        branchIds.add(branch_id);

        Object.keys(row).forEach(key => {
          const cleanKey = key.trim();
          if (cleanKey !== 'branch_id' && cleanKey.toLowerCase() !== 'period') {
            const amount = row[key] === '' || row[key] == null ? 0 : Number(row[key]);
            values.push([period, branch_id, cleanKey, amount]);
          }
        });
      });
    
      
      const branchIdArray = Array.from(branchIds);
      if (!branchIdArray.length) {
        return res.status(400).json({ error: 'No valid branch_id values found in CSV.' });
      }

      const branchPlaceholders = branchIdArray.map(() => '?').join(',');

  
      const deleteQuery = `
        DELETE FROM dashboard_total_achiveved
        WHERE period = ?
        AND branch_id IN (${branchPlaceholders})
        AND kpi IN (?, ?)
      `;

      pool.query(deleteQuery, [results[0].period, ...branchIdArray, 'balance_deposit', 'loan_gen'], (error) => {
        if (error) {
          console.error(' Delete error:', error);
          return res.status(500).json({ error: 'Internal server error (delete)' });
        }


      
        pool.query(
          'INSERT INTO dashboard_total_achiveved (period, branch_id, kpi, amount) VALUES ?',
          [values],
          (error) => {
            if (error) {
              console.error(' Insert error:', error);
              return res.status(500).json({ error: 'Internal server error (insert)' });
            }

            res.json({
              ok: true,
              inserted: values.length,
              deletedBranches: branchIdArray.length
            });
          }
        );
      });
    });
  });

  //upload salary
targetsRouter.post("/uploadSalary", upload.single("salaryFile"), (req, res) => {
  if (!req.file)
    return res.status(400).json({ error: "salaryFile required" });

  const results = [];

  Readable.from(req.file.buffer)
    .pipe(csv())
    .on("data", (data) => results.push(data))
    .on("end", () => {
      if (!results.length)
        return res.status(400).json({ error: "CSV file is empty or invalid." });

      const values = [];
      const pfSet = new Set();

      results.forEach((row) => {
        
        const periodKey = Object.keys(row).find(k => k.trim().toLowerCase() === "period");
        const pfKey = Object.keys(row).find(k => k.trim().toLowerCase() === "pf_no");
        const salaryKey = Object.keys(row).find(k => k.trim().toLowerCase() === "salary");
        const incrementKey = Object.keys(row).find(k => k.trim().toLowerCase() === "increment");

        const period = (row[periodKey] || "").trim();
        const PF_NO = (row[pfKey] || "").trim();
        const salary = Number(row[salaryKey] || 0);
        const increment = Number(row[incrementKey] || 0);

        if (!period || !PF_NO) return; 

        pfSet.add(PF_NO);
        values.push([period, PF_NO, salary, increment]);
      });

      const pfArray = Array.from(pfSet);
      if (!pfArray.length)
        return res.status(400).json({ error: "No valid PF_NO values found in CSV." });

      const placeholders = pfArray.map(() => "?").join(",");
      const periodValue = results[0].period || values[0][0];

      
      const deleteQuery = `
        DELETE FROM base_salary
        WHERE period = ?
        AND PF_NO IN (${placeholders})
      `;
  console.log(deleteQuery);
  
      pool.query(deleteQuery, [periodValue, ...pfArray], (deleteErr) => {
        if (deleteErr) {
          console.error("Delete error:", deleteErr);
          return res.status(500).json({ error: "Internal server error (delete)" });
        }

        // Step 2: Insert new rows
        const insertQuery = `
          INSERT INTO base_salary (period, PF_NO, salary, increment)
          VALUES ?
        `;

        pool.query(insertQuery, [values], (insertErr) => {
          if (insertErr) {
            console.error("Insert error:", insertErr);
            return res.status(500).json({ error: "Internal server error (insert)" });
          }

          res.json({
            ok: true,
            message: "Salary data uploaded successfully",
            inserted: values.length,
            deleted: pfArray.length,
          });
        });
      });
    });
});
