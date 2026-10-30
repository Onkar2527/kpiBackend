import fs from 'fs';
import path from 'path';
import pool from './db.js';

let csvPath = 'C:\\Users\\Onkar Desai\\Downloads\\NEW STAFF LIST AS ON 24-09-2026\\NEW STAFF LIST AS ON 24-09-2026.csv';
if (!fs.existsSync(csvPath)) {
  csvPath = 'C:\\Users\\Onkar Desai\\Downloads\\NEW STAFF LIST AS ON 24-09-2026.csv';
}

function parseCSVLine(text) {
  if (!text) return [];
  const result = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char === '"') {
      if (inQuotes && text[i+1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      result.push(cur.trim());
      cur = '';
    } else {
      cur += char;
    }
  }
  result.push(cur.trim());
  return result;
}

function excelDateToMySQLDate(serial) {
  if (!serial || serial === 'NULL' || serial === 'null' || String(serial).trim() === '') {
    return null;
  }
  const s = String(serial).trim();
  let d;
  if (/^\d+(\.\d+)?$/.test(s)) {
    const num = parseFloat(s);
    d = new Date(Math.round((num - 25569) * 86400 * 1000));
    if (isNaN(d.getTime())) return null;
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  } else {
    d = new Date(s);
    if (isNaN(d.getTime())) return null;
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
}

function cleanVal(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  if (s === '' || s.toUpperCase() === 'NULL') return null;
  return s;
}

function cleanInt(v, defaultVal = null) {
  const cleaned = cleanVal(v);
  if (cleaned === null) return defaultVal;
  const num = parseInt(cleaned, 10);
  return isNaN(num) ? defaultVal : num;
}

async function importStaff() {
  console.log('Reading CSV file:', csvPath);
  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.split(/\r?\n/).filter(l => l.trim().length > 0);
  const headers = parseCSVLine(lines[0]);

  console.log('Headers:', headers);
  console.log('Total rows:', lines.length - 1);

  const records = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    const rowObj = {};
    headers.forEach((h, idx) => {
      rowObj[h] = cols[idx];
    });

    const id = cleanInt(rowObj.id);
    const username = cleanVal(rowObj.username) || String(id);
    const name = cleanVal(rowObj.name) || '';
    const password_hash = cleanVal(rowObj.password_hash) || username;
    const role = cleanVal(rowObj.role) || '';
    const branch_id = cleanVal(rowObj.branch_id);
    const department_id = cleanInt(rowObj.department_id);
    const transfered = cleanInt(rowObj.transfered, 0);
    const PF_NO = cleanInt(rowObj.PF_NO);
    const address = cleanVal(rowObj.address);
    const hod_id = cleanVal(rowObj.hod_id);
    const resign = cleanInt(rowObj.resign, 0);
    const resign_date = excelDateToMySQLDate(rowObj.resign_date);
    const transfer_date = excelDateToMySQLDate(rowObj.transfer_date);
    const user_add_date = excelDateToMySQLDate(rowObj.user_add_date);
    const period = cleanVal(rowObj.period);

    records.push([
      id,
      username,
      name,
      password_hash,
      role,
      branch_id,
      department_id,
      transfered,
      PF_NO,
      address,
      hod_id,
      resign,
      resign_date,
      transfer_date,
      user_add_date,
      period
    ]);
  }

  console.log(`Prepared ${records.length} records. Clearing table and starting database insertion...`);

  await new Promise((resolve, reject) => {
    pool.query('SET FOREIGN_KEY_CHECKS = 0; DELETE FROM users; SET FOREIGN_KEY_CHECKS = 1;', (err, res) => {
      if (err) return reject(err);
      console.log('Users table cleared successfully.');
      resolve(res);
    });
  });

  const sql = `
    INSERT INTO users (
      id, username, name, password_hash, role, branch_id, department_id,
      transfered, PF_NO, address, hod_id, resign, resign_date, transfer_date, user_add_date, period
    ) VALUES ?
  `;

  // Insert in batches of 500
  const batchSize = 500;
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    await new Promise((resolve, reject) => {
      pool.query(sql, [batch], (err, res) => {
        if (err) {
          console.error(`Error inserting batch ${i / batchSize + 1}:`, err);
          return reject(err);
        }
        console.log(`Batch ${i / batchSize + 1} (${batch.length} records) processed successfully.`);
        resolve(res);
      });
    });
  }

  console.log('\n--- VERIFICATION ---');
  pool.query('SELECT COUNT(*) as total_users FROM users', (err, resCount) => {
    if (err) console.error(err);
    else console.log('Total users in DB:', resCount[0].total_users);

    pool.query('SELECT period, COUNT(*) as count FROM users GROUP BY period', (err2, resPeriod) => {
      if (err2) console.error(err2);
      else console.log('Users breakdown by period:', resPeriod);

      pool.query('SELECT * FROM users ORDER BY id ASC LIMIT 5', (err3, sampleAsc) => {
        if (err3) console.error(err3);
        else console.log('First 5 users in DB:', sampleAsc);

        pool.query('SELECT * FROM users ORDER BY id DESC LIMIT 5', (err4, sampleDesc) => {
          if (err4) console.error(err4);
          else console.log('Last 5 users in DB:', sampleDesc);

          pool.end();
        });
      });
    });
  });
}

importStaff().catch(err => {
  console.error('Fatal error during import:', err);
  process.exit(1);
});
