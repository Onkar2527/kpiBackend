import express from 'express';
import bcrypt from 'bcryptjs';
import pool from '../db.js';

export const auth = express.Router();

// POST /auth/login
// Expects JSON body with "username" and "password".  Returns a
// dummy token and the user profile if credentials match, or 401
// otherwise.
auth.post('/login', (req, res) => {
  const { username, password,period} = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ error: 'username and password required' });
  }
  pool.query('SELECT u.* ,b.name as branch_name FROM users u left join branches b on u.branch_id=b.code AND b.period = ? WHERE u.username = ? AND u.period = ? ', [period,username,period], (error, results) => {
    if (error) {
      console.error(error);
      return res.status(500).json({ error: 'Internal server error' });
    }
    if (results.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    const user = results[0];
    if (!bcrypt.compareSync(password, user.password_hash)) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    // For demo purposes we return a fixed token.  A real system
    // would generate a signed JWT including role and branch scopes.
    const token = 'dev-token-' + user.id;
    const { id, role, branch_id, name,username ,branch_name ,PF_NO,hod_id} = user;
    res.json({ token, user: { id, name, role, branchId: branch_id, username:username, branchName: branch_name ,PF_NO,hod_id} });
  });
});
