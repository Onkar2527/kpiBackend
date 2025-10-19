import express from 'express';
import { branches } from '../data.js';

// Router for branch related endpoints.

export const branchesRouter = express.Router();

// GET /branches
// Returns the list of branches.  No authentication is enforced
// here; in a real system you might restrict by role or scope.
branchesRouter.get('/', (_req, res) => {
  res.json(branches);
});