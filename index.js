import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { auth } from './routes/auth.js';

dotenv.config();
import { branchesRouter } from './routes/branches.js';
import { targetsRouter } from './routes/targets.js';
import { allocationsRouter } from './routes/allocations.js';
import { entriesRouter } from './routes/entries.js';
import { summaryRouter } from './routes/summary.js';
import { mastersRouter } from './routes/masters.js';
import { kpisRouter } from './routes/kpis.js';
import periodsRouter from './routes/periods.js';
import { KpiRouter } from './routes/kpi_master.js';
import {performanceMasterRouter} from './routes/performanceMaster.js'
import {transferRouter} from './routes/transfer.js'

// Create and configure the Express app.  CORS is enabled for all
// origins to simplify local development.  The JSON body parser
// enables handling of POST bodies.
const app = express();
app.use(cors());
app.use(express.json());

// Health check endpoint for quick diagnostics.
app.get('/api/health', (_req, res) => res.json({ ok: true }));

// Mount routers.  Each router handles a logical subset of the
// application's REST API.  All endpoints are namespaced under /api.
app.use('/api/auth', auth);
app.use('/api/branches', branchesRouter);
app.use('/api/targets', targetsRouter);
app.use('/api/allocations', allocationsRouter);
app.use('/api/entries', entriesRouter);
app.use('/api/summary', summaryRouter);
app.use('/api/masters', mastersRouter);
app.use('/api/kpis', kpisRouter);
app.use('/api/periods', periodsRouter);
app.use('/api/kpi_master',KpiRouter);
app.use('/api/performnceMaster',performanceMasterRouter);
app.use('/api/trans',transferRouter);

// Start listening for HTTP requests on the configured port.
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`KPI server listening on http://localhost:${PORT}`);
});
