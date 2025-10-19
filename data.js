// In‑memory data store and helpers for the KPI system backend.
//
// This module defines a simple in‑memory data model for demonstration
// purposes. A production implementation should replace this with a
// proper database layer.

import bcrypt from 'bcryptjs';

// A set of users with usernames, hashed passwords and roles.  Each user
// may optionally belong to a branch.  In a real system this data
// would be persisted in a database with password hashing and salting.
export const users = [
  {
    id: 'U01',
    username: 'hoadmin',
    name: 'HO Admin',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'HO',
    branchId: null
  },
  {
    id: 'U02',
    username: 'insadmin',
    name: 'Insurance Admin',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'INSURANCE',
    branchId: null
  },
  {
    id: 'U03',
    username: 'bm01',
    name: 'Pratik Mane',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'BM',
    branchId: 'B01'
  },
  {
    id: 'U04',
    username: 'bm02',
    name: 'Manasi Deshmukh',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'BM',
    branchId: 'B02'
  },
  {
    id: 'U05',
    username: 'asha',
    name: 'Asha Kulkarni',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'STAFF',
    branchId: 'B01'
  },
  {
    id: 'U06',
    username: 'rohan',
    name: 'Rohan Patil',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'STAFF',
    branchId: 'B01'
  },
  {
    id: 'U07',
    username: 'meera',
    name: 'Meera Desai',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'STAFF',
    branchId: 'B01'
  },
  {
    id: 'U08',
    username: 'amit',
    name: 'Amit Joshi',
    passwordHash: bcrypt.hashSync('password123', 10),
    role: 'STAFF',
    branchId: 'B02'
  }
];

// Branch definitions.  Branch IDs correspond to those used in the
// KPI seeds and allocations.  Add more branches as required.
export const branches = [
  { id: 'B01', code: 'SGL1', name: 'Sangli Main' },
  { id: 'B02', code: 'KOP1', name: 'Kolhapur City' }
];

// A simple in‑memory object keyed by period then branch.  Each KPI
// contains an amount and publication state.  The example data
// matches the demonstration from the specification: deposit,
// loan_gen, loan_amulya (count), insurance, recovery and audit.
export const targets = {
  '2025-09': {
    B01: {
      deposit: { amount: 25000000, state: 'published' },
      loan_gen: { amount: 20000000, state: 'published' },
      loan_amulya: { amount: 50, state: 'published' },
      insurance: { amount: 40000, state: 'published' },
      recovery: { amount: 2500000, state: 'published' },
      audit: { amount: 100, state: 'published' }
    },
    B02: {
      deposit: { amount: 12000000, state: 'published' },
      loan_gen: { amount: 9000000, state: 'published' },
      loan_amulya: { amount: 20, state: 'published' },
      insurance: { amount: 20000, state: 'published' },
      recovery: { amount: 900000, state: 'published' },
      audit: { amount: 40, state: 'published' }
    }
  }
};

// Published staff allocations by period and branch.  Targets for
// deposit, loan_gen, loan_amulya and recovery are split equally
// among staff.  Insurance and audit are branch‑level and therefore
// excluded from staff allocations.  Each allocation row has a
// publication state.
export const allocations = {
  '2025-09': {
    B01: {
      U05: { deposit: 8333334, loan_gen: 6666667, loan_amulya: 17, recovery: 833334, state: 'published' },
      U06: { deposit: 8333333, loan_gen: 6666667, loan_amulya: 17, recovery: 833333, state: 'published' },
      U07: { deposit: 8333333, loan_gen: 6666666, loan_amulya: 16, recovery: 833333, state: 'published' }
    },
    B02: {}
  }
};

// KPI entries list.  Each entry references a period, branch,
// employee and KPI code.  A status of Verified counts toward
// Achieved totals; Pending or Returned do not.  This sample data
// matches the demonstration totals for branch B01 in September 2025.
export const entries = [
  { id: 'E1', period: '2025-09', branchId: 'B01', employeeId: 'U05', kpi: 'deposit', accountNo: 'D-1001', value: 6000000, date: '2025-09-05', status: 'Verified' },
  { id: 'E2', period: '2025-09', branchId: 'B01', employeeId: 'U06', kpi: 'deposit', accountNo: 'D-1002', value: 4000000, date: '2025-09-05', status: 'Verified' },
  { id: 'E3', period: '2025-09', branchId: 'B01', employeeId: 'U07', kpi: 'deposit', accountNo: 'D-1003', value: 2500000, date: '2025-09-05', status: 'Verified' },
  { id: 'E4', period: '2025-09', branchId: 'B01', employeeId: 'U05', kpi: 'loan_gen', accountNo: 'L-2001', value: 2000000, date: '2025-09-05', status: 'Verified' },
  { id: 'E5', period: '2025-09', branchId: 'B01', employeeId: 'U06', kpi: 'loan_gen', accountNo: 'L-2002', value: 2000000, date: '2025-09-05', status: 'Verified' },
  { id: 'E6', period: '2025-09', branchId: 'B01', employeeId: 'U07', kpi: 'loan_gen', accountNo: 'L-2003', value: 1000000, date: '2025-09-05', status: 'Verified' },
  { id: 'E7', period: '2025-09', branchId: 'B01', employeeId: 'U05', kpi: 'loan_amulya', accountNo: 'A-3001', value: 7, date: '2025-09-05', status: 'Verified' },
  { id: 'E8', period: '2025-09', branchId: 'B01', employeeId: 'U06', kpi: 'loan_amulya', accountNo: 'A-3002', value: 5, date: '2025-09-05', status: 'Verified' },
  { id: 'E9', period: '2025-09', branchId: 'B01', employeeId: 'U07', kpi: 'loan_amulya', accountNo: 'A-3003', value: 3, date: '2025-09-05', status: 'Verified' },
  { id: 'E10', period: '2025-09', branchId: 'B01', employeeId: 'U05', kpi: 'insurance', accountNo: 'IN-4001', value: 30000, date: '2025-09-05', status: 'Verified' },
  { id: 'E11', period: '2025-09', branchId: 'B01', employeeId: 'U05', kpi: 'recovery', accountNo: 'R-5001', value: 500000, date: '2025-09-05', status: 'Verified' },
  { id: 'E12', period: '2025-09', branchId: 'B01', employeeId: 'U06', kpi: 'recovery', accountNo: 'R-5002', value: 400000, date: '2025-09-05', status: 'Verified' },
  { id: 'E13', period: '2025-09', branchId: 'B01', employeeId: 'U07', kpi: 'recovery', accountNo: 'R-5003', value: 300000, date: '2025-09-05', status: 'Verified' },
  { id: 'E14', period: '2025-09', branchId: 'B01', employeeId: 'U06', kpi: 'audit', accountNo: 'AU-6001', value: 20, date: '2025-09-05', status: 'Verified' },
  { id: 'E15', period: '2025-09', branchId: 'B01', employeeId: 'U07', kpi: 'audit', accountNo: 'AU-6002', value: 15, date: '2025-09-05', status: 'Verified' },
  { id: 'E16', period: '2025-09', branchId: 'B01', employeeId: 'U05', kpi: 'audit', accountNo: 'AU-6003', value: 15, date: '2025-09-05', status: 'Verified' }
];

// Helper to ensure an object exists at a given key.
export function ensure(obj, key, initFn) {
  if (!Object.prototype.hasOwnProperty.call(obj, key)) {
    obj[key] = initFn();
  }
  return obj[key];
}

// Helper to pick a subset of an object's properties.  Accepts an
// array of keys and returns a new object containing only those
// properties present on the source.
export function pick(obj, keys) {
  return keys.reduce((acc, k) => {
    if (Object.prototype.hasOwnProperty.call(obj, k)) acc[k] = obj[k];
    return acc;
  }, {});
}