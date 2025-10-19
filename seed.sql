-- Seed data for the KPI system database.

-- Users
INSERT INTO users (id, username, name, password_hash, role, branch_id) VALUES
('U01', 'hoadmin', 'HO Admin', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'HO', NULL),
('U02', 'insadmin', 'Insurance Admin', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'INSURANCE', NULL),
('U03', 'bm01', 'Pratik Mane', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'BM', 'B01'),
('U04', 'bm02', 'Manasi Deshmukh', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'BM', 'B02'),
('U05', 'asha', 'Asha Kulkarni', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'STAFF', 'B01'),
('U06', 'rohan', 'Rohan Patil', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'STAFF', 'B01'),
('U07', 'meera', 'Meera Desai', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'STAFF', 'B01'),
('U08', 'amit', 'Amit Joshi', '$2a$10$JPjZk7F3SoScNvD3NgXqKOHSS/.kI8op3qosbddLu/MyS8X4EsR9W', 'STAFF', 'B02');

-- Branches
INSERT INTO branches (id, code, name) VALUES
('B01', 'SGL1', 'Sangli Main'),
('B02', 'KOP1', 'Kolhapur City');

-- Targets
INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES
('2025-09', 'B01', 'deposit', 25000000, 'published'),
('2025-09', 'B01', 'loan_gen', 20000000, 'published'),
('2025-09', 'B01', 'loan_amulya', 50, 'published'),
('2025-09', 'B01', 'insurance', 40000, 'published'),
('2025-09', 'B01', 'recovery', 2500000, 'published'),
('2025-09', 'B01', 'audit', 100, 'published'),
('2025-09', 'B02', 'deposit', 12000000, 'published'),
('2025-09', 'B02', 'loan_gen', 9000000, 'published'),
('2025-09', 'B02', 'loan_amulya', 20, 'published'),
('2025-09', 'B02', 'insurance', 20000, 'published'),
('2025-09', 'B02', 'recovery', 900000, 'published'),
('2025-09', 'B02', 'audit', 40, 'published');

-- Allocations
INSERT INTO allocations (period, branch_id, user_id, kpi, amount, state) VALUES
('2025-09', 'B01', 'U05', 'deposit', 8333334, 'published'),
('2025-09', 'B01', 'U05', 'loan_gen', 6666667, 'published'),
('2025-09', 'B01', 'U05', 'loan_amulya', 17, 'published'),
('2025-09', 'B01', 'U05', 'recovery', 833334, 'published'),
('2025-09', 'B01', 'U06', 'deposit', 8333333, 'published'),
('2025-09', 'B01', 'U06', 'loan_gen', 6666667, 'published'),
('2025-09', 'B01', 'U06', 'loan_amulya', 17, 'published'),
('2025-09', 'B01', 'U06', 'recovery', 833333, 'published'),
('2025-09', 'B01', 'U07', 'deposit', 8333333, 'published'),
('2025-09', 'B01', 'U07', 'loan_gen', 6666666, 'published'),
('2025-09', 'B01', 'U07', 'loan_amulya', 16, 'published'),
('2025-09', 'B01', 'U07', 'recovery', 833333, 'published');

-- Entries
INSERT INTO entries (id, period, branch_id, employee_id, kpi, account_no, value, date, status) VALUES
('E1', '2025-09', 'B01', 'U05', 'deposit', 'D-1001', 6000000, '2025-09-05', 'Verified'),
('E2', '2025-09', 'B01', 'U06', 'deposit', 'D-1002', 4000000, '2025-09-05', 'Verified'),
('E3', '2025-09', 'B01', 'U07', 'deposit', 'D-1003', 2500000, '2025-09-05', 'Verified'),
('E4', '2025-09', 'B01', 'U05', 'loan_gen', 'L-2001', 2000000, '2025-09-05', 'Verified'),
('E5', '2025-09', 'B01', 'U06', 'loan_gen', 'L-2002', 2000000, '2025-09-05', 'Verified'),
('E6', '2025-09', 'B01', 'U07', 'loan_gen', 'L-2003', 1000000, '2025-09-05', 'Verified'),
('E7', '2025-09', 'B01', 'U05', 'loan_amulya', 'A-3001', 7, '2025-09-05', 'Verified'),
('E8', '2025-09', 'B01', 'U06', 'loan_amulya', 'A-3002', 5, '2025-09-05',. 'Verified'),
('E9', '2025-09', 'B01', 'U07', 'loan_amulya', 'A-3003', 3, '2025-09-05', 'Verified'),
('E10', '2025-09', 'B01', 'U05', 'insurance', 'IN-4001', 30000, '2025-09-05', 'Verified'),
('E11', '2025-09', 'B01', 'U05', 'recovery', 'R-5001', 500000, '2025-09-05', 'Verified'),
('E12', '2025-09', 'B01', 'U06', 'recovery', 'R-5002', 400000, '2025-09-05', 'Verified'),
('E13', '2025-09', 'B01', 'U07', 'recovery', 'R-5003', 300000, '2025-09-05', 'Verified'),
('E14', '2025-09', 'B01', 'U06', 'audit', 'AU-6001', 20, '2025-09-05', 'Pending'),
('E15', '2025-09', 'B01', 'U07', 'audit', 'AU-6002', 15, '2025-09-05', 'Pending'),
('E16', '2025-09', 'B01', 'U05', 'audit', 'AU-6003', 15, '2025-09-05', 'Pending');
