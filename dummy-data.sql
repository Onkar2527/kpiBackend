-- Clear existing data for the new branches
DELETE FROM targets WHERE branch_id IN ('B03', 'B04', 'B05');
DELETE FROM entries WHERE branch_id IN ('B03', 'B04', 'B05');
DELETE FROM allocations WHERE branch_id IN ('B03', 'B04', 'B05');

-- Insert new targets for the branches
INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES
('2025-09', 'B03', 'deposit', 25000000, 'published'), ('2025-09', 'B03', 'loan_gen', 20000000, 'published'), ('2025-09', 'B03', 'loan_amulya', 50, 'published'), ('2025-09', 'B03', 'insurance', 40000, 'published'), ('2025-09', 'B03', 'recovery', 2500000, 'published'), ('2025-09', 'B03', 'audit', 100, 'published'),
('2025-09', 'B04', 'deposit', 25000000, 'published'), ('2025-09', 'B04', 'loan_gen', 20000000, 'published'), ('2025-09', 'B04', 'loan_amulya', 50, 'published'), ('2025-09', 'B04', 'insurance', 40000, 'published'), ('2025-09', 'B04', 'recovery', 2500000, 'published'), ('2025-09', 'B04', 'audit', 100, 'published'),
('2025-09', 'B05', 'deposit', 25000000, 'published'), ('2025-09', 'B05', 'loan_gen', 20000000, 'published'), ('2025-09', 'B05', 'loan_amulya', 50, 'published'), ('2025-09', 'B05', 'insurance', 40000, 'published'), ('2025-09', 'B05', 'recovery', 2500000, 'published'), ('2025-09', 'B05', 'audit', 100, 'published');

-- Insert new entries for the branches
-- B03: Weightage Score between 5 and 10
INSERT INTO entries (id, period, branch_id, employee_id, kpi, value, status, account_no, date, verified_at) VALUES
('E31', '2025-09', 'B03', 'BM03', 'deposit', 50000000, 'Verified', 'ACC031', '2025-09-15', NOW()),
('E32', '2025-09', 'B03', 'BM03', 'loan_gen', 25000000, 'Verified', 'ACC032', '2025-09-15', NOW()),
('E33', '2025-09', 'B03', 'BM03', 'loan_amulya', 40, 'Verified', 'ACC033', '2025-09-15', NOW()),
('E34', '2025-09', 'B03', 'BM03', 'insurance', 20000, 'Verified', 'ACC034', '2025-09-15', NOW()),
('E35', '2025-09', 'B03', 'BM03', 'recovery', 1500000, 'Verified', 'ACC035', '2025-09-15', NOW()),
('E36', '2025-09', 'B03', 'BM03', 'audit', 70, 'Verified', 'ACC036', '2025-09-15', NOW());

-- B04: Weightage Score between 10 and 12.49
INSERT INTO entries (id, period, branch_id, employee_id, kpi, value, status, account_no, date, verified_at) VALUES
('E37', '2025-09', 'B04', 'BM04', 'deposit', 32000000, 'Verified', 'ACC037', '2025-09-15', NOW()),
('E38', '2025-09', 'B04', 'BM04', 'loan_gen', 25000000, 'Verified', 'ACC038', '2025-09-15', NOW()),
('E39', '2025-09', 'B04', 'BM04', 'loan_amulya', 70, 'Verified', 'ACC039', '2025-09-15', NOW()),
('E40', '2025-09', 'B04', 'BM04', 'insurance', 50000, 'Verified', 'ACC040', '2025-09-15', NOW()),
('E41', '2025-09', 'B04', 'BM04', 'recovery', 2000000, 'Verified', 'ACC041', '2025-09-15', NOW()),
('E42', '2025-09', 'B04', 'BM04', 'audit', 90, 'Verified', 'ACC042', '2025-09-15', NOW());

-- B05: Weightage Score >= 12.5
INSERT INTO entries (id, period, branch_id, employee_id, kpi, value, status, account_no, date, verified_at) VALUES
('E43', '2025-09', 'B05', 'BM05', 'deposit', 35000000, 'Verified', 'ACC043', '2025-09-15', NOW()),
('E44', '2025-09', 'B05', 'BM05', 'loan_gen', 30000000, 'Verified', 'ACC044', '2025-09-15', NOW()),
('E45', '2025-09', 'B05', 'BM05', 'loan_amulya', 70, 'Verified', 'ACC045', '2025-09-15', NOW()),
('E46', '2025-09', 'B05', 'BM05', 'insurance', 50000, 'Verified', 'ACC046', '2025-09-15', NOW()),
('E47', '2025-09', 'B05', 'BM05', 'recovery', 2500000, 'Verified', 'ACC047', '2025-09-15', NOW()),
('E48', '2025-09', 'B05', 'BM05', 'audit', 100, 'Verified', 'ACC048', '2025-09-15', NOW());
