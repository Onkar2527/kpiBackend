-- Clear existing data for the new branches
DELETE FROM targets WHERE branch_id IN ('B02', 'B03', 'B04', 'B05');
DELETE FROM entries WHERE branch_id IN ('B02', 'B03', 'B04', 'B05');
DELETE FROM allocations WHERE branch_id IN ('B02', 'B03', 'B04', 'B05');

-- Clear existing data for the original branch
DELETE FROM targets WHERE period = '2025-09' AND branch_id = 'B01';
DELETE FROM entries WHERE period = '2025-09' AND branch_id = 'B01';
DELETE FROM allocations WHERE period = '2025-09' AND branch_id = 'B01';

-- Delete the new users and branches
DELETE FROM users WHERE id IN ('BM02', 'BM03', 'BM04', 'BM05');
DELETE FROM branches WHERE id IN ('B02', 'B03', 'B04', 'B05');
