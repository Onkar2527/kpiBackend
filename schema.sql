CREATE TABLE departments (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE users (
  id VARCHAR(255) PRIMARY KEY,
  username VARCHAR(255) UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  role VARCHAR(255) NOT NULL,
  branch_id VARCHAR(255),
  department_id INT,
  FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE branches (
  id VARCHAR(255) PRIMARY KEY,
  code VARCHAR(255) UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL
);

CREATE TABLE targets (
  id INT PRIMARY KEY AUTO_INCREMENT,
  period VARCHAR(7) NOT NULL,
  branch_id VARCHAR(255) NOT NULL,
  kpi VARCHAR(255) NOT NULL,
  amount DECIMAL(15, 2) NOT NULL,
  state VARCHAR(255) NOT NULL
);

CREATE TABLE allocations (
  id INT PRIMARY KEY AUTO_INCREMENT,
  period VARCHAR(7) NOT NULL,
  branch_id VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  kpi VARCHAR(255) NOT NULL,
  amount DECIMAL(15, 2) NOT NULL,
  state VARCHAR(255) NOT NULL
);

CREATE TABLE entries (
  id VARCHAR(255) PRIMARY KEY,
  period VARCHAR(7) NOT NULL,
  branch_id VARCHAR(255) NOT NULL,
  employee_id VARCHAR(255) NOT NULL,
  kpi VARCHAR(255) NOT NULL,
  account_no VARCHAR(255),
  value DECIMAL(15, 2) NOT NULL,
  date DATE NOT NULL,
  status VARCHAR(255) NOT NULL,
  verified_at DATETIME
);

CREATE TABLE weightage (
  kpi VARCHAR(255) PRIMARY KEY,
  weightage INT
);

CREATE TABLE role_kpis (
  id INT PRIMARY KEY AUTO_INCREMENT,
  role VARCHAR(255) NOT NULL,
  kpi_name VARCHAR(255) NOT NULL,
  weightage INT NOT NULL,
  kpi_type VARCHAR(50) NOT NULL, -- 'manual' or 'target_based'
  UNIQUE KEY (role, kpi_name)
);

CREATE TABLE kpi_evaluations (
  id INT PRIMARY KEY AUTO_INCREMENT,
  period VARCHAR(7) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  role_kpi_id INT NOT NULL,
  score DECIMAL(5, 2) NOT NULL,
  evaluator_id VARCHAR(255) NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id),
  FOREIGN KEY (role_kpi_id) REFERENCES role_kpis(id),
  FOREIGN KEY (evaluator_id) REFERENCES users(id)
);

CREATE TABLE periods (
  id INT PRIMARY KEY AUTO_INCREMENT,
  period VARCHAR(7) NOT NULL
);
