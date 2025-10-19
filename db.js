import mysql from 'mysql';

// Create a connection pool for the database.
// TODO: Replace the placeholder credentials with your actual database credentials.
// const pool = mysql.createPool({
//   connectionLimit: 10,
//   host: '20.197.10.226',
//   user: 'appuser4',
//   password: 'StrongP@ssw0rd!',
//   database: 'kpi_system_prod'
// });

const pool = mysql.createPool({
  connectionLimit: 10,
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'kpi_system_prod'
});

export default pool;
