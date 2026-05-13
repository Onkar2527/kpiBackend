import mysql from 'mysql2';

// Create a connection pool for the database.
// TODO: Replace the placeholder credentials with your actual database credentials.

//Beereshwar Database
const pool = mysql.createPool({

  connectionLimit: 30,
  queueLimit: 0,
  waitForConnections: true,
  multipleStatements: true,

  acquireTimeout: 60000,
  connectTimeout: 60000,

  enableKeepAlive: true,
  keepAliveInitialDelay: 0,

  host: '3.110.60.145',
  user: 'root',
  password: 'Kred@Pool123',
  database: 'kpi_system_prod',

  timezone: "+05:30",
});

//local Database

// const pool = mysql.createPool({
//   connectionLimit: 10,
//   host: 'localhost',
//   user: 'root',
//   password: 'root',
//   database: 'kpi_demo',
//   timezone: "+05:30"
// });

//UAT Database
// const pool = mysql.createPool({
//   connectionLimit: 10,
//   host: '20.197.10.226',
//   user: 'appuser4',
//   password: 'StrongP@ssw0rd!',
//   database: 'kpi_system_uat',
//   timezone: "+05:30"
// });

export default pool;
