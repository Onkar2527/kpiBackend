import bcrypt from 'bcryptjs';

const users = [
  // KERUR BRANCH
  { id: '1187', name: 'NARASAPPA SHANKAR CHOUGALE', branch_id: '57' },
  { id: '516', name: 'VIMAL SUDHAKAR KORE', branch_id: '57' },
  { id: '708', name: 'SATISH MARUTI  KORE', branch_id: '57' },
  { id: '1255', name: 'PRASHANT RAOSAB BASSARGI', branch_id: '57' },
  { id: '1897', name: 'POOJA PUNDALIK BADAKAR', branch_id: '57' },
  { id: '2361', name: 'RAJASHREE BASAVARAJ KOKARE', branch_id: '57' },
  // DATTAWAD BRANCH
  { id: '365', name: 'SHAILA MAHESH SHIRAGAVE', branch_id: '119' },
  { id: '1604', name: 'RAHUL SEETARAM MALAGE', branch_id: '119' },
  { id: '1960', name: 'SANDIP PIRAPPA CHOUGULE', branch_id: '119' },
  { id: '2023', name: 'ARCHANA PRADEEP SIDNALE', branch_id: '119' },
  { id: '2287', name: 'NILAM PRAKASH PATIL', branch_id: '119' }
];

users.forEach(user => {
  const passwordHash = bcrypt.hashSync(user.id, 10);
  console.log(`INSERT INTO users (id, username, name, password_hash, role, branch_id, department_id) VALUES ('${user.id}', '${user.id}', '${user.name}', '${passwordHash}', 'STAFF', '${user.branch_id}', NULL);`);
});
