import bcrypt from 'bcryptjs';

const users = [
  { id: '229', name: 'SANJEEV APPASAB EROOLE', branch_id: '45' },
  { id: '755', name: 'SHWETA SADANAND YANAPE', branch_id: '45' },
  { id: '1192', name: 'SUDHIR DAMODAR POTADAR', branch_id: '45' },
  { id: '1717', name: 'RUPALI SANTOSH MANE', branch_id: '45' },
  { id: '1921', name: 'SAVAKKA VIKAS GURAV', branch_id: '45' }
];

users.forEach(user => {
  const passwordHash = bcrypt.hashSync(user.id, 10);
  console.log(`INSERT INTO users (id, username, name, password_hash, role, branch_id, department_id) VALUES ('${user.id}', '${user.id}', '${user.name}', '${passwordHash}', 'STAFF', '${user.branch_id}', NULL);`);
});
