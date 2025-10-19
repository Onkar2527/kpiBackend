import bcrypt from 'bcryptjs';

const users = [
  { id: '50', name: 'ASHOK SIDDAPPA BAKALE', branch_id: '1' },
  { id: '633', name: 'PALLAVI ANIL MANE', branch_id: '1' },
  { id: '1153', name: 'NAVANATH MAHADEV KHOT', branch_id: '1' },
  { id: '1272', name: 'GIRIGOUDA SATYAGOUDA PATIL', branch_id: '1' },
  { id: '1352', name: 'NINGAPPA ROHIDAS HEGRE', branch_id: '1' },
  { id: '1684', name: 'ASHARANI VIRUPAXI BHIVASE', branch_id: '1' },
  { id: '1713', name: 'MAHANTESH KALLAPPA HITANE', branch_id: '1' },
  { id: '1803', name: 'SAGAR JAYANAND JADHAV', branch_id: '1' },
  { id: '1903', name: 'SHREEDEVI SUKADEV EXAMBE', branch_id: '1' },
  { id: '1904', name: 'SHILPA ANIL MALAGE', branch_id: '1' },
  { id: '2194', name: 'PARVATI LAXMAN JOLLE', branch_id: '1' },
  { id: '2215', name: 'HALASIDD SHANKAR KATTIKAR', branch_id: '1' },
  { id: '2344', name: 'SIDDAPPA MALAKARI YADRANVI', branch_id: '1' },
  { id: '308', name: 'RAVINDRA MAHADEV KAMATE', branch_id: '8' },
  { id: '1334', name: 'BALARAM LAXMAN SURANNAVR', branch_id: '8' },
  { id: '169', name: 'MAHADEVI PRAKASH KARADE', branch_id: '8' },
  { id: '2040', name: 'YASH RAJENDRA JUGALE', branch_id: '8' },
  { id: '2245', name: 'SUREKHA KRISHNA MALI', branch_id: '8' },
  { id: '2381', name: 'PRADNYA SANJAY BALIKAI', branch_id: '8' },
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
