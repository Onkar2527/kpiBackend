import express from "express";
import pool from "../db.js";

export const performanceMasterRouter = express.Router();
//get kpi role wise
performanceMasterRouter.post("/getKpiRoleWise", (req, res) => {
  const { role } = req.body;

  if (!role) {
    return res.status(400).json({ error: "Role is required" });
  }

  pool.getConnection((err, connection) => {
    if (err) {
      console.error("Connection error:", err);
      return res.status(500).json({ error: "Internal server error" });
    }

    const query = `
      SELECT r.id,k.kpi_name  FROM role_kpi_mapping r join kpi_master k on r.kpi_id = k.id WHERE role = ?
    `;

    connection.query(query, [role], (error, results) => {
      connection.release();

      if (error) {
        console.error("Query error:", error);
        return res.status(500).json({ error: "Database fetch failed" });
      }

      if (results.length === 0) {
        return res.status(404).json({ message: "No KPI found for this role" });
      }

      res.json({
        message: "KPI fetched successfully",
        data: results,
      });
    });
  });
});

//get specific staff data
performanceMasterRouter.get("/specfic-ALLstaff-scores", (req, res) => {
  const { period, ho_staff_id, role } = req.query;

  if (!period || !ho_staff_id || !role)
    return res
      .status(400)
      .json({ error: "period, ho_staff_id, and role are required" });

  const kpiWeightageQuery = `
      SELECT 
        rkm.id AS role_kpi_mapping_id,
        km.kpi_name,
        rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
    `;

  pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
    if (err) {
      console.error("Error fetching KPI weightage:", err);
      return res.status(500).json({ error: "Failed to fetch KPI weightage" });
    }

    if (!kpiWeightage.length)
      return res.status(404).json({ error: "No KPI found for this role" });

    // Fetch normal entries
    const userEntryQuery = `
        SELECT 
          role_kpi_mapping_id, 
          value AS achieved 
        FROM user_kpi_entry 
        WHERE period = ? AND user_id = ? AND deleted_at IS NULL
    `;

    pool.query(userEntryQuery, [period, ho_staff_id], (err2, userEntries) => {
      if (err2) {
        console.error("Error fetching user KPI entries:", err2);
        return res
          .status(500)
          .json({ error: "Failed to fetch user KPI entries" });
      }

      // Fetch INSURANCE value separately
      const insuranceQuery = `
        SELECT value AS achieved 
        FROM entries 
        WHERE kpi='insurance' AND employee_id = ?
      `;

      pool.query(insuranceQuery, [ho_staff_id], (err3, insuranceRows) => {
        if (err3) {
          console.error("Error fetching insurance value:", err3);
          return res
            .status(500)
            .json({ error: "Failed to fetch insurance value" });
        }

        const insuranceValue = insuranceRows.length
          ? Number(insuranceRows[0].achieved)
          : 0;

        const achievedMap = {};
        userEntries.forEach(
          (e) => (achievedMap[e.role_kpi_mapping_id] = e.achieved)
        );

        let totalWeightageScore = 0;
        const scores = {};

        kpiWeightage.forEach((row) => {
          const { role_kpi_mapping_id, kpi_name, weightage } = row;

          let achieved = 0;

          if (kpi_name.toLowerCase() === "insurance") {
            achieved = insuranceValue;
          } else {
            achieved = parseFloat(achievedMap[role_kpi_mapping_id]) || 0;
          }

          const target =
            kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

          let score = 0;
          if (achieved > 0) {
            const ratio = achieved / target;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;
          }

          let weightageScore = (score * weightage) / 100;

          if (kpi_name.toLowerCase() === "insurance" && score === 0) {
            weightageScore = -2;
          }

          totalWeightageScore += weightageScore;

          scores[kpi_name] = {
            score: Number(score.toFixed(2)),
            achieved,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        });

        scores.total = Number(totalWeightageScore.toFixed(2));

        return res.json(scores);
      });
    });
  });
});

//Reusable function to calculate KPI score for one ho_staff
// function calculateStaffScores(period, staffId, role) {
//   return new Promise((resolve, reject) => {
//     const kpiWeightageQuery = `
//       SELECT rkm.id AS role_kpi_mapping_id, km.kpi_name, rkm.weightage
//       FROM role_kpi_mapping rkm
//       JOIN kpi_master km ON km.id = rkm.kpi_id
//       WHERE rkm.role = ? AND rkm.deleted_at IS NULL
//     `;

//     pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
//       if (err) return reject("Failed to fetch KPI weightage");

//       const userEntryQuery = `
//         SELECT role_kpi_mapping_id, value AS achieved
//         FROM user_kpi_entry
//         WHERE period = ? AND user_id = ? AND deleted_at IS NULL
//       `;

//       pool.query(userEntryQuery, [period, staffId], (err2, userEntries) => {
//         if (err2) return reject("Failed to fetch user KPI entries");

//         const achievedMap = {};
//         userEntries.forEach(
//           (e) => (achievedMap[e.role_kpi_mapping_id] = e.achieved)
//         );

//         let totalWeightageScore = 0;
//         const scores = {};

//         kpiWeightage.forEach((row) => {
//           const { role_kpi_mapping_id, kpi_name, weightage } = row;
//           const achieved =
//             parseFloat(achievedMap[row.role_kpi_mapping_id]) || 0;
//           const target =
//             kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

//           let score = 0;
//           if (achieved > 0) {
//             const ratio = achieved / target;
//             if (ratio <= 1) score = ratio * 10;
//             else if (ratio < 1.25) score = 10;
//             else score = 12.5;
//           }

//           let weightageScore = (score * weightage) / 100;
//           if (kpi_name.toLowerCase() === "insurance" && score === 0)
//             weightageScore = -2;

//           totalWeightageScore += weightageScore;

//           scores[kpi_name] = {
//             score: Number(score.toFixed(2)),
//             achieved,
//             weightage,
//             weightageScore: Number(weightageScore.toFixed(2)),
//           };
//         });

//         scores.total = Number(totalWeightageScore.toFixed(2));

//         resolve(scores);
//       });
//     });
//   });
// }

async function calculateStaffScores(period, staffId, role) {
  try {
    //  Fetch KPI weightage
    const kpiWeightage = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT rkm.id AS role_kpi_mapping_id, km.kpi_name, rkm.weightage
        FROM role_kpi_mapping rkm
        JOIN kpi_master km ON km.id = rkm.kpi_id
        WHERE rkm.role = ? AND rkm.deleted_at IS NULL
        `,
        [role],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        }
      );
    });

    //  Fetch user KPI entries
    const userEntries = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT role_kpi_mapping_id, value AS achieved
        FROM user_kpi_entry
        WHERE period = ? AND user_id = ? AND deleted_at IS NULL
        `,
        [period, staffId],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        }
      );
    });

    // Map achieved values
    const achievedMap = {};
    userEntries.forEach(
      (e) => (achievedMap[e.role_kpi_mapping_id] = Number(e.achieved || 0))
    );

    let totalWeightageScore = 0;
    const scores = {};

    //  SAME scoring logic
    for (const row of kpiWeightage) {
      const { role_kpi_mapping_id, kpi_name, weightage } = row;
      const achieved = achievedMap[role_kpi_mapping_id] || 0;
      const target = kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

      let score = 0;
      if (achieved > 0) {
        const ratio = achieved / target;
        if (ratio <= 1) score = ratio * 10;
        else if (ratio < 1.25) score = 10;
        else score = 12.5;
      }

      let weightageScore = (score * weightage) / 100;
      if (kpi_name.toLowerCase() === "insurance" && score === 0)
        weightageScore = -2;

      totalWeightageScore += weightageScore;

      scores[kpi_name] = {
        score: Number(score.toFixed(2)),
        achieved,
        weightage,
        weightageScore: Number(weightageScore.toFixed(2)),
      };
    }

    scores.total = Number(totalWeightageScore.toFixed(2));
    return scores;
  } catch (err) {
    throw err;
  }
}

async function calculateBMScores(period, branchId) {
  try {
    const results = await new Promise((resolve, reject) => {
      const query = `
        SELECT
            k.kpi,
            CASE WHEN k.kpi = 'audit' THEN 100 ELSE t.amount END AS target,
            w.weightage,
            e.total_achieved AS achieved
        FROM
            (
                SELECT 'deposit' as kpi UNION 
                SELECT 'loan_gen' UNION 
                SELECT 'loan_amulya' UNION 
                SELECT 'recovery' UNION 
                SELECT 'audit' UNION 
                SELECT 'insurance'
            ) k
        LEFT JOIN targets t 
          ON k.kpi = t.kpi AND t.period = ? AND t.branch_id = ?
        LEFT JOIN (
            SELECT kpi, SUM(value) AS total_achieved 
            FROM entries 
            WHERE period = ? AND branch_id = ? AND status = 'Verified'
            GROUP BY kpi
        ) e ON k.kpi = e.kpi
        LEFT JOIN weightage w ON k.kpi = w.kpi
      `;

      pool.query(query, [period, branchId, period, branchId], (err, rows) => {
        if (err) return reject(err);
        resolve(rows);
      });
    });

    const [bmRow, insuranceRow] = await Promise.all([
      // BM ID
      new Promise((resolve, reject) => {
        pool.query(
          `SELECT id FROM users WHERE branch_id=? AND role='BM' LIMIT 1`,
          [branchId],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows?.[0]?.id || 0);
          }
        );
      }),

      // Insurance achieved (BM specific)
      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT SUM(value) AS achieved
          FROM entries
          WHERE period=? AND kpi='insurance'
            AND employee_id = (
              SELECT id FROM users WHERE branch_id=? AND role='BM' LIMIT 1
            )
          `,
          [period, branchId],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows?.[0]?.achieved || 0);
          }
        );
      }),
    ]);

    results.forEach((row) => {
      if (row.kpi === "insurance") {
        row.achieved = insuranceRow;
      }
    });

    const bmKpis = [
      "deposit",
      "loan_gen",
      "loan_amulya",
      "recovery",
      "audit",
      "insurance",
    ];

    const calculateScores = (cap) => {
      const scores = {};
      bmKpis.forEach((kpi) => {
        scores[kpi] = {
          score: 0,
          target: 0,
          achieved: 0,
          weightage: 0,
          weightageScore: 0,
        };
      });

      let totalWeightageScore = 0;

      results.forEach((row) => {
        if (!bmKpis.includes(row.kpi)) return;

        let outOf10 = 0;
        const target = row.target || 0;
        const achieved = row.achieved || 0;
        const weightage = row.weightage || 0;

        if (!target) {
          scores[row.kpi] = {
            score: 0,
            target: 0,
            achieved,
            weightage,
            weightageScore: 0,
          };
          return;
        }

        const ratio = achieved / target;

        switch (row.kpi) {
          case "deposit":
          case "loan_gen":
            if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else outOf10 = 12.5;
            break;

          case "loan_amulya":
            if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else outOf10 = 12.5;
            break;

          case "insurance":
            if (ratio === 0) outOf10 = -2;
            else if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else outOf10 = 12.5;
            break;

          case "recovery":
          case "audit":
            if (ratio <= 1) outOf10 = ratio * 10;
            else outOf10 = 12.5;
            break;
        }

        outOf10 = Math.max(0, Math.min(cap, isNaN(outOf10) ? 0 : outOf10));

        const weightageScore =
          row.kpi === "insurance" && ratio === 0
            ? -2
            : (outOf10 * weightage) / 100;

        scores[row.kpi] = {
          score: outOf10,
          target,
          achieved,
          weightage,
          weightageScore,
        };

        totalWeightageScore += weightageScore;
      });

      scores.total = totalWeightageScore;
      return scores;
    };

    const preliminaryScores = calculateScores(12.5);
    const insuranceScore = preliminaryScores.insurance?.score || 0;
    const recoveryScore = preliminaryScores.recovery?.score || 0;

    const cap =
      preliminaryScores.total > 10 &&
      insuranceScore < 7.5 &&
      recoveryScore < 7.5
        ? 10
        : 12.5;

    const finalScores = calculateScores(cap);

    return finalScores;
  } catch (err) {
    throw err;
  }
}

// function calculateBMScores(period, branchId) {
//   return new Promise((resolve, reject) => {
//     const query = `
//       SELECT
//           k.kpi,
//           CASE WHEN k.kpi = 'audit' THEN 100 ELSE t.amount END AS target,
//           w.weightage,
//           e.total_achieved AS achieved
//       FROM
//           (
//               SELECT 'deposit' as kpi UNION
//               SELECT 'loan_gen' UNION
//               SELECT 'loan_amulya' UNION
//               SELECT 'recovery' UNION
//               SELECT 'audit' UNION
//               SELECT 'insurance'
//           ) as k
//       LEFT JOIN targets t ON k.kpi = t.kpi AND t.period = ? AND t.branch_id = ?
//       LEFT JOIN (
//           SELECT kpi, SUM(value) AS total_achieved
//           FROM entries
//           WHERE period = ? AND branch_id = ? AND status = 'Verified'
//           GROUP BY kpi
//       ) e ON k.kpi = e.kpi
//       LEFT JOIN weightage w ON k.kpi = w.kpi
//     `;

//     pool.query(query, [period, branchId, period, branchId], (err, results) => {
//       if (err) return reject(err);

//       pool.query(
//         `SELECT id FROM users WHERE branch_id=? AND role='BM'`,
//         [branchId],
//         (err2, bmRows) => {
//           if (err2) return reject(err2);

//           const BM = bmRows?.[0]?.id || 0;

//           pool.query(
//             `
//             SELECT SUM(value) AS achieved
//             FROM entries
//             WHERE period=? AND employee_id=? AND kpi='insurance'
//           `,
//             [period, BM],
//             (err3, insRows) => {
//               if (err3) return reject(err3);

//               const insuranceAchieved = insRows?.[0]?.achieved || 0;

//               // SAME mapping logic
//               results = results.map((row) => {
//                 if (row.kpi === "insurance") {
//                   row.achieved = insuranceAchieved;
//                 }
//                 return row;
//               });

//               const bmKpis = [
//                 "deposit",
//                 "loan_gen",
//                 "loan_amulya",
//                 "recovery",
//                 "audit",
//                 "insurance",
//               ];

//               const calculateScores = (cap) => {
//                 const scores = {};
//                 bmKpis.forEach((kpi) => {
//                   scores[kpi] = {
//                     score: 0,
//                     target: 0,
//                     achieved: 0,
//                     weightage: 0,
//                     weightageScore: 0,
//                   };
//                 });

//                 let totalWeightageScore = 0;

//                 results.forEach((row) => {
//                   if (!bmKpis.includes(row.kpi)) return;

//                   let outOf10 = 0;

//                   if (!row.target || row.target === 0) {
//                     scores[row.kpi] = {
//                       score: 0,
//                       target: 0,
//                       achieved: row.achieved || 0,
//                       weightage: row.weightage || 0,
//                       weightageScore: 0,
//                     };
//                     return;
//                   }

//                   const ratio = row.achieved / row.target;
//                   const auditRatio = row.kpi === "audit" ? ratio : 0;
//                   const recoveryRatio = row.kpi === "recovery" ? ratio : 0;

//                   switch (row.kpi) {
//                     case "deposit":
//                     case "loan_gen":
//                       if (ratio <= 1) outOf10 = ratio * 10;
//                       else if (ratio < 1.25) outOf10 = 10;
//                       else if (auditRatio >= 0.75 && recoveryRatio >= 0.75)
//                         outOf10 = 12.5;
//                       else outOf10 = 10;
//                       break;

//                     case "loan_amulya":
//                       if (ratio <= 1) outOf10 = ratio * 10;
//                       else if (ratio < 1.25) outOf10 = 10;
//                       else outOf10 = 12.5;
//                       break;

//                     case "insurance":
//                       if (ratio === 0) outOf10 = -2;
//                       else if (ratio <= 1) outOf10 = ratio * 10;
//                       else if (ratio < 1.25) outOf10 = 10;
//                       else outOf10 = 12.5;
//                       break;

//                     case "recovery":
//                     case "audit":
//                       if (ratio <= 1) outOf10 = ratio * 10;
//                       else outOf10 = 12.5;
//                       break;
//                   }

//                   outOf10 = Math.max(
//                     0,
//                     Math.min(cap, isNaN(outOf10) ? 0 : outOf10)
//                   );

//                   const weightageScore =
//                     row.kpi === "insurance" && ratio === 0
//                       ? -2
//                       : (outOf10 * (row.weightage || 0)) / 100;

//                   scores[row.kpi] = {
//                     score: outOf10,
//                     target: row.target,
//                     achieved: row.achieved || 0,
//                     weightage: row.weightage || 0,
//                     weightageScore,
//                   };

//                   totalWeightageScore += weightageScore;
//                 });

//                 scores.total = totalWeightageScore;
//                 return scores;
//               };

//               // SAME cap logic
//               const preliminaryScores = calculateScores(12.5);
//               const insuranceScore = preliminaryScores.insurance?.score || 0;
//               const recoveryScore = preliminaryScores.recovery?.score || 0;

//               const cap =
//                 preliminaryScores.total > 10 &&
//                 insuranceScore < 7.5 &&
//                 recoveryScore < 7.5
//                   ? 10
//                   : 12.5;

//               const finalScores = calculateScores(cap);

//               resolve(finalScores);
//             }
//           );
//         }
//       );
//     });
//   });
// }

// simple agm
// performanceMasterRouter.get("/ho-Allhod-scores", (req, res) => {
//   const { period, hod_id, role } = req.query;

//   if (!period || !hod_id) {
//     return res.status(400).json({ error: "period and hod_id are required" });
//   }

//   const hodKpiQuery = `
//     SELECT k.kpi_name, r.id AS role_kpi_mapping_id, r.weightage
//     FROM role_kpi_mapping r
//     JOIN kpi_master k ON r.kpi_id = k.id
//     WHERE r.role = ?  `;

//   pool.query(hodKpiQuery, [role], (err, hodKpis) => {
//     if (err) return res.status(500).json({ error: "Failed to fetch KPI list" });
//     if (!hodKpis.length)
//       return res.status(404).json({ error: "No KPIs for AGM role" });

//     const staffQuery = `SELECT id FROM users WHERE hod_id = ?`;

//     pool.query(staffQuery, [hod_id], async (err2, staffList) => {
//       if (err2)
//         return res.status(500).json({ error: "Failed to fetch HO staff" });

//       const staffIds = staffList.map((s) => s.id);

//       let staffScores = [];
//       for (const sid of staffIds) {
//         try {
//           const scoreObj = await calculateStaffScores(period, sid, "HO_STAFF");
//           staffScores.push(scoreObj);
//         } catch (e) {
//           staffScores.push({ total: 0 });
//         }
//       }

//       const totalScores = staffScores.map((s) => s.total || 0);

//       const avgStaffScore =
//         totalScores.reduce((sum, val) => sum + val, 0) / totalScores.length;

//       const fixedAvg = Number(avgStaffScore.toFixed(2));

//       let finalKpis = {};

//       //second kpi logic

//       const branchCodes = await new Promise((resolve, reject) => {
//         pool.query(
//           "SELECT code FROM branches WHERE incharge_id = ?",
//           [hod_id],
//           (err, rows) => {
//             if (err) return reject(err);
//             resolve(rows.map((r) => r.code));
//           }
//         );
//       });

//       let branchWiseAverage = {};

//       for (const code of branchCodes) {
//         const bmScore = await calculateBMScores(period, code);
//         const totals = bmScore.total || 0;
//         branchWiseAverage[code] = Number(totals.toFixed(2));

//       }
//       const branchValues = Object.values(branchWiseAverage);

//       const branchTotal = branchValues.reduce((sum, val) => sum + val, 0);

//       const branchAvgScore = Number(
//         (branchTotal / branchValues.length).toFixed(2)
//       );

//       const kpiMap = {};
//       hodKpis.forEach((k) => {
//         kpiMap[k.kpi_name] = {
//           id: k.role_kpi_mapping_id,
//           weightage: k.weightage,
//         };
//       });

//       //insurance kpi calaculation
//       const insuranceValue = await new Promise((resolve, reject) => {
//         if (!kpiMap["insurance"]) return resolve(0);
//         pool.query(
//           "SELECT value FROM entries WHERE kpi='insurance' and employee_id = ? and period = ? ",
//           [hod_id, period],
//           (err, rows) => {
//             if (err) return reject(err);
//             resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//           }
//         );
//       });
//       //HO Building Cleanliness
//       const Cleanliness = await new Promise((resolve, reject) => {
//         if (!kpiMap["HO Building Cleanliness"]) return resolve(0);
//         pool.query(
//           "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//           [kpiMap["HO Building Cleanliness"].id, hod_id, period],
//           (err, rows) => {
//             if (err) return reject(err);
//             resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//           }
//         );
//       });

//       //Management  Discretion
//       const Management = await new Promise((resolve, reject) => {
//         if (!kpiMap["Management  Discretion"]) return resolve(0);
//         pool.query(
//           "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//           [kpiMap["Management  Discretion"].id, hod_id, period],
//           (err, rows) => {
//             if (err) return reject(err);
//             resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//           }
//         );
//       });

//       //Internal Audit performance
//       const InternalAudit = await new Promise((resolve, reject) => {
//         if (!kpiMap["Internal Audit performance"]) return resolve(0);

//         pool.query(
//           "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//           [kpiMap["Internal Audit performance"].id, hod_id, period],
//           (err, rows) => {
//             if (err) return reject(err);
//             resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//           }
//         );
//       });

//       //IT
//       const IT = await new Promise((resolve, reject) => {
//         if (!kpiMap["IT"]) return resolve(0);
//         pool.query(
//           "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//           [kpiMap["IT"].id, hod_id, period],
//           (err, rows) => {
//             if (err) return reject(err);
//             resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//           }
//         );
//       });

//       //Insurance Business Development
//       const InsuranceBusinessDevelopment = await new Promise(
//         (resolve, reject) => {
//           if (!kpiMap["Insurance Business Development"]) return resolve(0);
//           pool.query(
//             "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//             [kpiMap["Insurance Business Development"].id, hod_id, period],
//             (err, rows) => {
//               if (err) return reject(err);
//               resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//             }
//           );
//         }
//       );

//       let Total = 0;

//       for (const kpi of hodKpis) {
//         const name = kpi.kpi_name;
//         const weightage = kpi.weightage;

//         let avg = 0;
//         let result = 0;
//         let weightageScore = 0;

//         if (name.toLowerCase().includes("section")) {
//           avg = fixedAvg;
//         } else if (
//           name.toLowerCase().includes("branch") ||
//           name.toLowerCase().includes("visits")
//         ) {
//           avg = branchAvgScore;
//         } else if (name.toLowerCase().includes("clean")) {
//           avg = Cleanliness;
//         } else if (name.toLowerCase().includes("management")) {
//           avg = Management;
//         } else if (
//           name.toLowerCase().includes("audit") &&
//           !name.toLowerCase().includes("internal")
//         ) {
//           avg = InternalAudit;
//         } else if (name.toLowerCase().includes("internal audit")) {
//           avg = InternalAudit;
//         } else if (name.toLowerCase().includes("it")) {
//           avg = IT;
//         } else if (name.toLowerCase().includes("business development")) {
//           avg = InsuranceBusinessDevelopment;
//         } else if (name.toLowerCase().includes("insurance")) {
//           const target = 40000;
//           avg = insuranceValue;

//           if (avg <= target) result = (avg / target) * 10;
//           else if (avg / target < 1.25) result = 10;
//           else result = 12.5;

//           weightageScore = avg === 0 ? -2 : (result / 100) * weightage;

//           finalKpis[name] = {
//             score: Number(result.toFixed(2)),
//             achieved: avg || 0,
//             weightage,
//             weightageScore: Number(weightageScore.toFixed(2)),
//           };

//           Total += weightageScore;
//           continue;
//         }

//         if (avg === 0) result = 0;
//         else if (avg <= weightage) result = (avg / weightage) * 10;
//         else if (avg / weightage < 1.25) result = 12.5;
//         else result = 0;

//         weightageScore = (result / 100) * weightage;
//         Total += weightageScore;

//         finalKpis[name] = {
//           score: Number(result.toFixed(2)),
//           achieved: avg || 0,
//           weightage,
//           weightageScore: Number(weightageScore.toFixed(2)),
//         };
//       }

//       res.json({
//         hod_id,
//         period,
//         kpis: finalKpis,
//         total: Total,
//       });
//     });
//   });
// });

performanceMasterRouter.get("/ho-Allhod-scores", async (req, res) => {
  const { period, hod_id, role } = req.query;

  if (!period || !hod_id) {
    return res.status(400).json({ error: "period and hod_id are required" });
  }

  try {
    const hodKpis = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT k.kpi_name, r.id AS role_kpi_mapping_id, r.weightage
        FROM role_kpi_mapping r
        JOIN kpi_master k ON r.kpi_id = k.id
        WHERE r.role = ?
        `,
        [role],
        (err, rows) => {
          if (err) return reject(err);
          if (!rows.length) return reject("No KPIs found");
          resolve(rows);
        }
      );
    });

    const staffIds = await new Promise((resolve, reject) => {
      pool.query(
        `SELECT id FROM users WHERE hod_id = ?`,
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows.map((r) => r.id));
        }
      );
    });

    const staffScores = await Promise.all(
      staffIds.map((id) =>
        calculateStaffScores(period, id, "HO_STAFF").catch(() => ({ total: 0 }))
      )
    );

    const fixedAvg = Number(
      (
        staffScores.reduce((s, v) => s + (v.total || 0), 0) /
        (staffScores.length || 1)
      ).toFixed(2)
    );

    const branchCodes = await new Promise((resolve, reject) => {
      pool.query(
        `SELECT code FROM branches WHERE incharge_id = ?`,
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows.map((r) => r.code));
        }
      );
    });

    const branchTotals = await Promise.all(
      branchCodes.map((code) =>
        calculateBMScores(period, code)
          .then((r) => Number((r.total || 0).toFixed(2)))
          .catch(() => 0)
      )
    );

    const branchAvgScore = Number(
      (
        branchTotals.reduce((s, v) => s + v, 0) / (branchTotals.length || 1)
      ).toFixed(2)
    );

    const kpiMap = {};
    hodKpis.forEach((k) => {
      kpiMap[k.kpi_name] = {
        id: k.role_kpi_mapping_id,
        weightage: k.weightage,
      };
    });

    const userKpiValues = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT role_kpi_mapping_id, SUM(value) AS total
        FROM user_kpi_entry
        WHERE user_id=? AND period=?
        GROUP BY role_kpi_mapping_id
        `,
        [hod_id, period],
        (err, rows) => {
          if (err) return reject(err);
          const map = {};
          rows.forEach(
            (r) => (map[r.role_kpi_mapping_id] = Number(r.total || 0))
          );
          resolve(map);
        }
      );
    });

    const insuranceValue = await new Promise((resolve, reject) => {
      if (!kpiMap["insurance"]) return resolve(0);
      pool.query(
        `
        SELECT SUM(value) AS total
        FROM entries
        WHERE kpi='insurance' AND employee_id=? AND period=?
        `,
        [hod_id, period],
        (err, rows) => {
          if (err) return reject(err);
          resolve(Number(rows[0]?.total || 0));
        }
      );
    });

    const getVal = (name) => userKpiValues[kpiMap[name]?.id] || 0;

    const Cleanliness = getVal("HO Building Cleanliness");
    const Management = getVal("Management  Discretion");
    const InternalAudit = getVal("Internal Audit performance");
    const IT = getVal("IT");
    const InsuranceBusinessDevelopment = getVal(
      "Insurance Business Development"
    );

    let finalKpis = {};
    let Total = 0;

    for (const kpi of hodKpis) {
      const name = kpi.kpi_name;
      const weightage = kpi.weightage;

      let avg = 0;
      let result = 0;
      let weightageScore = 0;

      if (name.toLowerCase().includes("section")) avg = fixedAvg;
      else if (
        name.toLowerCase().includes("branch") ||
        name.toLowerCase().includes("visits")
      )
        avg = branchAvgScore;
      else if (name.toLowerCase().includes("clean")) avg = Cleanliness;
      else if (name.toLowerCase().includes("management")) avg = Management;
      else if (name.toLowerCase().includes("audit")) avg = InternalAudit;
      else if (name.toLowerCase().includes("it")) avg = IT;
      else if (name.toLowerCase().includes("business development"))
        avg = InsuranceBusinessDevelopment;

      // INSURANCE SPECIAL
      if (name.toLowerCase().includes("insurance")) {
        const target = 40000;
        avg = insuranceValue;

        if (avg <= target) result = (avg / target) * 10;
        else if (avg / target < 1.25) result = 10;
        else result = 12.5;

        weightageScore = avg === 0 ? -2 : (result / 100) * weightage;
      } else {
        if (avg === 0) result = 0;
        else if (avg <= weightage) result = (avg / weightage) * 10;
        else if (avg / weightage < 1.25) result = 12.5;
        else result = 0;

        weightageScore = (result / 100) * weightage;
      }

      Total += weightageScore;

      finalKpis[name] = {
        score: Number(result.toFixed(2)),
        achieved: avg || 0,
        weightage,
        weightageScore: Number(weightageScore.toFixed(2)),
      };
    }

    res.json({
      hod_id,
      period,
      kpis: finalKpis,
      total: Number(Total.toFixed(2)),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to calculate HOD scores" });
  }
});

//all HO_STAFF kpis
performanceMasterRouter.get("/ho-staff-scores-all", (req, res) => {
  const { period, hod_id, role } = req.query;

  if (!period || !hod_id || !role) {
    return res.status(400).json({
      error: "period, hod_id, and role required",
    });
  }

  const kpiQuery = `
      SELECT 
        rkm.id AS role_kpi_mapping_id,
        km.kpi_name,
        rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
      ORDER BY rkm.id
    `;

  pool.query(kpiQuery, [role], (err, kpiList) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: "Failed KPI list" });
    }

    if (!kpiList.length) {
      return res.status(404).json({ error: "No KPI found for role" });
    }

    const staffQuery = `
        SELECT id AS staffId, name AS staffName
        FROM users
        WHERE role = 'HO_staff' AND hod_id = ?
      `;

    pool.query(staffQuery, [hod_id], async (err2, staffRows) => {
      if (err2) {
        console.error(err2);
        return res.status(500).json({ error: "Failed to load staff" });
      }

      if (!staffRows.length) {
        return res.json([]);
      }

      // Final result array
      const result = [];

      for (const staff of staffRows) {
        const staffId = staff.staffId;

        // Fetch their entries
        const userEntries = await new Promise((resolve) => {
          pool.query(
            `
              SELECT role_kpi_mapping_id, value AS achieved
              FROM user_kpi_entry
              WHERE period = ? AND user_id = ? AND deleted_at IS NULL
            `,
            [period, staffId],
            (err, rows) => resolve(rows || [])
          );
        });

        const insuranceValue = await new Promise((resolve) => {
          pool.query(
            `SELECT value AS achieved FROM entries WHERE kpi='insurance' AND employee_id = ?`,
            [staffId],
            (err1, rows) => resolve(rows || [])
          );
        });

        // Create achieved map
        const achievedMap = {};
        userEntries.forEach((e) => {
          achievedMap[e.role_kpi_mapping_id] = Number(e.achieved || 0);
        });

        const staffObj = {
          staffId,
          staffName: staff.staffName,
        };

        let totalWeightageScore = 0;

        for (const kpi of kpiList) {
          const { role_kpi_mapping_id, kpi_name, weightage } = kpi;

          let achieved = 0;

          if (kpi_name.toLowerCase() === "insurance") {
            achieved = insuranceValue.length
              ? Number(insuranceValue[0].achieved)
              : 0;
          } else {
            achieved =
              achievedMap[role_kpi_mapping_id] !== undefined
                ? achievedMap[role_kpi_mapping_id]
                : 0;
          }

          const target =
            kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

          let score = 0;

          if (achieved > 0) {
            const ratio = achieved / target;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;
          }

          let weightageScore = (score * weightage) / 100;

          if (kpi_name.toLowerCase() === "insurance" && score === 0) {
            weightageScore = -2;
          }

          totalWeightageScore += weightageScore;

          // Save inside staff object under KPI name
          staffObj[kpi_name] = {
            score: Number(score.toFixed(2)),
            achieved,
            weightage,
            weightageScore:
              kpi_name.toLowerCase() === "insurance" && weightageScore === 0
                ? -2
                : Number(weightageScore.toFixed(2)),
          };
        }

        staffObj.total = Number(totalWeightageScore.toFixed(2));

        result.push(staffObj);
      }

      res.json(result);
    });
  });
});

// Save or update HO staff KPI scores
performanceMasterRouter.post("/save-or-update-ho-staff-kpi", (req, res) => {
  const { user_id, period, master_user_id, scores } = req.body;

  if (!user_id || !master_user_id || !period || !scores) {
    return res.status(400).json({
      error: "user_id, master_user_id, period and scores are required",
    });
  }

  const filteredScores = scores.filter(
    (s) => s.kpi_name.toLowerCase() !== "insurance"
  );

  if (filteredScores.length === 0) {
    return res.json({ message: "No KPI to save (Insurance excluded)" });
  }

  const insertMessages = [];
  const updateMessages = [];

  const promises = filteredScores.map((s) => {
    return new Promise((resolve) => {
      const checkSql = `
        SELECT id FROM user_kpi_entry
        WHERE user_id = ? AND role_kpi_mapping_id = ? AND period = ?
      `;

      pool.query(
        checkSql,
        [user_id, s.role_kpi_mapping_id, period],
        (err, rows) => {
          if (err) return resolve({ error: err });

          if (rows.length > 0) {
            // UPDATE
            const updateSql = `
              UPDATE user_kpi_entry
              SET value = ?, master_user_id = ?, updated_at = CURRENT_TIMESTAMP
              WHERE user_id = ? AND role_kpi_mapping_id = ? AND period = ?
            `;

            pool.query(
              updateSql,
              [s.value, master_user_id, user_id, s.role_kpi_mapping_id, period],
              (err2) => {
                if (!err2) {
                  updateMessages.push(`${s.kpi_name} updated successfully`);
                }
                resolve({ updated: true, err: err2 });
              }
            );
          } else {
            // INSERT
            const insertSql = `
              INSERT INTO user_kpi_entry 
                (user_id, role_kpi_mapping_id, period, value, master_user_id)
              VALUES (?, ?, ?, ?, ?)
            `;

            pool.query(
              insertSql,
              [user_id, s.role_kpi_mapping_id, period, s.value, master_user_id],
              (err3) => {
                if (!err3) {
                  insertMessages.push(`${s.kpi_name} inserted successfully`);
                }
                resolve({ inserted: true, err: err3 });
              }
            );
          }
        }
      );
    });
  });

  Promise.all(promises).then((results) => {
    const errors = results.filter((r) => r.err);

    if (errors.length > 0) {
      return res.status(500).json({
        error: "Some KPI values failed to save",
        details: errors,
      });
    }

    res.json({
      message: "KPI save/update completed ",
      inserted: insertMessages,
      updated: updateMessages,
    });
  });
});
//Dashboard Data
// performanceMasterRouter.post("/get-Total-Ho_staff-details", (req, res) => {
//   const { hod_id, period } = req.body;

//   if (!hod_id || !period) {
//     return res.status(400).json({
//       error: "hod_id and period are required",
//     });
//   }

//   const dashboardResult = {};

//   const query1 = `
//     SELECT username, name, role
//     FROM users
//     WHERE role='HO_STAFF' AND hod_id = ?
//   `;

//   pool.query(query1, [hod_id], (err1, hoStaffResult) => {
//     if (err1) {
//       console.error(err1);
//       return res.status(500).json({ error: "Database error (HO Staff)" });
//     }

//     dashboardResult.totalHOStaff = hoStaffResult;
//     dashboardResult.totalHOStaffCount = hoStaffResult.length;

//     const query2 = `
//       SELECT code, name
//       FROM branches
//       WHERE incharge_id = ?
//     `;

//     pool.query(query2, [hod_id], async (err2, branches) => {
//       if (err2) {
//         console.error(err2);
//         return res.status(500).json({ error: "Database error (Branches)" });
//       }

//       const branchesWithScore = [];

//       for (let branch of branches) {
//         const branchScore = await calculateBMScores(period, branch.code);

//         branchesWithScore.push({
//           ...branch,
//           bmTotalScore: branchScore.total,
//         });
//       }

//       dashboardResult.totalBranches = branchesWithScore;
//       dashboardResult.totalBranchesCount = branchesWithScore.length;

//       const query3 = `
//         SELECT username, name, role
//         FROM users
//         WHERE role IN ('AGM','DGM','AGM_IT','AGM_INSURANCE','AGM_AUDIT')
//       `;

//       pool.query(query3, (err3, agmDgmResult) => {
//         if (err3) {
//           console.error(err3);
//           return res.status(500).json({ error: "Database error (AGM/DGM)" });
//         }

//         dashboardResult.totalAGMDGM = agmDgmResult;
//         dashboardResult.totalAGMDGMCount = agmDgmResult.length;

//         res.json(dashboardResult);
//       });
//     });
//   });
// });

performanceMasterRouter.post("/get-Total-Ho_staff-details",async (req, res) => {
    const { hod_id, period } = req.body;

    if (!hod_id || !period) {
      return res.status(400).json({
        error: "hod_id and period are required",
      });
    }

    try {
      const dashboardResult = {};

      const hoStaffResult = await new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT username, name, role
          FROM users
          WHERE role='HO_STAFF' AND hod_id = ?
          `,
          [hod_id],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows);
          }
        );
      });

      dashboardResult.totalHOStaff = hoStaffResult;
      dashboardResult.totalHOStaffCount = hoStaffResult.length;

      const branches = await new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT code, name
          FROM branches
          WHERE incharge_id = ?
          `,
          [hod_id],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows);
          }
        );
      });

      const branchesWithScore = await Promise.all(
        branches.map(async (branch) => {
          try {
            const branchScore = await calculateBMScores(period, branch.code);
            return {
              ...branch,
              bmTotalScore: branchScore.total || 0,
            };
          } catch {
            return {
              ...branch,
              bmTotalScore: 0,
            };
          }
        })
      );

      dashboardResult.totalBranches = branchesWithScore;
      dashboardResult.totalBranchesCount = branchesWithScore.length;

      const agmDgmResult = await new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT username, name, role
          FROM users
          WHERE role IN ('AGM','DGM','AGM_IT','AGM_INSURANCE','AGM_AUDIT')
          `,
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows);
          }
        );
      });

      dashboardResult.totalAGMDGM = agmDgmResult;
      dashboardResult.totalAGMDGMCount = agmDgmResult.length;

      res.json(dashboardResult);
    } catch (err) {
      console.error(err);
      res.status(500).json({
        error: "Failed to load HO staff dashboard data",
      });
    }
  }
);

// const calculateHodAllScores = (
//   pool,
//   period,
//   hod_id,
//   role,
//   calculateStaffScores,
//   calculateBMScores
// ) => {
//   return new Promise((resolve, reject) => {
//     const hodKpiQuery = `
//       SELECT k.kpi_name, r.id AS role_kpi_mapping_id, r.weightage
//       FROM role_kpi_mapping r
//       JOIN kpi_master k ON r.kpi_id = k.id
//       WHERE r.role = ?`;

//     pool.query(hodKpiQuery, [role], (err, hodKpis) => {
//       if (err) return reject({ error: "Failed to fetch KPI list" });
//       if (!hodKpis.length) return reject({ error: "No KPIs for AGM role" });

//       const staffQuery = `SELECT id FROM users WHERE hod_id = ?`;

//       pool.query(staffQuery, [hod_id], async (err2, staffList) => {
//         if (err2) return reject({ error: "Failed to fetch HO staff" });

//         const staffIds = staffList.map((s) => s.id);

//         let staffScores = [];
//         for (const sid of staffIds) {
//           try {
//             const scoreObj = await calculateStaffScores(
//               period,
//               sid,
//               "HO_STAFF"
//             );
//             staffScores.push(scoreObj);
//           } catch (e) {
//             staffScores.push({ total: 0 });
//           }
//         }

//         const totalScores = staffScores.map((s) => s.total || 0);

//         const avgStaffScore =
//           totalScores.reduce((sum, val) => sum + val, 0) / totalScores.length;

//         const fixedAvg = Number(avgStaffScore.toFixed(2));

//         let finalKpis = {};

//         const branchCodes = await new Promise((resolve, reject) => {
//           pool.query(
//             "SELECT code FROM branches WHERE incharge_id = ?",
//             [hod_id],
//             (err, rows) => {
//               if (err) return reject(err);
//               resolve(rows.map((r) => r.code));
//             }
//           );
//         });

//         let branchWiseAverage = {};

//         for (const code of branchCodes) {
//         const bmScore = await calculateBMScores(period, code);
//         const totals = bmScore.total || 0;
//         branchWiseAverage[code] = Number(totals.toFixed(2));
//         }

//         const branchValues = Object.values(branchWiseAverage);
//         const branchTotal = branchValues.reduce((sum, v) => sum + v, 0);
//         const branchAvgScore = Number(
//           (branchTotal / branchValues.length).toFixed(2)
//         );

//         const kpiMap = {};
//         hodKpis.forEach((k) => {
//           kpiMap[k.kpi_name] = {
//             id: k.role_kpi_mapping_id,
//             weightage: k.weightage,
//           };
//         });

//         //insurance kpi calaculation
//         const insuranceValue = await new Promise((resolve, reject) => {
//           if (!kpiMap["insurance"]) return resolve(0);
//           pool.query(
//             "SELECT value FROM entries WHERE kpi='insurance' and employee_id = ? and period = ? ",
//             [hod_id, period],
//             (err, rows) => {
//               if (err) return reject(err);
//               resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//             }
//           );
//         });
//         //HO Building Cleanliness
//         const Cleanliness = await new Promise((resolve, reject) => {
//           if (!kpiMap["HO Building Cleanliness"]) return resolve(0);

//           pool.query(
//             "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//             [kpiMap["HO Building Cleanliness"].id, hod_id, period],
//             (err, rows) => {
//               if (err) return reject(err);
//               resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//             }
//           );
//         });

//         //Management  Discretion
//         const Management = await new Promise((resolve, reject) => {
//           if (!kpiMap["Management  Discretion"]) return resolve(0);
//           pool.query(
//             "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//             [kpiMap["Management  Discretion"].id, hod_id, period],
//             (err, rows) => {
//               if (err) return reject(err);
//               resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//             }
//           );
//         });

//         //Internal Audit performance
//         const InternalAudit = await new Promise((resolve, reject) => {
//           if (!kpiMap["Internal Audit performance"]) return resolve(0);
//           pool.query(
//             "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//             [kpiMap["Internal Audit performance"].id, hod_id, period],
//             (err, rows) => {
//               if (err) return reject(err);
//               resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//             }
//           );
//         });

//         //IT
//         const IT = await new Promise((resolve, reject) => {
//           if (!kpiMap["IT"]) return resolve(0);
//           pool.query(
//             "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//             [kpiMap["IT"].id, hod_id, period],
//             (err, rows) => {
//               if (err) return reject(err);
//               resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//             }
//           );
//         });

//         //Insurance Business Development
//         const InsuranceBusinessDevelopment = await new Promise(
//           (resolve, reject) => {
//             if (!kpiMap["Insurance Business Development"]) return resolve(0);
//             pool.query(
//               "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
//               [kpiMap["Insurance Business Development"].id, hod_id, period],
//               (err, rows) => {
//                 if (err) return reject(err);
//                 resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
//               }
//             );
//           }
//         );

//         let Total = 0;

//         for (const kpi of hodKpis) {
//           const name = kpi.kpi_name;
//           const weightage = kpi.weightage;

//           let avg = 0;
//           let result = 0;
//           let weightageScore = 0;

//           if (name.toLowerCase().includes("section")) {
//             avg = fixedAvg;
//           } else if (
//             name.toLowerCase().includes("branch") ||
//             name.toLowerCase().includes("visits")
//           ) {
//             avg = branchAvgScore;
//           } else if (name.toLowerCase().includes("clean")) {
//             avg = Cleanliness;
//           } else if (name.toLowerCase().includes("management")) {
//             avg = Management;
//           } else if (
//             name.toLowerCase().includes("audit") &&
//             !name.toLowerCase().includes("internal")
//           ) {
//             avg = InternalAudit;
//           } else if (name.toLowerCase().includes("internal audit")) {
//             avg = InternalAudit;
//           } else if (name.toLowerCase().includes("it")) {
//             avg = IT;
//           } else if (name.toLowerCase().includes("business development")) {
//             avg = InsuranceBusinessDevelopment;
//           }

//           else if (name.toLowerCase().includes("insurance")) {
//             const target = 40000;
//             avg = insuranceValue;

//             if (avg <= target) result = (avg / target) * 10;
//             else if (avg / target < 1.25) result = 10;
//             else result = 12.5;

//             weightageScore = avg === 0 ? -2 : (result / 100) * weightage;

//             finalKpis[name] = {
//               score: Number(result.toFixed(2)),
//               achieved: avg || 0,
//               weightage,
//               weightageScore: Number(weightageScore.toFixed(2)),
//             };

//             Total += weightageScore;
//             continue;
//           }

//           if (avg === 0) result = 0;
//           else if (avg <= weightage) result = (avg / weightage) * 10;
//           else if (avg / weightage < 1.25) result = 12.5;
//           else result = 0;

//           weightageScore = (result / 100) * weightage;
//           Total += weightageScore;

//           finalKpis[name] = {
//             score: Number(result.toFixed(2)),
//             achieved: avg || 0,
//             weightage,
//             weightageScore: Number(weightageScore.toFixed(2)),
//           };
//         }

//         resolve({
//           hod_id,
//           period,
//           kpis: finalKpis,
//           total: Total,
//         });
//       });
//     });
//   });
// };

// //All AGM-DGM Scores
// performanceMasterRouter.get("/AGM-DGM-Scores", async (req, res) => {
//   const { period } = req.query;

//   if (!period) {
//     return res.status(400).json({ error: "period required" });
//   }

//   const allAGmQuery = `
//     SELECT id AS hod_id, role ,name
//     FROM users
//     WHERE role IN ('AGM','DGM','AGM_AUDIT','AGM_INSURANCE','AGM_IT')
//   `;

//   pool.query(allAGmQuery, async (err, rows) => {
//     if (err) {
//       console.error(err);
//       return res.status(500).json({
//         error: "Database error fetching AGMs/DGMs",
//       });
//     }

//     const agmList = rows;
//     const results = [];

//     for (const agm of agmList) {
//       try {
//         const scoreData = await calculateHodAllScores(
//           pool,
//           period,
//           agm.hod_id,
//           agm.role,
//           calculateStaffScores,
//           calculateBMScores
//         );

//         results.push({
//           hod_id: agm.hod_id,
//           name: agm.name,
//           role: agm.role,
//           ...scoreData,
//         });
//       } catch (err) {
//         console.error(err);
//         return res.status(500).json(err);
//       }
//     }

//     // Return all AGM/DGM results together
//     res.json(results);
//   });
// });

const calculateHodAllScores = async (
  pool,
  period,
  hod_id,
  role,
  calculateStaffScores,
  calculateBMScores
) => {
  try {
    const hodKpis = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT k.kpi_name, r.id AS role_kpi_mapping_id, r.weightage
        FROM role_kpi_mapping r
        JOIN kpi_master k ON r.kpi_id = k.id
        WHERE r.role = ?
        `,
        [role],
        (err, rows) => {
          if (err) return reject(err);
          if (!rows.length) return reject("No KPIs found for role");
          resolve(rows);
        }
      );
    });

    // KPI MAP
    const kpiMap = {};
    hodKpis.forEach((k) => {
      kpiMap[k.kpi_name] = {
        id: k.role_kpi_mapping_id,
        weightage: k.weightage,
      };
    });

    const staffIds = await new Promise((resolve, reject) => {
      pool.query(
        "SELECT id FROM users WHERE hod_id = ?",
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows.map((r) => r.id));
        }
      );
    });

    const staffScores = await Promise.all(
      staffIds.map((id) =>
        calculateStaffScores(period, id, "HO_STAFF").catch(() => ({ total: 0 }))
      )
    );

    const staffAvgScore = Number(
      (
        staffScores.reduce((s, v) => s + (v.total || 0), 0) /
        (staffScores.length || 1)
      ).toFixed(2)
    );

    const branchCodes = await new Promise((resolve, reject) => {
      pool.query(
        "SELECT code FROM branches WHERE incharge_id = ?",
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows.map((r) => r.code));
        }
      );
    });

    const bmTotals = await Promise.all(
      branchCodes.map((code) =>
        calculateBMScores(period, code)
          .then((res) => Number((res.total || 0).toFixed(2)))
          .catch(() => 0)
      )
    );

    const branchAvgScore = Number(
      (bmTotals.reduce((s, v) => s + v, 0) / (bmTotals.length || 1)).toFixed(2)
    );

    const userKpiValues = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT role_kpi_mapping_id, SUM(value) AS total
        FROM user_kpi_entry
        WHERE user_id = ? AND period = ?
        GROUP BY role_kpi_mapping_id
        `,
        [hod_id, period],
        (err, rows) => {
          if (err) return reject(err);
          const map = {};
          rows.forEach(
            (r) => (map[r.role_kpi_mapping_id] = Number(r.total || 0))
          );
          resolve(map);
        }
      );
    });

    const insuranceValue = await new Promise((resolve, reject) => {
      if (!kpiMap["insurance"]) return resolve(0);
      pool.query(
        `
        SELECT SUM(value) AS total
        FROM entries
        WHERE kpi='insurance' AND employee_id=? AND period=?
        `,
        [hod_id, period],
        (err, rows) => {
          if (err) return reject(err);
          resolve(Number(rows[0]?.total || 0));
        }
      );
    });

    const getKpiVal = (name) => userKpiValues[kpiMap[name]?.id] || 0;

    const Cleanliness = getKpiVal("HO Building Cleanliness");
    const Management = getKpiVal("Management  Discretion");
    const InternalAudit = getKpiVal("Internal Audit performance");
    const IT = getKpiVal("IT");
    const InsuranceBusinessDevelopment = getKpiVal(
      "Insurance Business Development"
    );

    let finalKpis = {};
    let Total = 0;

    for (const kpi of hodKpis) {
      const name = kpi.kpi_name;
      const weightage = kpi.weightage;

      let avg = 0;
      let result = 0;
      let weightageScore = 0;

      if (name.toLowerCase().includes("section")) avg = staffAvgScore;
      else if (
        name.toLowerCase().includes("branch") ||
        name.toLowerCase().includes("visits")
      )
        avg = branchAvgScore;
      else if (name.toLowerCase().includes("clean")) avg = Cleanliness;
      else if (name.toLowerCase().includes("management")) avg = Management;
      else if (name.toLowerCase().includes("audit")) avg = InternalAudit;
      else if (name.toLowerCase().includes("it")) avg = IT;
      else if (name.toLowerCase().includes("business development"))
        avg = InsuranceBusinessDevelopment;

      // INSURANCE SPECIAL LOGIC (UNCHANGED)
      if (name.toLowerCase().includes("insurance")) {
        const target = 40000;
        avg = insuranceValue;

        if (avg <= target) result = (avg / target) * 10;
        else if (avg / target < 1.25) result = 10;
        else result = 12.5;

        weightageScore = avg === 0 ? -2 : (result / 100) * weightage;
      } else {
        if (avg === 0) result = 0;
        else if (avg <= weightage) result = (avg / weightage) * 10;
        else if (avg / weightage < 1.25) result = 12.5;
        else result = 0;

        weightageScore = (result / 100) * weightage;
      }

      Total += weightageScore;

      finalKpis[name] = {
        score: Number(result.toFixed(2)),
        achieved: avg || 0,
        weightage,
        weightageScore: Number(weightageScore.toFixed(2)),
      };
    }
    
    
    return {
      hod_id,
      period,
      kpis: finalKpis,
      total: Number(Total.toFixed(2)),
    };
  } catch (err) {
    throw err;
  }
};

performanceMasterRouter.get("/AGM-DGM-Scores", async (req, res) => {
  const { period } = req.query;

  if (!period) {
    return res.status(400).json({ error: "period required" });
  }

  try {
    const agmList = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT id AS hod_id, role, name
        FROM users
        WHERE role IN ('AGM','DGM','AGM_AUDIT','AGM_INSURANCE','AGM_IT')
        `,
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        }
      );
    });

    // PARALLEL AGM CALCULATION
    const results = await Promise.all(
      agmList.map(async (agm) => {
        const scoreData = await calculateHodAllScores(
          pool,
          period,
          agm.hod_id,
          agm.role,
          calculateStaffScores,
          calculateBMScores
        );

        return {
          hod_id: agm.hod_id,
          name: agm.name,
          role: agm.role,
          ...scoreData,
        };
      })
    );

    res.json(results);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to calculate AGM/DGM scores" });
  }
});

performanceMasterRouter.post("/deputation-report", (req, res) => {
  const { period, department } = req.body;

  let sql = `
    SELECT
      emp_id,
      name,
      place,
      design,
      branch,
      work_at,
      weightage_score,
      department
    FROM deputation_staff
    WHERE 1 = 1
  `;

  const params = [];

  if (department && department !== "All") {
    sql += " AND department = ?";
    params.push(department);
  }

  if (period) {
    sql += " AND period = ?";
    params.push(period);
  }

  sql += " ORDER BY department, name";

  pool.query(sql, params, (err, rows) => {
    if (err) return res.status(500).json(err);
    res.json(rows);
  });
});
performanceMasterRouter.post("/getAllBranchesScore", async (req, res) => {
  const { period } = req.body;

  if (!period) {
    return res.status(400).json({ error: "period is required" });
  }

  try {
    const branchReportData = {};

   
    const branches = await new Promise((resolve, reject) => {
      pool.query(
        `SELECT code, name FROM branches`,
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        }
      );
    });


    const branchesWithScore = await Promise.all(
      branches.map(async (branch) => {
        try {
          // Fetch BM
          const bmRows = await new Promise((resolve, reject) => {
            pool.query(
              `SELECT PF_NO, name FROM users WHERE role='BM' AND branch_id = ?`,
              [branch.code],
              (err, rows) => {
                if (err) return reject(err);
                resolve(rows);
              }
            );
          });

          const bm = bmRows[0] || null;

        
          const branchScore = await calculateBMScores(period, branch.code);
         
          
          return {
            ...branch,
            bmId: bm?.PF_NO ?? null,
            bmName: bm?.name ?? null,
            bmTotalScore: branchScore?.total ?? 0,
          };
        } catch (err) {
          console.error(`BM score failed for branch ${branch.code}`, err);
          return {
            ...branch,
            bmId: null,
            bmName: null,
            bmTotalScore: 0,
          };
        }
      })
    );

    branchReportData.totalBranches = branchesWithScore;
    branchReportData.totalBranchesCount = branchesWithScore.length;

    res.json(branchReportData);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Failed to load branch report data",
    });
  }
});
//single clerk kpi score
function calculateStaffScoresCb(period, employeeId, branchId, callback) {
  // STEP 1: allocations + achieved
  pool.query(
    `
    SELECT a.kpi, a.amount AS target, w.weightage, e.total_achieved AS achieved
    FROM (SELECT kpi, amount, user_id FROM allocations WHERE period = ? AND user_id = ?) AS a
    LEFT JOIN (
      SELECT kpi, SUM(value) AS total_achieved
      FROM entries
      WHERE period = ? AND employee_id = ? AND status = 'Verified'
      GROUP BY kpi
    ) AS e ON a.kpi = e.kpi
    LEFT JOIN weightage w ON a.kpi = w.kpi
    `,
    [period, employeeId, period, employeeId],
    (err, results) => {
      if (err) return callback(err);

      // STEP 2: branch targets
      pool.query(
        `
        SELECT t.kpi, t.amount AS target, w.weightage, e.total_achieved AS achieved
        FROM (SELECT kpi, amount FROM targets WHERE period = ? AND branch_id = ? AND kpi IN ('deposit','loan_gen','loan_amulya','recovery','audit','insurance')) AS t
        LEFT JOIN (
          SELECT kpi, SUM(value) AS total_achieved
          FROM entries
          WHERE period = ? AND branch_id = ? AND status = 'Verified'
          GROUP BY kpi
        ) AS e ON t.kpi = e.kpi
        LEFT JOIN weightage w ON t.kpi = w.kpi
        `,
        [period, branchId, period, branchId],
        (err, branchResults) => {
          if (err) return callback(err);

          // STEP 3: insurance achieved (employee specific)
          pool.query(
            `
            SELECT SUM(value) AS achieved
            FROM entries
            WHERE period = ? AND employee_id = ? AND kpi = 'insurance'
            `,
            [period, employeeId],
            (err, insRows) => {
              if (err) return callback(err);

              const insuranceAchieved = insRows?.[0]?.achieved || 0;

              branchResults.forEach((row) => {
                if (row.kpi === "insurance") {
                  row.achieved = insuranceAchieved;
                }
              });

              // STEP 4: score calculation
              const scores = {};
              let totalWeightageScore = 0;

              results.forEach((row) => {
                if (row.kpi === "recovery") {
                  const br = branchResults.find(
                    (b) => b.kpi === "recovery"
                  );
                  if (br) {
                    row.target = br.target;
                    row.achieved = br.achieved;
                  }
                }

                const score = calculateScore(
                  row.kpi,
                  row.achieved,
                  row.target
                );

                const weightageScore =
                  row.kpi === "insurance" && score === 0
                    ? -2
                    : (score * row.weightage) / 100;

                scores[row.kpi] = {
                  score,
                  target: row.target || 0,
                  achieved: row.achieved || 0,
                  weightage: row.weightage || 0,
                  weightageScore,
                };

                totalWeightageScore += weightageScore;
              });

              scores.total = totalWeightageScore;

              callback(null, scores);
            }
          );
        }
      );
    }
  );
}
function calculateScore(kpi, achieved, target) {
  let outOf10 = 0;
  if (!target) return 0;

  const ratio = achieved / target;

  switch (kpi) {
    case "deposit":
    case "loan_gen":
      outOf10 = ratio <= 1 ? ratio * 10 : ratio < 1.25 ? 10 : 12.5;
      break;

    case "loan_amulya":
      outOf10 = ratio <= 1 ? ratio * 10 : ratio < 1.25 ? 10 : 12.5;
      break;

    case "insurance":
      if (ratio === 0) outOf10 = -2;
      else outOf10 = ratio <= 1 ? ratio * 10 : ratio < 1.25 ? 10 : 12.5;
      break;

    case "recovery":
    case "audit":
      outOf10 = ratio <= 1 ? ratio * 10 : 12.5;
      break;
  }

  return Math.max(0, Math.min(12.5, isNaN(outOf10) ? 0 : outOf10));
}

//single hostaff 
function calculateSpecificAllStaffScoresCb(
  period,
  ho_staff_id,
  role,
  callback
) {
 
  const kpiWeightageQuery = `
    SELECT 
      rkm.id AS role_kpi_mapping_id,
      km.kpi_name,
      rkm.weightage
    FROM role_kpi_mapping rkm
    JOIN kpi_master km ON km.id = rkm.kpi_id
    WHERE rkm.role = ? AND rkm.deleted_at IS NULL
  `;

  pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
    if (err) return callback(err);
    if (!kpiWeightage.length)
      return callback(new Error("No KPI found for this role"));

 
    const userEntryQuery = `
      SELECT 
        role_kpi_mapping_id, 
        value AS achieved 
      FROM user_kpi_entry 
      WHERE period = ? AND user_id = ? AND deleted_at IS NULL
    `;

    pool.query(
      userEntryQuery,
      [period, ho_staff_id],
      (err2, userEntries) => {
        if (err2) return callback(err2);

        
        const insuranceQuery = `
          SELECT value AS achieved 
          FROM entries 
          WHERE kpi='insurance' AND employee_id = ?
        `;

        pool.query(
          insuranceQuery,
          [ho_staff_id],
          (err3, insuranceRows) => {
            if (err3) return callback(err3);

            const insuranceValue = insuranceRows.length
              ? Number(insuranceRows[0].achieved)
              : 0;

            const achievedMap = {};
            userEntries.forEach(
              (e) => (achievedMap[e.role_kpi_mapping_id] = e.achieved)
            );

           
            let totalWeightageScore = 0;
            const scores = {};

            kpiWeightage.forEach((row) => {
              const { role_kpi_mapping_id, kpi_name, weightage } = row;

              let achieved = 0;

              if (kpi_name.toLowerCase() === "insurance") {
                achieved = insuranceValue;
              } else {
                achieved =
                  parseFloat(achievedMap[role_kpi_mapping_id]) || 0;
              }

              const target =
                kpi_name.toLowerCase() === "insurance"
                  ? 40000
                  : weightage;

              let score = 0;
              if (achieved > 0) {
                const ratio = achieved / target;
                if (ratio <= 1) score = ratio * 10;
                else if (ratio < 1.25) score = 10;
                else score = 12.5;
              }

              let weightageScore = (score * weightage) / 100;

              if (kpi_name.toLowerCase() === "insurance" && score === 0) {
                weightageScore = -2;
              }

              totalWeightageScore += weightageScore;

              scores[kpi_name] = {
                score: Number(score.toFixed(2)),
                achieved,
                weightage,
                weightageScore: Number(weightageScore.toFixed(2)),
              };
            });

            scores.total = Number(totalWeightageScore.toFixed(2));

            callback(null, scores);
          }
        );
      }
    );
  });
}

//attender score
async function getBranchAttendersScores(period, branchId, hod_id) {
  if (!period) {
    throw new Error("period is required");
  }

  
 
  const attenderKpis = await new Promise((resolve, reject) => {
    pool.query(
      `
      SELECT k.kpi_name, r.id AS role_kpi_mapping_id, r.weightage
      FROM role_kpi_mapping r
      JOIN kpi_master k ON r.kpi_id = k.id
      WHERE r.role = 'Attender'
      `,
      (err, rows) => {
        if (err) return reject(err);
        resolve(rows);
      }
    );
  });

  if (!attenderKpis.length) return [];

  
  const normalizedBranchId =
    branchId && branchId !== "null" && branchId !== "undefined"
      ? branchId
      : null;

  
  const users = await new Promise((resolve, reject) => {
    let sql, params;

    if (normalizedBranchId) {
      sql = "SELECT id, name FROM users WHERE branch_id=? AND role='Attender'";
      params = [normalizedBranchId];
    } else {
      sql = "SELECT id, name FROM users WHERE hod_id=? AND role='Attender'";
      params = [hod_id];
    }

    pool.query(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });

  if (!users.length) return [];

  
  const response = await Promise.all(
    users.map(async (user) => {
      const [userKpis, insuranceRow] = await Promise.all([
        // Manual KPIs
        new Promise((resolve, reject) => {
          pool.query(
            `
            SELECT role_kpi_mapping_id, SUM(value) AS total
            FROM user_kpi_entry
            WHERE user_id=? AND period=?
            GROUP BY role_kpi_mapping_id
            `,
            [user.id, period],
            (err, rows) => {
              if (err) return reject(err);
              const map = {};
              rows.forEach(
                (r) => (map[r.role_kpi_mapping_id] = Number(r.total || 0))
              );
              resolve(map);
            }
          );
        }),

        // Insurance
        new Promise((resolve, reject) => {
          pool.query(
            `
            SELECT SUM(value) AS total
            FROM entries
            WHERE kpi='insurance' AND employee_id=? AND period=?
            `,
            [user.id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(Number(rows[0]?.total || 0));
            }
          );
        }),
      ]);

      const finalKpis = {};
      let totalScore = 0;

      for (const kpi of attenderKpis) {
        let achieved = 0;
        let target = kpi.weightage;

        if (
          kpi.kpi_name === "Cleanliness" ||
          kpi.kpi_name === "Attitude, Behavior & Discipline"
        ) {
          achieved = userKpis[kpi.role_kpi_mapping_id] || 0;
        }

        if (kpi.kpi_name.toLowerCase().includes("insurance")) {
          achieved = insuranceRow;
          target = 40000;
        }

        let score = 0;
        if (achieved > 0) {
          const ratio = achieved / target;
          if (ratio <= 1) score = ratio * 10;
          else if (ratio <= 1.25) score = 10;
          else score = 12.5;
        }

        const weightageScore =
          kpi.kpi_name.toLowerCase().includes("insurance") && achieved === 0
            ? -2
            : (score * kpi.weightage) / 100;

        finalKpis[kpi.kpi_name] = {
          target,
          achieved,
          weightage: kpi.weightage,
          score,
          weightageScore,
        };

        totalScore += weightageScore;
        
        
      }
      return {
        staffId: user.id,
        staffName: user.name,
        total: Number(totalScore.toFixed(2)),
        kpis: finalKpis,
      };
    })
  );

  return response;
}


//user report
// performanceMasterRouter.post("/getAllUserReport", (req, res) => {
//   const { period } = req.body;

//   if (!period) {
//     return res.status(400).json({ error: "period is required" });
//   }

//   const agmScores = [];
//   let gmUser = null;

//   pool.query(
//     `
//     SELECT u.id, u.username, u.name, u.role, u.branch_id, u.hod_id,b.name as branch_name
//     FROM users u left join branches b on u.branch_id=b.id
//     WHERE u.role IN (
//       "BM","Clerk","HO_STAFF","GM",
//       "AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE",
//       "Attender"
//     )
//     `,
//     (err, users) => {
//       if (err) {
//         console.error(err);
//         return res.status(500).json({ error: "Failed to load users" });
//       }

//       if (!users.length) {
//         return res.json({ totalUsers: 0, users: [] });
//       }

//       const results = [];
//       let completed = 0;

//       const done = () => {
//         completed++;

//         if (completed === users.length) {

//           if (gmUser && agmScores.length) {
//             const totalAGMs = agmScores.length || 1;
//             const totalWeightage = 100;
//             const perAGMWeightage = totalWeightage / totalAGMs;

//             const gmFinalTotal = agmScores.reduce((acc, agm) => {
//               const weightageScore =
//                 (agm.total * perAGMWeightage) / 100;
//               return acc + weightageScore;
//             }, 0);

//             results.push({
//               ...gmUser,
//               bmTotalScore: Number(gmFinalTotal.toFixed(2)),
//             });
//           }

//           return res.json({
//             totalUsers: results.length,
//             users: results,
//           });
//         }
//       };

//       users.forEach((user) => {

     
//         if (user.role === "GM") {
//           gmUser = user;
//           done();
//         }

        
//         else if (user.role === "BM") {
//           calculateBMScores(period, user.branch_id)
//             .then((bmScore) => {
//               results.push({
//                 ...user,
//                 bmTotalScore: bmScore?.total ?? 0,
//               });
//               done();
//             })
//             .catch(() => {
//               results.push({ ...user, bmTotalScore: 0 });
//               done();
//             });
//         }

    
//         else if (user.role === "Clerk") {
//           calculateStaffScoresCb(
//             period,
//             user.id,
//             user.branch_id,
//             (err, clerkScore) => {
//               results.push({
//                 ...user,
//                 bmTotalScore: err ? 0 : clerkScore?.total ?? 0,
//               });
//               done();
//             }
//           );
//         }

      
//         else if (user.role === "HO_STAFF") {
//           calculateSpecificAllStaffScoresCb(
//             period,
//             user.id,
//             user.role,
//             (err, hoStaffScore) => {
//               results.push({
//                 ...user,
//                 bmTotalScore: err ? 0 : hoStaffScore?.total ?? 0,
//               });
//               done();
//             }
//           );
//         }

//         else if (
//           ["AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE"].includes(user.role)
//         ) {
//           calculateHodAllScores(
//             pool,
//             period,
//             user.id,
//             user.role,
//             calculateStaffScores,
//             calculateBMScores
//           )
//             .then((hodScore) => {
//               const total = hodScore?.total ?? 0;

//               agmScores.push({
//                 userId: user.id,
//                 role: user.role,
//                 total,
//               });

//               results.push({
//                 ...user,
//                 bmTotalScore: total,
//               });

//               done();
//             })
//             .catch(() => {
//               agmScores.push({
//                 userId: user.id,
//                 role: user.role,
//                 total: 0,
//               });

//               results.push({
//                 ...user,
//                 bmTotalScore: 0,
//               });

//               done();
//             });
//         }

     
//         else if (user.role === "Attender") {
//           getBranchAttendersScores(
//             period,
//             user.branch_id,
//             user.hod_id
//           )
//             .then((attenders) => {
//               const current = attenders.find(
//                 (a) => a.staffId === user.id
//               );

//               results.push({
//                 ...user,
//                 bmTotalScore: current?.total ?? 0,
//               });

//               done();
//             })
//             .catch(() => {
//               results.push({ ...user, bmTotalScore: 0 });
//               done();
//             });
//         }

        
//         else {
//           results.push({ ...user, bmTotalScore: 0 });
//           done();
//         }
//       });
//     }
//   );
// });
function getAllUsers(pool, cb) {
  pool.query(
    `
    SELECT u.id, u.username, u.name, u.role,
           u.branch_id, u.hod_id,
           b.name AS branch_name
    FROM users u
    LEFT JOIN branches b ON u.branch_id = b.id
    WHERE u.role IN (
      "BM","Clerk","HO_STAFF","GM",
      "AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE",
      "Attender"
    )
    `,
    cb
  );
}

performanceMasterRouter.post("/usersBM", async (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter(u => u.role === "BM");

    const result = await Promise.all(
      list.map(async (u) => {
        const branchId = Number(u.branch_id);
        const score = await calculateBMScores(
          String(period),
          branchId
        );

        return {
          ...u,
          bmTotalScore: score?.total ?? 0
        };
      })
    );

    res.json({ users: result });
  });
});

performanceMasterRouter.post("/usersClerk", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter(u => u.role === "Clerk");

    const result = await Promise.all(
      list.map(u =>
        new Promise(resolve => {
          calculateStaffScoresCb(
            (period),        
            (u.id),
            (u.branch_id),
            (err, clerkScore) => {
              resolve({
                ...u,
                bmTotalScore: err ? 0 : clerkScore?.total ?? 0
              });
            }
          );
        })
      )
    );

    res.json({ users: result });
  });
});

performanceMasterRouter.post("/usersHOStaff", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter(u => u.role === "HO_STAFF");

    const result = await Promise.all(
      list.map(u =>
        new Promise(resolve => {
          calculateSpecificAllStaffScoresCb(
            String(period),          // ✅ primitive
            Number(u.id),
            u.role,
            (err, hoStaffScore) => {
              resolve({
                ...u,
                bmTotalScore: err ? 0 : hoStaffScore?.total ?? 0
              });
            }
          );
        })
      )
    );

    res.json({ users: result });
  });
});


performanceMasterRouter.post("/usersAttender", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter(u => u.role === "Attender");

    const result = await Promise.all(
      list.map(async (u) => {
        try {
          const attenders = await getBranchAttendersScores(
            String(period),           
            Number(u.branch_id),
            Number(u.hod_id)
          );

          const current = attenders.find(
            (a) => a.staffId === u.id
          );

          return {
            ...u,
            bmTotalScore: current?.total ?? 0
          };
        } catch (err) {
          return {
            ...u,
            bmTotalScore: 0
          };
        }
      })
    );

    res.json({ users: result });
  });
});

performanceMasterRouter.post("/usersAgmGm", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const agmUsers = users.filter(u =>
      ["AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE"].includes(u.role)
    );

    const gmUser = users.find(u => u.role === "GM") || null;

    const agmScores = [];

    /* ---------- AGM SCORES (PARALLEL) ---------- */
    const agmResults = await Promise.all(
      agmUsers.map(async (u) => {
        try {
          const hodScore = await calculateHodAllScores(
            pool,
            String(period),          // ✅ primitive
            Number(u.id),
            u.role,
            calculateStaffScores,
            calculateBMScores
          );

          const total = hodScore?.total ?? 0;
          agmScores.push(total);

          return {
            ...u,
            bmTotalScore: total
          };
        } catch (err) {
          agmScores.push(0);
          return {
            ...u,
            bmTotalScore: 0
          };
        }
      })
    );

    /* ---------- GM CALCULATION (UNCHANGED LOGIC) ---------- */
    const results = [...agmResults];

    if (gmUser && agmScores.length) {
      const per = 100 / agmScores.length;
      const gmScore = agmScores.reduce(
        (sum, a) => sum + (a * per) / 100,
        0
      );

      results.push({
        ...gmUser,
        bmTotalScore: Number(gmScore.toFixed(2))
      });
    }

    res.json({ users: results });
  });
});




//departmentwise report
performanceMasterRouter.post("/getAllBranchesScoreSectionsWise", async (req, res) => {
  const { period , department} = req.body;

  if (!period) {
    return res.status(400).json({ error: "period is required" });
  }

  try {
    const branchReportData = {};

   
    const branches = await new Promise((resolve, reject) => {
      pool.query(
        `SELECT code, name FROM branches`,
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        }
      );
    });


    const branchesWithScore = await Promise.all(
      branches.map(async (branch) => {
        try {
          // Fetch BM
          const bmRows = await new Promise((resolve, reject) => {
            pool.query(
              `SELECT PF_NO, name FROM users WHERE role='BM' AND branch_id = ?`,
              [branch.code],
              (err, rows) => {
                if (err) return reject(err);
                resolve(rows);
              }
            );
          });

          const bm = bmRows[0] || null;

        
          const branchScore = await calculateBMScores(period, branch.code);
          
          console.log(branchScore )
          
          const deptKey = String(department || "").trim().toLowerCase();

      const matchedKey = Object.keys(branchScore || {}).find(
        key => key.toLowerCase() === deptKey
      );
      const formattedDepartment =
      department
        ? department.charAt(0).toUpperCase() + department.slice(1)
        : null;
      return {
        ...branch,
        bmId: bm?.PF_NO ?? null,
        bmName: bm?.name ?? null,
        department:formattedDepartment,
        departmentData: matchedKey ? branchScore[matchedKey] : null
      };

        } catch (err) {
          console.error(`BM score failed for branch ${branch.code}`, err);
          return {
            ...branch,
            bmId: null,
            bmName: null,
            bmTotalScore: 0,
          };
        }
      })
    );

    branchReportData.totalBranches = branchesWithScore;
    branchReportData.totalBranchesCount = branchesWithScore.length;

    res.json(branchReportData);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Failed to load branch report data",
    });
  }
});