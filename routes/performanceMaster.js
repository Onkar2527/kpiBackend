import express from "express";
import pool from "../db.js";
import { getTransferKpiHistory } from "./summary.js";

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
// performanceMasterRouter.get("/specfic-ALLstaff-scores", (req, res) => {
//   const { period, ho_staff_id, role, hod_id } = req.query;

//   if (!period || !ho_staff_id || !role)
//     return res
//       .status(400)
//       .json({ error: "period, ho_staff_id, and role are required" });

//   const kpiWeightageQuery = `
//       SELECT 
//         rkm.id AS role_kpi_mapping_id,
//         km.kpi_name,
//         rkm.weightage
//       FROM role_kpi_mapping rkm
//       JOIN kpi_master km ON km.id = rkm.kpi_id
//       WHERE rkm.role = ? AND rkm.deleted_at IS NULL
//     `;

//   pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
//     if (err) {
//       console.error("Error fetching KPI weightage:", err);
//       return res.status(500).json({ error: "Failed to fetch KPI weightage" });
//     }

//     if (!kpiWeightage.length)
//       return res.status(404).json({ error: "No KPI found for this role" });

//     // Fetch normal entries
//     const userEntryQuery = `
//         SELECT 
//           role_kpi_mapping_id, 
//           value AS achieved 
//         FROM user_kpi_entry 
//         WHERE period = ? AND user_id = ? AND deleted_at IS NULL AND master_user_id = ?
//     `;

//     pool.query(
//       userEntryQuery,
//       [period, ho_staff_id, hod_id],
//       (err2, userEntries) => {
//         if (err2) {
//           console.error("Error fetching user KPI entries:", err2);
//           return res
//             .status(500)
//             .json({ error: "Failed to fetch user KPI entries" });
//         }

//         // Fetch INSURANCE value separately
//         const insuranceQuery = `
//         SELECT value AS achieved 
//         FROM entries 
//         WHERE kpi='insurance' AND employee_id = ?
//       `;

//         pool.query(insuranceQuery, [ho_staff_id], (err3, insuranceRows) => {
//           if (err3) {
//             console.error("Error fetching insurance value:", err3);
//             return res
//               .status(500)
//               .json({ error: "Failed to fetch insurance value" });
//           }

//           const insuranceValue = insuranceRows.length
//             ? Number(insuranceRows[0].achieved)
//             : 0;

//           const achievedMap = {};
//           userEntries.forEach(
//             (e) => (achievedMap[e.role_kpi_mapping_id] = e.achieved),
//           );

//           let totalWeightageScore = 0;
//           const scores = {};

//           kpiWeightage.forEach((row) => {
//             const { role_kpi_mapping_id, kpi_name, weightage } = row;

//             let achieved = 0;

//             if (kpi_name.toLowerCase() === "insurance") {
//               achieved = insuranceValue;
//             } else {
//               achieved = parseFloat(achievedMap[role_kpi_mapping_id]) || 0;
//             }

//             const target =
//               kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

//             let score = 0;
//             if (achieved > 0) {
//               const ratio = achieved / target;
//               if (ratio <= 1) score = ratio * 10;
//               else if (ratio < 1.25) score = 10;
//               else score = 12.5;
//             }

//             let weightageScore = (score * weightage) / 100;

//             if (kpi_name.toLowerCase() === "insurance" && score === 0) {
//               weightageScore = -2;
//             }

//             totalWeightageScore += weightageScore;

//             scores[kpi_name] = {
//               score: Number(score.toFixed(2)),
//               achieved,
//               weightage,
//               weightageScore: Number(weightageScore.toFixed(2)),
//             };
//           });

//           scores.total = Number(totalWeightageScore.toFixed(2));

//           return res.json(scores);
//         });
//       },
//     );
//   });
// });
//get specific staff data // change for transfer logic and optimized // 23-02-2026
performanceMasterRouter.get("/specfic-ALLstaff-scores", async (req, res) => {
  const { period, ho_staff_id, role, hod_id } = req.query;

  if (!period || !ho_staff_id || !role) {
    return res
      .status(400)
      .json({ error: "period, ho_staff_id, and role are required" });
  }

  try {
   
    const kpiWeightage = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT 
          rkm.id AS role_kpi_mapping_id,
          km.kpi_name,
          rkm.weightage
        FROM role_kpi_mapping rkm
        JOIN kpi_master km ON km.id = rkm.kpi_id
        WHERE rkm.role = ? AND rkm.deleted_at IS NULL
        `,
        [role],
        (err, rows) => (err ? reject(err) : resolve(rows))
      );
    });

    if (!kpiWeightage.length) {
      return res.status(404).json({ error: "No KPI found for this role" });
    }

    const [userEntries, insuranceRows] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT role_kpi_mapping_id, value AS achieved
          FROM user_kpi_entry
          WHERE period = ?
          AND user_id = ?
          AND deleted_at IS NULL
          AND master_user_id = ?
          `,
          [period, ho_staff_id, hod_id],
          (err, rows) => (err ? reject(err) : resolve(rows))
        );
      }),

      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT value AS achieved
          FROM entries
          WHERE kpi='insurance' AND employee_id = ?
          `,
          [ho_staff_id],
          (err, rows) => (err ? reject(err) : resolve(rows))
        );
      }),
    ]);

    const insuranceValue = insuranceRows.length
      ? Number(insuranceRows[0].achieved)
      : 0;

    const achievedMap = {};
    userEntries.forEach((e) => {
      achievedMap[e.role_kpi_mapping_id] = e.achieved;
    });

    let totalWeightageScore = 0;
    const scores = {};

    
    for (const row of kpiWeightage) {
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
    }

    const currentTotal = Number(totalWeightageScore.toFixed(2));

   
    const [hoHistory, attHistory, transferHistory] = await Promise.all([
      new Promise((resolve) => {
        getHoStaffTransferHistory(
          pool,
          period,
          ho_staff_id,
          (err, data) => resolve(err ? [] : data)
        );
      }),

      new Promise((resolve) => {
        getAttenderTransferHistory(
          pool,
          period,
          ho_staff_id,
          (err, data) => resolve(err ? [] : data)
        );
      }),

      getTransferKpiHistory(pool, period, ho_staff_id),
    ]);

    const previousHoScores =
      hoHistory?.[0]?.transfers?.map((t) =>
        Number(t.total_weightage_score)
      ) || [];

    const previousAttenderScores =
      attHistory?.[0]?.transfers?.map((t) =>
        Number(t.total_weightage_score)
      ) || [];

    const previousTransferScores =
      transferHistory?.all_scores?.map(Number) || [];

    const allScores = [
      ...previousHoScores,
      ...previousAttenderScores,
      ...previousTransferScores,
      currentTotal,
    ];

    const finalAvg =
      allScores.length > 0
        ? allScores.reduce((a, b) => a + b, 0) / allScores.length
        : 0;
    scores.originalTotal=Number(totalWeightageScore.toFixed(2));
    scores.total = Number(finalAvg.toFixed(2));
    

    return res.json(scores);
  } catch (error) {
    console.error(error);
    return res.status(500).json({
      error: "Failed to fetch staff scores",
    });
  }
});
//function to get single - single staff score calculation
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
        },
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
        },
      );
    });

    // Map achieved values
    const achievedMap = {};
    userEntries.forEach(
      (e) => (achievedMap[e.role_kpi_mapping_id] = Number(e.achieved || 0)),
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

// funtion to get single BM score calculation
async function calculateBMScores(period, branchId) {
  try {
    const results = await new Promise((resolve, reject) => {
      const query = `
       SELECT
    k.kpi,
    CASE 
        WHEN k.kpi = 'audit' THEN 100
        WHEN k.kpi = 'insurance' THEN COALESCE(a.amount,0)
        ELSE t.amount
    END AS target,
    w.weightage,
    COALESCE(e.total_achieved, 0) AS achieved
FROM
(
    SELECT 'deposit' AS kpi UNION ALL
    SELECT 'loan_gen' UNION ALL
    SELECT 'loan_amulya' UNION ALL
    SELECT 'recovery' UNION ALL
    SELECT 'audit' UNION ALL
    SELECT 'insurance'
) k

-- Get BM of branch
LEFT JOIN users bm
  ON bm.branch_id = ?
  AND bm.role = 'BM'
  AND bm.resign = 0

-- KPI targets
LEFT JOIN targets t
  ON t.kpi = k.kpi
  AND t.period = ?
  AND t.branch_id = ?

-- Insurance allocation
LEFT JOIN allocations a
  ON k.kpi = 'insurance'
  AND a.user_id = bm.id
  AND a.period = ?

-- Weightage
LEFT JOIN weightage w
  ON w.kpi = k.kpi

-- Achieved values
LEFT JOIN (
    SELECT
        e.kpi,
        SUM(e.value) AS total_achieved
    FROM entries e
    LEFT JOIN users bm
        ON bm.branch_id = ?
        AND bm.role = 'BM'
        AND bm.resign = 0
    WHERE
        e.period = ?
        AND e.status = 'Verified'
        AND (
            (e.kpi IN ('insurance','audit','recovery') AND e.employee_id = bm.id)
            OR
            (e.kpi NOT IN ('audit','recovery','insurance') AND e.branch_id = ?)
        )
    GROUP BY e.kpi
) e
ON k.kpi = e.kpi;
      `;

      pool.query(query, [  branchId, period, branchId, period, branchId, period, branchId], (err, rows) => {
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
          },
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
          },
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
        const auditRatio = row.kpi === "audit" ? ratio : 0;
        const recoveryRatio = row.kpi === "recovery" ? ratio : 0;

        switch (row.kpi) {
          case "deposit":
          case "loan_gen":
            if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else if (auditRatio >= 0.75 && recoveryRatio >= 0.75)
              outOf10 = 12.5;
            else outOf10 = 10;
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

async function calculateBMScoresFortrasfer(period, branchId) {
  try {
    const results = await new Promise((resolve, reject) => {
      const query = `
       SELECT
    k.kpi,
    CASE 
        WHEN k.kpi = 'audit' THEN 100
        WHEN k.kpi = 'insurance' THEN COALESCE(a.amount,0)
        ELSE t.amount
    END AS target,
    w.weightage,
    COALESCE(e.total_achieved, 0) AS achieved
FROM
(
    SELECT 'deposit' AS kpi UNION ALL
    SELECT 'loan_gen' UNION ALL
    SELECT 'loan_amulya' UNION ALL
    SELECT 'recovery' UNION ALL
    SELECT 'audit' UNION ALL
    SELECT 'insurance'
) k

-- Get BM of branch
LEFT JOIN users bm
  ON bm.branch_id = ?
  AND bm.role = 'BM'
  AND bm.resign = 0

-- KPI targets
LEFT JOIN targets t
  ON t.kpi = k.kpi
  AND t.period = ?
  AND t.branch_id = ?

-- Insurance allocation
LEFT JOIN allocations a
  ON k.kpi = 'insurance'
  AND a.user_id = bm.id
  AND a.period = ?

-- Weightage
LEFT JOIN weightage w
  ON w.kpi = k.kpi

-- Achieved values
LEFT JOIN (
    SELECT
        e.kpi,
        SUM(e.value) AS total_achieved
    FROM entries e
    LEFT JOIN users bm
        ON bm.branch_id = ?
        AND bm.role = 'BM'
        AND bm.resign = 0
    WHERE
        e.period = ?
        AND e.status = 'Verified'
        AND (
            (e.kpi IN ('insurance','audit','recovery') AND e.employee_id = bm.id)
            OR
            (e.kpi NOT IN ('audit','recovery','insurance') AND e.branch_id = ?)
        )
    GROUP BY e.kpi
) e
ON k.kpi = e.kpi;
      `;

      pool.query(
        query,
        [branchId, period, branchId, period, branchId, period, branchId],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        },
      );
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
          },
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
          },
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
        const auditRatio = row.kpi === "audit" ? ratio : 0;
        const recoveryRatio = row.kpi === "recovery" ? ratio : 0;

        switch (row.kpi) {
          case "deposit":
          case "loan_gen":
            if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else if (auditRatio >= 0.75 && recoveryRatio >= 0.75)
              outOf10 = 12.5;
            else outOf10 = 10;
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
   
    
    const hoHistory = await new Promise((resolve, reject) => {
      getHoStaffTransferHistory(pool, period, bmRow, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });

    const previousHoScores =
      hoHistory?.[0]?.transfers?.map((t) => t.total_weightage_score) || [];

    const attHistory = await new Promise((resolve, reject) => {
      getAttenderTransferHistory(pool, period, bmRow, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });

    const previousAttenderScores =
      attHistory?.[0]?.transfers?.map((t) => t.total_weightage_score) || [];

    const transferHistory = await getTransferKpiHistory(
      pool,
      period,
      bmRow,
    );

    const previousTransferScores = transferHistory?.all_scores || [];

    const allScores = [
      ...previousHoScores,
      ...previousAttenderScores,
      ...previousTransferScores,
      finalScores.total,
    ];
   
    
    const finalAvg =
      allScores.length > 0
        ? allScores.reduce((a, b) => a + b, 0) / allScores.length
        : 0;

    finalScores.total = Number(finalAvg.toFixed(2));
   
    
    return finalScores;
  } catch (err) {
    throw err;
  }
}
// function to get all hod scores
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
        },
      );
    });

    const staffIds = await new Promise((resolve, reject) => {
      pool.query(
        `SELECT id FROM users WHERE hod_id = ?`,
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows.map((r) => r.id));
        },
      );
    });

    const staffScores = await Promise.all(
      staffIds.map((id) =>
        calculateStaffScores(period, id, "HO_STAFF").catch(() => ({
          total: 0,
        })),
      ),
    );

    const fixedAvg = Number(
      (
        staffScores.reduce((s, v) => s + (v.total || 0), 0) /
        (staffScores.length || 1)
      ).toFixed(2),
    );

    const branchCodes = await new Promise((resolve, reject) => {
      pool.query(
        `SELECT code FROM branches WHERE incharge_id = ?`,
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows.map((r) => r.code));
        },
      );
    });

    const branchTotals = await Promise.all(
      branchCodes.map((code) =>
        calculateBMScores(period, code)
          .then((r) => Number((r.total || 0).toFixed(2)))
          .catch(() => 0),
      ),
    );

    const branchAvgScore = Number(
      (
        branchTotals.reduce((s, v) => s + v, 0) / (branchTotals.length || 1)
      ).toFixed(2),
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
            (r) => (map[r.role_kpi_mapping_id] = Number(r.total || 0)),
          );
          resolve(map);
        },
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
        },
      );
    });

    const getVal = (name) => userKpiValues[kpiMap[name]?.id] || 0;

    const Cleanliness = getVal("HO Building Cleanliness");
    const Management = getVal("Management  Discretion");
    const InternalAudit = getVal("Internal Audit performance");
    const IT = getVal("IT");
    const InsuranceBusinessDevelopment = getVal(
      "Insurance Business Development",
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
// performanceMasterRouter.get("/ho-staff-scores-all", (req, res) => {
//   const { period, hod_id, role } = req.query;

//   if (!period || !hod_id || !role) {
//     return res.status(400).json({
//       error: "period, hod_id, and role required",
//     });
//   }

//   const kpiQuery = `
//       SELECT 
//         rkm.id AS role_kpi_mapping_id,
//         km.kpi_name,
//         rkm.weightage
//       FROM role_kpi_mapping rkm
//       JOIN kpi_master km ON km.id = rkm.kpi_id
//       WHERE rkm.role = ? AND rkm.deleted_at IS NULL
//       ORDER BY rkm.id
//     `;

//   pool.query(kpiQuery, [role], (err, kpiList) => {
//     if (err) {
//       console.error(err);
//       return res.status(500).json({ error: "Failed KPI list" });
//     }

//     if (!kpiList.length) {
//       return res.status(404).json({ error: "No KPI found for role" });
//     }

//     const staffQuery = `
//         SELECT id AS staffId, name AS staffName
//         FROM users
//         WHERE role = 'HO_staff' AND hod_id = ?
//       `;

//     pool.query(staffQuery, [hod_id], async (err2, staffRows) => {
//       if (err2) {
//         console.error(err2);
//         return res.status(500).json({ error: "Failed to load staff" });
//       }

//       if (!staffRows.length) {
//         return res.json([]);
//       }

//       // Final result array
//       const result = [];

//       for (const staff of staffRows) {
//         const staffId = staff.staffId;

//         // Fetch their entries
//         const userEntries = await new Promise((resolve) => {
//           pool.query(
//             `
//               SELECT role_kpi_mapping_id, value AS achieved
//               FROM user_kpi_entry
//               WHERE period = ? AND user_id = ? AND deleted_at IS NULL
//             `,
//             [period, staffId],
//             (err, rows) => resolve(rows || []),
//           );
//         });

//         const insuranceValue = await new Promise((resolve) => {
//           pool.query(
//             `SELECT value AS achieved FROM entries WHERE kpi='insurance' AND employee_id = ?`,
//             [staffId],
//             (err1, rows) => resolve(rows || []),
//           );
//         });

//         // Create achieved map
//         const achievedMap = {};
//         userEntries.forEach((e) => {
//           achievedMap[e.role_kpi_mapping_id] = Number(e.achieved || 0);
//         });

//         const staffObj = {
//           staffId,
//           staffName: staff.staffName,
//         };

//         let totalWeightageScore = 0;

//         for (const kpi of kpiList) {
//           const { role_kpi_mapping_id, kpi_name, weightage } = kpi;

//           let achieved = 0;

//           if (kpi_name.toLowerCase() === "insurance") {
//             achieved = insuranceValue.length
//               ? Number(insuranceValue[0].achieved)
//               : 0;
//           } else {
//             achieved =
//               achievedMap[role_kpi_mapping_id] !== undefined
//                 ? achievedMap[role_kpi_mapping_id]
//                 : 0;
//           }

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

//           if (kpi_name.toLowerCase() === "insurance" && score === 0) {
//             weightageScore = -2;
//           }

//           totalWeightageScore += weightageScore;

//           // Save inside staff object under KPI name
//           staffObj[kpi_name] = {
//             score: Number(score.toFixed(2)),
//             achieved,
//             weightage,
//             weightageScore:
//               kpi_name.toLowerCase() === "insurance" && weightageScore === 0
//                 ? -2
//                 : Number(weightageScore.toFixed(2)),
//           };
//         }

//         const finalScores = {
//           total: Number(totalWeightageScore.toFixed(2)),
//         };

//         const hoHistory = await new Promise((resolve) => {
//           getHoStaffTransferHistory(pool, period, staffId, (err, data) => {
//             if (err) return resolve([]);
//             resolve(data);
//           });
//         });

//         const previousHoScores =
//           hoHistory?.[0]?.transfers?.map((t) =>
//             Number(t.total_weightage_score),
//           ) || [];

//         const attHistory = await new Promise((resolve) => {
//           getAttenderTransferHistory(pool, period, staffId, (err, data) => {
//             if (err) return resolve([]);
//             resolve(data);
//           });
//         });

//         const previousAttenderScores =
//           attHistory?.[0]?.transfers?.map((t) =>
//             Number(t.total_weightage_score),
//           ) || [];

//         const transferHistory = await getTransferKpiHistory(
//           pool,
//           period,
//           staffId,
//         );

//         const previousTransferScores =
//           transferHistory?.all_scores?.map(Number) || [];

//         const allScores = [
//           ...previousHoScores,
//           ...previousAttenderScores,
//           ...previousTransferScores,
//           finalScores.total,
//         ];

//         const finalAvg =
//           allScores.length > 0
//             ? allScores.reduce((a, b) => a + b, 0) / allScores.length
//             : 0;

//         staffObj.total = Number(finalAvg.toFixed(2));

//         result.push(staffObj);
//       }

//       res.json(result);
//     });
//   });
// });
//all HO_STAFF kpis /// change for transfer logic and optimized // 23-02-2026
performanceMasterRouter.get("/ho-staff-scores-all", async (req, res) => {
  const { period, hod_id, role } = req.query;

  if (!period || !hod_id || !role) {
    return res.status(400).json({
      error: "period, hod_id, and role required",
    });
  }

  try {
   
    const kpiList = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT 
          rkm.id AS role_kpi_mapping_id,
          km.kpi_name,
          rkm.weightage
        FROM role_kpi_mapping rkm
        JOIN kpi_master km ON km.id = rkm.kpi_id
        WHERE rkm.role = ? AND rkm.deleted_at IS NULL
        ORDER BY rkm.id
        `,
        [role],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        }
      );
    });

    if (!kpiList.length) {
      return res.status(404).json({ error: "No KPI found for role" });
    }

   
    const staffRows = await new Promise((resolve, reject) => {
      pool.query(
        `
        SELECT id AS staffId, name AS staffName
        FROM users
        WHERE role = 'HO_staff' AND hod_id = ?
        `,
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        }
      );
    });

    if (!staffRows.length) {
      return res.json([]);
    }

    const staffIds = staffRows.map((s) => s.staffId);

  
    const allUserEntries = await new Promise((resolve) => {
      pool.query(
        `
        SELECT role_kpi_mapping_id, value AS achieved, user_id
        FROM user_kpi_entry
        WHERE period = ?
        AND user_id IN (?)
        AND deleted_at IS NULL
        `,
        [period, staffIds],
        (err, rows) => resolve(rows || [])
      );
    });

 
    const allInsuranceEntries = await new Promise((resolve) => {
      pool.query(
        `
        SELECT employee_id, value AS achieved
        FROM entries
        WHERE kpi = 'insurance'
        AND employee_id IN (?)
        `,
        [staffIds],
        (err, rows) => resolve(rows || [])
      );
    });

    const userEntryMap = {};
    allUserEntries.forEach((e) => {
      if (!userEntryMap[e.user_id]) {
        userEntryMap[e.user_id] = {};
      }
      userEntryMap[e.user_id][e.role_kpi_mapping_id] = Number(
        e.achieved || 0
      );
    });

    const insuranceMap = {};
    allInsuranceEntries.forEach((e) => {
      insuranceMap[e.employee_id] = Number(e.achieved || 0);
    });

    const result = [];

   
    for (const staff of staffRows) {
      const staffId = staff.staffId;

      const achievedMap = userEntryMap[staffId] || {};
      const insuranceValue = insuranceMap[staffId] || 0;

      const staffObj = {
        staffId,
        staffName: staff.staffName,
      };

      let totalWeightageScore = 0;

      for (const kpi of kpiList) {
        const { role_kpi_mapping_id, kpi_name, weightage } = kpi;

        let achieved = 0;

        if (kpi_name.toLowerCase() === "insurance") {
          achieved = insuranceValue;
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

        staffObj[kpi_name] = {
          score: Number(score.toFixed(2)),
          achieved,
          weightage,
          weightageScore:
            kpi_name.toLowerCase() === "insurance" &&
            weightageScore === 0
              ? -2
              : Number(weightageScore.toFixed(2)),
        };
      }

      
      const finalScores = {
        total: Number(totalWeightageScore.toFixed(2)),
      };

      const hoHistory = await new Promise((resolve) => {
        getHoStaffTransferHistory(
          pool,
          period,
          staffId,
          (err, data) => {
            if (err) return resolve([]);
            resolve(data);
          }
        );
      });

      const previousHoScores =
        hoHistory?.[0]?.transfers?.map((t) =>
          Number(t.total_weightage_score)
        ) || [];

      const attHistory = await new Promise((resolve) => {
        getAttenderTransferHistory(
          pool,
          period,
          staffId,
          (err, data) => {
            if (err) return resolve([]);
            resolve(data);
          }
        );
      });

      const previousAttenderScores =
        attHistory?.[0]?.transfers?.map((t) =>
          Number(t.total_weightage_score)
        ) || [];

      const transferHistory = await getTransferKpiHistory(
        pool,
        period,
        staffId
      );

      const previousTransferScores =
        transferHistory?.all_scores?.map(Number) || [];

      const allScores = [
        ...previousHoScores,
        ...previousAttenderScores,
        ...previousTransferScores,
        finalScores.total,
      ];

      const finalAvg =
        allScores.length > 0
          ? allScores.reduce((a, b) => a + b, 0) /
            allScores.length
          : 0;
      staffObj.originalTotal= Number(totalWeightageScore.toFixed(2));
      staffObj.total = Number(finalAvg.toFixed(2));

      result.push(staffObj);
    }

    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Failed to load HO staff scores",
    });
  }
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
    (s) => s.kpi_name.toLowerCase() !== "insurance",
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
              },
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
              },
            );
          }
        },
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

//get Dashboard Data
performanceMasterRouter.post(
  "/get-Total-Ho_staff-details",
  async (req, res) => {
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
          },
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
          },
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
        }),
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
          },
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
  },
);

//function to calculate hod calculation
const calculateHodAllScores = async (
  pool,
  period,
  hod_id,
  role,
  calculateStaffScores,
  calculateBMScores,
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
        },
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
        },
      );
    });

    const staffScores = await Promise.all(
      staffIds.map((id) =>
        calculateStaffScores(period, id, "HO_STAFF").catch(() => ({
          total: 0,
        })),
      ),
    );

    const staffAvgScore = Number(
      (
        staffScores.reduce((s, v) => s + (v.total || 0), 0) /
        (staffScores.length || 1)
      ).toFixed(2),
    );

    const branchCodes = await new Promise((resolve, reject) => {
      pool.query(
        "SELECT code FROM branches WHERE incharge_id = ?",
        [hod_id],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows.map((r) => r.code));
        },
      );
    });

    const bmTotals = await Promise.all(
      branchCodes.map((code) =>
        calculateBMScores(period, code)
          .then((res) => Number((res.total || 0).toFixed(2)))
          .catch(() => 0),
      ),
    );

    const branchAvgScore = Number(
      (bmTotals.reduce((s, v) => s + v, 0) / (bmTotals.length || 1)).toFixed(2),
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
            (r) => (map[r.role_kpi_mapping_id] = Number(r.total || 0)),
          );
          resolve(map);
        },
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
        },
      );
    });

    const getKpiVal = (name) => userKpiValues[kpiMap[name]?.id] || 0;

    const Cleanliness = getKpiVal("HO Building Cleanliness");
    const Management = getKpiVal("Management  Discretion");
    const InternalAudit = getKpiVal("Internal Audit performance");
    const IT = getKpiVal("IT");
    const InsuranceBusinessDevelopment = getKpiVal(
      "Insurance Business Development",
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

//calculate AGM-DGM Score
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
        },
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
          calculateBMScores,
        );

        return {
          hod_id: agm.hod_id,
          name: agm.name,
          role: agm.role,
          ...scoreData,
        };
      }),
    );

    res.json(results);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to calculate AGM/DGM scores" });
  }
});

//Deputation Staff Report API
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
      pool.query(`SELECT code, name FROM branches`, (err, rows) => {
        if (err) return reject(err);
        resolve(rows);
      });
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
              },
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
      }),
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
  pool.query(
    `
 SELECT 
    k.kpi,

    CASE 
        WHEN k.kpi = 'recovery'
        AND emp.transfer_date IS NULL
        THEN COALESCE(t.amount,0)

        WHEN k.kpi = 'recovery'
        AND emp.transfer_date BETWEEN 
            STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
            AND
            STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')
        THEN COALESCE(a.amount,0)

        ELSE COALESCE(a.amount,0)
    END AS target,

    COALESCE(w.weightage, 0) AS weightage,
    COALESCE(e.total_achieved, 0) AS achieved

FROM (
    SELECT 'deposit' AS kpi
    UNION ALL SELECT 'loan_gen'
    UNION ALL SELECT 'loan_amulya'
    UNION ALL SELECT 'audit'
    UNION ALL SELECT 'recovery'
    UNION ALL SELECT 'insurance'
) k

JOIN users emp 
ON emp.id = ?

LEFT JOIN allocations a 
    ON k.kpi = a.kpi
    AND a.period = ?
    AND a.user_id = emp.id
    AND a.branch_id = ?

LEFT JOIN targets t
    ON t.kpi = k.kpi
    AND t.period = ?
    AND t.branch_id = emp.branch_id

LEFT JOIN weightage w 
    ON k.kpi = w.kpi

LEFT JOIN (
    SELECT 
        e.kpi,
        SUM(e.value) AS total_achieved
    FROM entries e
    JOIN users emp2 ON emp2.id = ?
    JOIN users bm 
        ON bm.branch_id = emp2.branch_id 
        AND bm.role = 'BM'
    WHERE 
        e.period = ?
        AND e.status = 'Verified'
        AND e.branch_id = emp2.branch_id
        AND (
            (
                e.kpi IN ('audit','recovery')
                AND (
                    (
                        emp2.transfer_date IS NULL
                        AND e.employee_id = bm.id
                    )
                    OR
                    (
                        emp2.transfer_date BETWEEN 
                        STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                        AND
                        STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')
                        AND e.employee_id = emp2.id
                    )
                )
            )
            OR
            (
                e.kpi NOT IN ('audit','recovery')
                AND e.employee_id = emp2.id
            )
        )
    GROUP BY e.kpi
) e 
ON k.kpi = e.kpi
  `,
    [period,period, employeeId,period, branchId,period,  employeeId,period,period,period],
    (err, results) => {
     
      
      if (err) return callback(err);

      pool.query(
        `
      SELECT t.kpi, t.amount AS target, w.weightage, e.total_achieved AS achieved
      FROM (SELECT kpi, amount FROM targets WHERE period = ? AND branch_id = ? 
            AND kpi IN ('deposit','loan_gen','loan_amulya','recovery','audit','insurance')) AS t
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

              const scores = {};
              let totalWeightageScore = 0;

              results.forEach((row) => {
                if (row.kpi === "recovery") {
                  const br = branchResults.find((b) => b.kpi === "recovery");
                  if (br) {
                    row.target = br.target;
                    row.achieved = br.achieved;
                  }
                }

                const score = calculateScore(row.kpi, row.achieved, row.target);

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

              getHoStaffTransferHistory(
                pool,
                period,
                employeeId,
                (err, hoHistory) => {
                  if (err) return callback(err);

                  const previousHoScores =
                    hoHistory?.[0]?.transfers?.map(
                      (t) => t.total_weightage_score,
                    ) || [];

                  getAttenderTransferHistory(
                    pool,
                    period,
                    employeeId,
                    (err2, attHistory) => {
                      if (err2) return callback(err2);

                      const previousAttenderScores =
                        attHistory?.[0]?.transfers?.map(
                          (t) => t.total_weightage_score,
                        ) || [];

                      getTransferKpiHistory(pool, period, employeeId)
                        .then((transferHistory) => {
                          const previousTransferScores =
                            transferHistory?.all_scores || [];

                          const allScores = [
                            ...previousHoScores,
                            ...previousAttenderScores,
                            ...previousTransferScores,
                            totalWeightageScore,
                          ];

                          const finalAvg =
                            allScores.length > 0
                              ? allScores.reduce((a, b) => a + b, 0) /
                                allScores.length
                              : 0;

                          scores.total = Number(finalAvg.toFixed(2));
                          
                          
                          callback(null, scores);
                        })
                        .catch(callback);
                    },
                  );
                },
              );
            },
          );
        },
      );
    },
  );
}
// helper function for the calculateScore accroding to the kpis
function calculateScore(kpi, achieved, target) {
  let outOf10 = 0;
  if (!target) return 0;

  const ratio = achieved / target;
  const auditRatio = kpi === "audit" ? ratio : 0;
 const recoveryRatio = kpi === "recovery" ? ratio : 0;
      

  switch (kpi) {
    case "deposit":
    case "loan_gen":
      outOf10 = ratio <= 1 ? ratio * 10 : ratio < 1.25 ? 10 : (auditRatio >= 0.75 && recoveryRatio >= 0.75) ? 12.5 : 10;
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
  callback,
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

    pool.query(userEntryQuery, [period, ho_staff_id], (err2, userEntries) => {
      if (err2) return callback(err2);

      const insuranceQuery = `
          SELECT value AS achieved 
          FROM entries 
          WHERE kpi='insurance' AND employee_id = ?
        `;

      pool.query(insuranceQuery, [ho_staff_id], (err3, insuranceRows) => {
        if (err3) return callback(err3);

        const insuranceValue = insuranceRows.length
          ? Number(insuranceRows[0].achieved)
          : 0;

        const achievedMap = {};
        userEntries.forEach(
          (e) => (achievedMap[e.role_kpi_mapping_id] = e.achieved),
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

        getHoStaffTransferHistory(
          pool,
          period,
          ho_staff_id,
          (err, hoHistory) => {
            if (err) return callback(err);

            const previousHoScores =
              hoHistory?.[0]?.transfers?.map((t) => t.total_weightage_score) ||
              [];

            getAttenderTransferHistory(
              pool,
              period,
              ho_staff_id,
              (err2, attHistory) => {
                if (err2) return callback(err2);

                const previousAttenderScores =
                  attHistory?.[0]?.transfers?.map(
                    (t) => t.total_weightage_score,
                  ) || [];

                getTransferKpiHistory(pool, period, ho_staff_id)
                  .then((transferHistory) => {
                    const previousTransferScores =
                      transferHistory?.all_scores || [];

                    const allScores = [
                      ...previousHoScores,
                      ...previousAttenderScores,
                      ...previousTransferScores,
                      totalWeightageScore,
                    ];

                    const finalAvg =
                      allScores.length > 0
                        ? allScores.reduce((a, b) => a + b, 0) /
                          allScores.length
                        : 0;

                    scores.total = Number(finalAvg.toFixed(2));

                    callback(null, scores);
                  })
                  .catch(callback);
              },
            );
          },
        );
      });
    });
  });
}
//Transfer BM logic for the Report
function getTransferBmScores(pool, period, branchId, callback) {

  function getFY(period) {
    const [s, e] = period.split("-");
    const sy = parseInt(s);
    const ey = sy - (sy % 100) + parseInt(e);
    return {
      start: new Date(Date.UTC(sy, 3, 1)),
      end: new Date(Date.UTC(ey, 2, 31)),
    };
  }

  function monthDiffStart(d1, d2) {
    return Math.max(
      0,
      (d2.getFullYear() - d1.getFullYear()) * 12 +
      (d2.getMonth() - d1.getMonth()) +
      1
    );
  }

  const fy = getFY(period);

  pool.query(
    `SELECT id FROM users 
     WHERE branch_id=? AND role='BM' 
     ORDER BY transfer_date DESC`,
    [branchId],
    (err, BMRows) => {
      if (err) return callback(err);

      const BMID = BMRows?.[0]?.id || BMRows?.[1]?.id || 0;
      if (!BMID) return callback(null, []);

      pool.query(
        `SELECT * FROM bm_transfer_target 
         WHERE staff_id=? AND period=? 
         ORDER BY id DESC LIMIT 1`,
        [BMID, period],
        (err, bmRows) => {
          if (err) return callback(err);
          if (!bmRows.length) return callback(null, []);

          const bm = bmRows[0];
          const transferDate = new Date(bm.transfer_date);
          const totalMonths = monthDiffStart(transferDate, fy.end);

          const bmTargets = {
            deposit: bm.deposit_target || 0,
            loan_gen: bm.loan_gen_target || 0,
            loan_amulya: bm.loan_amulya_target || 0,
            audit: bm.audit_target || 0,
            recovery: bm.recovery_target || 0,
            insurance: bm.insurance_target || 0,
          };

          const monthStart = new Date(
            transferDate.getFullYear(),
            transferDate.getMonth(),
            1
          );

          pool.query(
            `SELECT kpi, SUM(value) AS achieved
             FROM entries
             WHERE branch_id=? 
             AND period=? 
             AND status='Verified'
             AND date >= ? AND date <= ?
             GROUP BY kpi`,
            [branchId, period, monthStart, fy.end],
            (err, entryRows) => {
              if (err) return callback(err);

              const achievedMap = {};
              entryRows.forEach(
                (r) => (achievedMap[r.kpi] = r.achieved || 0)
              );

              pool.query(
                `SELECT SUM(value) AS achieved
                 FROM entries
                 WHERE period=? AND employee_id=? 
                 AND kpi='insurance'
                 AND status='Verified'`,
                [period, BMID],
                (err, insRows) => {
                  if (err) return callback(err);

                  achievedMap["insurance"] =
                    insRows?.[0]?.achieved || 0;

                  pool.query(
                    `SELECT * FROM weightage`,
                    (err, wRows) => {
                      if (err) return callback(err);

                      const weightageMap = {};
                      wRows.forEach(
                        (w) => (weightageMap[w.kpi] = w.weightage)
                      );

                      const bmKpis = [
                        "deposit",
                        "loan_gen",
                        "loan_amulya",
                        "recovery",
                        "audit",
                        "insurance",
                      ];

                      function calculateScores(cap) {
                        const scores = {};
                        let total = 0;

                        bmKpis.forEach((kpi) => {
                          const target = bmTargets[kpi] || 0;
                          const achieved = achievedMap[kpi] || 0;
                          const weight = weightageMap[kpi] || 0;

                          let outOf10 = 0;

                          if (target > 0) {
                            const ratio = achieved / target;
                            const auditRatio = kpi === "audit" ? ratio : 0;
                            const recoveryRatio = kpi === "recovery" ? ratio : 0;

                            switch (kpi) {
                              case "deposit":
                              case "loan_gen":
                              case "loan_amulya":  
                              if (ratio <= 1) outOf10 = ratio * 10;
                              else if (ratio < 1.25) outOf10 = 10;
                              else if (
                                auditRatio >= 0.75 &&
                                recoveryRatio >= 0.75
                              )
                                outOf10 = 12.5;
                              else outOf10 = 10;
                              break;

                             
                              case "recovery":
                              case "audit":
                                outOf10 = ratio <= 1 ? ratio * 10 : 12.5;
                                break;

                              case "insurance":
                                if (ratio === 0) outOf10 = -2;
                                else if (ratio <= 1) outOf10 = ratio * 10;
                                else outOf10 = 12.5;
                                break;
                            }
                          }

                          outOf10 = Math.max(0, Math.min(cap, outOf10));

                          let weightScore =
                            kpi === "insurance" && outOf10 === 0
                              ? -2
                              : (outOf10 * weight) / 100;

                          scores[kpi] = {
                            score: outOf10,
                            target,
                            achieved,
                            weightage: weight,
                            weightageScore: weightScore,
                          };

                          total += weightScore;
                        });

                        scores["total"] = total;
                        return scores;
                      }

                      const prelim = calculateScores(12.5);

                      const cap =
                        prelim.total > 10 &&
                        prelim.insurance.score < 7.5 &&
                        prelim.recovery.score < 7.5
                          ? 10
                          : 12.5;

                      const finalScores = calculateScores(cap);
                      const totalWeightageScore = finalScores.total;

                      

                      getHoStaffTransferHistory(pool, period, BMID, (errHo, hoHistory) => {
                        if (errHo) return callback(errHo);

                        const previousHoScores =
                          hoHistory?.[0]?.transfers?.map(
                            (t) => t.total_weightage_score
                          ) || [];

                        getAttenderTransferHistory(pool, period, BMID, (errAtt, attHistory) => {
                          if (errAtt) return callback(errAtt);

                          const previousAttenderScores =
                            attHistory?.[0]?.transfers?.map(
                              (t) => t.total_weightage_score
                            ) || [];

                          getTransferKpiHistory(pool, period, BMID)
                            .then((transferHistory) => {

                              const previousTransferScores =
                                transferHistory?.all_scores || [];

                              const allScores = [
                                ...previousHoScores,
                                ...previousAttenderScores,
                                ...previousTransferScores,
                                totalWeightageScore,
                              ];
                              

                              const finalAvg =
                                allScores.length > 0
                                  ? allScores.reduce((a, b) => a + b, 0) /
                                    allScores.length
                                  : 0;
                               finalScores.total=finalAvg;
                              return callback(null, {
                                ...finalScores,
                                totalMonthsWorked: totalMonths,
                                previousHoScores,
                                previousAttenderScores,
                                previousTransferScores,
                                finalAverageScore: finalAvg,
                              });

                            })
                            .catch((errTransfer) => callback(errTransfer));
                        });
                      });

                    }
                  );
                }
              );
            }
          );
        }
      );
    }
  );
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

      // KPI LOOP (UNCHANGED)
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

      

      const hoHistory = await new Promise((resolve) => {
        getHoStaffTransferHistory(pool, period, user.id, (err, data) => {
          if (err) return resolve([]);
          resolve(data);
        });
      });

      const previousHoScores =
        hoHistory?.[0]?.transfers?.map(t => t.total_weightage_score) || [];

      const attHistory = await new Promise((resolve) => {
        getAttenderTransferHistory(pool, period, user.id, (err, data) => {
          if (err) return resolve([]);
          resolve(data);
        });
      });

      const previousAttenderScores =
        attHistory?.[0]?.transfers?.map(t => t.total_weightage_score) || [];

      const transferHistory = await getTransferKpiHistory(pool, period, user.id);
      const previousTransferScores =
        transferHistory?.all_scores || [];

      const allScores = [
        ...previousHoScores,
        ...previousAttenderScores,
        ...previousTransferScores,
        totalScore
      ];

      const finalAvg =
        allScores.length > 0
          ? allScores.reduce((a, b) => a + b, 0) / allScores.length
          : 0;

      totalScore = Number(finalAvg.toFixed(2));

      return {
        staffId: user.id,
        staffName: user.name,
        total: totalScore,
        kpis: finalKpis,
      };
    })
  );

  return response;
}
//helper function for get all users
function getAllUser(pool, period, cb) {

  const startYear = parseInt(period.substring(0, 4));

  const startDate = `${startYear}-04-01`;
  const endDate = `${startYear + 1}-04-01`;  

  const query = `
    SELECT 
        u.id, 
        u.username, 
        u.name, 
        u.role,
        u.branch_id, 
        u.hod_id,
        b.name AS branch_name,
        u.transfer_date,

        CASE 
            WHEN u.transfer_date >= ?
             AND u.transfer_date < ?
            THEN 1 
            ELSE 0 
        END AS transfer_flag

    FROM users u
    LEFT JOIN branches b ON u.branch_id = b.code
    WHERE u.role IN (
        "BM","Clerk","HO_STAFF","GM",
        "AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE",
        "Attender"
    )
    AND (u.resign IS NULL OR u.resign != 1)
  `;

  pool.query(query, [startDate, endDate], cb);
}
function getAllUsers(pool, cb) {
  pool.query(
    `
    SELECT u.id, u.username, u.name, u.role,
           u.branch_id, u.hod_id,
           b.name AS branch_name
    FROM users u
    LEFT JOIN branches b ON u.branch_id = b.code
    WHERE u.role IN (
      "BM","Clerk","HO_STAFF","GM",
      "AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE",
      "Attender"
    )
    `,
    cb,
  );
}
//give the BM Data only form this api
performanceMasterRouter.post("/usersBM", async (req, res) => {
  const { period } = req.body;

  getAllUser(pool, period, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter((u) => u.role === "BM");

    try {
      const result = await Promise.all(
        list.map(async (u) => {
          const branchId = Number(u.branch_id);

          let score;
          if (u.transfer_flag === 1) {
            const scoreTransfer = await new Promise((resolve, reject) => {
              getTransferBmScores(pool, period, branchId, (err, data) => {
                if (err) return reject(err);
                resolve(data);
              });
            });

            score =
              scoreTransfer && scoreTransfer.length > 0
                ? scoreTransfer
                : await calculateBMScoresFortrasfer(String(period), branchId);
          } else {
            score = await calculateBMScores(String(period), branchId);
          }

          return {
            ...u,
            bmTotalScore: score?.total ?? 0,
          };
        }),
      );

      res.json({ users: result });
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: "Failed to calculate scores" });
    }
  });
});

//give the clerk Data of 1/4
performanceMasterRouter.post("/usersClerk/part1", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter((u) => u.role === "Clerk");

    const quarter = Math.ceil(list.length / 4);
    const part = list.slice(0, quarter);

    const result = await Promise.all(
      part.map(
        (u) =>
          new Promise((resolve) => {
            calculateStaffScoresCb(
              period,
              u.id,
              u.branch_id,
              (err, clerkScore) => {
                resolve({
                  ...u,
                  bmTotalScore: err ? 0 : (clerkScore?.total ?? 0),
                });
              },
            );
          }),
      ),
    );

    res.json({ users: result, part: 1 });
  });
});
//give the clerk Data of 1/2
performanceMasterRouter.post("/usersClerk/part2", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter((u) => u.role === "Clerk");

    const quarter = Math.ceil(list.length / 4);
    const part = list.slice(quarter, quarter * 2);

    const result = await Promise.all(
      part.map(
        (u) =>
          new Promise((resolve) => {
            calculateStaffScoresCb(
              period,
              u.id,
              u.branch_id,
              (err, clerkScore) => {
                resolve({
                  ...u,
                  bmTotalScore: err ? 0 : (clerkScore?.total ?? 0),
                });
              },
            );
          }),
      ),
    );

    res.json({ users: result, part: 2 });
  });
});
//give the clerk Data of 3/4
performanceMasterRouter.post("/usersClerk/part3", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool,async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter((u) => u.role === "Clerk");

    const quarter = Math.ceil(list.length / 4);
    const part = list.slice(quarter * 2, quarter * 3);

    const result = await Promise.all(
      part.map(
        (u) =>
          new Promise((resolve) => {
            calculateStaffScoresCb(
              period,
              u.id,
              u.branch_id,
              (err, clerkScore) => {
                resolve({
                  ...u,
                  bmTotalScore: err ? 0 : (clerkScore?.total ?? 0),
                });
              },
            );
          }),
      ),
    );

    res.json({ users: result, part: 3 });
  });
});
//give the clerk Data of all remaing staff
performanceMasterRouter.post("/usersClerk/part4", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter((u) => u.role === "Clerk");

    const quarter = Math.ceil(list.length / 4);
    const part = list.slice(quarter * 3);

    const result = await Promise.all(
      part.map(
        (u) =>
          new Promise((resolve) => {
            calculateStaffScoresCb(
              period,
              u.id,
              u.branch_id,
              (err, clerkScore) => {
                resolve({
                  ...u,
                  bmTotalScore: err ? 0 : (clerkScore?.total ?? 0),
                });
              },
            );
          }),
      ),
    );

    res.json({ users: result, part: 4 });
  });

});
//get all HO_STAFF Score
performanceMasterRouter.post("/usersHOStaff", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter((u) => u.role === "HO_STAFF");

    const result = await Promise.all(
      list.map(
        (u) =>
          new Promise((resolve) => {
            calculateSpecificAllStaffScoresCb(
              String(period),
              Number(u.id),
              u.role,
              (err, hoStaffScore) => {
                resolve({
                  ...u,
                  bmTotalScore: err ? 0 : (hoStaffScore?.total ?? 0),
                });
              },
            );
          }),
      ),
    );

    res.json({ users: result });
  });
});
//get all Attender Score
performanceMasterRouter.post("/usersAttender", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const list = users.filter((u) => u.role === "Attender");

    const result = await Promise.all(
      list.map(async (u) => {
        try {
          const attenders = await getBranchAttendersScores(
            String(period),
            Number(u.branch_id),
            Number(u.hod_id),
          );

          const current = attenders.find((a) => a.staffId === u.id);

          return {
            ...u,
            bmTotalScore: current?.total ?? 0,
          };
        } catch (err) {
          return {
            ...u,
            bmTotalScore: 0,
          };
        }
      }),
    );

    res.json({ users: result });
  });
});
//get all AGM/DGM/GM Score
performanceMasterRouter.post("/usersAgmGm", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, async (err, users) => {
    if (err) {
      return res.status(500).json({ error: "Failed to load users" });
    }

    const agmUsers = users.filter((u) =>
      ["AGM", "DGM", "AGM_IT", "AGM_AUDIT", "AGM_INSURANCE"].includes(u.role),
    );

    const gmUser = users.find((u) => u.role === "GM") || null;

    const agmScores = [];

    const agmResults = await Promise.all(
      agmUsers.map(async (u) => {
        try {
          const hodScore = await calculateHodAllScores(
            pool,
            String(period),
            Number(u.id),
            u.role,
            calculateStaffScores,
            calculateBMScores,
          );

          const total = hodScore?.total ?? 0;
          agmScores.push(total);

          return {
            ...u,
            bmTotalScore: total,
          };
        } catch (err) {
          agmScores.push(0);
          return {
            ...u,
            bmTotalScore: 0,
          };
        }
      }),
    );

    /* ---------- GM CALCULATION (UNCHANGED LOGIC) ---------- */
    const results = [...agmResults];

    if (gmUser && agmScores.length) {
      const per = 100 / agmScores.length;
      const gmScore = agmScores.reduce((sum, a) => sum + (a * per) / 100, 0);

      results.push({
        ...gmUser,
        bmTotalScore: Number(gmScore.toFixed(2)),
      });
    }

    res.json({ users: results });
  });
});
//function for get single ho_staff history code 
export function getHoStaffTransferHistory(pool, period, ho_staff_id, callback) {
  const role = "HO_STAFF";

  const staffQuery = `
    SELECT id, name, resign
    FROM users
    WHERE id = ?
  `;

  pool.query(staffQuery, [ho_staff_id], (err0, staffRows) => {
    if (err0 || !staffRows.length) {
      return callback("Staff fetch failed");
    }

    const staff = staffRows[0];

    const kpiWeightageQuery = `
      SELECT km.kpi_name, rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
    `;

    pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
      if (err) {
        return callback("Weightage fetch failed");
      }

      const transferQuery = `
        SELECT ho.*, u.name AS hod_name, s.name AS old_hod_name
        FROM ho_staff_transfer ho
        LEFT JOIN users u ON ho.hod_id = u.id
        LEFT JOIN users s ON ho.old_hod_id = s.id
        WHERE ho.staff_id = ?
        AND ho.period = ?
        ORDER BY ho.transfer_date ASC
      `;

      pool.query(transferQuery, [ho_staff_id, period], (err2, rows) => {
        if (err2) {
          return callback("Transfer fetch failed");
        }

        const transfers = [];
        const branch_avg_kpi = {};
        let totalMonths = 0;
        let counter = {};

        let allTotals = [];

        rows.forEach((t) => {
          const achievedMap = {
            "Alloted Work": Number(t.Alloted_Work) || 0,
            "Discipline & Time Management":
              Number(t["Discipline_&_Time_Management"]) || 0,
            "General Work Performance": Number(t.General_Work_Performance) || 0,
            "Branch Communication": Number(t.Branch_Communication) || 0,
          };

          let total = 0;
          const scores = {};

          kpiWeightage.forEach((row) => {
            const { kpi_name, weightage } = row;
            if (kpi_name === "Insurance") return;

            const achieved = achievedMap[kpi_name] || 0;
            const ratio = achieved / weightage;

            let score = 0;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;

            const weightageScore = (score * weightage) / 100;
            total += weightageScore;

            scores[kpi_name] = {
              achieved,
              weightage,
              score: Number(score.toFixed(2)),
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          });

          const branch = t.old_hod_name || "UNKNOWN";

          counter[branch] = (counter[branch] || 0) + 1;
          const uniqueKey = `${branch}_${counter[branch]}`;

          branch_avg_kpi[uniqueKey] = {
            avg_kpi: Number(total.toFixed(2)),
            months: 1,
          };

          totalMonths += 1;
          allTotals.push(total);

          transfers.push({
            ...scores,
            total_weightage_score: Number(total.toFixed(2)),
            transfer_date: t.transfer_date,
            hod_name: t.hod_name,
            old_hod_name: t.old_hod_name,
          });
        });

        let avg_kpi = 0;
        if (allTotals.length > 0) {
          avg_kpi = allTotals.reduce((a, b) => a + b, 0) / allTotals.length;
        }

        const response = [
          {
            staff_id: staff.id,
            name: staff.name,
            period,
            resigned: staff.resign,
            transfers,
            branch_avg_kpi,
            total_months: totalMonths,
            avg_kpi: Number(avg_kpi.toFixed(2)),
          },
        ];

        callback(null, response);
      });
    });
  });
}
//function for get single attender history code 
export function getAttenderTransferHistory(pool, period, staff_id, callback) {
  const role = "Attender";

  const staffQuery = `
    SELECT id, name, resign
    FROM users
    WHERE id = ?
  `;

  pool.query(staffQuery, [staff_id], (err0, staffRows) => {
    if (err0 || !staffRows.length) {
      return callback("Staff fetch failed");
    }

    const staff = staffRows[0];

    const kpiWeightageQuery = `
      SELECT km.kpi_name, rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
    `;

    pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
      if (err) {
        return callback("Weightage fetch failed");
      }

      const transferQuery = `
        SELECT ho.*, 
            u.name AS hod_name, 
            s.name AS old_hod_name,
            b1.name AS new_branch,
            b2.name AS old_branch
        FROM attender_transfer ho
        LEFT JOIN users u ON ho.hod_id = u.id
        LEFT JOIN users s ON ho.old_hod_id = s.id
        LEFT JOIN branches b1 
          ON ho.branch_id COLLATE utf8mb4_unicode_ci = b1.code
        LEFT JOIN branches b2 
          ON ho.old_branch_id COLLATE utf8mb4_unicode_ci = b2.code
        WHERE ho.staff_id = ?
        AND ho.period COLLATE utf8mb4_unicode_ci = ?
        ORDER BY ho.transfer_date ASC;
      `;

      pool.query(transferQuery, [staff_id, period], (err2, rows) => {
        if (err2) {
          return callback("Transfer fetch failed");
        }

        const transfers = [];
        const branch_avg_kpi = {};
        let totalMonths = 0;
        let counter = {};

        let allTotals = [];

        rows.forEach((t) => {
          const achievedMap = {
            Cleanliness: Number(t.Cleanliness) || 0,
            "Attitude, Behavior & Discipline":
              Number(t["Attitude_Behavior_&_Discipline"]) || 0,
          };

          let total = 0;
          const scores = {};

          kpiWeightage.forEach((row) => {
            const { kpi_name, weightage } = row;
            if (kpi_name === "Insurance Target") return;

            const achieved = achievedMap[kpi_name] || 0;
            const ratio = achieved / weightage;

            let score = 0;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;

            const weightageScore = (score * weightage) / 100;
            total += weightageScore;

            scores[kpi_name] = {
              achieved,
              weightage,
              score: Number(score.toFixed(2)),
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          });

          const branch = t.old_hod_name || t.old_branch || "UNKNOWN";

          counter[branch] = (counter[branch] || 0) + 1;
          const uniqueKey = `${branch}_${counter[branch]}`;

          branch_avg_kpi[uniqueKey] = {
            avg_kpi: Number(total.toFixed(2)),
            months: 1,
          };

          totalMonths += 1;
          allTotals.push(total);

          transfers.push({
            ...scores,
            total_weightage_score: Number(total.toFixed(2)),
            transfer_date: t.transfer_date,
            hod_name: t.hod_name,
            old_hod_name: t.old_hod_name,
            new_branch_name: t.new_branch,
            old_branch_name: t.old_branch,
            old_designation: t.old_designation,
            new_designation: t.new_designation,
          });
        });

        let avg_kpi = 0;
        if (allTotals.length > 0) {
          avg_kpi = allTotals.reduce((a, b) => a + b, 0) / allTotals.length;
        }

        const response = [
          {
            staff_id: staff.id,
            name: staff.name,
            period,
            resigned: staff.resign,
            transfers,
            branch_avg_kpi,
            total_months: totalMonths,
            avg_kpi: Number(avg_kpi.toFixed(2)),
          },
        ];

        callback(null, response);
      });
    });
  });
}

//ho staff transfer history with kpi scores
performanceMasterRouter.get("/ho_staff_transfer_history", (req, res) => {
  const { period, ho_staff_id } = req.query;
  const role = "HO_STAFF";

  if (!period || !ho_staff_id) {
    return res.status(400).json({
      error: "period, ho_staff_id are required",
    });
  }

  const staffQuery = `
    SELECT id, name, resign,resign_date
    FROM users
    WHERE id = ?
  `;

  pool.query(staffQuery, [ho_staff_id], (err0, staffRows) => {
    if (err0 || !staffRows.length) {
      return res.status(500).json({ error: "Staff fetch failed" });
    }

    const staff = staffRows[0];

    const kpiWeightageQuery = `
      SELECT km.kpi_name, rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
    `;

    pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
      if (err) {
        return res.status(500).json({ error: "Weightage fetch failed" });
      }

      const transferQuery = `
        SELECT ho.*, u.name AS hod_name, s.name AS old_hod_name
        FROM ho_staff_transfer ho
        LEFT JOIN users u ON ho.hod_id = u.id
        LEFT JOIN users s ON ho.old_hod_id = s.id
        WHERE ho.staff_id = ?
        AND ho.period = ?
        ORDER BY ho.transfer_date ASC
      `;

      pool.query(transferQuery, [ho_staff_id, period], (err2, rows) => {
        if (err2) {
          return res.status(500).json({ error: "Transfer fetch failed" });
        }

        const transfers = [];
        const branch_avg_kpi = {};
        let totalMonths = 0;
        let counter = {};

        rows.forEach((t) => {
          const achievedMap = {
            "Alloted Work": Number(t.Alloted_Work) || 0,
            "Discipline & Time Management":
              Number(t["Discipline_&_Time_Management"]) || 0,
            "General Work Performance": Number(t.General_Work_Performance) || 0,
            "Branch Communication": Number(t.Branch_Communication) || 0,
          };

          let total = 0;
          const scores = {};

          kpiWeightage.forEach((row) => {
            const { kpi_name, weightage } = row;
            if (kpi_name === "Insurance") return;

            const achieved = achievedMap[kpi_name] || 0;
            const ratio = achieved / weightage;

            let score = 0;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;

            const weightageScore = (score * weightage) / 100;
            total += weightageScore;

            scores[kpi_name] = {
              achieved,
              weightage,
              score: Number(score.toFixed(2)),
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          });

          const branch = t.old_hod_name || "UNKNOWN";

          counter[branch] = (counter[branch] || 0) + 1;
          const uniqueKey = `${branch}_${counter[branch]}`;

          branch_avg_kpi[uniqueKey] = {
            avg_kpi: Number(total.toFixed(2)),
            months: 1,
          };

          totalMonths += 1;

          transfers.push({
            ...scores,
            total_weightage_score: Number(total.toFixed(2)),
            transfer_date: t.transfer_date,
            hod_name: t.hod_name,
            old_hod_name: t.old_hod_name,
          });
        });

        const response = [
          {
            staff_id: staff.id,
            name: staff.name,
            period,
            resigned: staff.resign,
            resign_date: staff.resign_date,
            transfers,
            branch_avg_kpi,
            total_months: totalMonths,
          },
        ];

        return res.json(response);
      });
    });
  });
});

//departmentwise report
performanceMasterRouter.post(
  "/getAllBranchesScoreSectionsWise",
  async (req, res) => {
    const { period, department } = req.body;

    if (!period) {
      return res.status(400).json({ error: "period is required" });
    }

    try {
      const branchReportData = {};

      const branches = await new Promise((resolve, reject) => {
        pool.query(`SELECT code, name FROM branches`, (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        });
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
                },
              );
            });

            const bm = bmRows[0] || null;

            const branchScore = await calculateBMScores(period, branch.code);

            const deptKey = String(department || "")
              .trim()
              .toLowerCase();

            const matchedKey = Object.keys(branchScore || {}).find(
              (key) => key.toLowerCase() === deptKey,
            );
            const formattedDepartment = department
              ? department.charAt(0).toUpperCase() + department.slice(1)
              : null;
            return {
              ...branch,
              bmId: bm?.PF_NO ?? null,
              bmName: bm?.name ?? null,
              department: formattedDepartment,
              departmentData: matchedKey ? branchScore[matchedKey] : null,
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
        }),
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
  },
);

//Attender transfer history wothb this kpis
performanceMasterRouter.get("/attender_transfer_history", (req, res) => {
  const { period, staff_id } = req.query;
  const role = "Attender";

  if (!period || !staff_id) {
    return res.status(400).json({
      error: "period, staff_id are required",
    });
  }

  const staffQuery = `
    SELECT id, name, resign,resign_date
    FROM users
    WHERE id = ?
  `;

  pool.query(staffQuery, [staff_id], (err0, staffRows) => {
    if (err0 || !staffRows.length) {
      return res.status(500).json({ error: "Staff fetch failed" });
    }

    const staff = staffRows[0];

    const kpiWeightageQuery = `
      SELECT km.kpi_name, rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
    `;

    pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
      if (err) {
        return res.status(500).json({ error: "Weightage fetch failed" });
      }

      const transferQuery = `
          SELECT ho.*, 
              u.name AS hod_name, 
              s.name AS old_hod_name,
              b1.name AS new_branch,
              b2.name AS old_branch
        FROM attender_transfer ho
        LEFT JOIN users u ON ho.hod_id = u.id
        LEFT JOIN users s ON ho.old_hod_id = s.id
        LEFT JOIN branches b1 
          ON ho.branch_id COLLATE utf8mb4_unicode_ci = b1.code
        LEFT JOIN branches b2 
          ON ho.old_branch_id COLLATE utf8mb4_unicode_ci = b2.code
        WHERE ho.staff_id = ?
        AND ho.period COLLATE utf8mb4_unicode_ci = ?
        ORDER BY ho.transfer_date ASC;
        `;

      pool.query(transferQuery, [staff_id, period], (err2, rows) => {
        if (err2) {
          return res.status(500).json({ error: "Transfer fetch failed" });
        }

        const transfers = [];
        const branch_avg_kpi = {};
        let totalMonths = 0;
        let counter = {};

        rows.forEach((t) => {
          const achievedMap = {
            Cleanliness: Number(t.Cleanliness) || 0,
            "Attitude, Behavior & Discipline":
              Number(t["Attitude_Behavior_&_Discipline"]) || 0,
          };

          let total = 0;
          const scores = {};

          kpiWeightage.forEach((row) => {
            const { kpi_name, weightage } = row;
            if (kpi_name === "Insurance Target") return;

            const achieved = achievedMap[kpi_name] || 0;

            const ratio = achieved / weightage;

            let score = 0;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;

            const weightageScore = (score * weightage) / 100;
            total += weightageScore;
            
            
            scores[kpi_name] = {
              achieved,
              weightage,
              score: Number(score.toFixed(2)),
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          });

          const branch = t.old_hod_name || t.old_branch || "UNKNOWN";

          counter[branch] = (counter[branch] || 0) + 1;
          const uniqueKey = `${branch}_${counter[branch]}`;

          branch_avg_kpi[uniqueKey] = {
            avg_kpi: Number(total.toFixed(2)),
            months: 1,
          };

          totalMonths += 1;

          transfers.push({
            ...scores,
            total_weightage_score: Number(total.toFixed(2)),
            transfer_date: t.transfer_date,
            hod_name: t.hod_name,
            old_hod_name: t.old_hod_name,
            new_branch_name: t.new_branch,
            old_branch_name: t.old_branch,
            old_designation: t.old_designation,
            new_designation: t.new_designation,
          });
        });

        const response = [
          {
            staff_id: staff.id,
            name: staff.name,
            period,
            resigned: staff.resign,
            resign_date: staff.resign_date,
            transfers,
            branch_avg_kpi,
            total_months: totalMonths,
          },
        ];

        return res.json(response);
      });
    });
  });
});


