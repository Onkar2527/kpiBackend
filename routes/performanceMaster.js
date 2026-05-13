import express from "express";
import pool from "../db.js";
import { getTransferKpiHistory } from "./summary.js";
import { log } from "console";
import pLimit from "p-limit";
const bmCalculationCache = new Map();
const insuranceCache = new Map();

setInterval(() => {
  bmCalculationCache.clear();
  insuranceCache.clear();
}, 600000);

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
        (err, rows) => (err ? reject(err) : resolve(rows)),
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
          (err, rows) => (err ? reject(err) : resolve(rows)),
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
          (err, rows) => (err ? reject(err) : resolve(rows)),
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

      const target = kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

      let score = 0;
      if (achieved > 0) {
        if (kpi_name.toLowerCase() === "insurance") {
          const ratio = achieved / target;
          if (ratio <= 1) score = ratio * 10;
          else if (ratio < 1.25) score = 10;
          else score = 12.5;
        } else {
          const ratio = achieved / target;
          if (ratio <= 1) score = ratio * 10;
          else if (ratio < 1.25) score = 10;
          else score = 12.5;
        }
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
        getHoStaffTransferHistory(pool, period, ho_staff_id, (err, data) =>
          resolve(err ? [] : data),
        );
      }),

      new Promise((resolve) => {
        getAttenderTransferHistory(pool, period, ho_staff_id, (err, data) =>
          resolve(err ? [] : data),
        );
      }),

      getTransferKpiHistory(pool, period, ho_staff_id),
    ]);

    const previousHoScores =
      hoHistory?.[0]?.transfers?.map((t) => Number(t.total_weightage_score)) ||
      [];

    const previousAttenderScores =
      attHistory?.[0]?.transfers?.map((t) => Number(t.total_weightage_score)) ||
      [];

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
    scores.originalTotal = Number(totalWeightageScore.toFixed(2));
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
const kpiWeightageCache = new Map();

async function calculateStaffScores(period, staffId, role) {
  try {
    // CACHE KPI WEIGHTAGE
    let kpiWeightage = kpiWeightageCache.get(role);

    if (!kpiWeightage) {
      kpiWeightage = await new Promise((resolve, reject) => {
        pool.query(
          `
              SELECT 
                rkm.id AS role_kpi_mapping_id,
                km.kpi_name,
                rkm.weightage
              FROM role_kpi_mapping rkm
              JOIN kpi_master km
                ON km.id = rkm.kpi_id
              WHERE rkm.role = ?
              AND rkm.deleted_at IS NULL
              `,
          [role],
          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(rows);
          },
        );
      });

      kpiWeightageCache.set(role, kpiWeightage);
    }

    // PARALLEL QUERIES
    const [userEntries, allInsuranceEntries] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT 
            role_kpi_mapping_id,
            value AS achieved
          FROM user_kpi_entry
          WHERE period = ?
          AND user_id = ?
          AND deleted_at IS NULL
          `,
          [period, staffId],
          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(rows);
          },
        );
      }),

      new Promise((resolve) => {
        pool.query(
          `
          SELECT 
            value AS achieved
          FROM entries
          WHERE kpi = 'insurance'
          AND employee_id = ?
          `,
          [staffId],
          (err, rows) => {
            resolve(rows || []);
          },
        );
      }),
    ]);

    const achievedMap = {};

    // FIND INSURANCE KPI ONCE
    let insuranceKpi = null;

    for (const k of kpiWeightage) {
      if (k.kpi_name.toLowerCase() === "insurance") {
        insuranceKpi = k;
        break;
      }
    }

    // FAST MAP
    for (const e of userEntries) {
      achievedMap[e.role_kpi_mapping_id] = Number(e.achieved || 0);
    }

    // INSURANCE MAP
    if (insuranceKpi) {
      let insuranceTotal = 0;

      for (const e of allInsuranceEntries) {
        insuranceTotal += Number(e.achieved || 0);
      }

      achievedMap[insuranceKpi.role_kpi_mapping_id] = insuranceTotal;
    }

    let totalWeightageScore = 0;

    const scores = {};

    for (const row of kpiWeightage) {
      const { role_kpi_mapping_id, kpi_name, weightage } = row;

      const achieved = achievedMap[role_kpi_mapping_id] || 0;

      const isInsurance = kpi_name.toLowerCase() === "insurance";

      const target = isInsurance ? 40000 : weightage;

      let score = 0;

      if (achieved > 0) {
        const ratio = achieved / target;

        if (ratio <= 1) {
          score = ratio * 10;
        } else if (ratio < 1.25) {
          score = 10;
        } else {
          score = 12.5;
        }
      }

      let weightageScore = (score * weightage) / 100;

      if (isInsurance && score === 0) {
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

    scores.total = Number(totalWeightageScore.toFixed(2));

    return scores;
  } catch (err) {
    throw err;
  }
}

// funtion to get single BM score calculation
// async function calculateBMScores(period, branchId) {

//   try {

//     const cacheKey = `${period}_${branchId}`;

//     if (bmCalculationCache.has(cacheKey)) {
//       return bmCalculationCache.get(cacheKey);
//     }

//     const resultsPromise = new Promise((resolve, reject) => {

//       const query = `
//       SELECT
//         k.kpi,
//         MAX(
//           CASE
//             WHEN k.kpi = 'audit' THEN 100
//             WHEN k.kpi = 'insurance' THEN COALESCE(a.amount,0)
//             ELSE COALESCE(t.amount,0)
//           END
//         ) AS target,

//         MAX(COALESCE(w.weightage,0)) AS weightage,

//         MAX(COALESCE(e.total_achieved, 0)) AS achieved

//       FROM
//       (
//         SELECT 'deposit' AS kpi UNION ALL
//         SELECT 'loan_gen' UNION ALL
//         SELECT 'loan_amulya' UNION ALL
//         SELECT 'recovery' UNION ALL
//         SELECT 'audit' UNION ALL
//         SELECT 'insurance'
//       ) k

//       LEFT JOIN users bm
//         ON bm.branch_id = ?
//         AND bm.role = 'BM'
//         AND bm.resign = 0
//         AND bm.period = ?

//       LEFT JOIN targets t
//         ON t.kpi = k.kpi
//         AND t.period = ?
//         AND t.branch_id = ?

//       LEFT JOIN allocations a
//         ON k.kpi = 'insurance'
//         AND a.user_id = bm.id
//         AND a.period = ?

//       LEFT JOIN weightage w
//         ON w.kpi = k.kpi

//       LEFT JOIN (
//         SELECT
//           e.kpi,
//           SUM(e.value) AS total_achieved

//         FROM entries e

//         LEFT JOIN users bm
//           ON bm.branch_id = ?
//           AND bm.role = 'BM'
//           AND bm.resign = 0
//           AND bm.period = ?

//         WHERE
//           e.period = ?
//           AND e.status = 'Verified'
//           AND (
//             (
//               e.kpi IN ('audit','recovery')
//               AND e.employee_id = bm.id
//             )
//             OR
//             (
//               e.kpi NOT IN ('audit','recovery')
//               AND e.branch_id = ?
//             )
//           )

//         GROUP BY e.kpi

//       ) e ON k.kpi = e.kpi

//       GROUP BY k.kpi
//       ORDER BY k.kpi;
//       `;

//       pool.query(
//         query,
//         [
//           branchId,
//           period,
//           period,
//           branchId,
//           period,
//           branchId,
//           period,
//           period,
//           branchId,
//         ],
//         (err, rows) => {

//           if (err) {
//             return reject(err);
//           }

//           resolve(rows || []);
//         }
//       );
//     });

//     const insuranceCacheKey =
//       `${period}_${branchId}_insurance`;

//     let insurancePromise;

//     if (insuranceCache.has(insuranceCacheKey)) {

//       insurancePromise =
//         Promise.resolve(
//           insuranceCache.get(
//             insuranceCacheKey
//           )
//         );
//     }

//     else {

//       insurancePromise =
//         new Promise((resolve, reject) => {

//           pool.query(
//             `
//             SELECT SUM(value) AS achieved
//             FROM entries
//             WHERE period=?
//             AND kpi='insurance'
//             AND employee_id = (
//               SELECT id
//               FROM users
//               WHERE branch_id=?
//               AND period=?
//               AND role='BM'
//               LIMIT 1
//             )
//             `,
//             [period, branchId, period],

//             (err, rows) => {

//               if (err) {
//                 return reject(err);
//               }

//               const val =
//                 rows?.[0]?.achieved || 0;

//               insuranceCache.set(
//                 insuranceCacheKey,
//                 val
//               );

//               resolve(val);
//             }
//           );
//         });
//     }

//     const [results, insuranceAchieved] =
//       await Promise.all([
//         resultsPromise,
//         insurancePromise,
//       ]);

//     let auditRatio = 0;
//     let recoveryRatio = 0;

//     for (let i = 0; i < results.length; i++) {

//       const row = results[i];

//       if (row.kpi === "insurance") {
//         row.achieved = insuranceAchieved;
//       }

//       if (row.kpi === "audit") {

//         auditRatio =
//           row.target
//             ? (row.achieved || 0) / row.target
//             : 0;
//       }

//       if (row.kpi === "recovery") {

//         recoveryRatio =
//           row.target
//             ? (row.achieved || 0) / row.target
//             : 0;
//       }
//     }

//     const calculateScores = (cap) => {

//       const scores = {};

//       let totalWeightageScore = 0;

//       for (let i = 0; i < results.length; i++) {

//         const row = results[i];

//         let outOf10 = 0;

//         const target =
//           Number(row.target) || 0;

//         const achieved =
//           Number(row.achieved) || 0;

//         const weightage =
//           Number(row.weightage) || 0;

//         let ratio = 0;

//         if (target > 0) {
//           ratio = achieved / target;
//         }

//         switch (row.kpi) {

//           case "deposit":
//           case "loan_gen":
//           case "loan_amulya":

//             if (ratio <= 1) {
//               outOf10 = ratio * 10;
//             }

//             else if (ratio < 1.25) {
//               outOf10 = 10;
//             }

//             else if (
//               auditRatio >= 0.75 &&
//               recoveryRatio >= 0.75
//             ) {
//               outOf10 = 12.5;
//             }

//             else {
//               outOf10 = 10;
//             }

//             break;

//           case "insurance":

//             if (ratio <= 0 || isNaN(ratio)) {
//               outOf10 = 0;
//             }

//             else if (ratio <= 1) {
//               outOf10 = ratio * 10;
//             }

//             else if (ratio < 1.25) {
//               outOf10 = 10;
//             }

//             else {
//               outOf10 = 12.5;
//             }

//             break;

//           case "recovery":
//           case "audit":

//             if (ratio <= 0 || isNaN(ratio)) {
//               outOf10 = 0;
//             }

//             else if (ratio <= 1) {
//               outOf10 = ratio * 10;
//             }

//             else {
//               outOf10 = 12.5;
//             }

//             break;
//         }

//         if (outOf10 > cap) {
//           outOf10 = cap;
//         }

//         else if (
//           outOf10 < 0 ||
//           isNaN(outOf10)
//         ) {
//           outOf10 = 0;
//         }

//         const weightageScore =
//           row.kpi === "insurance" &&
//           (ratio <= 0 || isNaN(ratio))
//             ? -2
//             : (outOf10 * weightage) / 100;

//         scores[row.kpi] = {
//           score: outOf10,
//           target,
//           achieved,
//           weightage,
//           weightageScore,
//         };

//         totalWeightageScore +=
//           weightageScore;
//       }

//       scores.total =
//         totalWeightageScore;

//       return scores;
//     };

//     const preliminaryScores =
//       calculateScores(12.5);

//     const insuranceScore =
//       preliminaryScores.insurance?.score || 0;

//     const recoveryScore =
//       preliminaryScores.recovery?.score || 0;

//     const cap =
//       preliminaryScores.total > 10 &&
//       insuranceScore < 7.5 &&
//       recoveryScore < 7.5
//         ? 10
//         : 12.5;

//     const finalResult =
//       calculateScores(cap);

//     bmCalculationCache.set(
//       cacheKey,
//       finalResult
//     );

//     return finalResult;

//   } catch (err) {

//     throw err;
//   }
// }
async function calculateBMScores(period, branchId = null) {
  try {
    const cacheKey = `${period}_${branchId || "ALL"}`;

    if (bmCalculationCache.has(cacheKey)) {
      return bmCalculationCache.get(cacheKey);
    }

    const results = await new Promise((resolve, reject) => {
      const query = `
        SELECT
            bm.branch_id,

            k.kpi,

            MAX(
                CASE 
                    WHEN k.kpi = 'audit'
                        THEN 100

                    WHEN k.kpi = 'insurance'
                        THEN COALESCE(a.amount,0)

                    ELSE COALESCE(t.amount,0)
                END
            ) AS target,

            MAX(COALESCE(w.weightage,0))
                AS weightage,

            MAX(COALESCE(
                e.total_achieved,
                0
            )) AS achieved

        FROM users bm

        CROSS JOIN
        (
            SELECT 'deposit' AS kpi
            UNION ALL
            SELECT 'loan_gen'
            UNION ALL
            SELECT 'loan_amulya'
            UNION ALL
            SELECT 'recovery'
            UNION ALL
            SELECT 'audit'
            UNION ALL
            SELECT 'insurance'
        ) k

        LEFT JOIN targets t
            ON t.kpi = k.kpi
            AND t.period = ?
            AND t.branch_id = bm.branch_id

        LEFT JOIN allocations a
            ON k.kpi = 'insurance'
            AND a.user_id = bm.id
            AND a.period = ?

        LEFT JOIN weightage w
            ON w.kpi = k.kpi

        LEFT JOIN
        (
            SELECT
                x.branch_id,
                x.kpi,
                SUM(x.value)
                    AS total_achieved

            FROM
            (

                SELECT
                    u.branch_id,
                    e.kpi,
                    e.value

                FROM entries e

                INNER JOIN users u
                    ON u.id = e.employee_id
                    AND u.role = 'BM'
                    AND u.resign = 0
                    AND u.period = ?

                WHERE
                    e.period = ?
                    AND e.status = 'Verified'
                    AND e.kpi IN (
                        'audit',
                        'recovery',
                        'insurance'
                    )

                UNION ALL

                SELECT
                    e.branch_id,
                    e.kpi,
                    e.value

                FROM entries e

                WHERE
                    e.period = ?
                    AND e.status = 'Verified'
                    AND e.kpi NOT IN (
                        'audit',
                        'recovery',
                        'insurance'
                    )

            ) x

            GROUP BY
                x.branch_id,
                x.kpi

        ) e
            ON e.branch_id = bm.branch_id
            AND e.kpi = k.kpi

        WHERE
            bm.role = 'BM'
            AND bm.resign = 0
            AND bm.period = ?
            ${branchId ? "AND bm.branch_id = ?" : ""}

        GROUP BY
            bm.branch_id,
            k.kpi

        ORDER BY
            bm.branch_id,
            k.kpi
        `;

      const params = [period, period, period, period, period, period];

      if (branchId) {
        params.push(branchId);
      }

      pool.query(
        query,
        params,

        (err, rows) => {
          if (err) {
            return reject(err);
          }

          resolve(rows || []);
        },
      );
    });

    const grouped = {};

    for (const row of results) {
      if (!grouped[row.branch_id]) {
        grouped[row.branch_id] = [];
      }

      grouped[row.branch_id].push(row);
    }

    const finalResponse = {};

    for (const branch in grouped) {
      const branchRows = grouped[branch];

      let auditRatio = 0;
      let recoveryRatio = 0;

      for (const row of branchRows) {
        if (row.kpi === "audit") {
          auditRatio = row.target ? row.achieved / row.target : 0;
        }

        if (row.kpi === "recovery") {
          recoveryRatio = row.target ? row.achieved / row.target : 0;
        }
      }

      const calculateScores = (cap) => {
        const scores = {};

        let totalWeightageScore = 0;

        for (const row of branchRows) {
          let outOf10 = 0;

          const target = Number(row.target) || 0;

          const achieved = Number(row.achieved) || 0;

          const weightage = Number(row.weightage) || 0;

          let ratio = 0;

          if (target > 0) {
            ratio = achieved / target;
          }

          switch (row.kpi) {
            case "deposit":
            case "loan_gen":
            case "loan_amulya":
              if (ratio <= 1) {
                outOf10 = ratio * 10;
              } else if (ratio < 1.25) {
                outOf10 = 10;
              } else if (auditRatio >= 0.75 && recoveryRatio >= 0.75) {
                outOf10 = 12.5;
              } else {
                outOf10 = 10;
              }

              break;

            case "insurance":
              if (ratio <= 0 || isNaN(ratio)) {
                outOf10 = 0;
              } else if (ratio <= 1) {
                outOf10 = ratio * 10;
              } else if (ratio < 1.25) {
                outOf10 = 10;
              } else {
                outOf10 = 12.5;
              }

              break;

            case "recovery":
            case "audit":
              if (ratio <= 0 || isNaN(ratio)) {
                outOf10 = 0;
              } else if (ratio <= 1) {
                outOf10 = ratio * 10;
              } else {
                outOf10 = 12.5;
              }

              break;
          }

          if (outOf10 > cap) {
            outOf10 = cap;
          } else if (outOf10 < 0 || isNaN(outOf10)) {
            outOf10 = 0;
          }

          const weightageScore =
            row.kpi === "insurance" && (ratio <= 0 || isNaN(ratio))
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
        }

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

      finalResponse[branch] = calculateScores(cap);
    }

    bmCalculationCache.set(cacheKey, finalResponse);

    return branchId ? finalResponse[branchId] : finalResponse;
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
      LEFT JOIN users bm
        ON bm.branch_id = ?
        AND bm.role = 'BM'
        AND bm.resign = 0
        AND bm.period = ?
      LEFT JOIN targets t
        ON t.kpi = k.kpi
        AND t.period = ?
        AND t.branch_id = ?
      LEFT JOIN allocations a
        ON k.kpi = 'insurance'
        AND a.user_id = bm.id
        AND a.period = ?
      LEFT JOIN weightage w
        ON w.kpi = k.kpi
      LEFT JOIN (
          SELECT
              e.kpi,
              SUM(e.value) AS total_achieved
          FROM entries e
          LEFT JOIN users bm
              ON bm.branch_id = ?
              AND bm.role = 'BM'
              AND bm.resign = 0
              AND bm.period = ?
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
        [
          branchId,
          period,
          period,
          branchId,
          period,
          branchId,
          period,
          period,
          branchId,
        ],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        },
      );
    });

    const [bmRow, insuranceRow] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `SELECT id FROM users WHERE branch_id=? AND period=? AND role='BM' LIMIT 1`,
          [branchId, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows?.[0]?.id || 0);
          },
        );
      }),

      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT SUM(value) AS achieved
          FROM entries
          WHERE period=? AND kpi='insurance'
            AND employee_id = (
              SELECT id FROM users WHERE branch_id=? AND period=? AND role='BM' LIMIT 1
            )
          `,
          [period, branchId, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows?.[0]?.achieved || 0);
          },
        );
      }),
    ]);

    // update insurance achieved
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
      let totalWeightageScore = 0;

      bmKpis.forEach((kpi) => {
        scores[kpi] = {
          score: 0,
          target: 0,
          achieved: 0,
          weightage: 0,
          weightageScore: 0,
        };
      });

      // 🔥 ONLY FIX: GLOBAL audit/recovery ratios
      const auditRow = results.find((r) => r.kpi === "audit");
      const recoveryRow = results.find((r) => r.kpi === "recovery");

      const auditRatio = auditRow ? auditRow.achieved / auditRow.target : 0;
      const recoveryRatio = recoveryRow
        ? recoveryRow.achieved / recoveryRow.target
        : 0;

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
          case "loan_amulya":
            if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else if (auditRatio >= 0.75 && recoveryRatio >= 0.75)
              outOf10 = 12.5;
            else outOf10 = 10;
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
// function to get all hod scores
const scoreCache = new Map();
const branchScoreCache = new Map();

performanceMasterRouter.get("/ho-Allhod-scores", async (req, res) => {
  const { period, hod_id, role } = req.query;

  if (!period || !hod_id) {
    return res.status(400).json({
      error: "period and hod_id are required",
    });
  }

  try {
    // =========================
    // MASTER QUERIES
    // =========================
    const [hodKpis, staffIdsRows, branchCodesRows] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `
              SELECT 
                k.kpi_name,
                r.id AS role_kpi_mapping_id,
                r.weightage

              FROM role_kpi_mapping r

              JOIN kpi_master k
                ON r.kpi_id = k.id

              WHERE r.role = ?
              `,

          [role],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            if (!rows.length) {
              return reject("No KPIs found");
            }

            resolve(rows);
          },
        );
      }),

      new Promise((resolve, reject) => {
        pool.query(
          `
              SELECT id

              FROM users

              WHERE hod_id = ?
              AND period = ?
              `,

          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(rows);
          },
        );
      }),

      new Promise((resolve, reject) => {
        pool.query(
          `
              SELECT code

              FROM branches

              WHERE incharge_id = ?
              AND period = ?
              `,

          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(rows);
          },
        );
      }),
    ]);

    const staffIds = staffIdsRows.map((r) => r.id);

    const branchCodes = branchCodesRows.map((r) => r.code);

    const batchSize = 5;

    // =========================
    // STAFF SCORES
    // =========================
    const staffScores = [];

    for (let i = 0; i < staffIds.length; i += batchSize) {
      const batch = staffIds.slice(i, i + batchSize);

      for (const id of batch) {
        try {
          const cacheKey = `${period}_${id}`;

          if (scoreCache.has(cacheKey)) {
            staffScores.push(scoreCache.get(cacheKey));

            continue;
          }

          const score = await calculateStaffScores(period, id, "HO_STAFF");

          scoreCache.set(cacheKey, score);

          staffScores.push(score);
        } catch {
          staffScores.push({
            total: 0,
          });
        }
      }
    }

    let fixedAvgTotal = 0;

    for (const v of staffScores) {
      fixedAvgTotal += v.total || 0;
    }

    const fixedAvg = Number(
      (fixedAvgTotal / (staffScores.length || 1)).toFixed(2),
    );

    // =========================
    // NEW BM FUNCTION
    // =========================
    const allBranchScores = await calculateBMScores(period);

    // =========================
    // BRANCH SCORES
    // =========================
    const branchTotals = [];

    for (let i = 0; i < branchCodes.length; i += batchSize) {
      const batch = branchCodes.slice(i, i + batchSize);

      for (const code of batch) {
        try {
          const cacheKey = `${period}_${code}`;

          if (branchScoreCache.has(cacheKey)) {
            branchTotals.push(branchScoreCache.get(cacheKey));

            continue;
          }

          const total = Number(
            (allBranchScores?.[code]?.total || 0).toFixed(2),
          );

          branchScoreCache.set(cacheKey, total);

          branchTotals.push(total);
        } catch {
          branchTotals.push(0);
        }
      }
    }

    let branchTotalValue = 0;

    for (const v of branchTotals) {
      branchTotalValue += v;
    }

    const branchAvgScore = Number(
      (branchTotalValue / (branchTotals.length || 1)).toFixed(2),
    );

    // =========================
    // KPI MAP
    // =========================
    const kpiMap = {};

    for (const k of hodKpis) {
      kpiMap[k.kpi_name] = {
        id: k.role_kpi_mapping_id,

        weightage: k.weightage,
      };
    }

    // =========================
    // KPI VALUES
    // =========================
    const [userKpiValues, insuranceValue] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `
              SELECT 
                role_kpi_mapping_id,
                SUM(value) AS total

              FROM user_kpi_entry

              WHERE user_id=?
              AND period=?

              GROUP BY role_kpi_mapping_id
              `,

          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            const map = {};

            for (const r of rows) {
              map[r.role_kpi_mapping_id] = Number(r.total || 0);
            }

            resolve(map);
          },
        );
      }),

      new Promise((resolve, reject) => {
        if (!Object.keys(kpiMap).some((k) => k.toLowerCase() === "insurance")) {
          return resolve(0);
        }

        pool.query(
          `
              SELECT 
                SUM(value) AS total

              FROM entries

              WHERE kpi='insurance'
              AND employee_id=?
              AND period=?
              `,

          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(Number(rows[0]?.total || 0));
          },
        );
      }),
    ]);

    const getVal = (name) => userKpiValues[kpiMap[name]?.id] || 0;

    const Cleanliness = getVal("HO Building Cleanliness");

    const Management = getVal("Management  Discretion");

    const InternalAudit = getVal("Internal Audit performance");

    const IT = getVal("IT");

    const InsuranceBusinessDevelopment = getVal(
      "Insurance Business Development",
    );

    // =========================
    // FINAL KPI
    // =========================
    let finalKpis = {};

    let Total = 0;

    for (const kpi of hodKpis) {
      const name = kpi.kpi_name;

      const weightage = kpi.weightage;

      const lowerName = name.toLowerCase();

      let avg = 0;
      let result = 0;
      let weightageScore = 0;
      let rangeCovert = 0;

      if (lowerName.includes("section")) {
        avg = fixedAvg;
      } else if (lowerName.includes("branch") || lowerName.includes("visits")) {
        avg = branchAvgScore;
      } else if (lowerName.includes("clean")) {
        avg = Cleanliness;
      } else if (lowerName.includes("management")) {
        avg = Management;
      } else if (lowerName.includes("audit")) {
        avg = InternalAudit;
      } else if (lowerName.includes("it")) {
        avg = IT;
      } else if (lowerName.includes("insurance business development")) {
        avg = InsuranceBusinessDevelopment;
      }

      rangeCovert = (avg / 10) * weightage;

      if (lowerName === "insurance") {
        const target = 40000;

        avg = insuranceValue;

        if (avg <= target) {
          result = (avg / target) * 10;
        } else if (avg / target < 1.25) {
          result = 10;
        }

        weightageScore = avg === 0 ? -2 : (result / 100) * weightage;
      } else if (
        lowerName.includes("section") ||
        lowerName.includes("branch") ||
        lowerName.includes("visits")
      ) {
        if (rangeCovert === 0) {
          result = 0;
        } else if (rangeCovert <= weightage) {
          result = (rangeCovert / weightage) * 10;
        } else if (rangeCovert / weightage < 1.25) {
          result = 12.5;
        } else {
          result = 0;
        }

        weightageScore = (result / 100) * weightage;
      } else {
        if (avg === 0) {
          result = 0;
        } else if (avg <= weightage) {
          result = (avg / weightage) * 10;
        } else if (avg / weightage < 1.25) {
          result = 12.5;
        } else {
          result = 0;
        }

        weightageScore = (result / 100) * weightage;
      }

      Total += weightageScore;

      finalKpis[name] = {
        score: Number(result.toFixed(2)),

        achieved:
          lowerName.includes("section") ||
          lowerName.includes("branch") ||
          lowerName.includes("visits")
            ? Number(rangeCovert.toFixed(2))
            : avg || 0,

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

    res.status(500).json({
      error: "Failed to calculate HOD scores",
    });
  }
});


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
        },
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
        WHERE role = 'HO_staff' AND hod_id = ? AND period = ?
        `,
        [hod_id, period],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        },
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
        (err, rows) => resolve(rows || []),
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
        (err, rows) => resolve(rows || []),
      );
    });

    const userEntryMap = {};

    allUserEntries.forEach((e) => {
      if (!userEntryMap[e.user_id]) {
        userEntryMap[e.user_id] = {};
      }

      userEntryMap[e.user_id][e.role_kpi_mapping_id] = Number(e.achieved || 0);
    });

    const insuranceMap = {};

    allInsuranceEntries.forEach((e) => {
      insuranceMap[e.employee_id] = Number(e.achieved || 0);
    });

    const result = [];

    // CACHE MAPS
    const hoHistoryCache = new Map();
    const attHistoryCache = new Map();
    const transferHistoryCache = new Map();

    // BATCH PROCESSING
    const batchSize = 10;

    for (let i = 0; i < staffRows.length; i += batchSize) {
      const batch = staffRows.slice(i, i + batchSize);

      const batchResults = await Promise.all(
        batch.map(async (staff) => {
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
              if (kpi_name.toLowerCase() === "insurance") {
                const ratio = achieved / target;

                if (ratio <= 1) score = ratio * 10;
                else if (ratio < 1.25) score = 10;
              } else {
                const ratio = achieved / target;

                if (ratio <= 1) score = ratio * 10;
                else if (ratio < 1.25) score = 10;
                else score = 12.5;
              }
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
                kpi_name.toLowerCase() === "insurance" && weightageScore === 0
                  ? -2
                  : Number(weightageScore.toFixed(2)),
            };
          }

          const finalScores = {
            total: Number(totalWeightageScore.toFixed(2)),
          };

          // HO HISTORY CACHE
          let hoHistory = hoHistoryCache.get(staffId);

          if (!hoHistory) {
            hoHistory = await new Promise((resolve) => {
              getHoStaffTransferHistory(pool, period, staffId, (err, data) => {
                if (err) return resolve([]);

                resolve(data);
              });
            });

            hoHistoryCache.set(staffId, hoHistory);
          }

          const previousHoScores =
            hoHistory?.[0]?.transfers?.map((t) =>
              Number(t.total_weightage_score),
            ) || [];

          // ATT HISTORY CACHE
          let attHistory = attHistoryCache.get(staffId);

          if (!attHistory) {
            attHistory = await new Promise((resolve) => {
              getAttenderTransferHistory(pool, period, staffId, (err, data) => {
                if (err) return resolve([]);

                resolve(data);
              });
            });

            attHistoryCache.set(staffId, attHistory);
          }

          const previousAttenderScores =
            attHistory?.[0]?.transfers?.map((t) =>
              Number(t.total_weightage_score),
            ) || [];

          // TRANSFER CACHE
          let transferHistory = transferHistoryCache.get(staffId);

          if (!transferHistory) {
            transferHistory = await getTransferKpiHistory(
              pool,
              period,
              staffId,
            );

            transferHistoryCache.set(staffId, transferHistory);
          }

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
              ? allScores.reduce((a, b) => a + b, 0) / allScores.length
              : 0;

          staffObj.originalTotal = Number(totalWeightageScore.toFixed(2));

          staffObj.total = Number(finalAvg.toFixed(2));

          return staffObj;
        }),
      );

      result.push(...batchResults);
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

  if (!user_id || !master_user_id || !period || !Array.isArray(scores)) {
    return res.status(400).json({
      error:
        "user_id, master_user_id, period and valid scores array are required",
    });
  }

  const filteredScores = scores.filter(
    (s) =>
      s &&
      s.role_kpi_mapping_id &&
      s.value !== undefined &&
      s.kpi_name?.toLowerCase() !== "insurance",
  );

  if (filteredScores.length === 0) {
    return res.json({
      message: "No KPI to save (Insurance excluded)",
    });
  }

  pool.getConnection((err, conn) => {
    if (err) return res.status(500).json(err);

    conn.beginTransaction((err) => {
      if (err) {
        conn.release();
        return res.status(500).json(err);
      }

      let completed = 0;
      let hasError = false;

      const inserted = [];
      const updated = [];

      filteredScores.forEach((s) => {
        const userId = Number(user_id);
        const roleId = Number(s.role_kpi_mapping_id);
        const value = Number(s.value);
        const periodVal = String(period).trim();
        const masterId = Number(master_user_id);

        conn.query(
          `UPDATE user_kpi_entry 
           SET value = ?, master_user_id = ?, updated_at = CURRENT_TIMESTAMP
           WHERE user_id = ? 
           AND role_kpi_mapping_id = ? 
           AND period = ?`,
          [value, masterId, userId, roleId, periodVal],
          (err, updateResult) => {
            if (err) return rollback(err);

            if (updateResult.affectedRows > 0) {
              updated.push(`${s.kpi_name} updated`);
              done();
            } else {
              conn.query(
                `INSERT INTO user_kpi_entry 
                 (user_id, role_kpi_mapping_id, period, value, master_user_id)
                 VALUES (?, ?, ?, ?, ?)`,
                [userId, roleId, periodVal, value, masterId],
                (err2) => {
                  if (err2) return rollback(err2);

                  inserted.push(`${s.kpi_name} inserted`);
                  done();
                },
              );
            }
          },
        );
      });

      function done() {
        completed++;
        if (completed === filteredScores.length && !hasError) {
          conn.commit((err) => {
            if (err) return rollback(err);

            conn.release();
            res.json({
              message: "KPI save/update successful",
              totalProcessed: filteredScores.length,
              inserted,
              updated,
            });
          });
        }
      }

      function rollback(error) {
        if (hasError) return;
        hasError = true;

        conn.rollback(() => {
          conn.release();
          console.error("Transaction Error =>", error);

          res.status(500).json({
            error: "Transaction failed ",
            details: error,
          });
        });
      }
    });
  });
});

//get Dashboard Data
const bmScoreCache = new Map();

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

      // PARALLEL LIGHT QUERIES ONLY
      const [hoStaffResult, branches, agmDgmResult] = await Promise.all([
        new Promise((resolve, reject) => {
          pool.query(
            `
            SELECT 
              username,
              name,
              role
            FROM users
            WHERE role='HO_STAFF'
            AND hod_id = ?
            AND period = ?
            `,
            [hod_id, period],
            (err, rows) => {
              if (err) {
                return reject(err);
              }

              resolve(rows);
            },
          );
        }),

        new Promise((resolve, reject) => {
          pool.query(
            `
            SELECT 
              code,
              name
            FROM branches
            WHERE incharge_id = ?
            AND period = ?
            `,
            [hod_id, period],
            (err, rows) => {
              if (err) {
                return reject(err);
              }

              resolve(rows);
            },
          );
        }),

        new Promise((resolve, reject) => {
          pool.query(
            `
            SELECT 
              username,
              name,
              role
            FROM users
            WHERE role IN (
              'AGM',
              'DGM',
              'AGM_IT',
              'AGM_INSURANCE',
              'AGM_AUDIT'
            )
            AND period = ?
            `,
            [period],
            (err, rows) => {
              if (err) {
                return reject(err);
              }

              resolve(rows);
            },
          );
        }),
      ]);

      dashboardResult.totalHOStaff = hoStaffResult;

      dashboardResult.totalHOStaffCount = hoStaffResult.length;

      // VERY IMPORTANT
      const batchSize = 2;

      const branchesWithScore = [];

      for (let i = 0; i < branches.length; i += batchSize) {
        const batch = branches.slice(i, i + batchSize);

        // SEQUENTIAL
        for (const branch of batch) {
          try {
            const cacheKey = `${period}_${branch.code}`;

            if (bmScoreCache.has(cacheKey)) {
              branchesWithScore.push({
                ...branch,

                bmTotalScore: bmScoreCache.get(cacheKey),
              });

              continue;
            }

            const branchScore = await calculateBMScores(period, branch.code);

            const total = branchScore.total || 0;

            bmScoreCache.set(cacheKey, total);

            branchesWithScore.push({
              ...branch,

              bmTotalScore: total,
            });
          } catch {
            branchesWithScore.push({
              ...branch,

              bmTotalScore: 0,
            });
          }
        }
      }

      dashboardResult.totalBranches = branchesWithScore;

      dashboardResult.totalBranchesCount = branchesWithScore.length;

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
const hodScoreCache = new Map();
const bmScoreCache1 = new Map();

const calculateHodAllScores = async (
  pool,
  period,
  hod_id,
  role,
  calculateStaffScores,
  calculateBMScores,
) => {
  try {
    // =========================
    // MASTER QUERIES
    // =========================
    const [hodKpis, staffRows, branchRows] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT 
            k.kpi_name,
            r.id AS role_kpi_mapping_id,
            r.weightage

          FROM role_kpi_mapping r

          JOIN kpi_master k
            ON r.kpi_id = k.id

          WHERE r.role = ?
          `,
          [role],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            if (!rows.length) {
              return reject("No KPIs found for role");
            }

            resolve(rows);
          },
        );
      }),

      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT id

          FROM users

          WHERE hod_id = ?
          AND period = ?
          `,
          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(rows);
          },
        );
      }),

      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT code

          FROM branches

          WHERE incharge_id = ?
          AND period = ?
          `,
          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(rows);
          },
        );
      }),
    ]);

    // =========================
    // KPI MAP
    // =========================
    const kpiMap = {};

    for (const k of hodKpis) {
      kpiMap[k.kpi_name] = {
        id: k.role_kpi_mapping_id,

        weightage: k.weightage,
      };
    }

    const staffIds = staffRows.map((r) => r.id);

    const branchCodes = branchRows.map((r) => r.code);

    // =========================
    // STAFF SCORES
    // =========================
    const limit = pLimit(1);

    const staffPromises = staffIds.map((id) =>
      limit(async () => {
        try {
          const cacheKey = `${period}_${id}`;

          if (hodScoreCache.has(cacheKey)) {
            return hodScoreCache.get(cacheKey);
          }

          const score = await calculateStaffScores(period, id, "HO_STAFF");

          hodScoreCache.set(cacheKey, score);

          return score;
        } catch {
          return {
            total: 0,
          };
        }
      }),
    );

    const staffScores = await Promise.all(staffPromises);

    let staffTotal = 0;

    for (const v of staffScores) {
      staffTotal += v.total || 0;
    }

    const staffAvgScore = Number(
      (staffTotal / (staffScores.length || 1)).toFixed(2),
    );

    // =========================
    // NEW BM FUNCTION
    // =========================
    const allBranchScores = await calculateBMScores(period);

    // =========================
    // BM TOTALS
    // =========================
    const bmPromises = branchCodes.map(async (code) => {
      try {
        const cacheKey = `${period}_${code}`;

        if (bmScoreCache1.has(cacheKey)) {
          return bmScoreCache1.get(cacheKey);
        }

        const total = Number((allBranchScores?.[code]?.total || 0).toFixed(2));

        bmScoreCache1.set(cacheKey, total);

        return total;
      } catch {
        return 0;
      }
    });

    const bmTotals = await Promise.all(bmPromises);

    let bmTotal = 0;

    for (const v of bmTotals) {
      bmTotal += v;
    }

    const branchAvgScore = Number(
      (bmTotal / (bmTotals.length || 1)).toFixed(2),
    );

    // =========================
    // KPI VALUES
    // =========================
    const [userKpiValues, insuranceValue] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `
          SELECT 
            role_kpi_mapping_id,
            SUM(value) AS total

          FROM user_kpi_entry

          WHERE user_id = ?
          AND period = ?

          GROUP BY role_kpi_mapping_id
          `,
          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            const map = {};

            for (const r of rows) {
              map[r.role_kpi_mapping_id] = Number(r.total || 0);
            }

            resolve(map);
          },
        );
      }),

      new Promise((resolve, reject) => {
        if (!Object.keys(kpiMap).some((k) => k.toLowerCase() === "insurance")) {
          return resolve(0);
        }

        pool.query(
          `
          SELECT 
            SUM(value) AS total

          FROM entries

          WHERE kpi='insurance'
          AND employee_id=?
          AND period=?
          `,
          [hod_id, period],

          (err, rows) => {
            if (err) {
              return reject(err);
            }

            resolve(Number(rows[0]?.total || 0));
          },
        );
      }),
    ]);

    const getKpiVal = (name) => userKpiValues[kpiMap[name]?.id] || 0;

    const Cleanliness = getKpiVal("HO Building Cleanliness");

    const Management = getKpiVal("Management  Discretion");

    const InternalAudit = getKpiVal("Internal Audit performance");

    const IT = getKpiVal("IT");

    const InsuranceBusinessDevelopment = getKpiVal(
      "Insurance Business Development",
    );

    // =========================
    // FINAL KPI
    // =========================
    let finalKpis = {};

    let Total = 0;

    for (const kpi of hodKpis) {
      const name = kpi.kpi_name;

      const weightage = kpi.weightage;

      const lowerName = name.toLowerCase();

      let avg = 0;
      let result = 0;
      let weightageScore = 0;
      let rangeCovert = 0;

      if (lowerName.includes("section")) {
        avg = staffAvgScore;
      } else if (lowerName.includes("branch") || lowerName.includes("visits")) {
        avg = branchAvgScore;
      } else if (lowerName.includes("clean")) {
        avg = Cleanliness;
      } else if (lowerName.includes("management")) {
        avg = Management;
      } else if (lowerName.includes("audit")) {
        avg = InternalAudit;
      } else if (lowerName.includes("it")) {
        avg = IT;
      } else if (lowerName.includes("insurance business development")) {
        avg = InsuranceBusinessDevelopment;
      }

      rangeCovert = (avg / 10) * weightage;

      // =========================
      // INSURANCE
      // =========================
      if (lowerName === "insurance") {
        const target = 40000;

        avg = insuranceValue;

        if (avg <= target) {
          result = (avg / target) * 10;
        } else if (avg / target < 1.25) {
          result = 10;
        }

        weightageScore = avg === 0 ? -2 : (result / 100) * weightage;
      }

      // =========================
      // SECTION / BRANCH
      // =========================
      else if (
        lowerName.includes("section") ||
        lowerName.includes("branch") ||
        lowerName.includes("visits")
      ) {
        if (rangeCovert === 0) {
          result = 0;
        } else if (rangeCovert <= weightage) {
          result = (rangeCovert / weightage) * 10;
        } else if (rangeCovert / weightage < 1.25) {
          result = 12.5;
        } else {
          result = 0;
        }

        weightageScore = (result / 100) * weightage;
      }

      // =========================
      // NORMAL KPI
      // =========================
      else {
        if (avg === 0) {
          result = 0;
        } else if (avg <= weightage) {
          result = (avg / weightage) * 10;
        } else if (avg / weightage < 1.25) {
          result = 12.5;
        } else {
          result = 0;
        }

        weightageScore = (result / 100) * weightage;
      }

      Total += weightageScore;

      finalKpis[name] = {
        score: Number(result.toFixed(2)),

        achieved:
          lowerName.includes("section") ||
          lowerName.includes("branch") ||
          lowerName.includes("visits")
            ? Number(rangeCovert.toFixed(2))
            : avg || 0,

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
const agmScoreCache = new Map();

performanceMasterRouter.get("/AGM-DGM-Scores", async (req, res) => {
  const { period } = req.query;

  if (!period) {
    return res.status(400).json({
      error: "period required",
    });
  }

  try {
    // LOAD AGM LIST
    const agmList = await new Promise((resolve, reject) => {
      pool.query(
        `
              SELECT 
                id AS hod_id,
                role,
                name
              FROM users
              WHERE role IN (
                'AGM',
                'DGM',
                'AGM_AUDIT',
                'AGM_INSURANCE',
                'AGM_IT'
              )
              AND period = ?
              `,
        [period],
        (err, rows) => {
          if (err) {
            return reject(err);
          }

          resolve(rows);
        },
      );
    });

    // BEST PERFORMANCE BALANCE
    const batchSize = 3;

    const results = [];

    for (let i = 0; i < agmList.length; i += batchSize) {
      const batch = agmList.slice(i, i + batchSize);

      // CONTROLLED PARALLEL
      const batchResults = await Promise.all(
        batch.map(async (agm) => {
          try {
            const cacheKey = `${period}_${agm.hod_id}_${agm.role}`;

            // CACHE HIT
            if (agmScoreCache.has(cacheKey)) {
              return {
                hod_id: agm.hod_id,

                name: agm.name,

                role: agm.role,

                ...agmScoreCache.get(cacheKey),
              };
            }

            // HEAVY FUNCTION
            const scoreData = await calculateHodAllScores(
              pool,
              period,
              agm.hod_id,
              agm.role,
              calculateStaffScores,
              calculateBMScores,
            );

            // SAVE CACHE
            agmScoreCache.set(cacheKey, scoreData);
            console.log(scoreData);
            return {
              hod_id: agm.hod_id,

              name: agm.name,

              role: agm.role,

              ...scoreData,
            };
          } catch (err) {
            console.error(err);

            return {
              hod_id: agm.hod_id,

              name: agm.name,

              role: agm.role,

              kpis: {},

              total: 0,
            };
          }
        }),
      );

      results.push(...batchResults);
    }

    res.json(results);
  } catch (err) {
    console.error(err);

    res.status(500).json({
      error: "Failed to calculate AGM/DGM scores",
    });
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

const branchScoreCache1 = new Map();

performanceMasterRouter.post("/getAllBranchesScore", async (req, res) => {
  const { period } = req.body;

  if (!period) {
    return res.status(400).json({
      error: "period is required",
    });
  }

  try {
    const branchReportData = {};

    // LIGHT QUERY
    const branches = await new Promise((resolve, reject) => {
      pool.query(
        `
              SELECT 
                code,
                name
              FROM branches
              WHERE period = ?
              `,
        [period],
        (err, rows) => {
          if (err) {
            return reject(err);
          }

          resolve(rows);
        },
      );
    });

    // CONTROLLED CONCURRENCY
    const limit = pLimit(2);

    const branchPromises = branches.map((branch) =>
      limit(async () => {
        try {
          const cacheKey = `${period}_${branch.code}`;

          // CACHE
          if (branchScoreCache1.has(cacheKey)) {
            return {
              ...branch,
              ...branchScoreCache1.get(cacheKey),
            };
          }

          // BM QUERY
          const bmRows = await new Promise((resolve, reject) => {
            pool.query(
              `
                      SELECT 
                        PF_NO,
                        name
                      FROM users
                      WHERE role='BM'
                      AND branch_id = ?
                      AND period = ?
                      LIMIT 1
                      `,
              [branch.code, period],
              (err, rows) => {
                if (err) {
                  return reject(err);
                }

                resolve(rows);
              },
            );
          });

          const bm = bmRows[0] || null;

          // HEAVY FUNCTION
          const branchScore = await calculateBMScores(period, branch.code);

          const result = {
            bmId: bm?.PF_NO ?? null,

            bmName: bm?.name ?? null,

            bmTotalScore: branchScore?.total ?? 0,
          };

          // SAVE CACHE
          branchScoreCache1.set(cacheKey, result);

          return {
            ...branch,
            ...result,
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

    const branchesWithScore = await Promise.all(branchPromises);

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
async function calculateStaffScoresCb(
  period,
) {

  try {

    const query = `
    SELECT 
        emp.id AS employee_id,
        emp.branch_id,
        emp.role,

        k.kpi,

        MAX(
            CASE 

                WHEN k.kpi = 'recovery'
                    AND (
                        emp.transfer_date 
                            BETWEEN fy.fy_start 
                            AND fy.fy_end

                        OR

                        emp.user_add_date 
                            BETWEEN fy.fy_start 
                            AND fy.fy_end
                    )

                THEN COALESCE(a.amount, 0)

                WHEN k.kpi = 'recovery'

                THEN COALESCE(t.amount, 0)

                ELSE COALESCE(a.amount, 0)

            END
        ) AS target,

        MAX(
            COALESCE(w.weightage, 0)
        ) AS weightage,

        MAX(
            COALESCE(
                e.total_achieved,
                0
            )
        ) AS achieved

    FROM users emp

    CROSS JOIN
    (
        SELECT 'deposit' AS kpi
        UNION ALL SELECT 'loan_gen'
        UNION ALL SELECT 'loan_amulya'
        UNION ALL SELECT 'audit'
        UNION ALL SELECT 'recovery'
        UNION ALL SELECT 'insurance'
    ) k

    CROSS JOIN
    (
        SELECT 
            STR_TO_DATE(
                '2025-04-01',
                '%Y-%m-%d'
            ) AS fy_start,

            STR_TO_DATE(
                '2026-03-31',
                '%Y-%m-%d'
            ) AS fy_end
    ) fy

    LEFT JOIN allocations a
        ON a.kpi = k.kpi
        AND a.period = ?
        AND a.user_id = emp.id
        AND a.branch_id = emp.branch_id

    LEFT JOIN targets t
        ON t.kpi = k.kpi
        AND t.period = ?
        AND t.branch_id = emp.branch_id

    LEFT JOIN weightage w
        ON w.kpi = k.kpi

    LEFT JOIN
    (
        SELECT
            z.employee_id,
            z.branch_id,
            z.kpi,

            SUM(z.value)
                AS total_achieved

        FROM
        (

            -- AUDIT / RECOVERY
            SELECT
                emp2.id AS employee_id,
                emp2.branch_id,
                e.kpi,
                e.value

            FROM users emp2

            LEFT JOIN users bm
                ON bm.branch_id =
                    emp2.branch_id
                AND bm.role = 'BM'
                AND bm.period =
                    emp2.period

            JOIN entries e
                ON e.period = ?
                AND e.status = 'Verified'
                AND e.branch_id =
                    emp2.branch_id

                AND e.kpi IN (
                    'audit',
                    'recovery'
                )

                AND
                (
                    (
                        (
                            emp2.user_add_date 
                                BETWEEN '2025-04-01'
                                AND '2026-03-31'

                            OR

                            emp2.transfer_date 
                                BETWEEN '2025-04-01'
                                AND '2026-03-31'
                        )

                        AND e.employee_id =
                            emp2.id
                    )

                    OR

                    (
                        (
                            emp2.user_add_date IS NULL

                            OR

                            emp2.user_add_date <
                                '2025-04-01'
                        )

                        AND
                        (
                            emp2.transfer_date IS NULL

                            OR

                            emp2.transfer_date <
                                '2025-04-01'
                        )

                        AND e.employee_id =
                            bm.id
                    )
                )

            WHERE
                emp2.period = ?
                AND emp2.role = 'Clerk'

            UNION ALL

            -- OTHER KPI
            SELECT
                e.employee_id,
                e.branch_id,
                e.kpi,
                e.value

            FROM entries e

            JOIN users emp3
                ON emp3.id = e.employee_id
                AND emp3.role = 'Clerk'
                AND emp3.period = ?
                AND emp3.branch_id =
                    e.branch_id

            WHERE
                e.period = ?
                AND e.status = 'Verified'
                AND e.kpi IN (
                    'deposit',
                    'loan_gen',
                    'loan_amulya',
                    'insurance'
                )

        ) z

        GROUP BY
            z.employee_id,
            z.branch_id,
            z.kpi

    ) e
        ON e.employee_id = emp.id
        AND e.branch_id = emp.branch_id
        AND e.kpi = k.kpi

    WHERE
        emp.period = ?
        AND emp.role = 'Clerk'

    GROUP BY
        emp.id,
        emp.branch_id,
        emp.role,
        k.kpi

    ORDER BY
        emp.branch_id,
        emp.id,
        k.kpi
    `;

    const results =
      await new Promise(
        (resolve, reject) => {

          pool.query(
            query,
            [
              period,
              period,

              period,
              period,
              period,
              period,

              period,
            ],

            (err, rows) => {

              if (err) {
                return reject(err);
              }

              resolve(rows || []);
            },
          );
        },
      );

    const grouped = {};

    // =========================
    // GROUP EMPLOYEE DATA
    // =========================
    for (const row of results) {

      const employeeId =
        row.employee_id;

      if (
        !grouped[employeeId]
      ) {

        grouped[employeeId] = [];
      }

      grouped[
        employeeId
      ].push(row);
    }

    const finalResponse = {};

    // =========================
    // CALCULATE SCORES
    // =========================
    for (const employeeId in grouped) {

      const employeeRows =
        grouped[employeeId];

      const scores = {};

      let totalWeightageScore = 0;

      let auditRatio = 0;
      let recoveryRatio = 0;

      for (const row of employeeRows) {

        if (
          row.kpi === "audit"
        ) {

          auditRatio =
            Number(
              row.achieved || 0,
            ) /
            Number(
              row.target || 1,
            );
        }

        if (
          row.kpi === "recovery"
        ) {

          recoveryRatio =
            Number(
              row.achieved || 0,
            ) /
            Number(
              row.target || 1,
            );
        }
      }

      for (const row of employeeRows) {

        const score =
          calculateScore(
            row.kpi,
            row.achieved,
            row.target,
            auditRatio,
            recoveryRatio,
          );

        const weightageScore =
          row.kpi ===
            "insurance" &&
          score === 0
            ? -2
            : (
                score *
                row.weightage
              ) / 100;

        scores[row.kpi] = {

          score,

          target:
            row.target || 0,

          achieved:
            row.achieved || 0,

          weightage:
            row.weightage || 0,

          weightageScore,
        };

        totalWeightageScore +=
          weightageScore;
      }

      // =========================
      // TRANSFER FUNCTIONS
      // =========================
      const [
        hoHistory,
        attHistory,
        transferHistory,
      ] = await Promise.all([

        new Promise(
          (resolve, reject) => {

            getHoStaffTransferHistory(
              pool,
              period,
              employeeId,

              (err, data) => {

                if (err) {
                  return reject(err);
                }

                resolve(data);
              },
            );
          },
        ),

        new Promise(
          (resolve, reject) => {

            getAttenderTransferHistory(
              pool,
              period,
              employeeId,

              (err, data) => {

                if (err) {
                  return reject(err);
                }

                resolve(data);
              },
            );
          },
        ),

        getTransferKpiHistory(
          pool,
          period,
          employeeId,
        ),

      ]);

      const previousHoScores =
        hoHistory?.[0]
          ?.transfers?.map(
            (t) =>
              t.total_weightage_score,
          ) || [];

      const previousAttenderScores =
        attHistory?.[0]
          ?.transfers?.map(
            (t) =>
              t.total_weightage_score,
          ) || [];

      const previousTransferScores =
        transferHistory
          ?.all_scores || [];

      const allScores = [

        ...previousHoScores,

        ...previousAttenderScores,

        ...previousTransferScores,

        totalWeightageScore,
      ];

      const finalAvg =
        allScores.length > 0
          ? (
              allScores.reduce(
                (a, b) => a + b,
                0,
              ) /
              allScores.length
            )
          : 0;

      if (
        Number(employeeId) === 2947
      ) {

        console.log(
          "All Scores =>",
          allScores,
        );

        console.log(
          "Final Avg =>",
          finalAvg,
        );
      }

      scores.total = Number(
        finalAvg.toFixed(2),
      );

      finalResponse[
        employeeId
      ] = scores;
    }

    return finalResponse;

  } catch (err) {

    throw err;
  }
}
// helper function for the calculateScore accroding to the kpis
function calculateScore(
  kpi,
  achieved,
  target,
  auditRatio = 0,
  recoveryRatio = 0,
) {
  let outOf10 = 0;

  achieved = Number(achieved) || 0;
  target = Number(target) || 0;

  if (!target) return 0;

  const ratio = achieved / target;

  switch (kpi) {
    case "deposit":
    case "loan_gen":
    case "loan_amulya":
      if (ratio <= 1) {
        outOf10 = ratio * 10;
      } else if (ratio < 1.25) {
        outOf10 = 10;
      } else if (auditRatio >= 0.75 && recoveryRatio >= 0.75) {
        outOf10 = 12.5;
      } else {
        outOf10 = 10;
      }

      break;

    case "insurance":
      if (ratio === 0) {
        outOf10 = -2;
      } else if (ratio <= 1) {
        outOf10 = ratio * 10;
      } else if (ratio < 1.25) {
        outOf10 = 10;
      } else {
        outOf10 = 12.5;
      }

      break;

    case "audit":
    case "recovery":
      if (ratio <= 1) {
        outOf10 = ratio * 10;
      } else if (ratio < 1.25) {
        outOf10 = 10;
      } else {
        outOf10 = 12.5;
      }

      break;

    default:
      outOf10 = 0;
  }

  return Math.max(
    0,
    Math.min(12.5, isNaN(outOf10) ? 0 : Number(outOf10.toFixed(2))),
  );
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
            if (kpi_name.toLowerCase() === "insurance") {
              const ratio = achieved / target;
              if (ratio <= 1) score = ratio * 10;
              else if (ratio < 1.25) score = 10;
            } else {
              const ratio = achieved / target;
              if (ratio <= 1) score = ratio * 10;
              else if (ratio < 1.25) score = 10;
              else score = 12.5;
            }
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
        1,
    );
  }

  const fy = getFY(period);

  pool.query(
    `SELECT id FROM users 
     WHERE branch_id=? AND role='BM' AND period = ?
     ORDER BY transfer_date DESC`,
    [branchId, period],
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
            1,
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
              entryRows.forEach((r) => (achievedMap[r.kpi] = r.achieved || 0));

              achievedMap["audit"] =
                (achievedMap["audit"] || 0) + (bm.audit_achieved || 0);
              achievedMap["recovery"] =
                (achievedMap["recovery"] || 0) + (bm.recovery_achieved || 0);
              pool.query(
                `SELECT SUM(value) AS achieved
                 FROM entries
                 WHERE period=? AND employee_id=? 
                 AND kpi='insurance'
                 AND status='Verified'`,
                [period, BMID],
                (err, insRows) => {
                  if (err) return callback(err);

                  achievedMap["insurance"] = insRows?.[0]?.achieved || 0;

                  pool.query(`SELECT * FROM weightage`, (err, wRows) => {
                    if (err) return callback(err);

                    const weightageMap = {};
                    wRows.forEach((w) => (weightageMap[w.kpi] = w.weightage));

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

                    getHoStaffTransferHistory(
                      pool,
                      period,
                      BMID,
                      (errHo, hoHistory) => {
                        if (errHo) return callback(errHo);

                        const previousHoScores =
                          hoHistory?.[0]?.transfers?.map(
                            (t) => t.total_weightage_score,
                          ) || [];

                        getAttenderTransferHistory(
                          pool,
                          period,
                          BMID,
                          (errAtt, attHistory) => {
                            if (errAtt) return callback(errAtt);

                            const previousAttenderScores =
                              attHistory?.[0]?.transfers?.map(
                                (t) => t.total_weightage_score,
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

                                finalScores.total = finalAvg;

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
                          },
                        );
                      },
                    );
                  });
                },
              );
            },
          );
        },
      );
    },
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
      },
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
      sql =
        "SELECT id, name FROM users WHERE branch_id=? AND period = ? AND role='Attender'";
      params = [normalizedBranchId, period];
    } else {
      sql =
        "SELECT id, name FROM users WHERE hod_id=? AND period = ? AND role='Attender'";
      params = [hod_id, period];
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
                (r) => (map[r.role_kpi_mapping_id] = Number(r.total || 0)),
              );
              resolve(map);
            },
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
            },
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
          if (kpi.kpi_name.toLowerCase().includes("insurance")) {
            const ratio = achieved / target;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;
          } else {
            const ratio = achieved / target;
            if (ratio <= 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;
          }
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
        hoHistory?.[0]?.transfers?.map((t) => t.total_weightage_score) || [];

      const attHistory = await new Promise((resolve) => {
        getAttenderTransferHistory(pool, period, user.id, (err, data) => {
          if (err) return resolve([]);
          resolve(data);
        });
      });

      const previousAttenderScores =
        attHistory?.[0]?.transfers?.map((t) => t.total_weightage_score) || [];

      const transferHistory = await getTransferKpiHistory(
        pool,
        period,
        user.id,
      );
      const previousTransferScores = transferHistory?.all_scores || [];

      const allScores = [
        ...previousHoScores,
        ...previousAttenderScores,
        ...previousTransferScores,
        totalScore,
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
    }),
  );

  return response;
}
//helper function for get all users
const bmUserScoreCache = new Map();

function getAllUser(pool, period, cb) {

  const startYear = parseInt(period.substring(0, 4));



  const startDate = `${startYear}-04-01`;
  const endDate = `${startYear + 1}-03-31`;


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
    LEFT JOIN branches b ON u.branch_id = b.code AND b.period = ?
    WHERE u.role IN (
        "BM","Clerk","HO_STAFF","GM",
        "AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE",
        "Attender"
    )
    AND (u.resign IS NULL OR u.resign != 1) AND u.period = ? AND u.resign = 0
  `;

  pool.query(query, [startDate, endDate, period, period], cb);
}
function getAllUsers(pool, period, cb) {
  pool.query(
    `
    SELECT u.id, u.username, u.name, u.role,
           u.branch_id, u.hod_id,
           b.name AS branch_name
    FROM users u
    LEFT JOIN branches b ON u.branch_id = b.code AND b.period = ? 
    WHERE u.role IN (
      "BM","Clerk","HO_STAFF","GM",
      "AGM","DGM","AGM_IT","AGM_AUDIT","AGM_INSURANCE",
      "Attender"
    ) AND u.period = ? and u.resign=0
    `,
    [period, period], cb
  );
}
//give the BM Data only form this api
performanceMasterRouter.post(
  "/usersBM",
  async (req, res) => {

    const { period } = req.body;

    getAllUser(
      pool,
      period,

      async (err, users) => {

        if (err) {

          return res.status(500).json({
            error:
              "Failed to load users",
          });
        }

        const list =
          users.filter(
            (u) =>
              u.role === "BM"
          );

        try {

          // NEW BM FUNCTION
          // RETURNS ALL BRANCHES
          const allBmScores =
            await calculateBMScores(
              String(period)
            );

          const result =
            await Promise.all(

              list.map(
                async (u) => {

                  const branchId =
                    Number(
                      u.branch_id
                    );

                  let score;

                  // TRANSFER BM
                  if (
                    u.transfer_flag === 1
                  ) {

                    const scoreTransfer =
                      await new Promise(
                        (
                          resolve,
                          reject,
                        ) => {

                          getTransferBmScores(
                            pool,
                            period,
                            branchId,

                            (
                              err,
                              data,
                            ) => {

                              if (err) {

                                return reject(
                                  err
                                );
                              }

                              resolve(
                                data
                              );
                            },
                          );
                        },
                      );

                    score =
                      scoreTransfer
                        ? scoreTransfer
                        : await calculateBMScoresFortrasfer(
                            String(
                              period
                            ),
                            branchId,
                          );
                  }

                  // NORMAL BM
                  else {

                    // FILTER ONLY CURRENT BRANCH
                    score =
                      allBmScores?.[
                        branchId
                      ];
                  }

                  return {

                    ...u,

                    bmTotalScore:
                      score?.total || 0,
                  };
                },
              ),
            );

          res.json({
            users: result,
          });

        } catch (error) {

          console.error(error);

          res.status(500).json({

            error:
              "Failed to calculate scores",
          });
        }
      },
    );
  },
);

//give the clerk Data of 1/4
performanceMasterRouter.post(
  "/usersClerk/part1",
  async (req, res) => {

    const { period } = req.body;

    getAllUsers(
      pool,
      period,

      async (err, users) => {

        if (err) {

          return res.status(500).json({

            error:
              "Failed to load users",
          });
        }

        const list =
          users.filter(
            (u) =>
              u.role === "Clerk"
          );

        const quarter =
          Math.ceil(
            list.length / 4
          );

        const part =
          list.slice(
            0,
            quarter
          );

        try {

          // =========================
          // ALL CLERK SCORES
          // =========================
          const allScores =
            await calculateStaffScoresCb(
              period
            );

          const result =
            await Promise.all(

              part.map(
                async (u) => {

                  try {

                    const clerkScore =
                      allScores?.[
                        u.id
                      ];

                    return {

                      ...u,

                      bmTotalScore:
                        clerkScore
                          ?.total ?? 0,
                    };

                  } catch {

                    return {

                      ...u,

                      bmTotalScore: 0,
                    };
                  }
                },
              ),
            );

         res.json({

  totalClerks: list.length,

  currentPartCount: result.length,

  users: result,

  part: 1,
});

        } catch (err) {

          console.error(err);

          res.status(500).json({

            error:
              "Failed to calculate scores",
          });
        }
      },
    );
  },
);

//give the clerk Data of 1/2
// =========================
// PART 2
// =========================
performanceMasterRouter.post(
  "/usersClerk/part2",
  async (req, res) => {

    const { period } = req.body;

    getAllUsers(
      pool,
      period,

      async (err, users) => {

        if (err) {

          return res.status(500).json({

            error:
              "Failed to load users",
          });
        }

        try {

          const list =
            users.filter(
              (u) =>
                u.role === "Clerk"
            );

          const quarter =
            Math.ceil(
              list.length / 4
            );

          const part =
            list.slice(
              quarter,
              quarter * 2,
            );

          // NEW FUNCTION
          const allScores =
            await calculateStaffScoresCb(
              period,
            );

          const result =
            part.map((u) => {

              const clerkScore =
                allScores?.[
                  u.id
                ];

              return {

                ...u,

                bmTotalScore:
                  clerkScore
                    ?.total ?? 0,
              };
            });

          res.json({

            totalClerks:
              list.length,

            currentPartCount:
              result.length,

            users: result,

            part: 2,
          });

        } catch (error) {

          console.error(error);

          res.status(500).json({

            error:
              "Failed to calculate scores",
          });
        }
      },
    );
  },
);

// =========================
// PART 3
// =========================
performanceMasterRouter.post(
  "/usersClerk/part3",
  async (req, res) => {

    const { period } = req.body;

    getAllUsers(
      pool,
      period,

      async (err, users) => {

        if (err) {

          return res.status(500).json({

            error:
              "Failed to load users",
          });
        }

        try {

          const list =
            users.filter(
              (u) =>
                u.role === "Clerk"
            );

          const quarter =
            Math.ceil(
              list.length / 4
            );

          const part =
            list.slice(
              quarter * 2,
              quarter * 3,
            );

          // NEW FUNCTION
          const allScores =
            await calculateStaffScoresCb(
              period,
            );

          const result =
            part.map((u) => {

              const clerkScore =
                allScores?.[
                  u.id
                ];

              return {

                ...u,

                bmTotalScore:
                  clerkScore
                    ?.total ?? 0,
              };
            });

          res.json({

            totalClerks:
              list.length,

            currentPartCount:
              result.length,

            users: result,

            part: 3,
          });

        } catch (error) {

          console.error(error);

          res.status(500).json({

            error:
              "Failed to calculate scores",
          });
        }
      },
    );
  },
);

// =========================
// PART 4
// =========================
performanceMasterRouter.post(
  "/usersClerk/part4",
  async (req, res) => {

    const { period } = req.body;

    getAllUsers(
      pool,
      period,

      async (err, users) => {

        if (err) {

          return res.status(500).json({

            error:
              "Failed to load users",
          });
        }

        try {

          const list =
            users.filter(
              (u) =>
                u.role === "Clerk"
            );

          const quarter =
            Math.ceil(
              list.length / 4
            );

          const part =
            list.slice(
              quarter * 3,
            );

          // NEW FUNCTION
          const allScores =
            await calculateStaffScoresCb(
              period,
            );

          const result =
            part.map((u) => {

              const clerkScore =
                allScores?.[
                  u.id
                ];

              return {

                ...u,

                bmTotalScore:
                  clerkScore
                    ?.total ?? 0,
              };
            });

          res.json({

            totalClerks:
              list.length,

            currentPartCount:
              result.length,

            users: result,

            part: 4,
          });

        } catch (error) {

          console.error(error);

          res.status(500).json({

            error:
              "Failed to calculate scores",
          });
        }
      },
    );
  },
);
//get all HO_STAFF Score
performanceMasterRouter.post("/usersHOStaff", (req, res) => {
  const { period } = req.body;

  getAllUsers(pool, period, async (err, users) => {
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

  getAllUsers(pool, period, async (err, users) => {
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

  getAllUsers(pool, period, async (err, users) => {
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
    WHERE id = ? AND period = ?
  `;

  pool.query(staffQuery, [ho_staff_id, period], (err0, staffRows) => {
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
        WHERE ho.staff_id = ? AND u.period = ? AND s.period = ?
        AND ho.period = ? 
        ORDER BY ho.transfer_date ASC
      `;

      pool.query(
        transferQuery,
        [ho_staff_id, period, period, period],
        (err2, rows) => {
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
              "General Work Performance":
                Number(t.General_Work_Performance) || 0,
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
        },
      );
    });
  });
}
//function for get single attender history code
export function getAttenderTransferHistory(pool, period, staff_id, callback) {
  const role = "Attender";

  const staffQuery = `
    SELECT id, name, resign
    FROM users
    WHERE id = ? AND period = ?
  `;

  pool.query(staffQuery, [staff_id, period], (err0, staffRows) => {
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
       SELECT 
    ho.*, 
    u.name AS hod_name, 
    s.name AS old_hod_name,
    b1.name AS new_branch,
    b2.name AS old_branch

FROM attender_transfer ho

LEFT JOIN users u 
    ON ho.hod_id = u.hod_id COLLATE utf8mb4_unicode_ci
    AND u.period COLLATE utf8mb4_unicode_ci = ?

LEFT JOIN users s 
    ON ho.old_hod_id = s.hod_id COLLATE utf8mb4_unicode_ci
    AND s.period COLLATE utf8mb4_unicode_ci = ?

LEFT JOIN branches b1 
    ON ho.branch_id COLLATE utf8mb4_unicode_ci = b1.code
    AND b1.period COLLATE utf8mb4_unicode_ci = ?

LEFT JOIN branches b2  
    ON ho.old_branch_id COLLATE utf8mb4_unicode_ci = b2.code
    AND b2.period COLLATE utf8mb4_unicode_ci = ?

WHERE ho.staff_id = ?
AND ho.period COLLATE utf8mb4_unicode_ci = ?

ORDER BY ho.transfer_date ASC
      `;

      pool.query(
        transferQuery,
        [period, period, period, period, staff_id, period],
        (err2, rows) => {
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
        },
      );
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
    WHERE id = ? AND period = ?
  `;

  pool.query(staffQuery, [ho_staff_id, period], (err0, staffRows) => {
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
        WHERE ho.staff_id = ? AND u.period COLLATE utf8mb4_unicode_ci = ? AND s.period COLLATE utf8mb4_unicode_ci = ?
        AND ho.period = ?
        ORDER BY ho.transfer_date ASC
      `;

      pool.query(
        transferQuery,
        [ho_staff_id, period, period, period],
        (err2, rows) => {
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
              "General Work Performance":
                Number(t.General_Work_Performance) || 0,
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
        },
      );
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
        pool.query(
          `SELECT code, name FROM branches where period = ?`,
          [period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows);
          },
        );
      });

      const branchesWithScore = await Promise.all(
        branches.map(async (branch) => {
          try {
            // Fetch BM
            const bmRows = await new Promise((resolve, reject) => {
              pool.query(
                `SELECT PF_NO, name FROM users WHERE role='BM' AND branch_id = ? AND period = ?`,
                [branch.code, period],
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
    WHERE id = ? AND period = ?
  `;

  pool.query(staffQuery, [staff_id, period], (err0, staffRows) => {
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
            SELECT 
    ho.*, 
    u.name AS hod_name, 
    s.name AS old_hod_name,
    b1.name AS new_branch,
    b2.name AS old_branch

FROM attender_transfer ho

LEFT JOIN users u 
    ON ho.hod_id = u.hod_id COLLATE utf8mb4_unicode_ci
    AND u.period COLLATE utf8mb4_unicode_ci = ?

LEFT JOIN users s 
    ON ho.old_hod_id = s.hod_id COLLATE utf8mb4_unicode_ci
    AND s.period COLLATE utf8mb4_unicode_ci = ?

LEFT JOIN branches b1 
    ON ho.branch_id COLLATE utf8mb4_unicode_ci = b1.code
    AND b1.period COLLATE utf8mb4_unicode_ci = ?

LEFT JOIN branches b2  
    ON ho.old_branch_id COLLATE utf8mb4_unicode_ci = b2.code
    AND b2.period COLLATE utf8mb4_unicode_ci = ?

WHERE ho.staff_id = ?
AND ho.period COLLATE utf8mb4_unicode_ci = ?

ORDER BY ho.transfer_date ASC
        `;

      pool.query(
        transferQuery,
        [period, period, period, period, staff_id, period],
        (err2, rows) => {
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
        },
      );
    });
  });
});

//get last transfer data according passing peroid
performanceMasterRouter.get("/getLasttransfer", (req, res) => {
  const { period, staff_id } = req.query;

  if (!period) {
    return res.status(400).json({ error: "Period is required" });
  }

  const query = `
    SELECT 
      staff_id,
          old_branch_id,
          new_branch_id,
          period,
          old_designation,
          new_designation
    FROM employee_transfer
    WHERE period = ? AND staff_id = ?
    ORDER BY transfer_date DESC
    LIMIT 1;
  `;

  pool.query(query, [period, staff_id], (err, results) => {
    if (err) {
      console.error("Error fetching last transfer:", err);
      return res
        .status(500)
        .json({ error: "Failed to fetch last transfer data" });
    }

    res.json(results[0] || null);
  });
});
