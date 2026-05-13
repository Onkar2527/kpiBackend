import express from "express";
import pool from "../db.js";
import {
  getHoStaffTransferHistory,
  getAttenderTransferHistory,
} from "./performanceMaster.js";
// Router providing summary endpoints for Achieved totals.

export const summaryRouter = express.Router();

// Summarises verified entries by KPI for the branch and period.
summaryRouter.get("/branch", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  pool.query(
    "SELECT kpi, SUM(value) as total FROM entries WHERE period = ? AND branch_id = ? AND status = ? GROUP BY kpi",
    [period, branchId, "Verified"],
    (error, results) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      const totals = {};
      results.forEach((row) => {
        totals[row.kpi] = row.total;
      });
      res.json(totals);
    },
  );
});

summaryRouter.post("/addAudit", (req, res) => {
  const { period, branch_id, employee_id, kpi, value, date, status } = req.body;

  if (!branch_id || !period || !value || !employee_id || !date || !status) {
    return res.status(400).json({
      error:
        "branch_id, period, value, employee, date, and status are required",
    });
  }

  const query = `
    INSERT INTO entries (period,branch_id,employee_id,kpi, value, date, status)
    VALUES (?, ?, ?, ?, ?, ?,?)
  `;

  const values = [period, branch_id, employee_id, kpi, value, date, status];

  pool.query(query, values, (error, result) => {
    if (error) {
      console.error("Error inserting audit entry:", error);
      return res.status(500).json({ error: "Internal server error" });
    }
    res.json({
      message: "Audit entry added successfully!",
      entryId: result.insertId,
    });
  });
});

// Returns computed KPI scores for all staff in the branch.
summaryRouter.get("/scores", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  const staffQuery = `
    SELECT
      u.id AS staffId,
      u.name AS staffName,
      a.kpi,
      SUM(a.amount) AS target,
      SUM(e.value) AS achieved
    FROM users u
    LEFT JOIN allocations a ON u.id = a.user_id AND a.period = ? AND a.branch_id = ? AND u.period = ?
    LEFT JOIN entries e ON u.id = e.employee_id AND e.period = ? AND e.branch_id = ? AND e.status = 'Verified' AND a.kpi = e.kpi
    WHERE u.branch_id = ? AND u.role IN ('ATTENDER', 'CLERK')
    GROUP BY u.id, u.name, a.kpi
  `;

  const branchQuery = `
    SELECT
        t.kpi,
        t.amount AS target,
        e.total_achieved AS achieved
    FROM
        (
            SELECT kpi, amount FROM targets WHERE period = ? AND branch_id = ? AND kpi IN ('recovery', 'audit', 'insurance')
        ) AS t
    LEFT JOIN
        (SELECT kpi, SUM(value) AS total_achieved FROM entries WHERE period = ? AND branch_id = ? AND status = 'Verified' GROUP BY kpi) AS e
    ON t.kpi = e.kpi
  `;

  pool.query(
    staffQuery,
    [period, branchId, period, period, branchId, branchId],
    (error, staffResults) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });

      pool.query(
        branchQuery,
        [period, branchId, period, branchId],
        (error, branchResults) => {
          if (error)
            return res.status(500).json({ error: "Internal server error" });

          const calculateScore = (kpi, achieved, target) => {
            let outOf10;
            const ratio = achieved / target;
            const auditRatio =
              row.kpi === "audit" ? row.achieved / row.target : 0;
            const recoveryRatio =
              row.kpi === "recovery" ? row.achieved / row.target : 0;

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

              case "recovery":
              case "audit":
                if (ratio <= 1) {
                  outOf10 = ratio * 10;
                } else {
                  outOf10 = 12.5;
                }
                break;
              default:
                outOf10 = 0;
            }
            return Math.max(0, Math.min(12.5, isNaN(outOf10) ? 0 : outOf10));
          };

          const branchScores = {};
          branchResults.forEach((row) => {
            branchScores[row.kpi] = calculateScore(
              row.kpi,
              row.achieved,
              row.target,
            );
          });

          const staffData = {};
          staffResults.forEach((row) => {
            if (!row.kpi) return;
            if (!staffData[row.staffId]) {
              staffData[row.staffId] = {
                staffId: row.staffId,
                staffName: row.staffName,
                kpis: {},
              };
            }
            staffData[row.staffId].kpis[row.kpi] = {
              score: calculateScore(row.kpi, row.achieved, row.target),
              target: row.target,
            };
          });

          const scores = Object.values(staffData).map((staff) => {
            const finalScores = {
              staffId: staff.staffId,
              staffName: staff.staffName,
              deposit: staff.kpis.deposit ? staff.kpis.deposit.score : 0,
              loan_gen: staff.kpis.loan_gen ? staff.kpis.loan_gen.score : 0,
              loan_amulya: staff.kpis.loan_amulya
                ? staff.kpis.loan_amulya.score
                : 0,
              recovery: branchScores.recovery || 0,
              insurance: branchScores.insurance || 0,
              audit: branchScores.audit || 0,
            };

            let totalScore = 0;
            let kpiCount = 0;

            if (staff.kpis.deposit && staff.kpis.deposit.target > 0) {
              totalScore += finalScores.deposit;
              kpiCount++;
            }
            if (staff.kpis.loan_gen && staff.kpis.loan_gen.target > 0) {
              totalScore += finalScores.loan_gen;
              kpiCount++;
            }
            if (staff.kpis.loan_amulya && staff.kpis.loan_amulya.target > 0) {
              totalScore += finalScores.loan_amulya;
              kpiCount++;
            }

            if (branchScores.recovery) {
              totalScore += finalScores.recovery;
              kpiCount++;
            }
            if (branchScores.insurance) {
              totalScore += finalScores.insurance;
              kpiCount++;
            }
            if (branchScores.audit) {
              totalScore += finalScores.audit;
              kpiCount++;
            }

            finalScores.total = kpiCount > 0 ? totalScore / kpiCount : 0;
            return finalScores;
          });

          res.json(scores);
        },
      );
    },
  );
});

// Returns summary counts for the BM dashboard.
summaryRouter.get("/bm-dashboard-counts", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  const counts = { balance_deposit: {}, loan_gen: {} };

  //deposit Data

  const totalPreviousTargetsQuery = `
    SELECT amount AS total 
    FROM dashboard_table 
    WHERE period = ? AND branch_id = ? AND kpi='balance_deposit'
  `;

  pool.query(
    totalPreviousTargetsQuery,
    [period, branchId],
    (error, results1) => {
      if (error)
        return res.status(500).json({ error: "Internal server error (prev)" });
      counts.balance_deposit.PreviousBalance = results1[0]?.total || 0;

      const totalTargetsQuery = `
      SELECT amount AS total 
      FROM targets 
      WHERE period = ? AND branch_id = ? AND kpi='deposit'
    `;

      pool.query(totalTargetsQuery, [period, branchId], (error, results2) => {
        if (error)
          return res
            .status(500)
            .json({ error: "Internal server error (target)" });
        counts.balance_deposit.totalPresentTarget = results2[0]?.total || 0;

        const total_target =
          counts.balance_deposit.PreviousBalance +
          counts.balance_deposit.totalPresentTarget;
        counts.balance_deposit.total = total_target;

        const totalTargetsQuery = `
      SELECT amount AS total 
      FROM dashboard_total_achiveved 
      WHERE period = ? AND branch_id = ? AND kpi='balance_deposit'
    `;

        pool.query(totalTargetsQuery, [period, branchId], (error, results3) => {
          if (error)
            return res
              .status(500)
              .json({ error: "Internal server error (target)" });
          counts.balance_deposit.totalAchieved = results3[0]?.total || 0;

          //loan Data

          const totalPreviousTargetsQuery = `
    SELECT amount AS total 
    FROM dashboard_table 
    WHERE period = ? AND branch_id = ? AND kpi='loan_gen'
  `;

          pool.query(
            totalPreviousTargetsQuery,
            [period, branchId],
            (error, results1) => {
              if (error)
                return res
                  .status(500)
                  .json({ error: "Internal server error (prev)" });
              counts.loan_gen.PreviousBalance = results1[0]?.total || 0;

              const totalTargetsQuery = `
      SELECT amount AS total 
      FROM targets 
      WHERE period = ? AND branch_id = ? AND kpi='loan_gen'
    `;

              pool.query(
                totalTargetsQuery,
                [period, branchId],
                (error, results2) => {
                  if (error)
                    return res
                      .status(500)
                      .json({ error: "Internal server error (target)" });
                  counts.loan_gen.totalPresentTarget = results2[0]?.total || 0;

                  const total_target =
                    counts.loan_gen.PreviousBalance +
                    counts.loan_gen.totalPresentTarget;
                  counts.loan_gen.total = total_target;

                  const totalTargetsQuery = `
      SELECT amount AS total 
      FROM dashboard_total_achiveved 
      WHERE period = ? AND branch_id = ? AND kpi='loan_gen'
    `;

                  pool.query(
                    totalTargetsQuery,
                    [period, branchId],
                    (error, results3) => {
                      if (error)
                        return res
                          .status(500)
                          .json({ error: "Internal server error (target)" });
                      counts.loan_gen.totalAchieved = results3[0]?.total || 0;

                      res.json(counts);
                    },
                  );
                },
              );
            },
          );
        });
      });
    },
  );
});

// Returns computed KPI scores for the branch manager. // this old api not existing transfer history //21-02-2026
summaryRouter.get("/bm-scores", async (req, res) => {
  const { period, branchId } = req.query;

  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  try {
    const query = ` 
    SELECT
        k.kpi,

        CASE 
            WHEN k.kpi = 'audit' THEN 100
            WHEN k.kpi = 'insurance' THEN COALESCE(MAX(a.amount),0)
            ELSE COALESCE(MAX(t.amount),0)
        END AS target,

        MAX(COALESCE(w.weightage,0)) AS weightage,
        MAX(COALESCE(e.total_achieved,0)) AS achieved

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
                (
                    e.kpi IN ('audit','recovery')
                    AND e.employee_id = bm.id
                )
                OR
                (
                    e.kpi NOT IN ('audit','recovery')
                    AND e.branch_id = ?
                )
            )

        GROUP BY e.kpi
    ) e
    ON e.kpi = k.kpi

    LEFT JOIN weightage w
        ON w.kpi = k.kpi

    GROUP BY k.kpi
    ORDER BY k.kpi;
    `;

    const [results, BMID] = await Promise.all([
      new Promise((resolve, reject) => {
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
      }),

      new Promise((resolve, reject) => {
        pool.query(
          `SELECT id FROM users WHERE branch_id=? AND role='BM' AND period = ?`,
          [branchId, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows);
          },
        );
      }),
    ]);

    const BM = BMID?.[0]?.id || 0;

    const [insRows] = await Promise.all([
      new Promise((resolve, reject) => {
        pool.query(
          `SELECT SUM(value) AS achieved
           FROM entries
           WHERE period = ? AND employee_id = ? AND kpi = 'insurance'`,
          [period, BM],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows);
          },
        );
      }),
    ]);

    const insuranceAchieved = insRows?.[0]?.achieved || 0;

    const updatedResults = results.map((row) => {
      if (row.kpi === "insurance") row.achieved = insuranceAchieved;
      return row;
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

      let auditRatio = 0;
      let recoveryRatio = 0;

      updatedResults.forEach((r) => {
        if (r.kpi === "audit") auditRatio = r.achieved / r.target;
        if (r.kpi === "recovery") recoveryRatio = r.achieved / r.target;
      });

      updatedResults.forEach((row) => {
        if (!bmKpis.includes(row.kpi)) return;

        let outOf10 = 0;
        const ratio = row.target ? row.achieved / row.target : 0;

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
            if (ratio === 0 || isNaN(ratio)) outOf10 = 0;
            else if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else outOf10 = 12.5;
            break;

          case "recovery":
          case "audit":
            if (isNaN(ratio)) outOf10 = 0;
            else if (ratio <= 1) outOf10 = ratio * 10;
            else outOf10 = 12.5;
            break;
        }

        outOf10 = Math.max(0, Math.min(cap, isNaN(outOf10) ? 0 : outOf10));

        const weightageScore =
          row.kpi === "insurance" && (isNaN(ratio) || ratio === 0)
            ? -2
            : (outOf10 * (row.weightage || 0)) / 100;

        scores[row.kpi] = {
          score: outOf10,
          target: row.target,
          achieved: row.achieved || 0,
          weightage: row.weightage || 0,
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

    res.json(finalScores);
  } catch (err) {
    console.error("BM API Error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Returns computed KPI scores for a specific staff member.// this old api not existing transfer history //21-02-2026
summaryRouter.get("/staff-scores", async (req, res) => {
  const { period, employeeId, branchId } = req.query;
  if (!period || !employeeId || !branchId)
    return res
      .status(400)
      .json({ error: "period, employeeId and branchId are required" });

  const userQuery =
    "SELECT role, branch_id FROM users WHERE id = ? AND period = ?";
  pool.query(userQuery, [employeeId, period], (error, userResults) => {
    if (error) return res.status(500).json({ error: "Internal server error" });
    if (userResults.length === 0)
      return res.status(404).json({ error: "User not found" });

    const userRole = userResults[0].role;
    const branchId = userResults[0].branch_id;

    const calculateScore = (kpi, achieved, target) => {
      let outOf10 = 0;
      if (!target) return 0;

      const ratio = achieved / target;

      if (!calculateScore._auditRatio) calculateScore._auditRatio = 0;
      if (!calculateScore._recoveryRatio) calculateScore._recoveryRatio = 0;

      if (
  calculateScore._auditRatio === undefined
) {
  calculateScore._auditRatio = 0;
}

if (
  calculateScore._recoveryRatio === undefined
) {
  calculateScore._recoveryRatio = 0;
}

      const auditRatio = calculateScore._auditRatio;
      const recoveryRatio = calculateScore._recoveryRatio;

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

        case "recovery":
        case "audit":
          if (ratio <= 1) {
            outOf10 = ratio * 10;
          } else {
            outOf10 = 12.5;
          }
          break;

        default:
          outOf10 = 0;
      }

      return Math.max(0, Math.min(12.5, isNaN(outOf10) ? 0 : outOf10));
    };

    if (userRole === "attender") {
      const roleKpisQuery = "SELECT * FROM role_kpis WHERE role = ?";
      pool.query(roleKpisQuery, [userRole], (err, roleKpis) => {
        if (err)
          return res.status(500).json({ error: "Internal server error" });

        const evaluationsQuery =
          "SELECT * FROM kpi_evaluations WHERE period = ? AND user_id = ?";
        pool.query(
          evaluationsQuery,
          [period, employeeId],
          (err, evaluations) => {
            if (err)
              return res.status(500).json({ error: "Internal server error" });

            const branchInsuranceQuery = `
                        SELECT t.amount AS target, e.total_achieved AS achieved
                        FROM (SELECT amount FROM targets WHERE period = ? AND branch_id = ? AND kpi = 'insurance') AS t
                        LEFT JOIN (SELECT SUM(value) AS total_achieved FROM entries WHERE period = ? AND branch_id = ? AND status = 'Verified' AND kpi = 'insurance') AS e ON 1=1
                    `;
            pool.query(
              branchInsuranceQuery,
              [period, branchId, period, branchId],
              (err, branchInsuranceResult) => {
                if (err)
                  return res
                    .status(500)
                    .json({ error: "Internal server error" });

                const scores = {};
                let totalWeightageScore = 0;
                const branchInsurance = branchInsuranceResult[0];

                roleKpis.forEach((kpi) => {
                  if (kpi.kpi_type === "manual") {
                    const evaluation = evaluations.find(
                      (ev) => ev.role_kpi_id === kpi.id,
                    );
                    if (evaluation) {
                      const score = evaluation.score;
                      const weightageScore = (score * kpi.weightage) / 100;
                      scores[kpi.kpi_name] = {
                        score,
                        weightage: kpi.weightage,
                        weightageScore,
                      };
                      totalWeightageScore += weightageScore;
                    }
                  } else if (kpi.kpi_name === "Insurance Target") {
                    if (branchInsurance && branchInsurance.target) {
                      const outOf10 = calculateScore(
                        "insurance",
                        branchInsurance.achieved,
                        branchInsurance.target,
                      );
                      const weightageScore = (outOf10 * kpi.weightage) / 100;

                      scores["Insurance Target"] = {
                        score: outOf10,
                        target: branchInsurance.target,
                        achieved: branchInsurance.achieved,
                        weightage: kpi.weightage,
                        weightageScore,
                      };
                      totalWeightageScore += weightageScore;
                    }
                  }
                });

                scores.total = totalWeightageScore;
                res.json(scores);
              },
            );
          },
        );
      });
    } else {
      const query = `
SELECT 
    k.kpi,

    MAX(
        CASE 
            WHEN k.kpi = 'recovery'
            AND (
                emp.transfer_date BETWEEN fy.fy_start AND fy.fy_end
                OR emp.user_add_date BETWEEN fy.fy_start AND fy.fy_end
            )
            THEN COALESCE(a.amount,0)

            WHEN k.kpi = 'recovery'
            THEN COALESCE(t.amount,0)

            ELSE COALESCE(a.amount,0)
        END
    ) AS target,

    MAX(COALESCE(w.weightage,0)) AS weightage,
    MAX(COALESCE(e.total_achieved,0)) AS achieved

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
    AND emp.period = ?

CROSS JOIN (
    SELECT 
        STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d') AS fy_start,
        STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d') AS fy_end
) fy

LEFT JOIN allocations a 
    ON k.kpi = a.kpi
    AND a.period = ?
    AND a.user_id = emp.id
    AND a.branch_id = emp.branch_id

LEFT JOIN targets t
    ON k.kpi = t.kpi
    AND t.period = ?
    AND t.branch_id = emp.branch_id

LEFT JOIN weightage w 
    ON k.kpi = w.kpi

LEFT JOIN (
    SELECT 
        e.kpi,
        SUM(e.value) AS total_achieved
    FROM entries e

    JOIN users emp2 
        ON emp2.id = ?
        AND emp2.period = ?

    WHERE 
        e.period = ?
        AND e.status = 'Verified'
        AND e.branch_id = emp2.branch_id

        AND (
            (
                e.kpi IN ('audit','recovery')

                AND (
                    -- NEW JOINED EMPLOYEE
                    (
                        emp2.user_add_date BETWEEN 
                            STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                        AND
                            STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')

                        AND e.employee_id = emp2.id
                    )

                    OR

                    -- TRANSFERRED EMPLOYEE
                    (
                        emp2.transfer_date BETWEEN 
                            STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                        AND
                            STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')

                        AND e.employee_id = emp2.id
                    )

                    OR

                    -- OLD EMPLOYEE → TAKE BM ENTRY
                    (
                        (
                            emp2.user_add_date IS NULL 
                            OR emp2.user_add_date <
                                STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                        )

                        AND

                        (
                            emp2.transfer_date IS NULL 
                            OR emp2.transfer_date <
                                STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                        )

                        AND e.employee_id IN (
                            SELECT id 
                            FROM users 
                            WHERE branch_id = emp2.branch_id
                            AND role = 'BM'
                            AND period = ?
                        )
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

GROUP BY k.kpi
ORDER BY k.kpi
 `;
      pool.query(
        query,
        [
          employeeId,
    period,

    period,
    period,

    period,
    period,

    employeeId,
    period,

    period,

    period,
    period,

    period,
    period,

    period,

    period,

    period
        ],
        (error, results) => {
          results;
          if (error)
            return res.status(500).json({ error: "Internal server error" });

          const branchQuery = `
                  SELECT t.kpi, t.amount AS target, w.weightage, e.total_achieved AS achieved
                  FROM (SELECT kpi, amount FROM targets WHERE period = ? AND branch_id = ? AND kpi IN ('deposit','loan_gen','loan_amulya','recovery', 'audit', 'insurance')) AS t
                  LEFT JOIN (SELECT kpi, SUM(value) AS total_achieved FROM entries WHERE period = ? AND branch_id = ? AND status = 'Verified' GROUP BY kpi) AS e ON t.kpi = e.kpi
                  LEFT JOIN weightage w ON t.kpi = w.kpi
                `;
          pool.query(
            branchQuery,
            [period, branchId, period, branchId],
            (error, branchResults) => {
              if (error)
                return res.status(500).json({ error: "Internal server error" });

              const branchScores = {};

              pool.query(
                `
        SELECT SUM(value) AS achieved
                FROM entries
                WHERE period = ? AND employee_id = ? AND kpi = 'insurance'
      `,
                [period, employeeId],
                (errIns, insRows) => {
                  if (errIns)
                    return res
                      .status(500)
                      .json({ error: "Internal server error" });

                  const insuranceAchieved = insRows?.[0]?.achieved || 0;

                  branchResults.map((row) => {
                    if (row.kpi === "insurance") {
                      row.achieved = insuranceAchieved;
                    }
                    return row;
                  });

                  branchResults.forEach((row) => {
                    const score = calculateScore(
                      row.kpi,
                      row.achieved,
                      row.target,
                    );
                    const weightageScore = (score * (row.weightage || 0)) / 100;
                    branchScores[row.kpi] = {
                      score,
                      target: row.target || 0,
                      achieved: row.achieved || 0,
                      weightage: row.weightage || 0,
                      weightageScore:
                        row.kpi === "insurance" && score === 0
                          ? -2
                          : isNaN(weightageScore)
                            ? 0
                            : weightageScore,
                    };
                  });

                  const scores = {};
                  let totalWeightageScore = 0;
                  results.forEach((row) => {
                    if (row.kpi) {
                      // if (row.kpi === "recovery") {
                      //   const branchRecovery = branchResults.find(
                      //     (b) => b.kpi === "recovery",
                      //   );

                      //   if (branchRecovery) {
                      //     row.target = branchRecovery.target;
                      //     row.achieved = branchRecovery.achieved;
                      //   }
                      // }
                      const score = calculateScore(
                        row.kpi,
                        row.achieved,
                        row.target,
                      );
                      const weightageScore = (score * row.weightage) / 100;

                      scores[row.kpi] = {
                        score,
                        target: row.target || 0,
                        achieved: row.achieved || 0,
                        weightage: row.weightage || 0,
                        weightageScore:
                          row.kpi === "insurance" &&
                          (score === 0 || score === undefined)
                            ? -2
                            : isNaN(weightageScore)
                              ? 0
                              : weightageScore,
                      };

                      totalWeightageScore += scores[row.kpi].weightageScore;
                    }
                  });

                  // scores.deposit = branchScores.deposit;
                  // scores.loan_gen = branchScores.loan_gen;
                  // scores.loan_amulya = branchScores.loan_amulya;
                  // scores.recovery = branchScores.recovery;
                  // scores.audit = branchScores.audit;
                  //  scores.insurance = branchScores.insurance;

                  // After calculating scores.total
                  scores["total"] = totalWeightageScore;

                  Promise.all([
                    new Promise((resolve) => {
                      getHoStaffTransferHistory(
                        pool,
                        period,
                        employeeId,
                        (err, data) => {
                          resolve(err ? [] : data);
                        },
                      );
                    }),
                    new Promise((resolve) => {
                      getAttenderTransferHistory(
                        pool,
                        period,
                        employeeId,
                        (err, data) => {
                          resolve(err ? [] : data);
                        },
                      );
                    }),
                    getTransferKpiHistory(pool, period, employeeId),
                  ])
                    .then(([hoHistory, attHistory, transferHistory]) => {
                      const previousHoScores =
                        hoHistory?.[0]?.transfers?.map(
                          (t) => t.total_weightage_score,
                        ) || [];

                      const previousAttenderScores =
                        attHistory?.[0]?.transfers?.map(
                          (t) => t.total_weightage_score,
                        ) || [];

                      const previousTransferScores =
                        transferHistory?.all_scores || [];

                      const allScores = [
                        ...previousHoScores,
                        ...previousAttenderScores,
                        ...previousTransferScores,
                        scores.total,
                      ];

                      const finalAvg =
                        allScores.length > 0
                          ? allScores.reduce((a, b) => a + b, 0) /
                            allScores.length
                          : 0;

                      scores.originalTotal = scores.total;
                      scores.total = Number(finalAvg.toFixed(2));

                      res.json(scores);
                    })
                    .catch((err) => {
                      console.error("History error:", err);
                      res.status(500).json({ error: "Internal server error" });
                    });
                },
              );
            },
          );
        },
      );
    }
  });
});

//calculate the trasfer staff kpi score this is a function
export async function getTransferKpiHistory(pool, period, staff_id) {
  return new Promise((resolve, reject) => {
    const query = `
      SELECT 
        e.*,
        u.name AS staff_name,
        b.name AS branch_name,
        u.resign AS resigned
      FROM employee_transfer e
      INNER JOIN users u ON u.id = e.staff_id  AND u.period = ?
      INNER JOIN branches b 
        ON b.code COLLATE utf8mb4_unicode_ci = 
           e.old_branch_id COLLATE utf8mb4_unicode_ci AND  b.period COLLATE utf8mb4_unicode_ci = ?
      WHERE 
        e.period COLLATE utf8mb4_unicode_ci = ?
        AND e.staff_id = ?
      ORDER BY e.transfer_date ASC;
    `;

    pool.query(query, [period, period, period, staff_id], (err, transfers) => {
      if (err) return reject("Error fetching transfer data");
      if (!transfers.length) return resolve([]);

      pool.query("SELECT kpi, weightage FROM weightage", (err, weightages) => {
        if (err) return reject("Error fetching weightage");

        const weightageMap = {};
        weightages.forEach((w) => (weightageMap[w.kpi] = w.weightage));

        const getMonthDiff = (startDate, endDate) => {
          const start = new Date(startDate);
          const end = new Date(endDate);
          return (
            (end.getFullYear() - start.getFullYear()) * 12 +
            (end.getMonth() - start.getMonth()) +
            1
          );
        };

        const getFinancialYearStart = (period) => {
          const startYear = parseInt(period.split("-")[0], 10);
          return new Date(startYear, 3, 1);
        };

        const calculateScore = (kpi, achieved, target) => {
          if (!target || target === 0) return 0;

          let outOf10;
          const ratio = achieved / target;
           const auditRatio = kpi === "audit" ? ratio:0;
            const recoveryRatio = kpi === "recovery" ? ratio : 0;

          switch (kpi) {
            case "deposit":
            case "loan_gen":
            case "loan_amulya":
              if (ratio <= 1) {
              outOf10 = ratio * 10;
            } else if (ratio > 1 && ratio < 1.25) {
              outOf10 = 10;
            } else if (
              ratio >= 1.25 &&
              auditRatio >= 0.75 &&
              recoveryRatio >= 0.75
            ) {
              outOf10 = 12.5;
            } else {
              outOf10 = 10;
            }
            break;

            case "recovery":
            case "audit":
              outOf10 = ratio < 1 ? ratio * 10 : 12.5;
              break;

            default:
              outOf10 = 0;
          }

          return Math.max(0, Math.min(12.5, outOf10));
        };

        const staffResult = {
          staff_id: transfers[0].staff_id,
          name: transfers[0].staff_name,
          period: transfers[0].period,
          resigned: transfers[0].resigned,
          transfers: [],
          avg_kpi: 0,
          branch_avg_kpi: {},
          all_scores: [],
        };

        let allBranchKpiScores = [];
        const branchWiseKpi = {};
        const branchTransferDates = {};

        transfers.forEach((t) => {
          const branchScores = {};
          let totalWeightageScore = 0;

          const kpis = [
            {
              key: "deposit",
              achieved: t.deposit_achieved,
              target: t.deposit_target,
            },
            {
              key: "loan_gen",
              achieved: t.loan_gen_achieved,
              target: t.loan_gen_target,
            },
            {
              key: "loan_amulya",
              achieved: t.loan_amulya_achieved,
              target: t.loan_amulya_target,
            },
            {
              key: "recovery",
              achieved: t.recovery_achieved,
              target: t.recovery_target,
            },
            {
              key: "audit",
              achieved: t.audit_achieved,
              target: t.audit_target,
            },
          ];

          kpis.forEach((row) => {
            if (!row.target) return;

            const score = calculateScore(row.key, row.achieved, row.target);
            const weightage = weightageMap[row.key] || 0;
            const weightageScore = (score * weightage) / 100;

            branchScores[row.key] = {
              achieved: row.achieved || 0,
              target: row.target || 0,
              score,
              weightage,
              weightageScore,
            };

            totalWeightageScore += weightageScore;
          });

         
          allBranchKpiScores.push(totalWeightageScore);

          if (!branchWiseKpi[t.branch_name]) {
            branchWiseKpi[t.branch_name] = { total: 0, count: 0 };
            branchTransferDates[t.branch_name] = [];
          }

          branchWiseKpi[t.branch_name].total += totalWeightageScore;
          branchWiseKpi[t.branch_name].count += 1;
          branchTransferDates[t.branch_name].push(new Date(t.transfer_date));

          staffResult.transfers.push({
            transfer_date: t.transfer_date,
            branch_name: t.branch_name,
            old_designation: t.old_designation,
            new_designation: t.new_designation,
            total_weightage_score: totalWeightageScore,
            ...branchScores,
          });
        });

        if (allBranchKpiScores.length > 0) {
          const sum = allBranchKpiScores.reduce((a, b) => a + b, 0);
          staffResult.avg_kpi = sum / allBranchKpiScores.length;
          staffResult.all_scores = allBranchKpiScores;
        }

        const fyStart = getFinancialYearStart(staffResult.period);

        Object.keys(branchWiseKpi).forEach((branch) => {
          const { total, count } = branchWiseKpi[branch];
          const avgKpi = count > 0 ? total / count : 0;

          const dates = branchTransferDates[branch].sort((a, b) => a - b);

          let months;
          if (dates.length === 1) {
            months = getMonthDiff(fyStart, dates[0]);
          } else {
            months = getMonthDiff(dates[0], dates[dates.length - 1]);
          }

          staffResult.branch_avg_kpi[branch] = {
            avg_kpi: avgKpi,
            months,
          };
        });
     
        resolve(staffResult);
      });
    });
  });
}

function getFinancialYearDates(period) {
  const startYear = parseInt(period.split("-")[0]); // 2025
  const endYear = startYear + 1; // 2026

  return {
    startDate: `${startYear}-04-01`,
    endDate: `${endYear}-03-31`,
  };
}
// get all staff score in give branch id
summaryRouter.get("/staff-scores-all", async (req, res) => {
  const { period, branchId } = req.query;

  if (!period || !branchId) {
    return res.status(400).json({ error: "period and branchId required" });
  }

  try {
    const calculateScore = (
      kpi,
      achieved = 0,
      target = 0,
      auditScore = 0,
      recoveryScore = 0,
    ) => {
      if (!target) return 0;

      const ratio = achieved / target;
      const auditRatio = auditScore / 10;
      const recoveryRatio = recoveryScore / 10;

      let outOf10 = 0;

      switch (kpi) {
        case "deposit":
        case "loan_gen":
        case "loan_amulya":
          if (ratio <= 1) outOf10 = ratio * 10;
            else if (ratio < 1.25) outOf10 = 10;
            else if (auditRatio >= 0.75 && recoveryRatio >= 0.75)
              outOf10 = 12.5;
            else outOf10 = 10;
            break;
 
        case "recovery":
        case "audit":
        case "insurance": 
          if (ratio <= 1) outOf10 = ratio * 10;
          else if (ratio < 1.25) outOf10 = 10;
          else outOf10 = 12.5;
          break;

        default:
          outOf10 = 0;
      }

      return Math.max(0, Math.min(12.5, outOf10));
    };

    const staffQuery = `
SELECT 
    u.id AS staffId,
    u.name AS staffName,
    k.kpi,

    CASE 
        WHEN k.kpi = 'recovery'
        AND u.transfer_date IS NULL
        AND (u.user_add_date IS NULL OR u.user_add_date <= 
            STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d'))
        THEN COALESCE(t.amount,0)

        WHEN k.kpi = 'recovery'
        AND (
            u.transfer_date BETWEEN 
                STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                AND STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')
            OR
            u.user_add_date BETWEEN 
                STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                AND STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')
        )
        THEN COALESCE(a.amount,0)

        ELSE COALESCE(a.amount,0)
    END AS target,

    COALESCE(w.weightage, 0) AS weightage,
    COALESCE(e.total_achieved, 0) AS achieved

FROM users u

CROSS JOIN (
    SELECT 'deposit' AS kpi
    UNION SELECT 'loan_gen'
    UNION SELECT 'loan_amulya'
    UNION SELECT 'recovery'
    UNION SELECT 'insurance'
    UNION SELECT 'audit'
) k

LEFT JOIN (
    SELECT user_id, kpi, MAX(amount) AS amount
    FROM allocations
    WHERE period = ?
    AND branch_id = ?
    GROUP BY user_id, kpi
) a 
ON a.user_id = u.id AND a.kpi = k.kpi

LEFT JOIN targets t
ON t.kpi = k.kpi
AND t.branch_id = u.branch_id
AND t.period = ?

LEFT JOIN (
    SELECT 
        e.kpi,
        e.branch_id,
        e.employee_id,
        SUM(e.value) AS total_achieved
    FROM entries e
    WHERE e.period = ?
    AND e.status = 'Verified'
    GROUP BY e.employee_id, e.kpi, e.branch_id
) e 
ON (
    (
        k.kpi NOT IN ('audit','recovery')
        AND e.employee_id = u.id
    )
    OR
    (
        k.kpi IN ('audit','recovery')
        AND (
            (
                u.user_add_date BETWEEN 
                STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                AND STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')
                AND e.employee_id = u.id
            )
            OR
            (
                u.transfer_date BETWEEN 
                STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d')
                AND STR_TO_DATE(CONCAT(2000 + RIGHT(?,2),'-03-31'),'%Y-%m-%d')
                AND e.employee_id = u.id
            )
            OR
            (
                u.transfer_date IS NULL
                AND (u.user_add_date IS NULL OR u.user_add_date <= 
                    STR_TO_DATE(CONCAT(LEFT(?,4),'-04-01'),'%Y-%m-%d'))
                AND e.employee_id = (
                    SELECT id 
                    FROM users 
                    WHERE role = 'BM' 
                    AND period = ? 
                    AND branch_id = u.branch_id
                    LIMIT 1
                )
            )
        )
    )
)
AND e.kpi = k.kpi
AND e.branch_id = u.branch_id

LEFT JOIN weightage w ON w.kpi = k.kpi

WHERE u.branch_id = ?
AND u.role = 'CLERK'
AND u.period = ?

ORDER BY u.id, k.kpi;
`;

    const results = await new Promise((resolve, reject) => {
      pool.query(
        staffQuery,
        [
          period,
          period,

          period,
          period,

          period,
          period,
          branchId,
          period,

          period,
          period,

          period,
          period,

          period,

          period,
          period,
          branchId,
          period,
        ],
        (err, rows) => {
          if (err) return reject(err);
          resolve(rows);
        },
      );
    });

    const staffScores = {};

    results.forEach((row) => {
      if (row.kpi === "audit" || row.kpi === "recovery") {
        const id = row.staffId;

        if (!staffScores[id]) {
          staffScores[id] = {
            staffId: id,
            staffName: row.staffName,
          };
        }

        const score = calculateScore(row.kpi, row.achieved, row.target);

        staffScores[id][row.kpi] = {
          score,
          target: Number(row.target),
          achieved: Number(row.achieved),
          weightage: Number(row.weightage),
          weightageScore: (score * row.weightage) / 100,
        };
      }
    });

    results.forEach((row) => {
      if (row.kpi === "audit" || row.kpi === "recovery") return;

      const id = row.staffId;

      const score = calculateScore(
        row.kpi,
        row.achieved,
        row.target,
        staffScores[id]?.audit?.score || 0,
        staffScores[id]?.recovery?.score || 0,
      );

      const weighted = (score * row.weightage) / 100;

      staffScores[id][row.kpi] = {
        score,
        target: Number(row.target),
        achieved: Number(row.achieved),
        weightage: Number(row.weightage),
        weightageScore: row.kpi === "insurance" && score === 0 ? -2 : weighted,
      };
    });

    const staffArray = Object.values(staffScores);

    for (const staff of staffArray) {
      let total = 0;

      [
        "deposit",
        "loan_gen",
        "loan_amulya",
        "recovery",
        "audit",
        "insurance",
      ].forEach((kpi) => {
        total += Number(staff[kpi]?.weightageScore || 0);
      });

      staff.originalTotal = total;

      if (
        staff.insurance?.score < 7.5 &&
        staff.recovery?.score < 7.5 &&
        total > 10
      ) {
        total = 10;
      }

      staff.total = Number(total.toFixed(2));
    }

    res.json(staffArray);
  } catch (err) {
    console.error("API Error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

//ho specific scores
summaryRouter.get("/ho-hod-scores", (req, res) => {
  const { period, hod_id } = req.query;

  if (!period || !hod_id) {
    return res.status(400).json({ error: "period and hod_id are required" });
  }

  // 1. Get KPI weightages
  const weightageQuery = `
    SELECT kpi, weightage 
    FROM weightage 
    WHERE kpi IN ('allocated_work','discipline_time','work_performance','branch_communication','insurance')
  `;

  pool.query(weightageQuery, (err, weightageResults) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: "Failed to fetch weightages" });
    }

    const weightageMap = {};
    weightageResults.forEach((row) => (weightageMap[row.kpi] = row.weightage));

    // 2. Get all HO staff IDs under this HOD and branch
    const staffQuery = `
      SELECT DISTINCT ho_staff_id AS staffId
      FROM ho_staff_kpi
      WHERE hod_id = ? AND period = ? 
    `;

    pool.query(staffQuery, [hod_id, period], (err, staffResults) => {
      if (err) {
        console.error(err);
        return res.status(500).json({ error: "Failed to fetch HO staff" });
      }

      if (!staffResults.length)
        return res.json({
          message: "No staff found for this branch",
          scores: {},
        });

      const staffIds = staffResults.map((s) => s.staffId);

      // 3. Get KPI data for all staff (filtered by branch_id)
      const kpiQuery = `
        SELECT ho_staff_id, allocated_work, discipline_time, work_performance, branch_communication, insurance
        FROM ho_staff_kpi
        WHERE period = ? AND hod_id = ? AND ho_staff_id IN (?)
      `;

      pool.query(kpiQuery, [period, hod_id, staffIds], (err, kpiResults) => {
        if (err) {
          console.error(err);
          return res.status(500).json({ error: "Failed to fetch KPI data" });
        }

        const kpis = [
          "allocated_work",
          "discipline_time",
          "work_performance",
          "branch_communication",
          "insurance",
        ];
        const avgAchieved = {};

        // 4. Calculate average achieved per KPI
        kpis.forEach((kpi) => {
          const total = kpiResults.reduce(
            (sum, row) => sum + parseFloat(row[kpi] || 0),
            0,
          );
          avgAchieved[kpi] = kpiResults.length ? total / kpiResults.length : 0;
        });

        // 5. Calculate score & weighted score
        const scores = {};
        let totalWeightageScore = 0;

        kpis.forEach((kpi) => {
          const achieved = avgAchieved[kpi];
          let score;

          if (kpi === "insurance") {
            score = (achieved / 40000) * 10;
          } else {
            const weightage = weightageMap[kpi] || 0;
            if (achieved <= weightage) {
              score = (achieved / weightage) * 10;
            } else {
              score = achieved / weightage < 1.25 ? 10 : 12.5;
            }
          }

          const weightage = weightageMap[kpi] || 0;
          let weightageScore = (score * weightage) / 100;
          if (kpi === "insurance" && score === 0) weightageScore = -2;

          scores[kpi] = { score, achieved, weightage, weightageScore };
          totalWeightageScore += weightageScore;
        });

        scores.total = totalWeightageScore;

        res.json({ hod_id, period, scores });
      });
    });
  });
});

//ho staff specific scores
summaryRouter.get("/specfic-hostaff-scores", (req, res) => {
  const { period, ho_staff_id, branch_id } = req.query;
  if (!period || !ho_staff_id)
    return res
      .status(400)
      .json({ error: "period, ho_staff_id and branch_id are required" });

  // 1. Get KPI weightages
  const weightageQuery = `SELECT kpi, weightage FROM weightage WHERE kpi IN ('allocated_work','discipline_time','work_performance','branch_communication','insurance')`;
  pool.query(weightageQuery, (err, weightageResults) => {
    if (err)
      return res.status(500).json({ error: "Failed to fetch weightage" });

    const weightageMap = {};
    weightageResults.forEach((w) => (weightageMap[w.kpi] = w.weightage));

    // 2. Get specific HO staff KPI
    const kpiQuery = `
      SELECT allocated_work, discipline_time, work_performance, branch_communication, insurance
      FROM ho_staff_kpi
      WHERE period = ? AND ho_staff_id = ? 
    `;
    pool.query(kpiQuery, [period, ho_staff_id], (err, results) => {
      if (err)
        return res.status(500).json({ error: "Failed to fetch HO staff KPIs" });
      if (!results.length)
        return res.status(404).json({ error: "HO staff KPI not found" });

      const kpis = [
        "allocated_work",
        "discipline_time",
        "work_performance",
        "branch_communication",
        "insurance",
      ];
      const scores = {};
      let totalWeightageScore = 0;

      const staffData = results[0];

      kpis.forEach((kpi) => {
        const achieved = parseFloat(staffData[kpi]) || 0;
        const target = kpi === "insurance" ? 40000 : weightageMap[kpi] || 0;

        // Score calculation logic
        let score;
        const ratio = achieved / target;
        if (ratio < 1) score = ratio * 10;
        else if (ratio < 1.25) score = 10;
        else score = 12.5;

        let weightageScore = (score * (weightageMap[kpi] || 0)) / 100;
        if (kpi === "insurance" && score === 0) weightageScore = -2;

        scores[kpi] = {
          score,
          target,
          achieved,
          weightage: weightageMap[kpi] || 0,
          weightageScore,
        };
        totalWeightageScore += weightageScore;
      });

      scores.total = totalWeightageScore;
      res.json(scores);
    });
  });
});

//get salary for given staff on his/her pfno
summaryRouter.get("/get-salary", (req, res) => {
  const { period, PF_NO } = req.query;
  if (!period || !PF_NO)
    return res.status(400).json({ error: "period and PF_NO are required" });

  const query = `select salary ,increment from base_salary where period=? and PF_NO=?`;

  pool.query(query, [period, PF_NO], (error, result) => {
    if (error) {
      return res.status(500).json({ error: "Internal server error" });
    }
    res.json(result);
  });
});

//get salary of whole branch on this branch code
summaryRouter.get("/get-salary-all-staff", (req, res) => {
  const { period, branch_id } = req.query;
  if (!period || !branch_id)
    return res.status(400).json({ error: "period and branch_id are required" });
  const query = `select u.id,b.salary ,b.increment from base_salary b join users u on u.PF_NO=b.PF_NO where b.period=? and b.branch_id=? and u.period = ?`;

  pool.query(query, [period, branch_id, period], (error, result) => {
    if (error) {
      return res.status(500).json({ error: "Internal server error" });
    }
    res.json(result);
  });
});
//get salary of ho staff and other authority
summaryRouter.get("/get-salary-all-agms", (req, res) => {
  const { period, hod_id } = req.query;
  if (!period || !hod_id)
    return res.status(400).json({ error: "period and hod_id are required" });
  const query = `select u.id,b.salary ,b.increment from base_salary b join users u on u.PF_NO=b.PF_NO where b.period=? and u.id=? and u.period=?`;

  pool.query(query, [period, hod_id, period], (error, result) => {
    if (error) {
      return res.status(500).json({ error: "Internal server error" });
    }
    res.json(result);
  });
});

function getMonthStartDate(dateInput) {
  const d = new Date(dateInput);

  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");

  return `${year}-${month}-01`;
}

//get trasfer BM score
summaryRouter.get("/transfer-bm-scores", (req, res) => {
  const { period, branchId } = req.query;

  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  function getFY(period) {
    const [s, e] = period.split("-");
    const sy = parseInt(s);
    const ey = sy - (sy % 100) + parseInt(e);

    return {
      start: new Date(Date.UTC(sy, 3, 1)),
      end: new Date(Date.UTC(ey, 2, 31)),
    };
  }

  const fy = getFY(period);

  pool.query(
    `SELECT id FROM users 
     WHERE branch_id=? AND period=? AND role='BM' 
     ORDER BY transfer_date DESC`,
    [branchId, period],
    (err, BMRows) => {
      if (err) return res.status(500).json({ error: "Internal server error" });

      const BMID = BMRows?.[0]?.id || BMRows?.[1]?.id || 0;

      pool.query(
        `SELECT *
         FROM bm_transfer_target 
         WHERE staff_id=? AND period=? 
         ORDER BY id DESC LIMIT 1`,
        [BMID, period],
        (err, bmRows) => {
          if (err)
            return res.status(500).json({ error: "Internal server error" });

          if (!bmRows.length) return res.status(200).json([]);

          const bm = bmRows[0] || {};
          const transferDate = new Date(bm.transfer_date);

          function monthDiffstart(d1, d2) {
            return Math.max(
              0,
              (d2.getFullYear() - d1.getFullYear()) * 12 +
                (d2.getMonth() - d1.getMonth()) +
                1,
            );
          }

          const totalMonths = monthDiffstart(transferDate, fy.end);

          const bmTargets = {
            deposit: bm.deposit_target || 0,
            loan_gen: bm.loan_gen_target || 0,
            loan_amulya: bm.loan_amulya_target || 0,
            audit: bm.audit_target || 0,
            recovery: bm.recovery_target || 0,
            insurance: bm.insurance_target || 0,
          };

          const mouthStart = getMonthStartDate(transferDate);

          const entriesQuery = `
            SELECT kpi, SUM(value) AS achieved 
            FROM entries
            WHERE branch_id=? 
            AND period=? 
            AND status='Verified'
            AND date >= ? AND date <= ?
            GROUP BY kpi
          `;

          pool.query(
            entriesQuery,
            [branchId, period, mouthStart, fy.end],
            (err, entryRows) => {
              if (err)
                return res.status(500).json({ error: "Internal server error" });

              const achievedMap = {};
              entryRows.forEach((r) => (achievedMap[r.kpi] = r.achieved || 0));

              achievedMap["audit"] =
                (achievedMap["audit"] || 0) + (bm.audit_achieved || 0);
              achievedMap["recovery"] =
                (achievedMap["recovery"] || 0) + (bm.recovery_achieved || 0);

              pool.query(
                `
                SELECT SUM(value) AS achieved
                FROM entries
                WHERE period=? AND employee_id=? 
                AND kpi='insurance'
                AND status='Verified'
              `,
                [period, BMID],
                (errIns, insRows) => {
                  if (errIns)
                    return res
                      .status(500)
                      .json({ error: "Internal server error" });

                  achievedMap["insurance"] = insRows?.[0]?.achieved || 0;

                  pool.query(`SELECT * FROM weightage`, (errW, wRows) => {
                    if (errW)
                      return res
                        .status(500)
                        .json({ error: "Internal server error" });

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

                    const calculateScores = (cap) => {
                      const scores = {};
                      let total = 0;

                      bmKpis.forEach((kpi) => {
                        const target = bmTargets[kpi] || 0;
                        const achieved = achievedMap[kpi] || 0;
                        const weight = weightageMap[kpi] || 0;

                        let outOf10 = 0;

                        if (target === 0) {
                          outOf10 = 0;
                        } else {
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
                              if (ratio <= 1) outOf10 = ratio * 10;
                              else outOf10 = 12.5;
                              break;

                            case "insurance":
                              if (ratio === 0) outOf10 = -2;
                              else if (ratio <= 1) outOf10 = ratio * 10;
                              else if (ratio < 1.25) outOf10 = 10;
                              else outOf10 = 12.5;
                              break;
                          }
                        }

                        outOf10 = Math.max(0, Math.min(cap, outOf10));

                        let weightScore = (outOf10 * weight) / 100;

                        if (kpi === "insurance" && outOf10 === 0) {
                          weightScore = -2;
                        }

                        scores[kpi] = {
                          score: outOf10,
                          target,
                          achieved,
                          weightage: weight,
                          weightageScore:
                            kpi === "insurance" && outOf10 === 0
                              ? -2
                              : weightScore,
                        };

                        total += weightScore;
                      });

                      scores["total"] = total;
                      return scores;
                    };

                    const prelim = calculateScores(12.5);

                    const cap =
                      prelim.total > 10 &&
                      prelim.insurance.score < 7.5 &&
                      prelim.recovery.score < 7.5
                        ? 10
                        : 12.5;

                    const finalScores = calculateScores(cap);

                    res.json({
                      ...finalScores,
                      totalMonthsWorked: totalMonths,
                    });
                  });
                },
              );
            },
          );
        },
      );
    },
  );
});

// all branch or gm  all attender api and also for get specific attender score /// this old api not existing transfer history //21-02-2026
summaryRouter.get("/branch-attenders", async (req, res) => {
  const { period, branchId, hod_id, staff_id } = req.query;

  if (!period) {
    return res.status(400).json({ error: "period is required" });
  }

  try {
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

    if (!attenderKpis.length) return res.json([]);

    const kpiMap = {};
    attenderKpis.forEach((k) => {
      kpiMap[k.kpi_name] = k;
    });

    const normalizedBranchId2 =
      branchId && branchId !== "null" && branchId !== "undefined"
        ? branchId
        : null;
    const normalizedStaffId =
      staff_id && staff_id !== "null" && staff_id !== "undefined"
        ? staff_id
        : null;

    const users = await new Promise((resolve, reject) => {
      let sql, params;
      if (normalizedStaffId) {
        sql =
          "SELECT id, branch_id, name FROM users WHERE id=? AND period=? AND role='Attender'";
        params = [staff_id, period];
      } else if (normalizedBranchId2) {
        sql =
          "SELECT id, branch_id, name FROM users WHERE branch_id=? AND period=? AND role='Attender'";
        params = [normalizedBranchId2, period];
      } else {
        sql =
          "SELECT id, hod_id, name FROM users WHERE hod_id=? AND period=? AND role='Attender'";
        params = [hod_id, period];
      }

      pool.query(sql, params, (err, rows) => {
        if (err) return reject(err);
        resolve(rows);
      });
    });

    if (!users.length) return res.json([]);

    let masterUserId = null;
    if (normalizedStaffId) {
      const staffUser = users[0];
      if (staffUser && staffUser.branch_id) {
        const bmUser = await new Promise((resolve, reject) => {
          pool.query(
            "SELECT id FROM users WHERE branch_id=? AND period=? AND role='BM' LIMIT 1",
            [staffUser.branch_id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(rows[0] || null);
            },
          );
        });
        masterUserId = bmUser ? bmUser.id : normalizedStaffId;
      } else {
        masterUserId = normalizedStaffId;
      }
    } else if (normalizedBranchId2) {
      const bmUser = await new Promise((resolve, reject) => {
        pool.query(
          "SELECT id FROM users WHERE branch_id=? AND period=? AND role='BM' LIMIT 1",
          [normalizedBranchId2, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows[0] || null);
          },
        );
      });
      masterUserId = bmUser ? bmUser.id : null;
    } else if (hod_id) {
      masterUserId = hod_id;
    }

    const response = await Promise.all(
      users.map(async (user) => {
        const [userKpis, insuranceRow] = await Promise.all([
          new Promise((resolve, reject) => {
            pool.query(
              `
              SELECT role_kpi_mapping_id, SUM(value) AS total
              FROM user_kpi_entry
              WHERE user_id=? AND period=? AND master_user_id=?
              GROUP BY role_kpi_mapping_id
              `,
              [user.id, period, masterUserId],
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

        for (const kpi of attenderKpis) {
          let achieved = 0;
          let target = kpi.weightage;

          if (kpi.kpi_name === "Cleanliness") {
            achieved = userKpis[kpi.role_kpi_mapping_id] || 0;
          }

          if (kpi.kpi_name === "Attitude, Behavior & Discipline") {
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
            else if (ratio < 1.25) score = 10;
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

        const currentTotal = Number(totalScore.toFixed(2));

        const [hoHistory, attHistory, transferHistory] = await Promise.all([
          new Promise((resolve) => {
            getHoStaffTransferHistory(pool, period, user.id, (err, data) =>
              resolve(err ? [] : data),
            );
          }),
          new Promise((resolve) => {
            getAttenderTransferHistory(pool, period, user.id, (err, data) =>
              resolve(err ? [] : data),
            );
          }),
          getTransferKpiHistory(pool, period, user.id),
        ]);

        const previousHoScores =
          hoHistory?.[0]?.transfers?.map((t) => t.total_weightage_score) || [];

        const previousAttenderScores =
          attHistory?.[0]?.transfers?.map((t) => t.total_weightage_score) || [];

        const previousTransferScores = transferHistory?.all_scores || [];

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

        return {
          staffId: user.id,
          staffName: user.name,
          total: Number(totalScore.toFixed(2)),
          kpis: finalKpis,
          avgTotal: Number(finalAvg.toFixed(2)),
        };
      }),
    );

    res.json(response);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});
