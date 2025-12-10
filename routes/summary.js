import express from "express";
import pool from "../db.js";

// Router providing summary endpoints for Achieved totals.

export const summaryRouter = express.Router();

// GET /summary/branch?period=YYYY-MM&branchId=B01
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
    }
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

// GET /summary/scores?period=YYYY-MM&branchId=B01
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
    LEFT JOIN allocations a ON u.id = a.user_id AND a.period = ? AND a.branch_id = ?
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
    [period, branchId, period, branchId, branchId],
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
            // switch (kpi) {
            //   case 'deposit':
            //   case 'loan_gen':
            //   case 'loan_amulya':
            //   case 'insurance':
            //     if (ratio < 1) {
            //       outOf10 = ratio * 10;
            //     } else if (ratio < 1.25) {
            //       outOf10 = 10;
            //     } else {
            //       outOf10 = 12.5;
            //     }
            //     break;
            //   case 'recovery':
            //   case 'audit':
            //     if (ratio < 1) {
            //       outOf10 = ratio * 10;
            //     } else {
            //       outOf10 = 12.5;
            //     }
            //     break;
            //   default:
            //     outOf10 = 0;
            // }
            switch (row.kpi) {
              case "deposit":
              case "loan_gen":
                if (ratio < 1) {
                  outOf10 = ratio * 10;
                } else if (ratio < 1.25) {
                  outOf10 = 10;
                } else if (auditRatio >= 0.75 && recoveryRatio >= 0.75) {
                  outOf10 = 12.5;
                } else {
                  outOf10 = 10;
                }
                break;

              case "loan_amulya":
                if (ratio < 1) {
                  outOf10 = ratio * 10;
                } else if (ratio < 1.25) {
                  outOf10 = 10;
                } else {
                  outOf10 = 12.5;
                }
                break;
              case "insurance":
                if (ratio === 0) {
                  outOf10 = -2;
                } else if (ratio < 1) {
                  outOf10 = ratio * 10;
                } else if (ratio < 1.25) {
                  outOf10 = 10;
                } else {
                  outOf10 = 12.5;
                }
                break;

              case "recovery":
              case "audit":
                if (ratio < 1) {
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
              row.target
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
        }
      );
    }
  );
});

// GET /summary/bm-dashboard-counts?period=YYYY-MM&branchId=B01
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
                    }
                  );
                }
              );
            }
          );
        });
      });
    }
  );
});

// GET /summary/bm-scores?period=YYYY-MM&branchId=B01
// Returns computed KPI scores for the branch manager.
summaryRouter.get("/bm-scores", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  const query = `
    SELECT
        k.kpi,
        CASE WHEN k.kpi = 'audit' THEN 100 ELSE t.amount END AS target,
        w.weightage,
        e.total_achieved  AS achieved
    FROM
        (
            SELECT 'deposit' as kpi UNION 
            SELECT 'loan_gen' UNION 
            SELECT 'loan_amulya' UNION 
            SELECT 'recovery' UNION 
            SELECT 'audit' UNION 
            SELECT 'insurance'
        ) as k
    LEFT JOIN targets t ON k.kpi = t.kpi AND t.period = ? AND t.branch_id = ?
    LEFT JOIN (
        SELECT kpi, SUM(value) AS total_achieved 
        FROM entries 
        WHERE period = ? AND branch_id = ? AND status = 'Verified' 
        GROUP BY kpi
    ) e ON k.kpi = e.kpi
    LEFT JOIN weightage w ON k.kpi = w.kpi
  `;

  pool.query(query, [period, branchId, period, branchId], (error, results) => {
    if (error) return res.status(500).json({ error: "Internal server error" });
    pool.query(
      `select id from users where branch_id = ? and role='BM'`,
      [branchId],
      (err, BMID) => {
        const BM = BMID?.[0]?.id || 0;

        pool.query(
          `
        SELECT SUM(value) AS achieved
                FROM entries
                WHERE period = ? AND employee_id = ? AND kpi = 'insurance'
      `,
          [period, BM],
          (errIns, insRows) => {
            if (errIns)
              return res.status(500).json({ error: "Internal server error" });

            const insuranceAchieved = insRows?.[0]?.achieved || 0;

            results = results.map((row) => {
              if (row.kpi === "insurance") {
                row.achieved = insuranceAchieved;
              }
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

                if (!row.target || row.target === 0) {
                  scores[row.kpi] = {
                    score: 0,
                    target: 0,
                    achieved: row.achieved || 0,
                    weightage: row.weightage || 0,
                    weightageScore: 0,
                  };
                  return;
                }

                const ratio = row.achieved / row.target;
                const auditRatio = row.kpi === "audit" ? ratio : 0;
                const recoveryRatio = row.kpi === "recovery" ? ratio : 0;

                switch (row.kpi) {
                  case "deposit":
                  case "loan_gen":
                    if (ratio < 1) {
                      outOf10 = ratio * 10;
                    } else if (ratio < 1.25) {
                      outOf10 = 10;
                    } else if (auditRatio >= 0.75 && recoveryRatio >= 0.75) {
                      outOf10 = 12.5;
                    } else {
                      outOf10 = 10;
                    }
                    break;

                  case "loan_amulya":
                    if (ratio < 1) {
                      outOf10 = ratio * 10;
                    } else if (ratio < 1.25) {
                      outOf10 = 10;
                    } else {
                      outOf10 = 12.5;
                    }
                    break;

                  case "insurance":
                    if (ratio === 0) {
                      outOf10 = -2;
                    } else if (ratio < 1) {
                      outOf10 = ratio * 10;
                    } else if (ratio < 1.25) {
                      outOf10 = 10;
                    } else {
                      outOf10 = 12.5;
                    }
                    break;

                  case "recovery":
                  case "audit":
                    if (ratio < 1) {
                      outOf10 = ratio * 10;
                    } else {
                      outOf10 = 12.5;
                    }
                    break;

                  default:
                    outOf10 = 0;
                }

                outOf10 = Math.max(
                  0,
                  Math.min(cap, isNaN(outOf10) ? 0 : outOf10)
                );

                const weightageScore = (outOf10 * (row.weightage || 0)) / 100;

                scores[row.kpi] = {
                  score: outOf10,
                  target: row.target,
                  achieved: row.achieved || 0,
                  weightage: row.weightage || 0,

                  weightageScore:
                    row.kpi === "insurance" && ratio === 0
                      ? -2
                      : weightageScore,
                };

                totalWeightageScore += scores[row.kpi].weightageScore;
              });

              scores["total"] = totalWeightageScore;
              return scores;
            };

            const preliminaryScores = calculateScores(12.5);

            const insuranceScore = preliminaryScores["insurance"]?.score || 0;
            const recoveryScore = preliminaryScores["recovery"]?.score || 0;

            const cap =
              preliminaryScores.total > 10 &&
              insuranceScore < 7.5 &&
              recoveryScore < 7.5
                ? 10
                : 12.5;

            const finalScores = calculateScores(cap);

            res.json(finalScores);
          }
        );
      }
    );
  });
});

// GET /summary/staff-scores?period=YYYY-MM&employeeId=E01
// Returns computed KPI scores for a specific staff member.
summaryRouter.get("/staff-scores", (req, res) => {
  const { period, employeeId, branchId } = req.query;
  if (!period || !employeeId || !branchId)
    return res
      .status(400)
      .json({ error: "period, employeeId and branchId are required" });

  const userQuery = "SELECT role, branch_id FROM users WHERE id = ?";
  pool.query(userQuery, [employeeId], (error, userResults) => {
    if (error) return res.status(500).json({ error: "Internal server error" });
    if (userResults.length === 0)
      return res.status(404).json({ error: "User not found" });

    const userRole = userResults[0].role;
    const branchId = userResults[0].branch_id;

    const calculateScore = (kpi, achieved, target) => {
      let outOf10;
      if (target === 0 && !target) {
        outOf10 = 0;
        return;
      }
      const ratio = achieved / target;
      const auditRatio = kpi === "audit" ? kpi.achieved / kpi.target : 0;
      const recoveryRatio = kpi === "recovery" ? kpi.achieved / kpi.target : 0;

      switch (kpi) {
        case "deposit":
        case "loan_gen":
          if (ratio < 1) {
            outOf10 = ratio * 10;
          } else if (ratio < 1.25) {
            outOf10 = 10;
          } else if (auditRatio >= 0.75 && recoveryRatio >= 0.75) {
            outOf10 = 12.5;
          } else {
            outOf10 = 10;
          }
          break;

        case "loan_amulya":
          if (ratio < 1) {
            outOf10 = ratio * 10;
          } else if (ratio < 1.25) {
            outOf10 = 10;
          } else {
            outOf10 = 12.5;
          }
          break;
        case "insurance":
          if (ratio === 0) {
            outOf10 = -2;
          } else if (ratio < 1) {
            outOf10 = ratio * 10;
          } else if (ratio < 1.25) {
            outOf10 = 10;
          } else {
            outOf10 = 12.5;
          }
          break;

        case "recovery":
        case "audit":
          if (ratio < 1) {
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
                      (ev) => ev.role_kpi_id === kpi.id
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
                        branchInsurance.target
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
              }
            );
          }
        );
      });
    } else {
      // Existing logic for other staff roles
      const query = `
              SELECT a.kpi, a.amount AS target, w.weightage, e.total_achieved AS achieved
              FROM (SELECT kpi, amount, user_id FROM allocations WHERE period = ? AND user_id = ?) AS a
              LEFT JOIN (SELECT kpi, SUM(value) AS total_achieved FROM entries WHERE period = ? AND employee_id = ? AND status = 'Verified' GROUP BY kpi) AS e ON a.kpi = e.kpi
              LEFT JOIN weightage w ON a.kpi = w.kpi
            `;
      pool.query(
        query,
        [period, employeeId, period, employeeId],
        (error, results) => {
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
                      row.target
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
                      if (row.kpi === "recovery") {
                        const branchRecovery = branchResults.find(
                          (b) => b.kpi === "recovery"
                        );
                        console.log(branchRecovery);
                        
                        if (branchRecovery) {
                          row.target = branchRecovery.target;
                          row.achieved = branchRecovery.achieved;
                        }
                      }
                      const score = calculateScore(
                        row.kpi,
                        row.achieved,
                        row.target
                      );
                      const weightageScore = (score * row.weightage) / 100;
                      scores[row.kpi] = {
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
                      totalWeightageScore += scores[row.kpi].weightageScore;
                    }
                  });
                  
                  
                  // scores.deposit = branchScores.deposit;
                  // scores.loan_gen = branchScores.loan_gen;
                  // scores.loan_amulya = branchScores.loan_amulya;
                  scores.recovery = branchScores.recovery;
                  // scores.audit = branchScores.audit;
                  // scores.insurance = branchScores.insurance;
                  

                  scores["total"] = totalWeightageScore;
                  console.log(scores);
                  
  
                  res.json(scores);
                }
              );
            }
          );
        }
      );
    }
  });
});
export async function getTransferKpiHistory(pool, period, staff_id) {
  return new Promise((resolve, reject) => {
    const query = `
      SELECT 
        e.*,
        u.name AS staff_name,
        b.name AS branch_name,
        u.resign as resigned
      FROM employee_transfer e
      INNER JOIN users u ON u.id = e.staff_id
      INNER JOIN branches b 
        ON b.code COLLATE utf8mb4_unicode_ci = 
           e.old_branch_id COLLATE utf8mb4_unicode_ci
      WHERE 
        e.period COLLATE utf8mb4_unicode_ci = ?
        AND e.staff_id = ?
      ORDER BY e.transfer_date ASC;
    `;

    pool.query(query, [period, staff_id], (err, transfers) => {
      if (err) return reject("Error fetching transfer data");
      if (!transfers.length) return resolve([]);

      pool.query("SELECT kpi, weightage FROM weightage", (err, weightages) => {
        if (err) return reject("Error fetching weightage");

        const weightageMap = {};
        weightages.forEach((w) => (weightageMap[w.kpi] = w.weightage));

        const calculateScore = (kpi, achieved, target) => {
          if (!target || target === 0) return 0;

          let outOf10;
          const ratio = achieved / target;

          switch (kpi) {
            case "deposit":
            case "loan_gen":
            case "loan_amulya":
              outOf10 = ratio < 1 ? ratio * 10 : ratio < 1.25 ? 10 : 12.5;
              break;
            case "insurance":
              outOf10 =
                ratio === 0
                  ? -2
                  : ratio < 1
                  ? ratio * 10
                  : ratio < 1.25
                  ? 10
                  : 12.5;
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
        };

        let allBranchKpiScores = [];

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
            {
              key: "insurance",
              achieved: t.insurance_achieved,
              target: t.insurance_target,
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
              weightageScore:
                row.key === "insurance" && score === 0 ? -2 : weightageScore,
            };

            totalWeightageScore += branchScores[row.key].weightageScore;
          });

          allBranchKpiScores.push(totalWeightageScore);

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
        }

        resolve(staffResult);
      });
    });
  });
}

summaryRouter.get("/staff-scores-all", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  const branchQuery = `
    SELECT
        k.kpi,
        CASE WHEN k.kpi = 'audit' THEN 100 ELSE COALESCE(t.amount, 0) END AS target,
        COALESCE(w.weightage, 0) AS weightage,
        COALESCE(e.total_achieved, 0) AS achieved
    FROM (SELECT 'recovery' as kpi UNION SELECT 'audit' UNION SELECT 'insurance') k
    LEFT JOIN targets t ON k.kpi = t.kpi AND t.period = ? AND t.branch_id = ?
    LEFT JOIN (
        SELECT kpi, SUM(value) AS total_achieved 
        FROM entries 
        WHERE period = ? AND branch_id = ? AND status='Verified'
        GROUP BY kpi
    ) e ON k.kpi = e.kpi
    LEFT JOIN weightage w ON k.kpi = w.kpi
  `;

  const calculateScore = (kpi, achieved = 0, target = 0) => {
    if (!target) return 0;

    const ratio = achieved / target;
    let outOf10 = 0;

    switch (kpi) {
      case "deposit":
      case "loan_gen":
        if (ratio < 1) outOf10 = ratio * 10;
        else if (ratio < 1.25) outOf10 = 10;
        else outOf10 = 12.5;
        break;

      case "loan_amulya":
        if (ratio < 1) outOf10 = ratio * 10;
        else if (ratio < 1.25) outOf10 = 10;
        else outOf10 = 12.5;
        break;

      case "insurance":
        if (achieved === 0) return 0;
        if (ratio < 1) outOf10 = ratio * 10;
        else if (ratio < 1.25) outOf10 = 10;
        else outOf10 = 12.5;
        break;

      case "recovery":
      case "audit":
        if (ratio < 1) outOf10 = ratio * 10;
        else outOf10 = 12.5;
        break;

      default:
        outOf10 = 0;
    }

    return Math.max(0, Math.min(12.5, outOf10));
  };

  pool.query(
    branchQuery,
    [period, branchId, period, branchId],
    (error, branchResults) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });

      const branchScores = {};
      branchResults.forEach((row) => {
        const score = calculateScore(row.kpi, row.achieved, row.target);
        const weighted = (score * row.weightage) / 100;

        branchScores[row.kpi] = {
          score,
          target: Number(row.target),
          achieved: Number(row.achieved),
          weightage: Number(row.weightage),
          weightageScore: isNaN(weighted) ? 0 : weighted,
        };
      });

      const staffQuery = `
        SELECT 
    u.id AS staffId,
    u.name AS staffName,
    k.kpi,
    COALESCE(a.amount, 0) AS target,
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
    WHERE period = ? AND  branch_id = ?
    GROUP BY user_id, kpi
) a ON a.user_id = u.id AND a.kpi = k.kpi

LEFT JOIN (
    SELECT employee_id, kpi, SUM(value) AS total_achieved
    FROM entries
    WHERE period = ? AND status='Verified' AND branch_id = ?
    GROUP BY employee_id, kpi
) e ON e.employee_id = u.id AND e.kpi = k.kpi

LEFT JOIN weightage w ON w.kpi = k.kpi

WHERE u.branch_id = ?
  AND u.role = 'CLERK'

ORDER BY u.id, k.kpi`;

      pool.query(
        staffQuery,
        [period, branchId, period, branchId, branchId],
        (error2, results) => {
          if (error2)
            return res.status(500).json({ error: "Internal server error" });

          const staffScores = {};

          results.forEach((row) => {
            const id = row.staffId;

            if (!staffScores[id]) {
              staffScores[id] = {
                staffId: id,
                staffName: row.staffName,
                total: 0,
              };
            }

            if (!row.kpi) return;

            const score = calculateScore(row.kpi, row.achieved, row.target);
            const weighted = (score * row.weightage) / 100;

            staffScores[id][row.kpi] = {
              score,
              target: Number(row.target),
              achieved: Number(row.achieved),
              weightage: Number(row.weightage),
              weightageScore:
                row.kpi === "insurance" && score === 0 ? -2 : weighted,
            };
          });

          const staffArray = Object.values(staffScores);

          const KPI_LIST = [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "audit",
            "insurance",
          ];

          staffArray.forEach((staff) => {
            KPI_LIST.forEach((kpi) => {
              if (!staff[kpi]) {
                staff[kpi] = {
                  target: 0,
                  achieved: 0,
                  weightage: 0,
                  score: 0,
                  weightageScore: 0,
                };
              }
            });
          });

          staffArray.forEach((staff) => {
            staff.audit.achieved = branchScores.audit.achieved;
            staff.audit.target = branchScores.audit.target;
            staff.audit.weightage = branchScores.audit.weightage;
          });
          staffArray.forEach((staff) => {
            staff.recovery.achieved = branchScores.recovery.achieved;
            staff.recovery.target = branchScores.recovery.target;
            staff.recovery.weightage = branchScores.recovery.weightage;
          });

          const promises = [];

          staffArray.forEach((staff) => {
            const sid = staff.staffId;

            // Previous KPI
            promises.push(
              new Promise(async (resolve, reject) => {
                try {
                  const history = await getTransferKpiHistory(
                    pool,
                    period,
                    sid
                  );

                  staff.previousKpi = Number(history.avg_kpi || 0);

                  resolve();
                } catch (err) {
                  reject(err);
                }
              })
            );

            // Insurance achieved
            promises.push(
              new Promise((resolve, reject) => {
                pool.query(
                  `SELECT SUM(value) AS achieved
                   FROM entries
                   WHERE period=? AND employee_id=? AND kpi='insurance'`,
                  [period, sid],
                  (err, ins) => {
                    if (err) return reject(err);
                    const achieved = Number(ins[0]?.achieved || 0);
                    staff.insurance.achieved = achieved;
                    resolve();
                  }
                );
              })
            );
          });

          Promise.all(promises)
            .then(() => {
              staffArray.forEach((staff) => {
                // recalc insurance score after patch
                const ins = staff.insurance;
                ins.score = calculateScore(
                  "insurance",
                  ins.achieved,
                  ins.target
                );
                ins.weightageScore =
                  ins.score === 0 ? -2 : (ins.score * ins.weightage) / 100;

                const ih = staff.audit;
                ih.score = calculateScore("audit", ih.achieved, ih.target);
                ih.weightageScore = (ih.score * ih.weightage) / 100;

                // TOTAL CALCULATION
                let total = 0;

                [
                  "deposit",
                  "loan_gen",
                  "loan_amulya",
                  "recovery",
                  "audit",
                  "insurance",
                ].forEach((kpi) => {
                  total += Number(staff[kpi].weightageScore || 0);
                });

                total += staff.previousKpi;

                // Penalty rule
                if (
                  staff.insurance.score < 7.5 &&
                  staff.recovery.score < 7.5 &&
                  total > 10
                )
                  total = 10;

                staff.total = total;
              });

              res.json(staffArray);
            })
            .catch((err) => {
              console.error("Promise error:", err);
              res.status(500).json({ error: "Internal server error" });
            });
        }
      );
    }
  );
});
// summaryRouter.get("/staff-scores-all", (req, res) => {
//   const { period, branchId } = req.query;
//   if (!period || !branchId)
//     return res.status(400).json({ error: "period and branchId required" });

//   const branchQuery = `
//     SELECT
//         k.kpi,
//         CASE WHEN k.kpi = 'audit' THEN 100 ELSE COALESCE(t.amount, 0) END AS target,
//         COALESCE(w.weightage, 0) AS weightage,
//         COALESCE(e.total_achieved, 0) AS achieved
//     FROM (SELECT 'recovery' as kpi UNION SELECT 'audit' UNION SELECT 'insurance') k
//     LEFT JOIN targets t ON k.kpi = t.kpi AND t.period = ? AND t.branch_id = ?
//     LEFT JOIN (
//         SELECT kpi, SUM(value) AS total_achieved
//         FROM entries
//         WHERE period = ? AND branch_id = ? AND status='Verified'
//         GROUP BY kpi
//     ) e ON k.kpi = e.kpi
//     LEFT JOIN weightage w ON k.kpi = w.kpi
//   `;

//   const calculateScore = (kpi, achieved = 0, target = 0) => {
//     if (!target) return 0;

//     const ratio = achieved / target;
//     let outOf10 = 0;

//     switch (kpi) {
//       case "deposit":
//       case "loan_gen":
//         if (ratio < 1) outOf10 = ratio * 10;
//         else if (ratio < 1.25) outOf10 = 10;
//         else outOf10 = 12.5;
//         break;

//       case "loan_amulya":
//         if (ratio < 1) outOf10 = ratio * 10;
//         else if (ratio < 1.25) outOf10 = 10;
//         else outOf10 = 12.5;
//         break;

//       case "insurance":
//         if (achieved === 0) return 0;
//         if (ratio < 1) outOf10 = ratio * 10;
//         else if (ratio < 1.25) outOf10 = 10;
//         else outOf10 = 12.5;
//         break;

//       case "recovery":
//       case "audit":
//         if (ratio < 1) outOf10 = ratio * 10;
//         else outOf10 = 12.5;
//         break;

//       default:
//         outOf10 = 0;
//     }

//     return Math.max(0, Math.min(12.5, outOf10));
//   };

//   pool.query(
//     branchQuery,
//     [period, branchId, period, branchId],
//     (error, branchResults) => {
//       if (error)
//         return res.status(500).json({ error: "Internal server error" });

//       const branchScores = {};
//       branchResults.forEach((row) => {
//         const score = calculateScore(row.kpi, row.achieved, row.target);
//         const weighted = (score * row.weightage) / 100;

//         branchScores[row.kpi] = {
//           score,
//           target: Number(row.target),
//           achieved: Number(row.achieved),
//           weightage: Number(row.weightage),
//           weightageScore: isNaN(weighted) ? 0 : weighted,
//         };
//       });

//       const staffQuery = `
//         SELECT
//     u.id AS staffId,
//     u.name AS staffName,
//     k.kpi,
//     COALESCE(a.amount, 0) AS target,
//     COALESCE(w.weightage, 0) AS weightage,
//     COALESCE(e.total_achieved, 0) AS achieved
// FROM users u

// CROSS JOIN (
//     SELECT 'deposit' AS kpi
//     UNION SELECT 'loan_gen'
//     UNION SELECT 'loan_amulya'
//     UNION SELECT 'recovery'
//     UNION SELECT 'insurance'
//     UNION SELECT 'audit'
// ) k

// LEFT JOIN (
//     SELECT user_id, kpi, MAX(amount) AS amount
//     FROM allocations
//     WHERE period = ?
//     GROUP BY user_id, kpi
// ) a ON a.user_id = u.id AND a.kpi = k.kpi

// LEFT JOIN (
//     SELECT employee_id, kpi, SUM(value) AS total_achieved
//     FROM entries
//     WHERE period = ? AND status='Verified'
//     GROUP BY employee_id, kpi
// ) e ON e.employee_id = u.id AND e.kpi = k.kpi

// LEFT JOIN weightage w ON w.kpi = k.kpi

// WHERE u.branch_id = ?
//   AND u.role = 'CLERK'

// ORDER BY u.id, k.kpi`;

//       pool.query(staffQuery, [period, period, branchId], (error2, results) => {
//         if (error2)
//           return res.status(500).json({ error: "Internal server error" });

//         const staffScores = {};

//         results.forEach((row) => {
//           const id = row.staffId;

//           if (!staffScores[id]) {
//             staffScores[id] = {
//               staffId: id,
//               staffName: row.staffName,
//               total: 0,
//             };
//           }

//           if (!row.kpi) return;

//           const score = calculateScore(row.kpi, row.achieved, row.target);
//           const weighted = (score * row.weightage) / 100;

//           staffScores[id][row.kpi] = {
//             score,
//             target: Number(row.target),
//             achieved: Number(row.achieved),
//             weightage: Number(row.weightage),
//             weightageScore:
//               row.kpi === "insurance" && score === 0 ? -2 : weighted,
//           };
//         });

//         const staffArray = Object.values(staffScores);

//         const KPI_LIST = [
//           "deposit",
//           "loan_gen",
//           "loan_amulya",
//           "recovery",
//           "audit",
//           "insurance",
//         ];

//         staffArray.forEach((staff) => {
//           KPI_LIST.forEach((kpi) => {
//             if (!staff[kpi]) {
//               staff[kpi] = {
//                 target: 0,
//                 achieved: 0,
//                 weightage: 0,
//                 score: 0,
//                 weightageScore: 0,
//               };
//             }
//           });
//         });

//         staffArray.forEach((staff) => {
//           staff.audit.achieved = branchScores.audit.achieved;
//           staff.audit.target = branchScores.audit.target;
//           staff.audit.weightage = branchScores.audit.weightage;
//         });

//         const promises = [];

//         staffArray.forEach((staff) => {
//           const sid = staff.staffId;

//           // Previous KPI
//           promises.push(
//             new Promise((resolve, reject) => {
//               pool.query(
//                 `SELECT COALESCE(SUM(kpi_total)/COUNT(*),0) AS avgKpi
//                    FROM employee_transfer WHERE staff_id=?`,
//                 [sid],
//                 (err, rows) => {
//                   if (err) return reject(err);
//                   staff.previousKpi = Number(rows[0]?.avgKpi || 0);
//                   resolve();
//                 }
//               );
//             })
//           );

//           // Insurance achieved
//           promises.push(
//             new Promise((resolve, reject) => {
//               pool.query(
//                 `SELECT SUM(value) AS achieved
//                    FROM entries
//                    WHERE period=? AND employee_id=? AND kpi='insurance'`,
//                 [period, sid],
//                 (err, ins) => {
//                   if (err) return reject(err);
//                   const achieved = Number(ins[0]?.achieved || 0);
//                   staff.insurance.achieved = achieved;
//                   resolve();
//                 }
//               );
//             })
//           );
//         });

//         Promise.all(promises)
//           .then(() => {
//             staffArray.forEach((staff) => {
//               // recalc insurance score after patch
//               const ins = staff.insurance;
//               ins.score = calculateScore("insurance", ins.achieved, ins.target);
//               ins.weightageScore =
//                 ins.score === 0 ? -2 : (ins.score * ins.weightage) / 100;

//               const ih = staff.audit;
//               ih.score = calculateScore("audit", ih.achieved, ih.target);
//               ih.weightageScore = (ih.score * ih.weightage) / 100;

//               // TOTAL CALCULATION
//               let total = 0;

//               [
//                 "deposit",
//                 "loan_gen",
//                 "loan_amulya",
//                 "recovery",
//                 "audit",
//                 "insurance",
//               ].forEach((kpi) => {
//                 total += Number(staff[kpi].weightageScore || 0);
//               });

//               total += staff.previousKpi;

//               // Penalty rule
//               if (
//                 staff.insurance.score < 7.5 &&
//                 staff.recovery.score < 7.5 &&
//                 total > 10
//               )
//                 total = 10;

//               staff.total = total;
//             });

//             res.json(staffArray);
//           })
//           .catch((err) => {
//             console.error("Promise error:", err);
//             res.status(500).json({ error: "Internal server error" });
//           });
//       });
//     }
//   );
// });

// GET /summary/staff-scores-all?period=YYYY-MM&branchId=B01
// Returns computed KPI scores for all staff in the branch.

// for calculating HO score based on achieved and weightage
function calculateHoScore(achieved, weightage) {
  if (achieved === 0) return 0;

  if (achieved < weightage) {
    return (achieved / weightage) * 10;
  } else {
    const ratio = achieved / weightage;
    if (ratio < 1.25) {
      return 10;
    } else {
      return 12.5;
    }
  }
}

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
            0
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

summaryRouter.get("/get-salary-all-staff", (req, res) => {
  const { period, branch_id } = req.query;
  if (!period || !branch_id)
    return res.status(400).json({ error: "period and branch_id are required" });
  const query = `select u.id,b.salary ,b.increment from base_salary b join users u on u.PF_NO=b.PF_NO where b.period=? and b.branch_id=?`;

  pool.query(query, [period, branch_id], (error, result) => {
    if (error) {
      return res.status(500).json({ error: "Internal server error" });
    }
    res.json(result);
  });
});

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
    `SELECT id FROM users WHERE branch_id=? AND role='BM'`,
    [branchId],
    (err, BMRows) => {
      if (err) return res.status(500).json({ error: "Internal server error" });

      const BMID = BMRows?.[0]?.id || 0;

      pool.query(
        `SELECT *
         FROM bm_transfer_target 
         WHERE staff_id=? AND period=? 
         ORDER BY id DESC LIMIT 1`,
        [BMID, period],
        (err, bmRows) => {
          if (err)
            return res.status(500).json({ error: "Internal server error" });

          if (!bmRows.length)
            return res
              .status(404)
              .json({ error: "BM transfer target not found" });

          const bm = bmRows[0];
          const transferDate = new Date(bm.transfer_date);

          function monthDiffstart(d1, d2) {
            return Math.max(
              0,
              (d2.getFullYear() - d1.getFullYear()) * 12 +
                (d2.getMonth() - d1.getMonth()) +
                1
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
            [branchId, period, transferDate, fy.end],
            (err, entryRows) => {
              if (err)
                return res.status(500).json({ error: "Internal server error" });

              const achievedMap = {};
              entryRows.forEach((r) => (achievedMap[r.kpi] = r.achieved || 0));

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

                    // KPIs
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
                              if (ratio < 1) outOf10 = ratio * 10;
                              else if (ratio < 1.25) outOf10 = 10;
                              else if (
                                auditRatio >= 0.75 &&
                                recoveryRatio >= 0.75
                              )
                                outOf10 = 12.5;
                              else outOf10 = 10;
                              break;

                            case "loan_amulya":
                            case "recovery":
                            case "audit":
                              if (ratio < 1) outOf10 = ratio * 10;
                              else outOf10 = 12.5;
                              break;

                            case "insurance":
                              if (ratio === 0) outOf10 = -2;
                              else if (ratio < 1) outOf10 = ratio * 10;
                              else if (ratio < 1.25) outOf10 = 10;
                              else outOf10 = 12.5;
                              break;
                          }
                        }

                        outOf10 = Math.max(0, Math.min(cap, outOf10));
                        const weightScore = (outOf10 * weight) / 100;

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
                }
              );
            }
          );
        }
      );
    }
  );
});
