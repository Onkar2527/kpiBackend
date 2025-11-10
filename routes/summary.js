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
  console.log(req.body);

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
        CASE WHEN k.kpi = 'audit' THEN 0 ELSE e.total_achieved END AS achieved
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
        if (bmKpis.includes(row.kpi)) {
          let outOf10;
          const ratio = row.achieved / row.target;
          const auditRatio =
            row.kpi === "audit" ? row.achieved / row.target : 0;
          const recoveryRatio =
            row.kpi === "recovery" ? row.achieved / row.target : 0;
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

          outOf10 = Math.max(0, Math.min(cap, isNaN(outOf10) ? 0 : outOf10));

          const weightageScore = (outOf10 * (row.weightage || 0)) / 100;
          scores[row.kpi] = {
            score: outOf10,
            target: row.target || 0,
            achieved: row.achieved || 0,
            weightage: row.weightage || 0,
            weightageScore:
              row.kpi === "insurance" && outOf10 === 0
                ? -2
                : isNaN(weightageScore)
                ? 0
                : weightageScore,
          };
          totalWeightageScore += scores[row.kpi].weightageScore;
        }
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
  });
});

// GET /summary/staff-scores?period=YYYY-MM&employeeId=E01
// Returns computed KPI scores for a specific staff member.
summaryRouter.get("/staff-scores", (req, res) => {
  const { period, employeeId,branchId } = req.query;
  if (!period || !employeeId || !branchId )
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
      const ratio = achieved / target;
      const auditRatio = kpi === "audit" ? kpi.achieved / kpi.target : 0;
      const recoveryRatio =
        kpi === "recovery" ? kpi.achieved / kpi.target : 0;
      
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
             
              
              branchResults.forEach((row) => {
                const score = calculateScore(row.kpi, row.achieved, row.target);
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
                      row.kpi === "insurance" && outOf10 === 0
                        ? -2
                        : isNaN(weightageScore)
                        ? 0
                        : weightageScore,
                  };
                  totalWeightageScore += scores[row.kpi].weightageScore;
                }
              });

              scores.deposit = branchScores.deposit;
              scores.loan_gen = branchScores.loan_gen;
              scores.loan_amulya = branchScores.loan_amulya;
              scores.recovery = branchScores.recovery;
              scores.audit = branchScores.audit;
              scores.insurance = branchScores.insurance;

              if (scores.recovery)
                totalWeightageScore += scores.recovery.weightageScore;
              if (scores.audit)
                totalWeightageScore += scores.audit.weightageScore;
              if (scores.insurance)
                totalWeightageScore += scores.insurance.weightageScore;

              scores["total"] = totalWeightageScore;
             
              
              res.json(scores);
            }
          );
        }
      );
    }
  });
});

summaryRouter.get("/staff-scores-all", (req, res) => {
  const { period, branchId } = req.query;
  if (!period || !branchId)
    return res.status(400).json({ error: "period and branchId required" });

  const branchQuery = `
    SELECT
        k.kpi,
        CASE WHEN k.kpi = 'audit' THEN 100 ELSE t.amount END AS target,
        w.weightage,
         e.total_achieved  AS achieved
    FROM
        (
            SELECT 'recovery' as kpi UNION 
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

  pool.query(
    branchQuery,
    [period, branchId, period, branchId],
    (error, branchResults) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });

      const calculateScore = (kpi, achieved, target) => {
        let outOf10;
        const ratio = achieved / target;
        const auditRatio = kpi === "audit" ? achieved / target : 0;
        const recoveryRatio = kpi === "recovery" ? achieved / target : 0;
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

      const branchScores = {};
      branchResults.forEach((row) => {
        const score = calculateScore(row.kpi, row.achieved, row.target);
        const weightageScore = (score * (row.weightage || 0)) / 100;
        branchScores[row.kpi] = {
          score: score,
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

      const query = `
      SELECT
        u.id AS staffId,
        u.name AS staffName,
        a.kpi,
        a.amount AS target,
        w.weightage,
        e.total_achieved AS achieved
      FROM users u
      LEFT JOIN allocations a ON u.id = a.user_id AND a.period = ?
      LEFT JOIN (
        SELECT employee_id, kpi, SUM(value) AS total_achieved
        FROM entries
        WHERE period = ? AND status = 'Verified'
        GROUP BY employee_id, kpi
      ) e ON u.id = e.employee_id AND a.kpi = e.kpi
      LEFT JOIN weightage w ON a.kpi = w.kpi
      WHERE u.branch_id = ? AND u.role IN ('CLERK')
    `;

      pool.query(query, [period, period, branchId], (error, results) => {
        if (error)
          return res.status(500).json({ error: "Internal server error" });

        const staffScores = {};
        results.forEach((row) => {
          if (!staffScores[row.staffId]) {
            staffScores[row.staffId] = {
              staffId: row.staffId,
              staffName: row.staffName,
              total: 0,
            };
          }

          if (["deposit", "loan_gen", "loan_amulya"].includes(row.kpi)) {
            let outOf10;
            const ratio = row.achieved / row.target;
            switch (row.kpi) {
              case "deposit":
              case "loan_gen":
                if (ratio < 1) {
                  outOf10 = ratio * 10;
                } else if (ratio < 1.25) {
                  outOf10 = 10;
                } else {
                  outOf10 = 12.5;
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
              default:
                outOf10 = 0;
            }
            outOf10 = Math.max(0, Math.min(12.5, isNaN(outOf10) ? 0 : outOf10));
            const weightageScore = (outOf10 * row.weightage) / 100;

            staffScores[row.staffId][row.kpi] = {
              score: outOf10,
              target: row.target || 0,
              achieved: row.achieved || 0,
              weightage: row.weightage || 0,
              weightageScore:
                row.kpi === "insurance" && outOf10 === 0
                  ? -2
                  : isNaN(weightageScore)
                  ? 0
                  : weightageScore,
            };
          }
        });

        const defaultBranchKpi = {
          score: 0,
          target: 0,
          achieved: 0,
          weightage: 0,
          weightageScore: 0,
        };
        Object.values(staffScores).forEach((staff) => {
          staff.recovery = branchScores.recovery || defaultBranchKpi;
          staff.audit = branchScores.audit || defaultBranchKpi;
          staff.insurance = branchScores.insurance || defaultBranchKpi;
          let totalWeightageScore = 0;
          [
            "deposit",
            "loan_gen",
            "loan_amulya",
            "recovery",
            "audit",
            "insurance",
          ].forEach((kpi) => {
            if (staff[kpi]) {
              totalWeightageScore += staff[kpi].weightageScore;
            }
          });
          const query = `
        SELECT COALESCE(SUM(kpi_total)/COUNT(*), 0) AS avgKpi 
        FROM employee_transfer 
        WHERE staff_id = ?
      `;

          pool.query(query, [staff.staffId], (error, result) => {
            if (error)
              return res.status(500).json({ error: "Internal server error" });
            const previousKpi = result[0]?.avgKpi || 0;

            const insuranceRation = staff["insurance"].score;
            const recoveryRation = staff["recovery"].score;
            const total = totalWeightageScore + previousKpi;
            
            
            if (insuranceRation < 7.5 && recoveryRation < 7.5 && total > 10) {
              total = 10;
            }
            staff.total = total;
          });
        });
        res.json(Object.values(staffScores));
      });
    }
  );
});
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

summaryRouter.get("/ho-staff-scores-all", (req, res) => {
  const { period, hod_id, branch_id } = req.query;

  if (!period || !hod_id) {
    return res
      .status(400)
      .json({ error: "period, hod_id, and branch_id required" });
  }

  // 1. Get weightage dynamically
  const weightageQuery = `SELECT kpi, weightage FROM weightage`;

  pool.query(weightageQuery, (err, weightageResults) => {
    if (err)
      return res.status(500).json({ error: "Failed to fetch weightage" });

    const weightageMap = {};
    weightageResults.forEach((row) => {
      weightageMap[row.kpi] = row.weightage;
    });

    // 2. Get HO staff under the specific HOD and branch
    const query = `
      SELECT u.id AS staffId, u.name AS staffName,
             h.allocated_work, h.discipline_time, h.work_performance,
             h.branch_communication, h.insurance
      FROM users u
      LEFT JOIN ho_staff_kpi h 
        ON u.id = h.ho_staff_id 
      WHERE u.role = 'HO_staff' and u.hod_id=?
        
    `;

    pool.query(query, [hod_id], (error, results) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      if (!results || results.length === 0) return res.json([]);

      const staffScores = [];

      results.forEach((row) => {
        const staff = {
          staffId: row.staffId,
          staffName: row.staffName,
          total: 0,
        };

        let totalWeightageScore = 0;

        Object.keys(weightageMap).forEach((kpi) => {
          const achieved = row[kpi] || 0;
          let score;

          if (kpi === "insurance") {
            score = (achieved / 40000) * 10;
          } else {
            score = calculateHoScore(achieved, weightageMap[kpi]);
          }

          let weightageScore = (score * weightageMap[kpi]) / 100;
          if (kpi === "insurance" && achieved === 0) weightageScore = -2;

          staff[kpi] = {
            score,
            achieved,
            weightage: weightageMap[kpi],
            weightageScore,
          };

          totalWeightageScore += weightageScore;
        });

        staff.total = totalWeightageScore;
        staffScores.push(staff);
      });

      res.json(staffScores);
    });
  });
});
//get all ho staff scores under a hod
// summaryRouter.get('/ho-staff-scores-all', (req, res) => {
//   const { period, hod_id } = req.query;
//   if (!period || !hod_id) return res.status(400).json({ error: 'period and hod_id required' });

//   // 1. Get weightage dynamically
//   const weightageQuery = `SELECT kpi, weightage FROM weightage`;

//   pool.query(weightageQuery, (err, weightageResults) => {
//     if (err) return res.status(500).json({ error: 'Failed to fetch weightage' });

//     const weightageMap = {};
//     weightageResults.forEach(row => {
//       weightageMap[row.kpi] = row.weightage;
//     });

//     // 2. Get all HO Staff under the HOD for the period (even if no KPI record exists)
//     const query = `
//       SELECT u.id AS staffId, u.name AS staffName,
//              h.allocated_work, h.discipline_time, h.work_performance,
//              h.branch_communication, h.insurance
//       FROM users u
//       LEFT JOIN ho_staff_kpi h
//         ON u.id = h.ho_staff_id
//        AND h.period = ?
//        AND h.hod_id = ?
//       WHERE u.role = 'HO_staff'
//     `;

//     pool.query(query, [period, hod_id], (error, results) => {
//       if (error) return res.status(500).json({ error: 'Internal server error' });
//       if (!results || results.length === 0) return res.json([]);

//       const staffScores = [];

//       results.forEach(row => {
//         const staff = {
//           staffId: row.staffId,
//           staffName: row.staffName,
//           total: 0
//         };

//         let totalWeightageScore = 0;

//         Object.keys(weightageMap).forEach(kpi => {
//           const achieved = row[kpi] || 0;  // If no KPI, default to 0
//           let score;
//           if(kpi === 'insurance'){
//             score = achieved/40000*10;
//           }else{
//             score = calculateHoScore(achieved, weightageMap[kpi]);
//           }
//           let weightageScore = (score * weightageMap[kpi]) / 100;

//           if (kpi === 'insurance' && achieved === 0) weightageScore = -2;

//           staff[kpi] = {
//             score,
//             achieved,
//             weightage: weightageMap[kpi],
//             weightageScore
//           };

//           totalWeightageScore += weightageScore;
//         });

//         staff.total = totalWeightageScore;
//         staffScores.push(staff);
//       });

//       res.json(staffScores);
//     });
//   });
// });
// Save or update HO staff KPI scores
summaryRouter.post("/save-or-update-ho-staff-kpi", (req, res) => {
  const { branch_id, ho_staff_id, hod_id, period, scores } = req.body;

  if (!ho_staff_id || !hod_id || !period || !scores) {
    return res
      .status(400)
      .json({ error: "ho_staff_id, hod_id, period and scores are required" });
  }
  // Map scores array to object
  const scoreMap = {};
  scores.forEach((s) => {
    scoreMap[s.kpi] = s.achieved || 0;
  });

  const queryCheck = `select * from ho_staff_kpi where ho_staff_id=?`;
  pool.query(queryCheck, [ho_staff_id], (err, result) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: "Database error" });
    }
    if (result.length === 0) {
      // Insert new record
      const sql = `
    INSERT INTO ho_staff_kpi 
    ( hod_id, ho_staff_id, period, allocated_work, discipline_time, work_performance, branch_communication, insurance)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      allocated_work = VALUES(allocated_work),
      discipline_time = VALUES(discipline_time),
      work_performance = VALUES(work_performance),
      branch_communication = VALUES(branch_communication),
      insurance = VALUES(insurance),
      updated_at = CURRENT_TIMESTAMP
  `;

      const values = [
        hod_id,
        ho_staff_id,
        period,
        scoreMap["allocated_work"] || 0,
        scoreMap["discipline_time"] || 0,
        scoreMap["work_performance"] || 0,
        scoreMap["branch_communication"] || 0,
        scoreMap["insurance"] || 0,
      ];

      pool.query(sql, values, (err, result) => {
        if (err) {
          console.error(err);
          return res.status(500).json({ error: "Failed to save scores" });
        }
        res.json({ message: "HO Staff KPI saved successfully", result });
      });
    } else {
      const sql = `
    UPDATE ho_staff_kpi
    SET allocated_work = ?, 
        discipline_time = ?, 
        work_performance = ?, 
        branch_communication = ?, 
        insurance = ?, 
        updated_at = CURRENT_TIMESTAMP
    WHERE ho_staff_id = ? AND hod_id = ? AND period = ?
  `;

      const values = [
        scoreMap["allocated_work"] || 0,
        scoreMap["discipline_time"] || 0,
        scoreMap["work_performance"] || 0,
        scoreMap["branch_communication"] || 0,
        scoreMap["insurance"] || 0,
        ho_staff_id,
        hod_id,
        period,
      ];

      pool.query(sql, values, (err, result) => {
        if (err) {
          console.error(err);
          return res.status(500).json({ error: "Failed to update scores" });
        }

        if (result.affectedRows === 0) {
          return res
            .status(404)
            .json({ message: "Record not found to update" });
        }

        res.json({ message: "HO Staff KPI updated successfully", result });
      });
    }
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
    return res
      .status(400)
      .json({ error: "period and PF_NO are required" });

    const query=`select salary ,increment from base_salary where period=? and PF_NO=?`;

    pool.query(query,[period,PF_NO],(error,result)=>{
      if(error){
        return res.status(500).json({ error: "Internal server error" });
      }
      res.json(result);
    });
    });


