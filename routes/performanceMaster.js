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
            if (ratio < 1) score = ratio * 10;
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
function calculateStaffScores(period, staffId, role) {
  return new Promise((resolve, reject) => {
    const kpiWeightageQuery = `
      SELECT rkm.id AS role_kpi_mapping_id, km.kpi_name, rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
    `;

    pool.query(kpiWeightageQuery, [role], (err, kpiWeightage) => {
      if (err) return reject("Failed to fetch KPI weightage");

      const userEntryQuery = `
        SELECT role_kpi_mapping_id, value AS achieved
        FROM user_kpi_entry 
        WHERE period = ? AND user_id = ? AND deleted_at IS NULL
      `;

      pool.query(userEntryQuery, [period, staffId], (err2, userEntries) => {
        if (err2) return reject("Failed to fetch user KPI entries");

        const achievedMap = {};
        userEntries.forEach(
          (e) => (achievedMap[e.role_kpi_mapping_id] = e.achieved)
        );

        let totalWeightageScore = 0;
        const scores = {};

        kpiWeightage.forEach((row) => {
          const { role_kpi_mapping_id, kpi_name, weightage } = row;
          const achieved =
            parseFloat(achievedMap[row.role_kpi_mapping_id]) || 0;
          const target =
            kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

          let score = 0;
          if (achieved > 0) {
            const ratio = achieved / target;
            if (ratio < 1) score = ratio * 10;
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
        });

        scores.total = Number(totalWeightageScore.toFixed(2));

        resolve(scores);
      });
    });
  });
}
//Reusable function to calculate KPI score for many branch staff
function calculateBranchStaffScore(period, branchId) {
  return new Promise((resolve, reject) => {
    if (!period || !branchId) return reject("period and branchId required");

    const branchQuery = `
      SELECT
          k.kpi,
          CASE WHEN k.kpi = 'audit' THEN 100 ELSE COALESCE(t.amount, 0) END AS target,
          COALESCE(w.weightage, 0) AS weightage,
          COALESCE(e.total_achieved, 0) AS achieved
      FROM
          (SELECT 'recovery' as kpi 
           UNION SELECT 'audit' 
           UNION SELECT 'insurance') as k
      LEFT JOIN targets t ON k.kpi = t.kpi AND t.period = ? AND t.branch_id = ?
      LEFT JOIN (
          SELECT kpi, SUM(value) AS total_achieved 
          FROM entries 
          WHERE period = ? AND branch_id = ? AND status = 'Verified' 
          GROUP BY kpi
      ) e ON k.kpi = e.kpi
      LEFT JOIN weightage w ON k.kpi = w.kpi
    `;

    // CALCULATE SCORE FUNCTION
    const calculateScore = (kpi, achieved = 0, target = 0) => {
      if (!target || target == 0) return 0;
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
          if (achieved === 0) outOf10 = 0;
          else if (ratio < 1) outOf10 = ratio * 10;
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
      (err, branchResults) => {
        if (err) return reject(err);

        const branchScores = {};
        branchResults.forEach((row) => {
          const targetVal = Number(row.target || 0);
          const achievedVal = Number(row.achieved || 0);
          const weightageVal = Number(row.weightage || 0);

          const score = calculateScore(row.kpi, achievedVal, targetVal);
          const computedWeightageScore = (score * weightageVal) / 100;

          const weightageScore =
            targetVal === 0
              ? 0
              : row.kpi === "insurance" && score === 0
              ? -2
              : computedWeightageScore;

          branchScores[row.kpi] = {
            score,
            target: targetVal,
            achieved: achievedVal,
            weightage: weightageVal,
            weightageScore,
          };
        });

        const staffQuery = `
        SELECT
          u.id AS staffId,
          u.name AS staffName,
          a.kpi,
          COALESCE(a.amount, 0) AS target,
          COALESCE(w.weightage, 0) AS weightage,
          COALESCE(e.total_achieved, 0) AS achieved
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

        pool.query(staffQuery, [period, period, branchId], (err2, results) => {
          if (err2) return reject(err2);

          const staffScores = {};

          results.forEach((row) => {
            const staffId = row.staffId;
            if (!staffScores[staffId]) {
              staffScores[staffId] = {
                staffId,
                staffName: row.staffName,
                total: 0,
              };
            }

            if (!row.kpi) return;

            const kpi = row.kpi;
            const targetVal = Number(row.target || 0);
            const achievedVal = Number(row.achieved || 0);
            const weightageVal = Number(row.weightage || 0);

            const score = calculateScore(kpi, achievedVal, targetVal);
            const computedWeightageScore = (score * weightageVal) / 100;

            const weightageScore =
              targetVal === 0
                ? 0
                : kpi === "insurance" && score === 0
                ? -2
                : computedWeightageScore;

            staffScores[staffId][kpi] = {
              score,
              target: targetVal,
              achieved: achievedVal,
              weightage: weightageVal,
              weightageScore,
            };
          });

          const defaultKpi = {
            score: 0,
            target: 0,
            achieved: 0,
            weightage: 0,
            weightageScore: 0,
          };
          const staffArray = Object.values(staffScores);

          const promises = staffArray.map((staff) => {
            return new Promise((resolve2, reject2) => {
              staff.recovery =
                staff.recovery || branchScores.recovery || defaultKpi;
              staff.audit = staff.audit || branchScores.audit || defaultKpi;
              staff.insurance =
                staff.insurance || branchScores.insurance || defaultKpi;

              [
                "deposit",
                "loan_gen",
                "loan_amulya",
                "recovery",
                "audit",
                "insurance",
              ].forEach((kpi) => {
                if (!staff[kpi]) staff[kpi] = defaultKpi;
              });

              let totalWeightageScore = 0;
              [
                "deposit",
                "loan_gen",
                "loan_amulya",
                "recovery",
                "audit",
                "insurance",
              ].forEach((kpi) => {
                totalWeightageScore += Number(staff[kpi].weightageScore || 0);
              });

              // PREVIOUS AVERAGE KPI
              const prevQuery = `
              SELECT COALESCE(SUM(kpi_total)/COUNT(*), 0) AS avgKpi 
              FROM employee_transfer 
              WHERE staff_id = ?
            `;

              pool.query(prevQuery, [staff.staffId], (err3, prevResult) => {
                if (err3) return reject2(err3);

                const previousKpi = Number(prevResult[0]?.avgKpi || 0);

                let total = totalWeightageScore + previousKpi;

                const insuranceScore = Number(staff.insurance.score || 0);
                const recoveryScore = Number(staff.recovery.score || 0);

                if (insuranceScore < 7.5 && recoveryScore < 7.5 && total > 10) {
                  total = 10;
                }

                staff.total = total;
                resolve2();
              });
            });
          });

          Promise.all(promises)
            .then(() => resolve(staffArray))
            .catch(reject);
        });
      }
    );
  });
}
// simple agm
performanceMasterRouter.get("/ho-Allhod-scores", (req, res) => {
  const { period, hod_id, role } = req.query;

  if (!period || !hod_id) {
    return res.status(400).json({ error: "period and hod_id are required" });
  }

  const hodKpiQuery = `
    SELECT k.kpi_name, r.id AS role_kpi_mapping_id, r.weightage
    FROM role_kpi_mapping r
    JOIN kpi_master k ON r.kpi_id = k.id
    WHERE r.role = ?  `;

  pool.query(hodKpiQuery, [role], (err, hodKpis) => {
    if (err) return res.status(500).json({ error: "Failed to fetch KPI list" });
    if (!hodKpis.length)
      return res.status(404).json({ error: "No KPIs for AGM role" });

    const staffQuery = `SELECT id FROM users WHERE hod_id = ?`;

    pool.query(staffQuery, [hod_id], async (err2, staffList) => {
      if (err2)
        return res.status(500).json({ error: "Failed to fetch HO staff" });

      // if (!staffList.length)
      //   return res.json({
      //     hod_id,
      //     period,
      //     kpis: {},
      //     message: "No staff found",
      //   });

      const staffIds = staffList.map((s) => s.id);

      let staffScores = [];
      for (const sid of staffIds) {
        try {
          const scoreObj = await calculateStaffScores(period, sid, "HO_STAFF");
          staffScores.push(scoreObj);
        } catch (e) {
          staffScores.push({ total: 0 });
        }
      }

      const totalScores = staffScores.map((s) => s.total || 0);

      const avgStaffScore =
        totalScores.reduce((sum, val) => sum + val, 0) / totalScores.length;

      const fixedAvg = Number(avgStaffScore.toFixed(2));

      let finalKpis = {};

      //second kpi logic

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

      let branchWiseAverage = {};

      for (const code of branchCodes) {
        const staffList = await calculateBranchStaffScore(period, code);
        const totals = staffList.map((s) => Number(s.total) || 0);
        const avg =
          totals.length === 0
            ? 0
            : totals.reduce((a, b) => a + b, 0) / totals.length;

        branchWiseAverage[code] = Number(avg.toFixed(2));
      }
      const branchValues = Object.values(branchWiseAverage);
      const branchTotal = branchValues.reduce((sum, val) => sum + val, 0);
      const branchAvgScore = Number(
        (branchTotal / branchValues.length).toFixed(2)
      );
      const kpiMap = {};
      hodKpis.forEach((k) => {
        kpiMap[k.kpi_name] = {
          id: k.role_kpi_mapping_id,
          weightage: k.weightage,
        };
      });

      //insurance kpi calaculation
      const insuranceValue = await new Promise((resolve, reject) => {
        if (!kpiMap["insurance"]) return resolve(0);
        pool.query(
          "SELECT value FROM entries WHERE kpi='insurance' and employee_id = ? and period = ? ",
          [hod_id, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
          }
        );
      });
      //HO Building Cleanliness
      const Cleanliness = await new Promise((resolve, reject) => {
        if (!kpiMap["HO Building Cleanliness"]) return resolve(0);
        pool.query(
          "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
          [kpiMap["HO Building Cleanliness"].id, hod_id, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
          }
        );
      });

      //Management  Discretion
      const Management = await new Promise((resolve, reject) => {
        if (!kpiMap["Management  Discretion"]) return resolve(0);
        pool.query(
          "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
          [kpiMap["Management  Discretion"].id, hod_id, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
          }
        );
      });

      //Internal Audit performance
      const InternalAudit = await new Promise((resolve, reject) => {
        if (!kpiMap["Internal Audit performance"]) return resolve(0);
        pool.query(
          "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
          [kpiMap["Internal Audit performance"].id, hod_id, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
          }
        );
      });

      //IT
      const IT = await new Promise((resolve, reject) => {
        if (!kpiMap["IT"]) return resolve(0);
        pool.query(
          "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
          [kpiMap["IT"].id, hod_id, period],
          (err, rows) => {
            if (err) return reject(err);
            resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
          }
        );
      });

      //Insurance Business Development
      const InsuranceBusinessDevelopment = await new Promise(
        (resolve, reject) => {
          if (!kpiMap["Insurance Business Development"]) return resolve(0);
          pool.query(
            "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
            [kpiMap["Insurance Business Development"].id, hod_id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
            }
          );
        }
      );

      let Total = 0;
      hodKpis.forEach((kpi, index) => {
        if (
          (index === 0 && role === "AGM") ||
          role === "DGM" ||
          role === "AGM_AUDIT" ||
          role === "AGM_IT"
        ) {
          const weightage = kpi.weightage;
          const avg = fixedAvg;

          let result;
          if (avg < weightage) {
            result = (avg / weightage) * 10;
          } else if (avg / weightage < 1.25) {
            result = 12.5;
          } else {
            result = 0;
          }

          const weightageScore = (result / 100) * weightage;
          Total += weightageScore;
          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        } else if (
          (index === 1 && role === "AGM") ||
          role === "DGM" ||
          role === "AGM_AUDIT"
        ) {
          const weightage = kpi.weightage;
          const avg = branchAvgScore;

          let result;

          if (avg < weightage) {
            result = (avg / weightage) * 10;
          } else if (avg / weightage < 1.25) {
            result = 12.5;
          } else {
            result = 0;
          }

          const weightageScore = (result / 100) * weightage;
          Total += weightageScore;
          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        } else if (index === 2) {
          const weightage = kpi.weightage;
          const target = 40000;
          const avg = insuranceValue;

          let result;
          let weightageScore;

          if (avg < target) {
            result = (avg / target) * 10;
          } else if (avg / target < 1.25) {
            result = 10;
          } else {
            result = 12.5;
          }
          if (avg === 0) {
            weightageScore = -2;
          } else {
            weightageScore = (result / 100) * weightage;
          }

          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
          Total += weightageScore;
        } else if (
          (index === 3 && role === "AGM") ||
          role === "DGM" ||
          role === "AGM_IT"
        ) {
          const weightage = kpi.weightage;
          const avg = Cleanliness;

          let result;

          if (avg < weightage) {
            result = (avg / weightage) * 10;
          } else if (avg / weightage < 1.25) {
            result = 12.5;
          } else {
            result = 0;
          }

          const weightageScore = (result / 100) * weightage;
          Total += weightageScore;
          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        } else if (index === 4) {
          const weightage = kpi.weightage;
          const avg = Management;

          let result;

          if (avg < weightage) {
            result = (avg / weightage) * 10;
          } else if (avg / weightage < 1.25) {
            result = 12.5;
          } else {
            result = 0;
          }

          const weightageScore = (result / 100) * weightage;
          Total += weightageScore;
          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        } else if (index === 5 && role === "AGM_AUDIT") {
          const weightage = kpi.weightage;
          const avg = InternalAudit;

          let result;

          if (avg < weightage) {
            result = (avg / weightage) * 10;
          } else if (avg / weightage < 1.25) {
            result = 12.5;
          } else {
            result = 0;
          }

          const weightageScore = (result / 100) * weightage;
          Total += weightageScore;
          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        } else if (index === 6 && role === "AGM_IT") {
          const weightage = kpi.weightage;
          const avg = IT;

          let result;

          if (avg < weightage) {
            result = (avg / weightage) * 10;
          } else if (avg / weightage < 1.25) {
            result = 12.5;
          } else {
            result = 0;
          }

          const weightageScore = (result / 100) * weightage;
          Total += weightageScore;
          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        } else if (index === 7 && role === "AGM_INSURANCE") {
          const weightage = kpi.weightage;
          const avg = InsuranceBusinessDevelopment;

          let result;

          if (avg === 0) {
            result = 0;
          } else if (avg < weightage) {
            result = (avg / weightage) * 10;
          } else if (avg / weightage < 1.25) {
            result = 10;
          } else {
            result = 12.5;
          }

          const weightageScore = (result / 100) * weightage;
          Total += weightageScore;
          finalKpis[kpi.kpi_name] = {
            score: Number(result.toFixed(2)),
            achieved: avg || 0,
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        } else {
          finalKpis[kpi.kpi_name] = {
            score: 0,
            achieved: 0,
            weightage: kpi.weightage,
            weightageScore: 0,
          };
        }
      });

      res.json({
        hod_id,
        period,
        kpis: finalKpis,
        total: Total,
      });
    });
  });
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
            if (ratio < 1) score = ratio * 10;
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
      message: "KPI save/update completed (Insurance excluded)",
      inserted: insertMessages,
      updated: updateMessages,
    });
  });
});
//Dashboard Data
performanceMasterRouter.post("/get-Total-Ho_staff-details", (req, res) => {
  const { hod_id } = req.body;

  if (!hod_id) {
    return res.status(400).json({
      error: "hod_id is required",
    });
  }

  const dashboardResult = {};

  const query1 = `SELECT username, name, role FROM users WHERE role='HO_STAFF' AND hod_id = ?`;

  pool.query(query1, [hod_id], (err1, hoStaffResult) => {
    if (err1) {
      console.error(err1);
      return res.status(500).json({ error: "Database error (HO Staff)" });
    }

    dashboardResult.totalHOStaff = hoStaffResult;
    dashboardResult.totalHOStaffCount = hoStaffResult.length;

    const query2 = `SELECT code, name FROM branches WHERE incharge_id = ?`;

    pool.query(query2, [hod_id], (err2, branchResult) => {
      if (err2) {
        console.error(err2);
        return res.status(500).json({ error: "Database error (Branches)" });
      }

      dashboardResult.totalBranches = branchResult;
      dashboardResult.totalBranchesCount = branchResult.length;

      const query3 = `
        SELECT username, name, role  
        FROM users 
        WHERE role IN ('AGM','DGM','AGM_IT','AGM_INSURANCE','AGM_AUDIT')
      `;

      pool.query(query3, (err3, agmDgmResult) => {
        if (err3) {
          console.error(err3);
          return res.status(500).json({ error: "Database error (AGM/DGM)" });
        }

        dashboardResult.totalAGMDGM = agmDgmResult;
        dashboardResult.totalAGMDGMCount = agmDgmResult.length;

        res.json(dashboardResult);
      });
    });
  });
});

const calculateHodAllScores = (
  pool,
  period,
  hod_id,
  role,
  calculateStaffScores,
  calculateBranchStaffScore
) => {
  return new Promise((resolve, reject) => {
    const hodKpiQuery = `
      SELECT k.kpi_name, r.id AS role_kpi_mapping_id, r.weightage
      FROM role_kpi_mapping r
      JOIN kpi_master k ON r.kpi_id = k.id
      WHERE r.role = ?`;

    pool.query(hodKpiQuery, [role], (err, hodKpis) => {
      if (err) return reject({ error: "Failed to fetch KPI list" });
      if (!hodKpis.length) return reject({ error: "No KPIs for AGM role" });

      const staffQuery = `SELECT id FROM users WHERE hod_id = ?`;

      pool.query(staffQuery, [hod_id], async (err2, staffList) => {
        if (err2) return reject({ error: "Failed to fetch HO staff" });

        const staffIds = staffList.map((s) => s.id);

        let staffScores = [];
        for (const sid of staffIds) {
          try {
            const scoreObj = await calculateStaffScores(
              period,
              sid,
              "HO_STAFF"
            );
            staffScores.push(scoreObj);
          } catch (e) {
            staffScores.push({ total: 0 });
          }
        }

        const totalScores = staffScores.map((s) => s.total || 0);

        const avgStaffScore =
          totalScores.reduce((sum, val) => sum + val, 0) / totalScores.length;

        const fixedAvg = Number(avgStaffScore.toFixed(2));

        let finalKpis = {};

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

        let branchWiseAverage = {};

        for (const code of branchCodes) {
          const staffList = await calculateBranchStaffScore(period, code);
          const totals = staffList.map((s) => Number(s.total) || 0);
          const avg =
            totals.length === 0
              ? 0
              : totals.reduce((a, b) => a + b, 0) / totals.length;

          branchWiseAverage[code] = Number(avg.toFixed(2));
        }

        const branchValues = Object.values(branchWiseAverage);
        const branchTotal = branchValues.reduce((sum, v) => sum + v, 0);
        const branchAvgScore = Number(
          (branchTotal / branchValues.length).toFixed(2)
        );

        const kpiMap = {};
        hodKpis.forEach((k) => {
          kpiMap[k.kpi_name] = {
            id: k.role_kpi_mapping_id,
            weightage: k.weightage,
          };
        });

        //insurance kpi calaculation
        const insuranceValue = await new Promise((resolve, reject) => {
          if (!kpiMap["insurance"]) return resolve(0);
          pool.query(
            "SELECT value FROM entries WHERE kpi='insurance' and employee_id = ? and period = ? ",
            [hod_id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
            }
          );
        });
        //HO Building Cleanliness
        const Cleanliness = await new Promise((resolve, reject) => {
          if (!kpiMap["HO Building Cleanliness"]) return resolve(0);
          pool.query(
            "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
            [kpiMap["HO Building Cleanliness"].id, hod_id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
            }
          );
        });

        //Management  Discretion
        const Management = await new Promise((resolve, reject) => {
          if (!kpiMap["Management  Discretion"]) return resolve(0);
          pool.query(
            "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
            [kpiMap["Management  Discretion"].id, hod_id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
            }
          );
        });

        //Internal Audit performance
        const InternalAudit = await new Promise((resolve, reject) => {
          if (!kpiMap["Internal Audit performance"]) return resolve(0);
          pool.query(
            "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
            [kpiMap["Internal Audit performance"].id, hod_id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
            }
          );
        });

        //IT
        const IT = await new Promise((resolve, reject) => {
          if (!kpiMap["IT"]) return resolve(0);
          pool.query(
            "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
            [kpiMap["IT"].id, hod_id, period],
            (err, rows) => {
              if (err) return reject(err);
              resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
            }
          );
        });

        //Insurance Business Development
        const InsuranceBusinessDevelopment = await new Promise(
          (resolve, reject) => {
            if (!kpiMap["Insurance Business Development"]) return resolve(0);
            pool.query(
              "SELECT value FROM user_kpi_entry WHERE role_kpi_mapping_id = ? AND user_id = ? AND period = ?",
              [kpiMap["Insurance Business Development"].id, hod_id, period],
              (err, rows) => {
                if (err) return reject(err);
                resolve(rows.reduce((sum, r) => sum + Number(r.value || 0), 0));
              }
            );
          }
        );

        let Total = 0;
        hodKpis.forEach((kpi, index) => {
          if (
            (index === 0 && role === "AGM") ||
            role === "DGM" ||
            role === "AGM_AUDIT" ||
            role === "AGM_IT"
          ) {
            const weightage = kpi.weightage;
            const avg = fixedAvg;

            let result;
            if (avg < weightage) {
              result = (avg / weightage) * 10;
            } else if (avg / weightage < 1.25) {
              result = 12.5;
            } else {
              result = 0;
            }

            const weightageScore = (result / 100) * weightage;
            Total += weightageScore;
            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          } else if (
            (index === 1 && role === "AGM") ||
            role === "DGM" ||
            role === "AGM_AUDIT"
          ) {
            const weightage = kpi.weightage;
            const avg = branchAvgScore;

            let result;

            if (avg < weightage) {
              result = (avg / weightage) * 10;
            } else if (avg / weightage < 1.25) {
              result = 12.5;
            } else {
              result = 0;
            }

            const weightageScore = (result / 100) * weightage;
            Total += weightageScore;
            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          } else if (index === 2) {
            const weightage = kpi.weightage;
            const target = 40000;
            const avg = insuranceValue;

            let result;
            let weightageScore;

            if (avg < target) {
              result = (avg / target) * 10;
            } else if (avg / target < 1.25) {
              result = 10;
            } else {
              result = 12.5;
            }
            if (avg === 0) {
              weightageScore = -2;
            } else {
              weightageScore = (result / 100) * weightage;
            }

            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
            Total += weightageScore;
          } else if (
            (index === 3 && role === "AGM") ||
            role === "DGM" ||
            role === "AGM_IT"
          ) {
            const weightage = kpi.weightage;
            const avg = Cleanliness;

            let result;

            if (avg < weightage) {
              result = (avg / weightage) * 10;
            } else if (avg / weightage < 1.25) {
              result = 12.5;
            } else {
              result = 0;
            }

            const weightageScore = (result / 100) * weightage;
            Total += weightageScore;
            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          } else if (index === 4) {
            const weightage = kpi.weightage;
            const avg = Management;

            let result;

            if (avg < weightage) {
              result = (avg / weightage) * 10;
            } else if (avg / weightage < 1.25) {
              result = 12.5;
            } else {
              result = 0;
            }

            const weightageScore = (result / 100) * weightage;
            Total += weightageScore;
            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          } else if (index === 5 && role === "AGM_AUDIT") {
            const weightage = kpi.weightage;
            const avg = InternalAudit;

            let result;

            if (avg < weightage) {
              result = (avg / weightage) * 10;
            } else if (avg / weightage < 1.25) {
              result = 12.5;
            } else {
              result = 0;
            }

            const weightageScore = (result / 100) * weightage;
            Total += weightageScore;
            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          } else if (index === 6 && role === "AGM_IT") {
            const weightage = kpi.weightage;
            const avg = IT;

            let result;

            if (avg < weightage) {
              result = (avg / weightage) * 10;
            } else if (avg / weightage < 1.25) {
              result = 12.5;
            } else {
              result = 0;
            }

            const weightageScore = (result / 100) * weightage;
            Total += weightageScore;
            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          } else if (index === 7 && role === "AGM_INSURANCE") {
            const weightage = kpi.weightage;
            const avg = InsuranceBusinessDevelopment;

            let result;

            if (avg === 0) {
              result = 0;
            } else if (avg < weightage) {
              result = (avg / weightage) * 10;
            } else if (avg / weightage < 1.25) {
              result = 10;
            } else {
              result = 12.5;
            }

            const weightageScore = (result / 100) * weightage;
            Total += weightageScore;
            finalKpis[kpi.kpi_name] = {
              score: Number(result.toFixed(2)),
              achieved: avg || 0,
              weightage,
              weightageScore: Number(weightageScore.toFixed(2)),
            };
          } else {
            finalKpis[kpi.kpi_name] = {
              score: 0,
              achieved: 0,
              weightage: kpi.weightage,
              weightageScore: 0,
            };
          }
        });

        // SEND RESULT BACK
        resolve({
          hod_id,
          period,
          kpis: finalKpis,
          total: Total,
        });
      });
    });
  });
};

performanceMasterRouter.get("/AGM-DGM-Scores", async (req, res) => {
  const { period } = req.query;

  if (!period) {
    return res.status(400).json({ error: "period required" });
  }

  const allAGmQuery = `
    SELECT id AS hod_id, role ,name
    FROM users 
    WHERE role IN ('AGM','DGM','AGM_AUDIT','AGM_INSURANCE','AGM_IT')
  `;

  pool.query(allAGmQuery, async (err, rows) => {
    if (err) {
      console.error(err);
      return res.status(500).json({
        error: "Database error fetching AGMs/DGMs",
      });
    }

    const agmList = rows;
    const results = [];

    for (const agm of agmList) {
      try {
        const scoreData = await calculateHodAllScores(
          pool,
          period,
          agm.hod_id,
          agm.role,
          calculateStaffScores,
          calculateBranchStaffScore
        );

        results.push({
          hod_id: agm.hod_id,
          name: agm.name,
          role: agm.role,
          ...scoreData,
        });
      } catch (err) {
        console.error(err);
        return res.status(500).json(err);
      }
    }

    // Return all AGM/DGM results together
    res.json(results);
  });
});
