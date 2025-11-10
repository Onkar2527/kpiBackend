import express from "express";
import pool from "../db.js";


export const performanceMasterRouter = express.Router();
//get kpi role wise
performanceMasterRouter.post('/getKpiRoleWise', (req, res) => {
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
      SELECT k.kpi_name FROM role_kpi_mapping r join kpi_master k on r.kpi_id = k.id WHERE role = ?
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
        data: results
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
        return res.status(500).json({ error: "Failed to fetch user KPI entries" });
      }

     
      const achievedMap = {};
      userEntries.forEach((e) => (achievedMap[e.role_kpi_mapping_id] = e.achieved));

      let totalWeightageScore = 0;
      const scores = {};

      
      kpiWeightage.forEach((row) => {
        const { role_kpi_mapping_id, kpi_name, weightage } = row;
        const achieved = parseFloat(achievedMap[role_kpi_mapping_id]) || 0;
        const target = kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

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

      // 4️⃣ Add total
      scores.total = Number(totalWeightageScore.toFixed(2));

      // ✅ Always return full structure (even if userEntries empty)
      res.json(scores);
    });
  });
});

performanceMasterRouter.get("/ho-Allhod-scores", (req, res) => {
  const { period, hod_id } = req.query;

  if (!period || !hod_id) {
    return res.status(400).json({ error: "period and hod_id are required" });
  }

  // 1️⃣ Get all HO staff under this HOD
  const staffQuery = `
    SELECT id AS staffId, role
    FROM users
    WHERE hod_id = ? 
  `;

  pool.query(staffQuery, [hod_id], (err, staffResults) => {
    if (err) {
      console.error("Error fetching staff:", err);
      return res.status(500).json({ error: "Failed to fetch staff" });
    }

    if (!staffResults.length) {
      return res.json({
        message: "No staff found under this HOD",
        scores: {},
      });
    }

    const staffIds = staffResults.map((s) => s.staffId);
    console.log(staffIds[0]);
    
    const role = staffResults[0].role; // assume all HO staff under same role

    // 2️⃣ Get KPI weightages for that role
    const weightageQuery = `
      SELECT 
        rkm.id AS role_kpi_mapping_id,
        km.kpi_name,
        rkm.weightage
      FROM role_kpi_mapping rkm
      JOIN kpi_master km ON km.id = rkm.kpi_id
      WHERE rkm.role = ? AND rkm.deleted_at IS NULL
    `;

    pool.query(weightageQuery, [role], (err, kpiWeightage) => {
      if (err) {
        console.error("Error fetching KPI weightage:", err);
        return res.status(500).json({ error: "Failed to fetch KPI weightage" });
      }

      if (!kpiWeightage.length)
        return res.status(404).json({ error: "No KPI found for this role" });

     
      const entryQuery = `
        SELECT user_id, role_kpi_mapping_id, value AS achieved
        FROM user_kpi_entry
        WHERE period = ? AND user_id IN (?) AND deleted_at IS NULL
      `;

      pool.query(entryQuery, [period, staffIds], (err, entryResults) => {
        if (err) {
          console.error("Error fetching KPI entries:", err);
          return res.status(500).json({ error: "Failed to fetch KPI entries" });
        }

        // 🧩 Group achieved values per KPI across all staff
        const kpiSums = {};
        const kpiCounts = {};

        entryResults.forEach((entry) => {
          const { role_kpi_mapping_id, achieved } = entry;
          if (!kpiSums[role_kpi_mapping_id]) {
            kpiSums[role_kpi_mapping_id] = 0;
            kpiCounts[role_kpi_mapping_id] = 0;
          }
          kpiSums[role_kpi_mapping_id] += parseFloat(achieved) || 0;
          kpiCounts[role_kpi_mapping_id]++;
        });

        // 4️⃣ Calculate KPI averages + scores
        const scores = {};
        let totalWeightageScore = 0;

        kpiWeightage.forEach((kpi) => {
          const { role_kpi_mapping_id, kpi_name, weightage } = kpi;

          const avgAchieved =
            kpiCounts[role_kpi_mapping_id] > 0
              ? kpiSums[role_kpi_mapping_id] / kpiCounts[role_kpi_mapping_id]
              : 0;

          const target =
            kpi_name.toLowerCase() === "insurance" ? 40000 : weightage;

          // Score logic
          let score = 0;
          if (avgAchieved > 0) {
            const ratio = avgAchieved / target;
            if (ratio < 1) score = ratio * 10;
            else if (ratio < 1.25) score = 10;
            else score = 12.5;
          }

          let weightageScore = (score * weightage) / 100;
          if (kpi_name.toLowerCase() === "insurance" && score === 0)
            weightageScore = -2;

          totalWeightageScore += weightageScore;

          scores[kpi_name] = {
            achieved: Number(avgAchieved.toFixed(2)),
            target,
            score: Number(score.toFixed(2)),
            weightage,
            weightageScore: Number(weightageScore.toFixed(2)),
          };
        });

        scores.total = Number(totalWeightageScore.toFixed(2));

        res.json({ hod_id, period, scores });
      });
    });
  });
});



