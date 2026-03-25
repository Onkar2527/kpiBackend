import express from "express";
import multer from "multer";
import csv from "csv-parser";
import { Readable } from "stream";
import pool from "../db.js";
import { autoDistributeTargets } from "./allocations.js";
import { log } from "console";

// Create the insurance_targets table if it doesn't exist
pool.query(
  `
  CREATE TABLE IF NOT EXISTS insurance_targets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    period VARCHAR(7) NOT NULL,
    kpi VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    state VARCHAR(50) NOT NULL
  )
`,
  (error) => {
    if (error) {
      console.error("Error creating insurance_targets table:", error);
    }
  },
);

// Router implementing branch and insurance target endpoints.
export const targetsRouter = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

//upload the loan_amulya and deposit and loan gen  and audit file
targetsRouter.post("/upload", upload.single("targetFile"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "targetFile required" });

  const results = [];
  Readable.from(req.file.buffer)
    .pipe(csv())
    .on("data", (data) => results.push(data))
    .on("end", () => {
      let values = [];

      results.forEach((row) => {
        let periodKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "period",
        );
        const period = (row[periodKey] || "").trim();

        Object.keys(row).forEach((key) => {
          const cleanKey = key.trim();
          if (cleanKey && cleanKey !== "branch_id" && cleanKey !== "period") {
            const amount = row[key] === "" || row[key] == null ? 0 : row[key];
            values.push([period, row.branch_id, cleanKey, amount, "published"]);
          }
        });
      });

      results.forEach((row) => {
        let periodKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "period",
        );
        const period = (row[periodKey] || "").trim();
        if (!Object.keys(row).some((k) => k.trim() === "audit")) {
          values.push([period, row.branch_id, "audit", 100, "published"]);
        }
      });

      const branchIds = [...new Set(results.map((r) => r.branch_id))];
      const placeholders = branchIds.map(() => "?").join(",");

      const allKpis = [...new Set(values.map((v) => v[2]))];
      const kpiPlaceholders = allKpis.map(() => "?").join(",");

      const deleteQuery = `
        DELETE FROM targets
        WHERE period = ? 
        AND branch_id IN (${placeholders}) 
        AND kpi IN (${kpiPlaceholders})
      `;

      pool.query(
        deleteQuery,
        [results[0].period, ...branchIds, ...allKpis],
        (error) => {
          if (error) {
            console.error("Delete error:", error);
            return res.status(500).json({ error: "Internal server error" });
          }

          pool.query(
            "INSERT IGNORE INTO periods (period) VALUES (?)",
            [results[0].period],
            (error) => {
              if (error) console.error("Error inserting period:", error);
            },
          );

          pool.query(
            "INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES ?",
            [values],
            (error) => {
              if (error) {
                console.error("Insert error:", error);
                return res.status(500).json({ error: "Internal server error" });
              }

              branchIds.forEach((branchId) => {
                autoDistributeTargets(results[0].period, branchId, (err) => {
                  if (err)
                    console.error(
                      `Error auto-distributing targets for branch ${branchId}:`,
                      err,
                    );
                });
              });

              res.json({ ok: true });
            },
          );
        },
      );
    });
});

//upload the loan_amulya and deposit and loan gen  file
targetsRouter.post("/upload1", upload.single("targetFile"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "targetFile required" });

  const results = [];

  Readable.from(req.file.buffer)
    .pipe(csv())
    .on("data", (data) => results.push(data))
    .on("end", () => {
      if (results.length === 0) {
        return res.status(400).json({ error: "No data found in CSV" });
      }

      let values = [];

      results.forEach((row) => {
        const normalized = {};
        for (const key in row) {
          if (!key) continue;
          const cleanKey = key.trim().toLowerCase();
          normalized[cleanKey] = (row[key] || "").toString().trim();
        }

        const period = normalized["period"];
        const branchId = normalized["branch_id"];
        if (!period || !branchId) return;

        Object.keys(normalized).forEach((kpi) => {
          if (kpi !== "period" && kpi !== "branch_id" && kpi !== "") {
            const raw = normalized[kpi];
            const amount = raw === "" || raw == null ? 0 : parseFloat(raw) || 0;
            values.push([period, branchId, kpi, amount, "published"]);
          }
        });
      });

      const firstPeriod = results[0]["period"] || results[0].period;
      const branchIds = [
        ...new Set(results.map((r) => r["branch_id"] || r.branch_id)),
      ];
      const placeholders = branchIds.map(() => "?").join(",");

      const allKpis = [...new Set(values.map((v) => v[2]))];
      const kpiPlaceholders = allKpis.map(() => "?").join(",");

      const deleteQuery = `
        DELETE FROM targets 
        WHERE period = ? 
        AND branch_id IN (${placeholders}) 
        AND kpi IN (${kpiPlaceholders})
      `;

      pool.query(
        deleteQuery,
        [firstPeriod, ...branchIds, ...allKpis],
        (error) => {
          if (error) {
            console.error("Delete error:", error);
            return res.status(500).json({ error: "Internal server error" });
          }

          pool.query(
            "INSERT IGNORE INTO periods (period) VALUES (?)",
            [firstPeriod],
            (error) => {
              if (error) console.error("Error inserting period:", error);
            },
          );

          pool.query(
            "INSERT INTO targets (period, branch_id, kpi, amount, state) VALUES ?",
            [values],
            (error) => {
              if (error) {
                console.error("Insert error:", error);
                return res.status(500).json({ error: "Internal server error" });
              }

              branchIds.forEach((branchId) => {
                autoDistributeTargets(firstPeriod, branchId, (err) => {
                  if (err)
                    console.error(
                      `Error auto-distributing targets for branch ${branchId}:`,
                      err,
                    );
                });
              });

              res.json({
                ok: true,
                inserted: values.length,
                branches: branchIds,
              });
            },
          );
        },
      );
    });
});

//upload recovery file
targetsRouter.post("/upload-branch-specific",
  upload.single("targetFile"),
  (req, res) => {

    if (!req.file)
      return res.status(400).json({ error: "targetFile required" });

    const results = [];
    const processedBranches = new Set();
    const output = [];

    Readable.from(req.file.buffer)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {

        if (!results.length)
          return res.status(400).json({ error: "CSV empty" });

        const period = results[0].period;

        const fyStart = new Date(`${period.split("-")[0]}-04-01`);
        const fyEnd = new Date(`${2000 + Number(period.split("-")[1])}-03-31`);

        let rowIndex = 0;

        function nextRow() {

          if (rowIndex >= results.length) {
            return res.json({
              ok: true,
              results: output
            });
          }

          const row = results[rowIndex++];

          // 🔹 dynamically detect KPIs from CSV
          const kpis = Object.keys(row).filter(
            (k) => !["period", "branch_id"].includes(k)
          );

          let kpiIndex = 0;

          function nextKpi() {

            if (kpiIndex >= kpis.length) {
              return nextRow();
            }

            const kpi = kpis[kpiIndex++];

            uploadbranches(
              row,
              period,
              fyStart,
              fyEnd,
              processedBranches,
              output,
              nextKpi,
              kpi
            );

          }

          nextKpi();
        }

        nextRow();

      });
  }
);
function uploadbranches(
  row,
  period,
  fyStart,
  fyEnd,
  processedBranches,
  output,
  next,
  kpi
) {

  const sheetBranch = row.branch_id?.trim();
  const targetAmount = Number(row[kpi]?.trim() || 0);
  console.log(targetAmount,sheetBranch);
  
  if (!sheetBranch || !targetAmount) return next();
  if (processedBranches.has(sheetBranch)) return next();
  processedBranches.add(sheetBranch);

  const perMonth = targetAmount / 12;

  function countMonths(from, to) {
    const diff =
      (to.getFullYear() - from.getFullYear()) * 12 +
      (to.getMonth() - from.getMonth());
    return diff < 0 ? 0 : diff;
  }

  // FIND BM + transfer_date
  pool.query(
    "SELECT id,  transfer_date FROM users WHERE role='BM' AND branch_id=? AND resign=0 LIMIT 1",
    [sheetBranch],
    (err, bmRows) => {

      if (err || !bmRows.length) {
        output.push({
          branch: sheetBranch,
          error: err?.message || "No BM found",
        });
        return next();
      }

      const bm = bmRows[0];

      const bmTransferDate = bm.transfer_date
        ? new Date(bm.transfer_date)
        : null;

      const bmTransferred =
        bmTransferDate &&
        bmTransferDate >= fyStart &&
        bmTransferDate <= fyEnd;

      // INSERT TARGET
      pool.query(
        `INSERT INTO targets (period,branch_id,kpi,amount,state)
         VALUES (?, ?, ?, ?, 'published')`,
        [period, sheetBranch, kpi, targetAmount],
        (err) => {

          if (err) {
            output.push({ branch: sheetBranch, error: err.message });
            return next();
          }

          pool.query(
            `SELECT id, staff_id, old_branch_id, new_branch_id, transfer_date
             FROM employee_transfer
             WHERE period = ?
             AND (old_branch_id = ? OR new_branch_id = ?)
             ORDER BY staff_id, transfer_date ASC`,
            [period, sheetBranch, sheetBranch],
            (err, transferRows) => {

              if (err) {
                output.push({ branch: sheetBranch, error: err.message });
                return next();
              }

              if (!transferRows.length) {
                output.push({
                  branch: sheetBranch,
                  target: targetAmount,
                  transferred: [],
                });
                return next();
              }

              const byStaff = {};

              transferRows.forEach((tr) => {
                if (!byStaff[tr.staff_id]) byStaff[tr.staff_id] = [];
                byStaff[tr.staff_id].push(tr);
              });

              pool.getConnection((err, conn) => {

                if (err) {
                  output.push({ branch: sheetBranch, error: err.message });
                  return next();
                }

                conn.beginTransaction((err) => {

                  if (err) {
                    conn.release();
                    output.push({ branch: sheetBranch, error: err.message });
                    return next();
                  }

                  const updates = [];

                  for (const staffId in byStaff) {

                    const transfers = byStaff[staffId];
                    let currentStart = fyStart;

                    for (const tr of transfers) {

                      const tDate = new Date(tr.transfer_date);
                      const months = countMonths(currentStart, tDate);

                      if (months > 0 && tr.old_branch_id === sheetBranch) {

                        const value = Math.floor(perMonth * months);

                        updates.push({
                          type: "transfer",
                          id: tr.id,
                          staffId: staffId,
                          value,
                        });

                      }

                      currentStart = new Date(
                        Date.UTC(tDate.getFullYear(), tDate.getMonth(), 1)
                      );
                    }

                    const last = transfers[transfers.length - 1];

                    if (last.new_branch_id === sheetBranch) {

                      const months = countMonths(currentStart, fyEnd);

                      if (months > 0) {

                        const value = Math.floor(perMonth * months);

                        updates.push({
                          type: "allocation",
                          staffId: staffId,
                          value,
                        });

                      }
                    }
                  }

                  let pending = updates.length;

                  if (!pending) {
                    conn.commit(() => {
                      conn.release();
                      next();
                    });
                    return;
                  }

                  updates.forEach((u) => {

                    // If BM transferred → update employee_transfer
                    if (u.type === "transfer" && bmTransferred) {

                      conn.query(
                        `UPDATE employee_transfer
                         SET ${kpi}_target = COALESCE(${kpi}_target,0) + ?
                         WHERE id = ?`,
                        [u.value, u.id],
                        done
                      );

                    }

                    // Normal transfer
                    else if (u.type === "transfer") {

                      conn.query(
                        `UPDATE employee_transfer
                         SET ${kpi}_target = COALESCE(${kpi}_target,0) + ?
                         WHERE id = ?`,
                        [u.value, u.id],
                        done
                      );

                    }

                    // Allocation
                    else {

                      conn.query(
                        `INSERT INTO allocations
                         (period,branch_id,user_id,kpi,amount,state)
                         VALUES (?, ?, ?, ?, ?, 'published')`,
                        [period, sheetBranch, u.staffId, kpi, u.value],
                        done
                      );

                    }

                  });

                  function done(err) {

                    if (err) {
                      return conn.rollback(() => {
                        conn.release();
                        output.push({
                          branch: sheetBranch,
                          error: err.message,
                        });
                        next();
                      });
                    }

                    if (--pending === 0) {

                      conn.commit((err) => {

                        conn.release();

                        if (err) {
                          output.push({
                            branch: sheetBranch,
                            error: err.message,
                          });
                        } else {
                          output.push({
                            branch: sheetBranch,
                            kpi: kpi,
                            target: targetAmount,
                            transferred: updates,
                          });
                        }

                        next();
                      });

                    }

                  }

                });

              });

            }
          );

        }
      );

    }
  );
}

targetsRouter.post("/upload-insurance-global", (req, res) => {
  const { period, csvData } = req.body;
  if (!period || !csvData)
    return res.status(400).json({ error: "period and csvData required" });

  const lines = csvData.split("\n").slice(1); // Skip header
  const targets = lines.map((line) => {
    const [amount] = line.split(",");
    return [period, "insurance", amount, "published"];
  });

  pool.query("INSERT INTO periods (period) VALUES (?)", [period], (error) => {
    if (error) console.error("Error inserting period:", error);
  });
  pool.query(
    "INSERT INTO insurance_targets (period, kpi, amount, state) VALUES ?",
    [targets],
    (error) => {
      if (error)
        return res.status(500).json({ error: "Internal server error" });
      res.json({ ok: true });
    },
  );
});

// GET /targets
// Return targets for a branch and period.
targetsRouter.get("/", (req, res) => {
  const { period, branchId } = req.query;
  let query = "SELECT * FROM targets WHERE 1 = 1";
  const params = [];

  if (period) {
    query += " AND period = ?";
    params.push(period);
  }
  if (branchId) {
    query += " AND branch_id = ?";
    params.push(branchId);
  }

  pool.query(query, params, (error, results) => {
    if (error) return res.status(500).json({ error: "Internal server error" });
    res.json(results);
  });
});

//dashborad traget upload
targetsRouter.post(
  "/previousData",
  upload.single("prevoiustargetFile"),
  (req, res) => {
    if (!req.file)
      return res.status(400).json({ error: "prevoiustargetFile required" });

    const results = [];

    Readable.from(req.file.buffer)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        if (!results.length) {
          return res
            .status(400)
            .json({ error: "CSV file is empty or invalid." });
        }

        const values = [];
        const branchIds = new Set();

        results.forEach((row) => {
          let periodKey = Object.keys(row).find(
            (k) => k.trim().toLowerCase() === "period",
          );
          const period = (row[periodKey] || "").trim();

          const branch_id = (row.branch_id || "").trim();

          if (!period || !branch_id) return;

          branchIds.add(branch_id);

          Object.keys(row).forEach((key) => {
            const cleanKey = key.trim();
            if (
              cleanKey !== "branch_id" &&
              cleanKey.toLowerCase() !== "period"
            ) {
              const amount =
                row[key] === "" || row[key] == null ? 0 : Number(row[key]);
              values.push([period, branch_id, cleanKey, amount]);
            }
          });
        });

        const branchIdArray = Array.from(branchIds);
        if (!branchIdArray.length) {
          return res
            .status(400)
            .json({ error: "No valid branch_id values found in CSV." });
        }

        const branchPlaceholders = branchIdArray.map(() => "?").join(",");

        const deleteQuery = `
        DELETE FROM dashboard_table
        WHERE period = ?
        AND branch_id IN (${branchPlaceholders})
        AND kpi IN (?, ?)
      `;

        pool.query(
          deleteQuery,
          [results[0].period, ...branchIdArray, "balance_deposit", "loan_gen"],
          (error) => {
            if (error) {
              console.error(" Delete error:", error);
              return res
                .status(500)
                .json({ error: "Internal server error (delete)" });
            }

            pool.query(
              "INSERT INTO dashboard_table (period, branch_id, kpi, amount) VALUES ?",
              [values],
              (error) => {
                if (error) {
                  console.error(" Insert error:", error);
                  return res
                    .status(500)
                    .json({ error: "Internal server error (insert)" });
                }

                res.json({
                  ok: true,
                  inserted: values.length,
                  deletedBranches: branchIdArray.length,
                });
              },
            );
          },
        );
      });
  },
);

//dashborad total Achived upload
targetsRouter.post(
  "/totalAchieved",
  upload.single("totalAchievedFile"),
  (req, res) => {
    if (!req.file)
      return res.status(400).json({ error: "totalAchievedFile required" });

    const results = [];

    Readable.from(req.file.buffer)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        if (!results.length) {
          return res
            .status(400)
            .json({ error: "CSV file is empty or invalid." });
        }

        const values = [];
        const branchIds = new Set();

        results.forEach((row) => {
          let periodKey = Object.keys(row).find(
            (k) => k.trim().toLowerCase() === "period",
          );
          const period = (row[periodKey] || "").trim();

          const branch_id = (row.branch_id || "").trim();

          if (!period || !branch_id) return;

          branchIds.add(branch_id);

          Object.keys(row).forEach((key) => {
            const cleanKey = key.trim();
            if (
              cleanKey !== "branch_id" &&
              cleanKey.toLowerCase() !== "period"
            ) {
              const amount =
                row[key] === "" || row[key] == null ? 0 : Number(row[key]);
              values.push([period, branch_id, cleanKey, amount]);
            }
          });
        });

        const branchIdArray = Array.from(branchIds);
        if (!branchIdArray.length) {
          return res
            .status(400)
            .json({ error: "No valid branch_id values found in CSV." });
        }

        const branchPlaceholders = branchIdArray.map(() => "?").join(",");

        const deleteQuery = `
        DELETE FROM dashboard_total_achiveved
        WHERE period = ?
        AND branch_id IN (${branchPlaceholders})
        AND kpi IN (?, ?)
      `;

        pool.query(
          deleteQuery,
          [results[0].period, ...branchIdArray, "balance_deposit", "loan_gen"],
          (error) => {
            if (error) {
              console.error(" Delete error:", error);
              return res
                .status(500)
                .json({ error: "Internal server error (delete)" });
            }

            pool.query(
              "INSERT INTO dashboard_total_achiveved (period, branch_id, kpi, amount) VALUES ?",
              [values],
              (error) => {
                if (error) {
                  console.error(" Insert error:", error);
                  return res
                    .status(500)
                    .json({ error: "Internal server error (insert)" });
                }

                res.json({
                  ok: true,
                  inserted: values.length,
                  deletedBranches: branchIdArray.length,
                });
              },
            );
          },
        );
      });
  },
);

//upload salary
targetsRouter.post("/uploadSalary", upload.single("salaryFile"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "salaryFile required" });

  const results = [];

  Readable.from(req.file.buffer)
    .pipe(csv())
    .on("data", (data) => results.push(data))
    .on("end", () => {
      if (!results.length)
        return res.status(400).json({ error: "CSV file is empty or invalid." });

      const values = [];
      const pfSet = new Set();

      results.forEach((row) => {
        const periodKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "period",
        );
        const pfKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "pf_no",
        );
        const branchKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "branch_id",
        );
        const salaryKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "salary",
        );
        const incrementKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "increment",
        );

        const period = (row[periodKey] || "").trim();
        const PF_NO = (row[pfKey] || "").trim();
        const branch_id = (row[branchKey] || "").trim();
        const salary = Number(row[salaryKey] || 0);
        const increment = Number(row[incrementKey] || 0);

        if (!period || !PF_NO) return;

        pfSet.add(PF_NO);
        values.push([period, PF_NO, branch_id, salary, increment]);
      });

      const pfArray = Array.from(pfSet);
      if (!pfArray.length)
        return res
          .status(400)
          .json({ error: "No valid PF_NO values found in CSV." });

      const placeholders = pfArray.map(() => "?").join(",");
      const periodValue = results[0].period || values[0][0];

      const deleteQuery = `
        DELETE FROM base_salary
        WHERE period = ?
        AND PF_NO IN (${placeholders})
      `;

      pool.query(deleteQuery, [periodValue, ...pfArray], (deleteErr) => {
        if (deleteErr) {
          console.error("Delete error:", deleteErr);
          return res
            .status(500)
            .json({ error: "Internal server error (delete)" });
        }

        // Step 2: Insert new rows
        const insertQuery = `
          INSERT INTO base_salary (period, PF_NO, branch_id, salary, increment)
          VALUES ?
        `;

        pool.query(insertQuery, [values], (insertErr) => {
          if (insertErr) {
            console.error("Insert error:", insertErr);
            return res
              .status(500)
              .json({ error: "Internal server error (insert)" });
          }

          res.json({
            ok: true,
            message: "Salary data uploaded successfully",
            inserted: values.length,
            deleted: pfArray.length,
          });
        });
      });
    });
});

//upload achieved insurance
targetsRouter.post(
  "/uploadInsurance",
  upload.single("insuranceFile"),
  async (req, res) => {
    if (!req.file) {
      return res.status(400).json({ error: "insuranceFile required" });
    }

    try {
      const results = [];

      await new Promise((resolve, reject) => {
        Readable.from(req.file.buffer)
          .pipe(csv())
          .on("data", (data) => results.push(data))
          .on("end", resolve)
          .on("error", reject);
      });

      if (!results.length) {
        return res.status(400).json({ error: "CSV file is empty or invalid." });
      }

      const values = [];
      const employeeIds = [];

      for (const row of results) {
        const periodKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "period"
        );
        const pfKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "pf_no"
        );
        const insuranceKey = Object.keys(row).find(
          (k) => k.trim().toLowerCase() === "insurance"
        );

        const period = (row[periodKey] || "").trim();
        const PF_NO = (row[pfKey] || "").trim();
        const insurance = Number(row[insuranceKey] || 0);

        if (!period || !PF_NO) continue;

        const user = await new Promise((resolve) => {
          pool.query(
            `SELECT id,branch_id FROM users WHERE PF_NO = ?`,
            [PF_NO],
            (err, rs) => {
              if (err || !rs.length) resolve(null);
              else resolve(rs[0]);
            }
          );
        });

        if (!user) {
          console.log("User not found for PF_NO:", PF_NO);
          continue;
        }

        const kpi = "insurance";
        const status = "Verified";
        const branch_id = user.branch_id ? user.branch_id : null;
        const userID = user.id;

        values.push([period, branch_id, userID, kpi, insurance, status]);
        employeeIds.push(userID);
      }

      if (!values.length) {
        return res.status(400).json({ error: "No valid PF found in CSV." });
      }

      const periodToDelete = values[0][0];

      const deleteQuery = `
        DELETE FROM entries 
        WHERE period = ? 
        AND kpi = 'insurance' 
        AND employee_id IN (?)
      `;

      pool.query(deleteQuery, [periodToDelete, employeeIds], (deleteErr) => {
        if (deleteErr) {
          console.error("Delete error:", deleteErr);
          return res.status(500).json({ error: "Delete failed" });
        }

        const insertQuery = `
          INSERT INTO entries (period,branch_id, employee_id,kpi,value,status)
          VALUES ?
        `;

        pool.query(insertQuery, [values], (err) => {
          if (err) {
            console.error("Insert error:", err);
            return res.status(500).json({ error: "Insert failed" });
          }

          res.json({
            ok: true,
            uploaded: values.length,
            message: "Insurance data imported successfully",
          });
        });
      });
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: "Internal server error" });
    }
  }
);

export const getFinancialYearRange = (period) => {
  const [startStr, endStr] = period.split("-");

  const startYear = parseInt(startStr);
  const endYear = startYear - (startYear % 100) + parseInt(endStr);

  const start = new Date(Date.UTC(startYear, 3, 1));
  const end = new Date(Date.UTC(endYear, 2, 31));

  return { start, end };
};

//recovery achieved
targetsRouter.post(
  "/totalRecoveryAchieved",
  upload.single("totalRecoveryAchievedFile"),
  (req, res) => {
    if (!req.file)
      return res
        .status(400)
        .json({ error: "totalRecoveryAchievedFile required" });

    const rows = [];
    Readable.from(req.file.buffer)
      .pipe(csv())
      .on("data", (r) => rows.push(r))
      .on("end", () => processRows(rows, res))
      .on("error", (err) => {
        console.error(err);
        res.status(500).json({ error: "CSV parse error" });
      });

    // ❗ Removed early res.json() — Only final response is allowed
  },
);

function processRows(rows, res) {
  if (!rows.length) return res.status(400).json({ error: "CSV empty" });

  const period = rows[0].period?.trim();
  if (!period) return res.status(400).json({ error: "Missing period" });

  const fy = getFinancialYearRange1(period);
  const fyStart = fy.start;
  const fyEnd = fy.end;

  const processedBranches = new Set();
  const output = [];

  let index = 0;

  const next = () => {
    if (index >= rows.length) {
      return res.json({
        ok: true,
        message: "recovery achieved uploaded successfully",
        data: output,
      });
    }

    processBranch(
      rows[index++],
      period,
      fyStart,
      fyEnd,
      processedBranches,
      output,
      next,
    );
  };

  next();
}

function processBranch(
  row,
  period,
  fyStart,
  fyEnd,
  processedBranches,
  output,
  next
) {
  const sheetBranch = row.branch_id?.trim();
  const recoveryAmount = Number(row.recovery_amount?.trim() || 0);
  
  
  if (!sheetBranch || !recoveryAmount) return next();
  if (processedBranches.has(sheetBranch)) return next();
  processedBranches.add(sheetBranch);

  const perMonth = recoveryAmount / 12;

  function countMonths(from, to) {
    const diff =
      (to.getFullYear() - from.getFullYear()) * 12 +
      (to.getMonth() - from.getMonth());
    return diff < 0 ? 0 : diff;
  }

  pool.query(
    "SELECT id FROM users WHERE role='BM' AND branch_id=? AND resign=0 LIMIT 1",
    [sheetBranch],
    (err, bmRows) => {
      if (err || !bmRows.length) {
        output.push({
          branch: sheetBranch,
          error: err?.message || "No BM found",
        });
        return next();
      }

      const bmId = bmRows[0].id;

      // GIVE FULL RECOVERY TO BM
      pool.query(
        `INSERT INTO entries (period, branch_id, employee_id, kpi, value, status)
         VALUES (?, ?, ?, 'recovery', ?, 'Verified')`,
        [period, sheetBranch, bmId, recoveryAmount],
        (err) => {
          if (err) {
            output.push({ branch: sheetBranch, error: err.message });
            return next();
          }

          // GET TRANSFERS
          pool.query(
            `SELECT id, staff_id, old_branch_id, new_branch_id, transfer_date
             FROM employee_transfer
             WHERE period = ?
             AND (old_branch_id = ? OR new_branch_id = ?)
             ORDER BY staff_id, transfer_date ASC`,
            [period, sheetBranch, sheetBranch],
            (err, transferRows) => {
              
              
              if (err) {
                output.push({ branch: sheetBranch, error: err.message });
                return next();
              }

              if (!transferRows.length) {
                output.push({
                  branch: sheetBranch,
                  recovery: recoveryAmount,
                  bm_given: recoveryAmount,
                  transferred: [],
                });
                return next();
              }

              // GROUP BY STAFF
              const byStaff = {};
              for (const tr of transferRows) {
                if (!byStaff[tr.staff_id]) byStaff[tr.staff_id] = [];
                byStaff[tr.staff_id].push(tr);
              }

              pool.getConnection((err, conn) => {
                if (err) {
                  output.push({ branch: sheetBranch, error: err.message });
                  return next();
                }

                conn.beginTransaction((err) => {
                  if (err) {
                    conn.release();
                    output.push({ branch: sheetBranch, error: err.message });
                    return next();
                  }

                  const updates = [];

                  for (const staffId in byStaff) {
                    const transfers = byStaff[staffId];

                    let currentStart = fyStart;

                    for (const tr of transfers) {
                      const tDate = new Date(tr.transfer_date);

                      const months = countMonths(currentStart, tDate);

                      if (months > 0 && tr.old_branch_id === sheetBranch) {
                        const value = Math.floor(perMonth * months);

                        updates.push({
                          type: "transfer",
                          id: tr.id,
                          value,
                        });
                      }

                      currentStart = new Date(
                        Date.UTC(tDate.getFullYear(), tDate.getMonth(), 1)
                      );
                    }

                    // AFTER LAST TRANSFER
                    const last = transfers[transfers.length - 1];

                    if (last.new_branch_id === sheetBranch) {
                      const months = countMonths(currentStart, fyEnd);

                      if (months > 0) {
                        const value = Math.floor(perMonth * months);

                        updates.push({
                          type: "entry",
                          staffId: staffId,
                          value,
                        });
                      }
                    }
                  }

                  let pending = updates.length;

                  if (!pending) {
                    conn.commit(() => {
                      conn.release();
                      output.push({
                        branch: sheetBranch,
                        recovery: recoveryAmount,
                        bm_given: recoveryAmount,
                        transferred: [],
                      });
                      next();
                    });
                    return;
                  }

                  updates.forEach((u) => {
                    if (u.type === "transfer") {
                      conn.query(
                        `UPDATE employee_transfer
                         SET recovery_achieved = COALESCE(recovery_achieved,0) + ?
                         WHERE id = ?`,
                        [u.value, u.id],
                        done
                      );
                    } else {
                      conn.query(
                        `INSERT INTO entries
                         (period, branch_id, employee_id, kpi, value, status)
                         VALUES (?, ?, ?, 'recovery', ?, 'Verified')`,
                        [period, sheetBranch, u.staffId, u.value],
                        done
                      );
                    }
                  });

                  function done(err) {
                    if (err) {
                      return conn.rollback(() => {
                        conn.release();
                        output.push({
                          branch: sheetBranch,
                          error: err.message,
                        });
                        next();
                      });
                    }

                    if (--pending === 0) {
                      conn.commit((err) => {
                        conn.release();
                        if (err) {
                          output.push({
                            branch: sheetBranch,
                            error: err.message,
                          });
                        } else {
                          output.push({
                            branch: sheetBranch,
                            recovery: recoveryAmount,
                            bm_given: recoveryAmount,
                            transferred: updates,
                          });
                        }
                        next();
                      });
                    }
                  }
                });
              });
            }
          );
        }
      );
    }
  );
}

function getFinancialYearRange1(period) {
  const [startYear] = period.split("-");
  const s = Number(startYear);
  return {
    start: new Date(Date.UTC(s, 3, 1)),
    end: new Date(Date.UTC(s + 1, 2, 1)),
  };
}
//audit achieved
targetsRouter.post(
  "/totalAuditAchieved",
  upload.single("totalAuditAchievedFile"),
  (req, res) => {
    if (!req.file)
      return res.status(400).json({ error: "totalAuditAchievedFile required" });

    const rows = [];
    Readable.from(req.file.buffer)
      .pipe(csv())
      .on("data", (r) => rows.push(r))
      .on("end", () => processRows1(rows, res))
      .on("error", (err) => {
        console.error(err);
        res.status(500).json({ error: "CSV parse error" });
      });
  },
);

function processRows1(rows, res) {
  if (!rows.length) return res.status(400).json({ error: "CSV empty" });

  const period = rows[0].period?.trim();
  if (!period) return res.status(400).json({ error: "Missing period" });

  const fy = getFinancialYearRange2(period);
  const fyStart = fy.start;
  const fyEnd = fy.end;

  const processedBranches = new Set();
  const output = [];

  let index = 0;

  const next = () => {
    if (index >= rows.length) {
      return res.json({
        ok: true,
        message: "audit achieved uploaded successfully",
        data: output,
      });
    }

    processBranch1(
      rows[index++],
      period,
      fyStart,
      fyEnd,
      processedBranches,
      output,
      next,
    );
  }; 

  next();
}

function processBranch1(
  row,
  period,
  fyStart,
  fyEnd,
  processedBranches,
  output,
  next
) {
  const sheetBranch = row.branch_id?.trim();
  const auditAmount = Number(row.audit_amount?.trim() || 0);

  if (!sheetBranch || !auditAmount) return next();
  if (processedBranches.has(sheetBranch)) return next();
  processedBranches.add(sheetBranch);

  const perMonth = auditAmount / 12;

  function countMonths(from, to) {
    const diff =
      (to.getFullYear() - from.getFullYear()) * 12 +
      (to.getMonth() - from.getMonth());
    return diff < 0 ? 0 : diff;
  }

  pool.query(
    "SELECT id FROM users WHERE role='BM' AND branch_id=? AND resign=0 LIMIT 1",
    [sheetBranch],
    (err, bmRows) => {
      if (err || !bmRows.length) {
        output.push({
          branch: sheetBranch,
          error: err?.message || "No BM found",
        });
        return next();
      }

      const bmId = bmRows[0].id;

      // INSERT FULL AUDIT FOR BM
      pool.query(
        `INSERT INTO entries (period, branch_id, employee_id, kpi, value, status)
         VALUES (?, ?, ?, 'audit', ?, 'Verified')`,
        [period, sheetBranch, bmId, auditAmount],
        (err) => {
          if (err) {
            output.push({ branch: sheetBranch, error: err.message });
            return next();
          }

          // GET TRANSFER RECORDS
          pool.query(
            `SELECT id, staff_id, old_branch_id, new_branch_id, transfer_date
             FROM employee_transfer
             WHERE period = ?
             AND (old_branch_id = ? OR new_branch_id = ?)
             ORDER BY staff_id, transfer_date ASC`,
            [period, sheetBranch, sheetBranch],
            (err, transferRows) => {
              if (err) {
                output.push({ branch: sheetBranch, error: err.message });
                return next();
              }

              if (!transferRows.length) {
                output.push({
                  branch: sheetBranch,
                  bm_given: auditAmount,
                  transferred: [],
                });
                return next();
              }

              // GROUP TRANSFERS BY STAFF
              const byStaff = {};
              for (const tr of transferRows) {
                if (!byStaff[tr.staff_id]) byStaff[tr.staff_id] = [];
                byStaff[tr.staff_id].push(tr);
              }

              pool.getConnection((err, conn) => {
                if (err) {
                  output.push({ branch: sheetBranch, error: err.message });
                  return next();
                }

                conn.beginTransaction((err) => {
                  if (err) {
                    conn.release();
                    output.push({ branch: sheetBranch, error: err.message });
                    return next();
                  }

                  const updates = [];

                  for (const staffId in byStaff) {
                    const transfers = byStaff[staffId];

                    let currentStart = fyStart;

                    for (const tr of transfers) {
                      const tDate = new Date(tr.transfer_date);

                      const months = countMonths(currentStart, tDate);

                      if (months > 0 && tr.old_branch_id === sheetBranch) {
                        const value = Math.floor(perMonth * months);

                        updates.push({
                          type: "transfer",
                          id: tr.id,
                          value,
                        });
                      }

                      currentStart = new Date(
                        Date.UTC(tDate.getFullYear(), tDate.getMonth(), 1)
                      );
                    }

                    // AFTER LAST TRANSFER
                    const last = transfers[transfers.length - 1];

                    if (last.new_branch_id === sheetBranch) {
                      const months = countMonths(currentStart, fyEnd);

                      if (months > 0) {
                        const value = Math.floor(perMonth * months);

                        updates.push({
                          type: "entry",
                          staffId: staffId,
                          value,
                        });
                      }
                    }
                  }

                  let pending = updates.length;

                  if (!pending) {
                    conn.commit(() => {
                      conn.release();
                      output.push({
                        branch: sheetBranch,
                        bm_given: auditAmount,
                        transferred: [],
                      });
                      next();
                    });
                    return;
                  }

                  updates.forEach((u) => {
                    if (u.type === "transfer") {
                      conn.query(
                        `UPDATE employee_transfer
                         SET audit_achieved = COALESCE(audit_achieved,0) + ?
                         WHERE id = ?`,
                        [u.value, u.id],
                        done
                      );
                    } else {
                      conn.query(
                        `INSERT INTO entries
                         (period, branch_id, employee_id, kpi, value, status)
                         VALUES (?, ?, ?, 'audit', ?, 'Verified')`,
                        [period, sheetBranch, u.staffId, u.value],
                        done
                      );
                    }
                  });

                  function done(err) {
                    if (err) {
                      return conn.rollback(() => {
                        conn.release();
                        output.push({
                          branch: sheetBranch,
                          error: err.message,
                        });
                        next();
                      });
                    }

                    if (--pending === 0) {
                      conn.commit((err) => {
                        conn.release();
                        if (err) {
                          output.push({
                            branch: sheetBranch,
                            error: err.message,
                          });
                        } else {
                          output.push({
                            branch: sheetBranch,
                            bm_given: auditAmount,
                            transferred: updates,
                          });
                        }
                        next();
                      });
                    }
                  }
                });
              });
            }
          );
        }
      );
    }
  );
}

function getFinancialYearRange2(period) {
  const [startYear] = period.split("-");
  const s = Number(startYear);
  return {
    start: new Date(Date.UTC(s, 3, 1)),
    end: new Date(Date.UTC(s + 1, 2, 1)),
  };
}
//upload deputation staff
targetsRouter.post("/upload-deputation-staff",
  upload.single("deputationFile"),
  (req, res) => {
    if (!req.file)
      return res.status(400).json({ error: "deputationFile required" });

    const results = [];

    Readable.from(req.file.buffer)
      .pipe(csv())
      .on("data", (row) => results.push(row))
      .on("end", () => {
        if (!results.length)
          return res.status(400).json({ error: "CSV file is empty" });

        const values = results.map((row) => [
          (row.emp_id || "").trim(),
          (row.name || "").trim(),
          (row.place || "").trim(),
          (row.design || "").trim(),
          (row.branch || "").trim(),
          (row.work_at || "").trim(),
          row.weightage_score === "" || row.weightage_score == null
            ? 0
            : Number(row.weightage_score),
          (row.department || "").trim(),
          (row.period || "").trim(),
        ]);

        const empIds = [...new Set(values.map((v) => v[0]))];
        const periods = [...new Set(values.map((v) => v[8]))];

        if (!periods.length || !periods[0]) {
          return res.status(400).json({ error: "Period missing in CSV" });
        }

        const empPlaceholders = empIds.map(() => "?").join(",");
        const periodPlaceholders = periods.map(() => "?").join(",");

        const deleteQuery = `
          DELETE FROM deputation_staff
          WHERE emp_id IN (${empPlaceholders})
          AND period IN (${periodPlaceholders})
        `;

        pool.getConnection((connErr, conn) => {
          if (connErr) return res.status(500).json(connErr);

          conn.beginTransaction((txErr) => {
            if (txErr) {
              conn.release();
              return res.status(500).json(txErr);
            }

            conn.query(deleteQuery, [...empIds, ...periods], (delErr) => {
              if (delErr) {
                return conn.rollback(() => {
                  conn.release();
                  res.status(500).json({ error: "Delete failed" });
                });
              }

              const insertQuery = `
                  INSERT INTO deputation_staff
                  (emp_id, name, place, design, branch, work_at, weightage_score, department, period)
                  VALUES ?
                `;

              conn.query(insertQuery, [values], (insErr) => {
                if (insErr) {
                  return conn.rollback(() => {
                    conn.release();
                    res.status(500).json({ error: "Insert failed" });
                  });
                }

                conn.commit(() => {
                  conn.release();
                  res.json({
                    ok: true,
                    inserted: values.length,
                    periods,
                  });
                });
              });
            });
          });
        });
      });
  },
);

//upload insurance target
targetsRouter.post("/upload-insurance-target", upload.single("insuranceTargetFile"), (req, res) => {

  if (!req.file) return res.status(400).json({ error: "targetFile required" });

  const results = [];

  Readable.from(req.file.buffer)
    .pipe(csv())
    .on("data", (data) => results.push(data))
    .on("end", () => {

      if (!results.length) {
        return res.status(400).json({ error: "CSV empty" });
      }

      const allocations = [];
      const periods = new Set();

      pool.query(
        "SELECT id, PF_NO, branch_id FROM users",
        (err, users) => {

          if (err) return res.status(500).json(err);

          const userMap = {};
          users.forEach(u => {
            userMap[u.PF_NO] = u;
          });

          results.forEach(row => {

            const pfNo = (row.PF_NO || "").toString().trim();
            const amount = parseFloat(row.insurance || 0);
            const period = (row.period || "").toString().trim();

            if (!pfNo || !amount || !period) return;

            const user = userMap[pfNo];
            if (!user) return;

            periods.add(period);

            allocations.push([
              period,
              user.branch_id,
              user.id,
              "insurance",
              amount,
              "published"
            ]);

          });

          const periodList = [...periods];
          const placeholders = periodList.map(() => "?").join(",");

          const deleteQuery = `
            DELETE FROM allocations 
            WHERE period IN (${placeholders}) 
            AND kpi = 'insurance'
          `;

          pool.query(deleteQuery, periodList, (err) => {

            if (err) {
              console.error(err);
              return res.status(500).json({ error: "Delete failed" });
            }

            pool.query(
              `INSERT INTO allocations 
              (period, branch_id, user_id, kpi, amount, state)
              VALUES ?`,
              [allocations],
              (error) => {

                if (error) {
                  console.error(error);
                  return res.status(500).json({ error: "Insert failed" });
                }

                res.json({
                  ok: true,
                  inserted: allocations.length,
                  deleted_periods: periodList
                });

              }
            );

          });

        }
      );

    });

});