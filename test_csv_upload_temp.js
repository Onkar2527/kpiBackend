import fs from "fs";

const content = fs.readFileSync("d:/kpi/backend/server/routes/masters.js", "utf8");
const lines = content.split("\n");

lines.forEach((line, i) => {
  if (line.toLowerCase().includes("transfer-kpi")) {
    console.log(`Line ${i + 1}: ${line.trim()}`);
  }
});
