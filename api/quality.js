// Vercel Serverless Function: /api/quality
// Deterministic fixture used to show the check logic. This is not a live lake audit.

function runQualityAudit() {
  const records = [
    { symbol: "AAPL", date: "2026-10-13", daily_open: 181.5, daily_high: 182.5, daily_low: 180.8, daily_close: 182.0, daily_volume: 45000000 },
    { symbol: "MSFT", date: "2026-10-13", daily_open: 414.0, daily_high: 418.0, daily_low: 412.0, daily_close: 416.5, daily_volume: 22000000 },
    { symbol: "AAPL", date: "2026-10-13", daily_open: 181.5, daily_high: 182.5, daily_low: 180.8, daily_close: 182.0, daily_volume: 45000000 },
    { symbol: "INTC", date: "2026-10-13", daily_open: 31.0, daily_high: 31.5, daily_low: 30.8, daily_close: 31.2, daily_volume: null },
    { symbol: "TSLA", date: "2026-10-13", daily_open: 180.0, daily_high: 172.0, daily_low: 185.0, daily_close: 175.0, daily_volume: 24000000 }
  ];

  const totalRecords = records.length;
  let passedChecks = 0;
  let totalChecks = 0;
  const failedChecks = [];
  const seenKeys = new Set();
  let duplicateCount = 0;
  let missingCount = 0;
  let schemaFailureCount = 0;

  records.forEach((rec, idx) => {
    const recId = `${rec.symbol || 'NULL'}_${rec.date || idx}`;

    // Check 1: Symbol not null & valid string
    totalChecks++;
    if (rec.symbol && typeof rec.symbol === 'string' && rec.symbol.length <= 6) {
      passedChecks++;
    } else {
      failedChecks.push({
        checkId: "CHK_01_SYM",
        rule: "Non-Null Symbol",
        column: "symbol",
        recordId: recId,
        observed: String(rec.symbol),
        expected: "String (1-6 chars)",
        severity: "CRITICAL",
        explanation: "Symbol is null or improperly formatted."
      });
      missingCount++;
    }

    // Check 2: Date valid format YYYY-MM-DD
    totalChecks++;
    if (rec.date && /^\d{4}-\d{2}-\d{2}$/.test(rec.date)) {
      passedChecks++;
    } else {
      failedChecks.push({
        checkId: "CHK_02_DATE",
        rule: "ISO-8601 Date Format",
        column: "date",
        recordId: recId,
        observed: String(rec.date),
        expected: "YYYY-MM-DD",
        severity: "CRITICAL",
        explanation: "Date field is missing or not in standard ISO format."
      });
      schemaFailureCount++;
    }

    // Check 3: Primary Key Uniqueness (symbol + date)
    totalChecks++;
    const pk = `${rec.symbol}_${rec.date}`;
    if (seenKeys.has(pk)) {
      duplicateCount++;
      failedChecks.push({
        checkId: "CHK_03_PK_DUP",
        rule: "Primary Key Deduplication",
        column: "(symbol, date)",
        recordId: recId,
        observed: `Duplicate pair: ${pk}`,
        expected: "Unique Primary Key",
        severity: "CRITICAL",
        explanation: "Duplicate record detected. Upsert or deduplication required."
      });
    } else {
      seenKeys.add(pk);
      passedChecks++;
    }

    // Check 4: Positive Close Price
    totalChecks++;
    if (rec.daily_close && typeof rec.daily_close === 'number' && rec.daily_close > 0) {
      passedChecks++;
    } else {
      failedChecks.push({
        checkId: "CHK_04_PRICE",
        rule: "Positive Close Price",
        column: "daily_close",
        recordId: recId,
        observed: String(rec.daily_close),
        expected: "Float > 0.00",
        severity: "CRITICAL",
        explanation: "Close price must be a valid positive float."
      });
      schemaFailureCount++;
    }

    // Check 5: Non-Null & Non-negative Volume
    totalChecks++;
    if (rec.daily_volume !== null && rec.daily_volume !== undefined && typeof rec.daily_volume === 'number' && rec.daily_volume >= 0) {
      passedChecks++;
    } else {
      failedChecks.push({
        checkId: "CHK_05_VOL",
        rule: "Valid Trading Volume",
        column: "daily_volume",
        recordId: recId,
        observed: String(rec.daily_volume),
        expected: "Integer >= 0",
        severity: "WARNING",
        explanation: "Volume is null or invalid. May indicate incomplete market exchange data feed."
      });
      missingCount++;
    }

    // Check 6: High >= Low
    if (rec.daily_high && rec.daily_low) {
      totalChecks++;
      if (rec.daily_high >= rec.daily_low) {
        passedChecks++;
      } else {
        failedChecks.push({
          checkId: "CHK_06_HIGH_LOW",
          rule: "High >= Low Boundary",
          column: "(daily_high, daily_low)",
          recordId: recId,
          observed: `High: ${rec.daily_high.toFixed(2)}, Low: ${rec.daily_low.toFixed(2)}`,
          expected: "daily_high >= daily_low",
          severity: "CRITICAL",
          explanation: "Inverted intraday spread. High cannot be lower than Low."
        });
        schemaFailureCount++;
      }
    }
  });

  const passRate = totalChecks > 0 ? Number(((passedChecks / totalChecks) * 100).toFixed(2)) : 0.0;

  return {
    auditScope: "deterministic_fixture_not_live_lake",
    note: "Fixed examples of a duplicate key, a null volume, and an inverted high/low. The verified batch result is artifacts/latest_run_manifest.json.",
    totalRecordsEvaluated: totalRecords,
    totalChecksExecuted: totalChecks,
    passedChecksCount: passedChecks,
    failedChecksCount: failedChecks.length,
    validationPassRatePct: passRate,
    duplicateRecordsCount: duplicateCount,
    missingCriticalCount: missingCount,
    schemaFailuresCount: schemaFailureCount,
    latestDataTimestamp: "2026-10-13",
    dataFreshnessLatency: "See artifacts/latest_run_manifest.json",
    processingStatus: failedChecks.length === 0 ? "PASSED_CLEAN" : "ANOMALIES_DETECTED",
    failedChecksTable: failedChecks
  };
}

module.exports = (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Content-Type", "application/json");

  const audit = runQualityAudit();
  return res.status(200).json(audit);
};
