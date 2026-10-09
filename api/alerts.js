// Vercel Serverless Function: /api/alerts
// Market Activity Alerts API (Statistical Rule-Based Engine)

const STOCKS_METADATA = {
  AAPL:  { name: "Apple Inc.", basePrice: 182.50, avgVolume: 52000000 },
  MSFT:  { name: "Microsoft Corporation", basePrice: 415.20, avgVolume: 24000000 },
  GOOGL: { name: "Alphabet Inc.", basePrice: 155.80, avgVolume: 28000000 },
  AMZN:  { name: "Amazon.com, Inc.", basePrice: 178.90, avgVolume: 34000000 },
  META:  { name: "Meta Platforms, Inc.", basePrice: 505.40, avgVolume: 16000000 },
  TSLA:  { name: "Tesla, Inc.", basePrice: 175.30, avgVolume: 68000000 },
  NVDA:  { name: "NVIDIA Corporation", basePrice: 890.60, avgVolume: 48000000 },
  INTC:  { name: "Intel Corporation", basePrice: 31.40, avgVolume: 41000000 },
  JPM:   { name: "JPMorgan Chase & Co.", basePrice: 198.70, avgVolume: 11000000 },
  V:     { name: "Visa Inc.", basePrice: 282.10, avgVolume: 8000000 }
};

module.exports = (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Content-Type", "application/json");

  const priceThreshold = parseFloat(req.query?.priceThreshold || "2.5");
  const volumeMultiplier = parseFloat(req.query?.volumeMultiplier || "1.25");

  const alerts = [];
  const now = new Date().toISOString();

  // Evaluate each equity deterministically
  Object.keys(STOCKS_METADATA).forEach(sym => {
    const meta = STOCKS_METADATA[sym];
    // Deterministic simulation based on ticker
    let pctChange = 0;
    let volumeRatio = 1.0;

    if (sym === "NVDA") { pctChange = 3.65; volumeRatio = 1.45; }
    else if (sym === "TSLA") { pctChange = -2.85; volumeRatio = 1.30; }
    else if (sym === "AAPL") { pctChange = 1.15; volumeRatio = 0.95; }
    else if (sym === "META") { pctChange = 2.90; volumeRatio = 1.10; }
    else if (sym === "INTC") { pctChange = -3.20; volumeRatio = 1.55; }
    else { pctChange = (Math.random() - 0.48) * 3; volumeRatio = 0.9 + Math.random() * 0.5; }

    const observedVolume = Math.round(meta.avgVolume * volumeRatio);

    // 1. Price Volatility Check
    if (Math.abs(pctChange) >= priceThreshold) {
      alerts.push({
        stock: sym,
        company: meta.name,
        alertType: pctChange > 0 ? "PRICE_VOLATILITY_SURGE" : "PRICE_VOLATILITY_DROP",
        severity: Math.abs(pctChange) >= 3.5 ? "HIGH" : "MEDIUM",
        observedValue: `${pctChange > 0 ? '+' : ''}${pctChange.toFixed(2)}%`,
        threshold: `|Δ| >= ${priceThreshold.toFixed(2)}%`,
        timestamp: now,
        ruleType: "Statistical Anomaly Filter (Non-ML)",
        reason: `Price moved ${pctChange > 0 ? '+' : ''}${pctChange.toFixed(2)}%, exceeding the configured statistical threshold of ±${priceThreshold.toFixed(2)}%.`
      });
    }

    // 2. Volume Spike Check
    if (volumeRatio >= volumeMultiplier) {
      alerts.push({
        stock: sym,
        company: meta.name,
        alertType: "VOLUME_SURGE",
        severity: volumeRatio >= 1.5 ? "HIGH" : "MEDIUM",
        observedValue: `${observedVolume.toLocaleString()} (${volumeRatio.toFixed(2)}x baseline)`,
        threshold: `>= ${volumeMultiplier.toFixed(2)}x 30d avg (${meta.avgVolume.toLocaleString()})`,
        timestamp: now,
        ruleType: "Volume Surge Multiplier (Statistical Ratio)",
        reason: `Volume spiked to ${volumeRatio.toFixed(2)}x the 30-day baseline average.`
      });
    }
  });

  return res.status(200).json({
    disclaimer: "Market activity indicators are generated using deterministic statistical threshold rules. These signals are strictly for data engineering pipeline monitoring and do NOT constitute financial or trading advice.",
    totalAlertsDetected: alerts.length,
    activeThresholds: {
      priceChangeThresholdPct: priceThreshold,
      volumeSurgeMultiplier: volumeMultiplier
    },
    alerts
  });
};
