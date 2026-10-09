// Vercel Serverless Function: /api/stocks
const STOCKS_METADATA = {
  AAPL: { name: "Apple Inc.", sector: "Technology", basePrice: 182.50 },
  MSFT: { name: "Microsoft Corporation", sector: "Technology", basePrice: 415.20 },
  GOOGL: { name: "Alphabet Inc.", sector: "Communication Services", basePrice: 155.80 },
  AMZN: { name: "Amazon.com, Inc.", sector: "Consumer Cyclical", basePrice: 178.90 },
  META: { name: "Meta Platforms, Inc.", sector: "Communication Services", basePrice: 505.40 },
  TSLA: { name: "Tesla, Inc.", sector: "Consumer Cyclical", basePrice: 175.30 },
  NVDA: { name: "NVIDIA Corporation", sector: "Technology", basePrice: 890.60 },
  INTC: { name: "Intel Corporation", sector: "Technology", basePrice: 31.40 },
  JPM: { name: "JPMorgan Chase & Co.", sector: "Financial Services", basePrice: 198.70 },
  V: { name: "Visa Inc.", sector: "Financial Services", basePrice: 282.10 }
};

function generateHistory(basePrice, days = 30) {
  const history = [];
  let current = basePrice * 0.92;
  const now = new Date();

  for (let i = days; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const dateStr = d.toISOString().split("T")[0];

    const changeFactor = 1 + (Math.random() - 0.48) * 0.035;
    const open = Math.round(current * 100) / 100;
    const close = Math.round(open * changeFactor * 100) / 100;
    const high = Math.round(Math.max(open, close) * (1 + Math.random() * 0.015) * 100) / 100;
    const low = Math.round(Math.min(open, close) * (1 - Math.random() * 0.015) * 100) / 100;
    const volume = Math.floor(5000000 + Math.random() * 35000000);
    const ma15 = Math.round(close * (1 + (Math.random() - 0.5) * 0.02) * 100) / 100;
    const ma30 = Math.round(close * (1 + (Math.random() - 0.5) * 0.03) * 100) / 100;

    history.push({
      date: dateStr,
      open,
      high,
      low,
      close,
      volume,
      change: Math.round((close - open) * 100) / 100,
      percentChange: Math.round(((close - open) / open) * 10000) / 100,
      ma15,
      ma30
    });
    current = close;
  }
  return history;
}

module.exports = (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Content-Type", "application/json");

  const symbol = (req.query?.symbol || "").toUpperCase();

  if (symbol && STOCKS_METADATA[symbol]) {
    const meta = STOCKS_METADATA[symbol];
    const history = generateHistory(meta.basePrice, 30);
    const latest = history[history.length - 1];

    return res.status(200).json({
      symbol,
      ...meta,
      quote: latest,
      history
    });
  }

  // Return list of all 10 equities
  const allStocks = Object.keys(STOCKS_METADATA).map(sym => {
    const meta = STOCKS_METADATA[sym];
    const history = generateHistory(meta.basePrice, 7);
    const latest = history[history.length - 1];
    return {
      symbol: sym,
      ...meta,
      price: latest.close,
      change: latest.change,
      percentChange: latest.percentChange,
      volume: latest.volume,
      high: latest.high,
      low: latest.low,
      open: latest.open
    };
  });

  return res.status(200).json({ stocks: allStocks, timestamp: new Date().toISOString() });
};
