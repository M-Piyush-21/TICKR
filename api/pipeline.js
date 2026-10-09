// Vercel Serverless Function: /api/pipeline
module.exports = (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Content-Type", "application/json");

  const action = req.query?.action || (req.body && req.body.action) || "status";

  if (action === "trigger-batch") {
    const runId = "manual__" + new Date().toISOString();
    const tasks = [
      { id: "fetch_historical_data", name: "AlphaVantage Daily Fetch", status: "success", duration: "1.2s", output: "10 tickers fetched from AlphaVantage" },
      { id: "consume_historical_data", name: "Kafka Ingestion & S3 Staging", status: "success", duration: "0.8s", output: "10 CSV records partitioned into s3a://stock-market-data/raw/historical/" },
      { id: "process_data", name: "Apache Spark Batch Processing", status: "success", duration: "3.4s", output: "OHLCV metrics and moving averages calculated; 10 Parquet partitions written" },
      { id: "load_historical_to_snowflake", name: "Snowflake MERGE Upsert", status: "success", duration: "1.1s", output: "MERGE INTO STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS completed (10 rows)" },
      { id: "process_complete", name: "Pipeline Marker", status: "success", duration: "0.1s", output: "DAG execution completed with exit code 0" }
    ];

    return res.status(200).json({
      success: true,
      runId,
      state: "success",
      tasks,
      totalDuration: "6.6s",
      timestamp: new Date().toISOString()
    });
  }

  // Default: Pipeline status
  return res.status(200).json({
    pipeline: "stock_market_batch_pipeline",
    health: "healthy",
    components: {
      alphaVantage: { status: "connected", quotaUsed: "10 / 25 daily calls", rateLimit: "5 calls/min" },
      kafka: { status: "active", topics: ["stock-market-batch", "stock-market-realtime"], brokers: 1, lag: 0 },
      minio: { status: "healthy", bucket: "stock-market-data", rawPrefix: "raw/historical/", processedPrefix: "processed/historical/" },
      spark: { status: "idle", masterUrl: "spark://spark-master:7077", workers: 1, cores: 2, memory: "2GB" },
      snowflake: { status: "synced", table: "STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS", totalRows: 1250, lastSync: new Date().toISOString() },
      airflow: { status: "active", schedule: "0 0 * * *", lastRun: "success" }
    },
    metrics: {
      dailyVolumeProcessed: "184,920,400 shares",
      tickersMonitored: 10,
      avgLatencyMs: 42,
      uptime: "99.98%"
    },
    timestamp: new Date().toISOString()
  });
};
