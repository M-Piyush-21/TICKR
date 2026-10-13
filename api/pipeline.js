// Honest status for the static dashboard.
// This function does not probe Kafka, MinIO, Spark, Snowflake, or Airflow.

module.exports = (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Content-Type", "application/json");

  const action = req.query?.action || (req.body && req.body.action) || "status";

  if (action === "trigger-batch") {
    return res.status(200).json({
      success: false,
      simulated: true,
      state: "not_executed",
      message: "This endpoint does not trigger Airflow. Run python3 run_pipeline_local.py for the verified local batch.",
      timestamp: new Date().toISOString()
    });
  }

  return res.status(200).json({
    pipeline: "stock_market_batch_pipeline",
    verifiedPath: "python3 run_pipeline_local.py --execution-date YYYY-MM-DD",
    manifest: "artifacts/latest_run_manifest.json",
    components: {
      localBatch: { status: "verified_local", grain: "daily_candle", key: ["symbol", "date"] },
      alphaVantage: {
        status: "code_only_requires_api_key",
        plannedDailyRequests: 20,
        dailyLimit: 25,
        batchCalls: 10,
        quoteSnapshots: 5,
        retryReserve: 5
      },
      kafka: { status: "configured_not_exercised_by_local_demo" },
      minio: { status: "configured_not_exercised_by_local_demo" },
      spark: { status: "code_only_requires_spark_cluster" },
      snowflake: { status: "cloud_dependent" },
      airflow: { status: "code_only_requires_compose" },
      quoteSnapshot: { status: "code_only_quota_capped", grain: "quote_snapshot", cyclesPerDay: 1 }
    },
    timestamp: new Date().toISOString()
  });
};
