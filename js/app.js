/**
 * StockPulse Data Engineering Dashboard
 * Interactive Stock Market Data Pipeline Frontend & Architecture Visualizer
 */

// 10 Monitored Equities
const STOCKS_DATA = {
  AAPL:  { name: "Apple Inc.", sector: "Consumer Electronics", basePrice: 182.50, vol: "52.4M" },
  MSFT:  { name: "Microsoft Corporation", sector: "Software & Cloud", basePrice: 415.20, vol: "24.1M" },
  GOOGL: { name: "Alphabet Inc.", sector: "Internet Services", basePrice: 155.80, vol: "28.9M" },
  AMZN:  { name: "Amazon.com, Inc.", sector: "E-Commerce & Cloud", basePrice: 178.90, vol: "34.5M" },
  META:  { name: "Meta Platforms, Inc.", sector: "Social Media & AI", basePrice: 505.40, vol: "16.8M" },
  TSLA:  { name: "Tesla, Inc.", sector: "Automotive & Clean Energy", basePrice: 175.30, vol: "68.2M" },
  NVDA:  { name: "NVIDIA Corporation", sector: "Semiconductors & AI", basePrice: 890.60, vol: "48.7M" },
  INTC:  { name: "Intel Corporation", sector: "Semiconductors", basePrice: 31.40, vol: "41.2M" },
  JPM:   { name: "JPMorgan Chase & Co.", sector: "Banking & Financials", basePrice: 198.70, vol: "11.6M" },
  V:     { name: "Visa Inc.", sector: "Payment Technology", basePrice: 282.10, vol: "8.4M" }
};


// Component Deep-Dive Specs
const COMPONENT_SPECS = {
  alpha: {
    name: "AlphaVantage Market API",
    tag: "Data Ingestion Source",
    badgeClass: "node-alpha",
    role: "Provides official daily financial candles and streaming quote endpoints for the 10 monitored US equities.",
    problemSolved: "Simulates an authentic financial data provider with strict free-tier rate limitations (5 calls/min, 25 calls/day) rather than using unstable web scrapers.",
    decision: "Deliberate 12-second pacing and compact output size (last 100 trading days) guarantees reliable operation within API limits.",
    tech: "REST API, Requests, JSON",
    snippet: `params = {
    "function": "TIME_SERIES_DAILY",
    "symbol": symbol,
    "outputsize": "compact",
    "apikey": ALPHA_VANTAGE_API_KEY
}`
  },
  kafka: {
    name: "Apache Kafka 3.6",
    tag: "Distributed Message Broker",
    badgeClass: "node-kafka",
    role: "Decouples data producers from downstream storage and compute, buffering messages across two independent topics.",
    problemSolved: "Eliminates tight coupling. If downstream storage fails or experiences network latency, messages remain securely buffered in Kafka without dropping market data.",
    decision: "Two separate topics ('stock-market-batch' and 'stock-market-realtime') allow independent consumer retention and scaling.",
    tech: "confluent-kafka, Zookeeper, Murmur2 Partitioning",
    snippet: `producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "stock-data-producer"
})
producer.produce(topic, key=symbol, value=payload)
producer.flush()`
  },
  minio: {
    name: "MinIO S3 Object Storage",
    tag: "Data Lake Storage Layer",
    badgeClass: "node-minio",
    role: "Houses the Raw Bronze (CSV) and Processed Silver (Snappy Parquet) layers using S3-compatible cloud object storage APIs.",
    problemSolved: "Avoids costly enterprise cloud bills during local development while adhering 100% to standard Amazon S3 REST semantics.",
    decision: "Organized with Hive-style directory partitioning ('year=YYYY/month=MM/day=DD/') enabling partition pruning in Spark.",
    tech: "MinIO S3 Client, AWS S3A SDK, Snappy Parquet",
    snippet: `minio_client.fput_object(
    bucket_name="stock-market-data",
    object_name=f"raw/historical/year={year}/month={month}/day={day}/{symbol}.csv",
    file_path=temp_csv
)`
  },
  spark: {
    name: "Apache Spark 3.4",
    tag: "Distributed Compute Engine",
    badgeClass: "node-spark",
    role: "Executes both PySpark batch transformations (daily OHLCV) and Structured Streaming sliding window aggregations.",
    problemSolved: "Pandas processes data on a single CPU thread and crashes when datasets exceed RAM. Spark partitions work across cores and clusters.",
    decision: "Single processing engine for both batch and streaming avoids managing multiple compute frameworks.",
    tech: "PySpark, Spark SQL Window Functions, Hadoop-AWS S3A",
    snippet: `window_day = Window.partitionBy("symbol", "date")
df = df.withColumn("daily_open", F.first("open").over(window_day))
       .withColumn("daily_high", F.max("high").over(window_day))
       .withColumn("daily_low",  F.min("low").over(window_day))
       .withColumn("daily_close", F.last("close").over(window_day))`
  },
  snow: {
    name: "Snowflake Cloud Warehouse",
    tag: "Analytical Warehouse Layer",
    badgeClass: "node-snow",
    role: "Stores curated, deduplicated daily stock metrics in the Gold layer for business intelligence and executive reporting.",
    problemSolved: "Prevents duplicate rows on retried Airflow DAG runs and provides sub-second query performance for analytics.",
    decision: "Uses write_pandas (PUT + COPY INTO staging) combined with an atomic SQL MERGE INTO statement.",
    tech: "Snowflake Connector, write_pandas, MERGE Upsert",
    snippet: `write_pandas(conn, df, "TEMP_DAILY_STOCK_STAGE")
cursor.execute("""
    MERGE INTO DAILY_STOCK_METRICS target
    USING TEMP_DAILY_STOCK_STAGE stage
    ON target.symbol = stage.symbol AND target.date = stage.date
    WHEN MATCHED THEN UPDATE ...
    WHEN NOT MATCHED THEN INSERT ...
""")`
  },
  air: {
    name: "Apache Airflow 2.8",
    tag: "Workflow Orchestrator",
    badgeClass: "node-air",
    role: "Schedules, executes, and monitors the 5 sequential tasks in the batch pipeline Directed Acyclic Graph (DAG).",
    problemSolved: "Replaces brittle cron jobs with dependency management, automated retries, execution timeouts, and an intuitive UI.",
    decision: "Sequential dependency chain (fetch >> consume >> process >> load >> complete) with catchup=False.",
    tech: "Airflow DAG, BashOperator, PostgreSQL Backend",
    snippet: `fetch_historical_data >> consume_historical_data >> process_data >> load_to_snowflake >> process_complete`
  }
};

// Global App State
const state = {
  selectedSymbol: "AAPL",
  chartType: "candles", // 'candles' or 'line'
  timeframe: "30D",
  showMA15: true,
  showMA30: true,
  streamingActive: false,
  streamIntervalId: null,
  activeTab: "snowflake",
  searchTerm: "",
  activeJourneyIndex: 0,
  stocks: {},
  warehouseRecords: []
};

// Generate historical candles for a symbol
function generateCandles(basePrice, count = 40) {
  const candles = [];
  let currentPrice = basePrice * 0.94;
  const now = new Date();

  for (let i = count; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const dateStr = d.toISOString().split("T")[0];

    const drift = (Math.random() - 0.48) * 0.03;
    const open = Math.round(currentPrice * 100) / 100;
    const close = Math.round(open * (1 + drift) * 100) / 100;
    const spread = Math.abs(close - open);
    const high = Math.round((Math.max(open, close) + spread * (0.3 + Math.random() * 0.8)) * 100) / 100;
    const low = Math.round((Math.min(open, close) - spread * (0.3 + Math.random() * 0.8)) * 100) / 100;
    const volume = Math.floor(10000000 + Math.random() * 40000000);

    candles.push({
      date: dateStr,
      open,
      high,
      low,
      close,
      volume,
      change: Math.round((close - open) * 100) / 100,
      percentChange: Math.round(((close - open) / open) * 10000) / 100
    });
    currentPrice = close;
  }

  // Calculate Moving Averages (MA15, MA30)
  for (let i = 0; i < candles.length; i++) {
    const ma15Slice = candles.slice(Math.max(0, i - 14), i + 1);
    const ma15Avg = ma15Slice.reduce((sum, c) => sum + c.close, 0) / ma15Slice.length;
    candles[i].ma15 = Math.round(ma15Avg * 100) / 100;

    const ma30Slice = candles.slice(Math.max(0, i - 29), i + 1);
    const ma30Avg = ma30Slice.reduce((sum, c) => sum + c.close, 0) / ma30Slice.length;
    candles[i].ma30 = Math.round(ma30Avg * 100) / 100;
  }

  return candles;
}

// Initialize Stock Data
function initStockData() {
  Object.keys(STOCKS_DATA).forEach(sym => {
    const meta = STOCKS_DATA[sym];
    const candles = generateCandles(meta.basePrice, 45);
    const latest = candles[candles.length - 1];
    state.stocks[sym] = {
      ...meta,
      symbol: sym,
      candles,
      latest
    };

    state.warehouseRecords.push({
      symbol: sym,
      date: latest.date,
      daily_open: latest.open,
      daily_high: latest.high,
      daily_low: latest.low,
      daily_close: latest.close,
      daily_volume: latest.volume,
      daily_change: latest.change,
      ma_15: latest.ma15,
      ma_30: latest.ma30,
      last_updated: new Date().toISOString()
    });
  });
}

// Render Top Ticker Ribbon
function renderTickerRibbon() {
  const ribbon = document.getElementById("stockRibbon");
  if (!ribbon) return;

  ribbon.innerHTML = "";
  Object.keys(state.stocks).forEach(sym => {
    const stock = state.stocks[sym];
    const isPos = stock.latest.change >= 0;

    const card = document.createElement("div");
    card.className = `ticker-card ${sym === state.selectedSymbol ? "active" : ""}`;
    card.id = `ticker-${sym}`;
    card.onclick = () => selectSymbol(sym);

    card.innerHTML = `
      <div class="ticker-symbol">${sym}</div>
      <div class="ticker-price">$${stock.latest.close.toFixed(2)}</div>
      <div class="ticker-change ${isPos ? 'pos' : 'neg'}">
        ${isPos ? '+' : ''}${stock.latest.percentChange.toFixed(2)}%
      </div>
    `;
    ribbon.appendChild(card);
  });
}

// Select Symbol
function selectSymbol(sym) {
  state.selectedSymbol = sym;
  renderTickerRibbon();
  updateStockHero();
  drawChart();
}

// Update Hero Details
function updateStockHero() {
  const stock = state.stocks[state.selectedSymbol];
  if (!stock) return;

  const latest = stock.latest;
  const isPos = latest.change >= 0;

  document.getElementById("heroSymbol").textContent = stock.symbol;
  document.getElementById("heroName").textContent = stock.name;
  document.getElementById("heroPrice").textContent = `$${latest.close.toFixed(2)}`;

  const changeTag = document.getElementById("heroChange");
  changeTag.className = `price-change-tag ${isPos ? 'pos' : 'neg'}`;
  changeTag.textContent = `${isPos ? '+' : ''}$${latest.change.toFixed(2)} (${isPos ? '+' : ''}${latest.percentChange.toFixed(2)}%)`;

  document.getElementById("statOpen").textContent = `$${latest.open.toFixed(2)}`;
  document.getElementById("statHigh").textContent = `$${latest.high.toFixed(2)}`;
  document.getElementById("statLow").textContent = `$${latest.low.toFixed(2)}`;
  document.getElementById("statVol").textContent = Number(latest.volume).toLocaleString();
  document.getElementById("statMA15").textContent = `$${latest.ma15.toFixed(2)}`;
  document.getElementById("statMA30").textContent = `$${latest.ma30.toFixed(2)}`;
}

// HTML5 Canvas Chart Renderer
function drawChart() {
  const canvas = document.getElementById("stockCanvas");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");

  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);

  const width = rect.width;
  const height = rect.height;

  ctx.clearRect(0, 0, width, height);

  const stock = state.stocks[state.selectedSymbol];
  if (!stock || !stock.candles.length) return;

  let candleCount = 30;
  if (state.timeframe === "1W") candleCount = 7;
  else if (state.timeframe === "1M") candleCount = 20;
  else if (state.timeframe === "3M") candleCount = 40;
  else if (state.timeframe === "1Y") candleCount = 45;

  const data = stock.candles.slice(-candleCount);
  const n = data.length;

  let minPrice = Infinity;
  let maxPrice = -Infinity;
  let maxVol = 0;

  data.forEach(d => {
    if (d.low < minPrice) minPrice = d.low;
    if (d.high > maxPrice) maxPrice = d.high;
    if (d.volume > maxVol) maxVol = d.volume;
    if (state.showMA15 && d.ma15) {
      if (d.ma15 < minPrice) minPrice = d.ma15;
      if (d.ma15 > maxPrice) maxPrice = d.ma15;
    }
    if (state.showMA30 && d.ma30) {
      if (d.ma30 < minPrice) minPrice = d.ma30;
      if (d.ma30 > maxPrice) maxPrice = d.ma30;
    }
  });

  const pricePadding = (maxPrice - minPrice) * 0.08 || 5;
  minPrice -= pricePadding;
  maxPrice += pricePadding;

  const chartHeight = height * 0.75;
  const volumeHeight = height * 0.20;
  const paddingBottom = 25;
  const paddingRight = 60;
  const paddingLeft = 10;
  const paddingTop = 20;

  const chartWidth = width - paddingRight - paddingLeft;

  function getY(price) {
    return paddingTop + (1 - (price - minPrice) / (maxPrice - minPrice)) * (chartHeight - paddingTop);
  }

  function getX(index) {
    return paddingLeft + (index + 0.5) * (chartWidth / n);
  }

  // Draw Grid Lines & Price Labels
  ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
  ctx.lineWidth = 1;
  ctx.fillStyle = "#64748b";
  ctx.font = "10px JetBrains Mono, monospace";

  const gridSteps = 5;
  for (let i = 0; i <= gridSteps; i++) {
    const p = minPrice + (maxPrice - minPrice) * (i / gridSteps);
    const y = getY(p);

    ctx.beginPath();
    ctx.moveTo(paddingLeft, y);
    ctx.lineTo(width - paddingRight, y);
    ctx.stroke();

    ctx.fillText(`$${p.toFixed(2)}`, width - paddingRight + 6, y + 3);
  }

  // Draw Volume Histogram at bottom
  const volBaseY = height - paddingBottom;
  const colWidth = Math.max(2, (chartWidth / n) * 0.65);

  data.forEach((d, idx) => {
    const x = getX(idx) - colWidth / 2;
    const vHeight = (d.volume / maxVol) * volumeHeight;
    const isUp = d.close >= d.open;

    ctx.fillStyle = isUp ? "rgba(16, 185, 129, 0.25)" : "rgba(244, 63, 94, 0.25)";
    ctx.fillRect(x, volBaseY - vHeight, colWidth, vHeight);
  });

  // Render Line or Candlestick
  if (state.chartType === "line") {
    const gradient = ctx.createLinearGradient(0, paddingTop, 0, chartHeight);
    gradient.addColorStop(0, "rgba(56, 189, 248, 0.35)");
    gradient.addColorStop(1, "rgba(56, 189, 248, 0.0)");

    ctx.beginPath();
    ctx.moveTo(getX(0), chartHeight);
    data.forEach((d, i) => {
      ctx.lineTo(getX(i), getY(d.close));
    });
    ctx.lineTo(getX(n - 1), chartHeight);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();

    ctx.beginPath();
    data.forEach((d, i) => {
      const x = getX(i);
      const y = getY(d.close);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = "#38bdf8";
    ctx.lineWidth = 2.5;
    ctx.stroke();

  } else {
    data.forEach((d, idx) => {
      const x = getX(idx);
      const openY = getY(d.open);
      const closeY = getY(d.close);
      const highY = getY(d.high);
      const lowY = getY(d.low);

      const isUp = d.close >= d.open;
      const color = isUp ? "#10b981" : "#f43f5e";

      // Wick
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(x, highY);
      ctx.lineTo(x, lowY);
      ctx.stroke();

      // Body
      const topY = Math.min(openY, closeY);
      const bodyH = Math.max(2, Math.abs(openY - closeY));
      ctx.fillStyle = color;
      ctx.fillRect(x - colWidth / 2, topY, colWidth, bodyH);
    });
  }

  // Draw Moving Averages
  if (state.showMA15) {
    ctx.beginPath();
    let started = false;
    data.forEach((d, i) => {
      if (d.ma15) {
        const x = getX(i);
        const y = getY(d.ma15);
        if (!started) { ctx.moveTo(x, y); started = true; }
        else ctx.lineTo(x, y);
      }
    });
    ctx.strokeStyle = "#fb923c";
    ctx.lineWidth = 1.6;
    ctx.stroke();
  }

  if (state.showMA30) {
    ctx.beginPath();
    let started = false;
    data.forEach((d, i) => {
      if (d.ma30) {
        const x = getX(i);
        const y = getY(d.ma30);
        if (!started) { ctx.moveTo(x, y); started = true; }
        else ctx.lineTo(x, y);
      }
    });
    ctx.strokeStyle = "#a855f7";
    ctx.lineWidth = 1.6;
    ctx.stroke();
  }

  // Dates on X-axis
  const step = Math.ceil(n / 6);
  ctx.fillStyle = "#64748b";
  ctx.font = "10px JetBrains Mono, monospace";
  for (let i = 0; i < n; i += step) {
    const x = getX(i);
    const dStr = data[i].date.slice(5);
    ctx.fillText(dStr, x - 14, height - 8);
  }
}

// Trigger Batch DAG Run (Simulates the 5 tasks of Airflow DAG)
function triggerBatchDAG() {
  const btn = document.getElementById("btnTriggerBatch");
  btn.disabled = true;
  btn.innerHTML = `<span>⏳</span> Running DAG...`;

  const terminal = document.getElementById("terminalBody");
  terminal.innerHTML = "";

  function addLog(text, type = "info") {
    const line = document.createElement("div");
    line.className = `terminal-line ${type}`;
    const ts = new Date().toLocaleTimeString();
    line.textContent = `[${ts}] ${text}`;
    terminal.appendChild(line);
    terminal.scrollTop = terminal.scrollHeight;
  }

  addLog("🚀 [Airflow] Triggering DAG: stock_market_batch_pipeline", "highlight");

  const nodes = {
    alpha: document.getElementById("node-alpha"),
    kafka: document.getElementById("node-kafka"),
    minio: document.getElementById("node-minio"),
    spark: document.getElementById("node-spark"),
    snow:  document.getElementById("node-snow"),
    air:   document.getElementById("node-air")
  };

  // Task 1: Fetch
  setTimeout(() => {
    nodes.alpha.classList.add("highlight");
    addLog("Task 1/5: fetch_historical_data (AlphaVantage TIME_SERIES_DAILY)...", "info");
    addLog("✓ Polled 10 equities: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, INTC, JPM, V", "success");
  }, 500);

  // Task 2: Kafka & S3 Raw
  setTimeout(() => {
    nodes.alpha.classList.remove("highlight");
    nodes.kafka.classList.add("highlight");
    nodes.minio.classList.add("highlight");
    addLog("Task 2/5: consume_historical_data -> Publishing to Kafka topic 'stock-market-batch'...", "info");
    addLog("✓ Ingested 10 messages into MinIO: s3a://stock-market-data/raw/historical/year=2026/...", "success");
  }, 1600);

  // Task 3: Spark Batch
  setTimeout(() => {
    nodes.kafka.classList.remove("highlight");
    nodes.minio.classList.remove("highlight");
    nodes.spark.classList.add("highlight");
    addLog("Task 3/5: process_data -> Submitting PySpark Batch Job to spark://spark-master:7077...", "info");
    addLog("✓ Spark computed daily OHLCV, MA15, MA30 and partitioned by symbol into MinIO processed Parquet", "success");
  }, 2900);

  // Task 4: Snowflake MERGE
  setTimeout(() => {
    nodes.spark.classList.remove("highlight");
    nodes.snow.classList.add("highlight");
    addLog("Task 4/5: load_historical_to_snowflake -> Connecting to Snowflake (STOCKMARKETBATCH)...", "info");
    addLog("✓ Executed MERGE INTO DAILY_STOCK_METRICS (10 records upserted without duplication)", "success");
  }, 4200);

  // Task 5: Complete
  setTimeout(() => {
    nodes.snow.classList.remove("highlight");
    nodes.air.classList.add("highlight");
    addLog("Task 5/5: process_complete -> Batch DAG execution marked SUCCESS.", "highlight");

    setTimeout(() => nodes.air.classList.remove("highlight"), 1500);

    btn.disabled = false;
    btn.innerHTML = `<span>▶</span> Trigger Airflow Batch Run`;

    initStockData();
    renderTickerRibbon();
    updateStockHero();
    drawChart();
    renderExplorerTable();
  }, 5300);
}

// Toggle Live Real-Time Stream Mode
function toggleStreaming() {
  const btn = document.getElementById("btnToggleStream");
  state.streamingActive = !state.streamingActive;

  if (state.streamingActive) {
    btn.className = "btn btn-primary";
    btn.innerHTML = `<span class="dot" style="background:#4ade80;"></span> Streaming LIVE (Kafka)`;
    
    state.streamIntervalId = setInterval(() => {
      Object.keys(state.stocks).forEach(sym => {
        const s = state.stocks[sym];
        const last = s.candles[s.candles.length - 1];
        const tickDrift = (Math.random() - 0.49) * 0.006;
        const newClose = Math.round(last.close * (1 + tickDrift) * 100) / 100;
        const change = Math.round((newClose - last.open) * 100) / 100;
        const pct = Math.round((change / last.open) * 10000) / 100;

        last.close = newClose;
        last.high = Math.max(last.high, newClose);
        last.low = Math.min(last.low, newClose);
        last.change = change;
        last.percentChange = pct;
        last.volume += Math.floor(Math.random() * 5000);
      });

      renderTickerRibbon();
      updateStockHero();
      drawChart();
    }, 2500);

  } else {
    btn.className = "btn btn-secondary";
    btn.innerHTML = `<span>⚡</span> Stream Mode [OFF]`;
    clearInterval(state.streamIntervalId);
  }
}


// ==========================================================================
// COMPONENT INSPECTOR MODAL
// ==========================================================================
function inspectNode(componentKey) {
  const spec = COMPONENT_SPECS[componentKey];
  if (!spec) return;

  const modal = document.getElementById("inspectorModal");
  document.getElementById("inspectorName").textContent = spec.name;
  document.getElementById("inspectorTag").textContent = spec.tag;
  document.getElementById("inspectorRole").textContent = spec.role;
  document.getElementById("inspectorProblem").textContent = spec.problemSolved;
  document.getElementById("inspectorDecision").textContent = spec.decision;
  document.getElementById("inspectorTech").textContent = spec.tech;
  document.getElementById("inspectorSnippet").textContent = spec.snippet;

  modal.classList.add("active");
}

function closeInspectorModal() {
  document.getElementById("inspectorModal").classList.remove("active");
}


// Render Data Explorer Table
function renderExplorerTable() {
  const container = document.getElementById("explorerTableContainer");
  if (!container) return;

  const query = state.searchTerm.toLowerCase();

  if (state.activeTab === "snowflake") {
    let rows = state.warehouseRecords.filter(r => 
      r.symbol.toLowerCase().includes(query) || r.date.includes(query)
    );

    let html = `
      <table class="data-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Date</th>
            <th>Open</th>
            <th>High</th>
            <th>Low</th>
            <th>Close</th>
            <th>Volume</th>
            <th>MA (15)</th>
            <th>MA (30)</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
    `;

    rows.forEach(r => {
      html += `
        <tr>
          <td><strong style="color:var(--accent-cyan);">${r.symbol}</strong></td>
          <td>${r.date}</td>
          <td>$${r.daily_open.toFixed(2)}</td>
          <td>$${r.daily_high.toFixed(2)}</td>
          <td>$${r.daily_low.toFixed(2)}</td>
          <td>$${r.daily_close.toFixed(2)}</td>
          <td>${Number(r.daily_volume).toLocaleString()}</td>
          <td>$${r.ma_15.toFixed(2)}</td>
          <td>$${r.ma_30.toFixed(2)}</td>
          <td><span class="badge-tag badge-green">UPSERTED</span></td>
        </tr>
      `;
    });

    html += `</tbody></table>`;
    container.innerHTML = html;

  } else if (state.activeTab === "lake") {
    const lakeFiles = [
      { path: "s3a://stock-market-data/raw/historical/year=2026/month=10/day=09/AAPL_145301.csv", type: "Raw CSV", size: "1.4 KB", format: "CSV" },
      { path: "s3a://stock-market-data/raw/historical/year=2026/month=10/day=09/NVDA_145301.csv", type: "Raw CSV", size: "1.5 KB", format: "CSV" },
      { path: "s3a://stock-market-data/raw/historical/year=2026/month=10/day=09/MSFT_145301.csv", type: "Raw CSV", size: "1.4 KB", format: "CSV" },
      { path: "s3a://stock-market-data/processed/historical/date=2026-10-09/symbol=AAPL/part-00000.parquet", type: "Processed", size: "4.8 KB", format: "Snappy Parquet" },
      { path: "s3a://stock-market-data/processed/historical/date=2026-10-09/symbol=NVDA/part-00000.parquet", type: "Processed", size: "4.9 KB", format: "Snappy Parquet" },
      { path: "s3a://stock-market-data/processed/historical/date=2026-10-09/symbol=TSLA/part-00000.parquet", type: "Processed", size: "4.7 KB", format: "Snappy Parquet" }
    ];

    let html = `
      <table class="data-table">
        <thead>
          <tr>
            <th>Object Path (S3 Lake)</th>
            <th>Layer</th>
            <th>Format</th>
            <th>Size</th>
            <th>Access</th>
          </tr>
        </thead>
        <tbody>
    `;

    lakeFiles.forEach(f => {
      html += `
        <tr>
          <td><code style="color:var(--text-main); font-size:0.78rem;">${f.path}</code></td>
          <td><span class="badge-tag ${f.type === 'Raw CSV' ? 'badge-blue' : 'badge-purple'}">${f.type}</span></td>
          <td>${f.format}</td>
          <td>${f.size}</td>
          <td><span class="badge-tag badge-green">MinIO S3</span></td>
        </tr>
      `;
    });

    html += `</tbody></table>`;
    container.innerHTML = html;

  } else if (state.activeTab === "kafka") {
    let html = `
      <table class="data-table">
        <thead>
          <tr>
            <th>Topic</th>
            <th>Partition</th>
            <th>Offset</th>
            <th>Key</th>
            <th>Message Payload Preview</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><span class="badge-tag badge-purple">stock-market-batch</span></td>
            <td>0</td>
            <td>1042</td>
            <td>AAPL</td>
            <td>{"symbol":"AAPL","daily_close":181.87,"volume":52410200}</td>
            <td>Just now</td>
          </tr>
          <tr>
            <td><span class="badge-tag badge-purple">stock-market-batch</span></td>
            <td>1</td>
            <td>1043</td>
            <td>NVDA</td>
            <td>{"symbol":"NVDA","daily_close":906.05,"volume":37433186}</td>
            <td>Just now</td>
          </tr>
          <tr>
            <td><span class="badge-tag badge-blue">stock-market-realtime</span></td>
            <td>0</td>
            <td>4820</td>
            <td>MSFT</td>
            <td>{"symbol":"MSFT","price":417.16,"change":3.37}</td>
            <td>2s ago</td>
          </tr>
        </tbody>
      </table>
    `;
    container.innerHTML = html;
  }
}

// Switch Explorer Tab
function switchTab(tabId) {
  state.activeTab = tabId;
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.getElementById(`tab-${tabId}`).classList.add("active");
  renderExplorerTable();
}

// Window Resize Hook for Canvas
window.addEventListener("resize", () => {
  drawChart();
});

// Initialize on DOM load
document.addEventListener("DOMContentLoaded", () => {
  initStockData();
  renderTickerRibbon();
  updateStockHero();
  drawChart();
  renderExplorerTable();

  // Chart type toggles
  document.getElementById("btnCandles").onclick = () => {
    state.chartType = "candles";
    document.getElementById("btnCandles").classList.add("active");
    document.getElementById("btnLine").classList.remove("active");
    drawChart();
  };

  document.getElementById("btnLine").onclick = () => {
    state.chartType = "line";
    document.getElementById("btnLine").classList.add("active");
    document.getElementById("btnCandles").classList.remove("active");
    drawChart();
  };

  // Timeframe toggles
  document.querySelectorAll(".tf-btn").forEach(btn => {
    btn.onclick = () => {
      document.querySelectorAll(".tf-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      state.timeframe = btn.dataset.tf;
      drawChart();
    };
  });

  // MA toggles
  document.getElementById("toggleMA15").onclick = (e) => {
    state.showMA15 = !state.showMA15;
    e.target.classList.toggle("active", state.showMA15);
    drawChart();
  };

  document.getElementById("toggleMA30").onclick = (e) => {
    state.showMA30 = !state.showMA30;
    e.target.classList.toggle("active", state.showMA30);
    drawChart();
  };

  // Search input
  const searchInput = document.getElementById("tableSearch");
  if (searchInput) {
    searchInput.oninput = (e) => {
      state.searchTerm = e.target.value;
      renderExplorerTable();
    };
  }
});
