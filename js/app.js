/**
 * TICKR Data Engineering Dashboard
 * Interactive Stock Market Data Pipeline Frontend & Architecture Visualizer
 */

// NSE cash universe. Live candles replace these when artifacts/nse_market.json loads.
// basePrice is only the fallback generator's starting level, not a market price.
const STOCKS_DATA = {
  RELIANCE:   { name: "Reliance Industries", sector: "Energy", basePrice: 1400, vol: "—" },
  TCS:        { name: "Tata Consultancy Services", sector: "Information Technology", basePrice: 3800, vol: "—" },
  HDFCBANK:   { name: "HDFC Bank", sector: "Banking", basePrice: 1700, vol: "—" },
  INFY:       { name: "Infosys", sector: "Information Technology", basePrice: 1800, vol: "—" },
  ICICIBANK:  { name: "ICICI Bank", sector: "Banking", basePrice: 1300, vol: "—" },
  SBIN:       { name: "State Bank of India", sector: "Banking", basePrice: 800, vol: "—" },
  BHARTIARTL: { name: "Bharti Airtel", sector: "Telecom", basePrice: 1600, vol: "—" },
  ITC:        { name: "ITC", sector: "FMCG", basePrice: 450, vol: "—" },
  LT:         { name: "Larsen & Toubro", sector: "Infrastructure", basePrice: 3600, vol: "—" },
  HINDUNILVR: { name: "Hindustan Unilever", sector: "FMCG", basePrice: 2500, vol: "—" }
};

function inr(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return "₹" + n.toFixed(2);
}


// Component Deep-Dive Specs
const COMPONENT_SPECS = {
  alpha: {
    name: "AlphaVantage Market API",
    tag: "Data Ingestion Source",
    badgeClass: "node-alpha",
    role: "Optional US compose client. The live NSE path does not use this node; it reads Yahoo chart daily candles for SYMBOL.NS.",
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
    problemSolved: "Decouples the API client from storage. Manual commit-after-write is at-least-once: a commit failure after a successful write can replay, and the gold key collapses that replay.",
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
    role: "Canonicalizes daily candles for one execution date. It does not re-sum a daily bar, and it does not invent MA15 or MA30 from a single partition.",
    problemSolved: "Keeps the Spark job on the same grain as TIME_SERIES_DAILY. Scale beyond this 10-symbol demo is not measured in this repository.",
    decision: "Single processing engine for both batch and streaming avoids managing multiple compute frameworks.",
    tech: "PySpark, Spark SQL Window Functions, Hadoop-AWS S3A",
    snippet: `# Daily candle: collapse exact replays, do not sum volume.
# Conflicting (symbol, date) values are left out and fail the task.`
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
  selectedSymbol: "RELIANCE",
  chartType: "candles", // 'candles' or 'line'
  timeframe: "30D",
  showMA15: true,
  showMA30: true,
  streamingActive: false,
  streamIntervalId: null,
  activePlatformTab: "monitor",
  activeTab: "snowflake",
  searchTerm: "",
  activeJourneyIndex: 0,
  stocks: {},
  warehouseRecords: [],
  screener: {
    searchTerm: "",
    changeFilter: "all",
    maFilter: "all",
    volFilter: "all",
    sortBy: "symbol",
    sortDir: "asc"
  },
  alertsConfig: {
    volatilityThreshold: 2.5,
    volumeSurgeMultiplier: 1.25,
    maDivergenceLimit: 3.0
  },
  dataQualityAudit: null
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

  // Calculate Moving Averages (MA15, MA30) if sufficient historical data exists
  for (let i = 0; i < candles.length; i++) {
    if (i >= 14) {
      const ma15Slice = candles.slice(i - 14, i + 1);
      const ma15Avg = ma15Slice.reduce((sum, c) => sum + c.close, 0) / 15;
      candles[i].ma15 = Math.round(ma15Avg * 100) / 100;
    } else {
      candles[i].ma15 = null; // Honest: do not fabricate MA15 when fewer than 15 sessions exist
    }

    if (i >= 29) {
      const ma30Slice = candles.slice(i - 29, i + 1);
      const ma30Avg = ma30Slice.reduce((sum, c) => sum + c.close, 0) / 30;
      candles[i].ma30 = Math.round(ma30Avg * 100) / 100;
    } else {
      candles[i].ma30 = null;
    }
  }

  return candles;
}

// Initialize Stock Data
function initStockData() {
  state.warehouseRecords = [];
  Object.keys(STOCKS_DATA).forEach(sym => {
    const meta = STOCKS_DATA[sym];
    const candles = generateCandles(meta.basePrice, 45);
    const latest = candles[candles.length - 1];

    // Trailing 15-day average volume calculation
    const trailingCandles = candles.slice(Math.max(0, candles.length - 15));
    const avgVol = trailingCandles.reduce((acc, c) => acc + c.volume, 0) / trailingCandles.length;
    latest.avgVolume15 = Math.round(avgVol);

    state.stocks[sym] = {
      ...meta,
      symbol: sym,
      candles,
      latest,
      dataSource: "Browser fallback series"
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
      <div class="ticker-price">${inr(stock.latest.close)}</div>
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
  document.getElementById("heroPrice").textContent = `${inr(latest.close)}`;

  const changeTag = document.getElementById("heroChange");
  changeTag.className = `price-change-tag ${isPos ? 'pos' : 'neg'}`;
  changeTag.textContent = `${isPos ? '+' : ''}${inr(latest.change)} (${isPos ? '+' : ''}${latest.percentChange.toFixed(2)}%)`;

  document.getElementById("statOpen").textContent = `${inr(latest.open)}`;
  document.getElementById("statHigh").textContent = `${inr(latest.high)}`;
  document.getElementById("statLow").textContent = `${inr(latest.low)}`;
  document.getElementById("statVol").textContent = Number(latest.volume).toLocaleString();
  document.getElementById("statMA15").textContent = latest.ma15 !== null ? `${inr(latest.ma15)}` : "N/A (Insufficient Data)";
  document.getElementById("statMA30").textContent = latest.ma30 !== null ? `${inr(latest.ma30)}` : "N/A (Insufficient Data)";
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

    ctx.fillText(`${inr(p)}`, width - paddingRight + 6, y + 3);
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
  btn.innerHTML = `<span>⏳</span> Playing walkthrough...`;

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

  addLog("SIMULATED WALKTHROUGH. This button does not call Airflow, Kafka, or Alpha Vantage.", "highlight");
  addLog("The verified command is: python3 run_pipeline_local.py --execution-date 2026-10-13", "info");

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
    addLog("Illustrated task 1/5: fetch_historical_data would request one TIME_SERIES_DAILY candle per symbol.", "info");
    addLog("Illustration only. The live client is capped at 10 batch calls, 5 quote snapshots, and 5 retries.", "success");
  }, 500);

  // Task 2: Kafka & S3 Raw
  setTimeout(() => {
    nodes.alpha.classList.remove("highlight");
    nodes.kafka.classList.add("highlight");
    nodes.minio.classList.add("highlight");
    addLog("Illustrated task 2/5: consume would commit Kafka offsets only after the bronze write.", "info");
    addLog("Illustration only. The local demo writes JSON under data/lake and does not start Kafka.", "success");
  }, 1600);

  // Task 3: Spark Batch
  setTimeout(() => {
    nodes.kafka.classList.remove("highlight");
    nodes.minio.classList.remove("highlight");
    nodes.spark.classList.add("highlight");
    addLog("Illustrated task 3/5: Spark would keep one daily candle per (symbol, date) and would not sum replayed volume.", "info");
    addLog("Illustration only. MA15 and MA30 in the local fixture come from ordered daily closes.", "success");
  }, 2900);

  // Task 4: Snowflake MERGE
  setTimeout(() => {
    nodes.spark.classList.remove("highlight");
    nodes.snow.classList.add("highlight");
    addLog("Illustrated task 4/5: a warehouse MERGE updates the same (symbol, date) instead of inserting another row.", "info");
    addLog("Illustration only. This demo's gold file is data/warehouse/DAILY_STOCK_METRICS.json.", "success");
  }, 4200);

  // Task 5: Complete
  setTimeout(() => {
    nodes.snow.classList.remove("highlight");
    nodes.air.classList.add("highlight");
    addLog("Walkthrough finished. Check the Data Quality tab for the last real local-batch manifest.", "highlight");

    setTimeout(() => nodes.air.classList.remove("highlight"), 1500);

    btn.disabled = false;
    btn.innerHTML = `<span>▶</span> Simulate DAG Walkthrough`;

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
    btn.innerHTML = `<span class="dot" style="background:#4ade80;"></span> Quote Snapshot Demo [ON]`;
    
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
    btn.innerHTML = `<span>⚡</span> Quote Snapshot Demo [OFF]`;
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
          <td>${inr(r.daily_open)}</td>
          <td>${inr(r.daily_high)}</td>
          <td>${inr(r.daily_low)}</td>
          <td>${inr(r.daily_close)}</td>
          <td>${Number(r.daily_volume).toLocaleString()}</td>
          <td>${r.ma_15 !== null && r.ma_15 !== undefined ? `${inr(r.ma_15)}` : '<span style="color:var(--text-dim); font-size:0.75rem;">N/A (Insufficient Data)</span>'}</td>
          <td>${r.ma_30 !== null && r.ma_30 !== undefined ? `${inr(r.ma_30)}` : '<span style="color:var(--text-dim); font-size:0.75rem;">N/A (Insufficient Data)</span>'}</td>
          <td><span class="badge-tag badge-green">UPSERTED</span></td>
        </tr>
      `;
    });

    html += `</tbody></table>`;
    container.innerHTML = html;

  } else if (state.activeTab === "lake") {
    const lakeSymbols = Object.keys(state.stocks).slice(0, 3);
    const lakeFiles = lakeSymbols.flatMap(sym => [
      { path: `data/lake/raw/historical/year=2026/month=10/day=13/${sym}.json`, type: "Raw JSON", size: "session file", format: "JSON" },
      { path: `data/lake/processed/historical/date=2026-10-13/symbol=${sym}/part-00000.json`, type: "Processed", size: "session file", format: "JSON" }
    ]);

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
          ${Object.keys(state.stocks).slice(0, 2).map((sym, index) => {
            const latest = state.stocks[sym].latest;
            return `<tr>
            <td><span class="badge-tag badge-purple">stock-market-batch</span></td>
            <td>${index}</td>
            <td>—</td>
            <td>${sym}</td>
            <td>{"symbol":"${sym}","daily_close":${latest.close},"volume":${latest.volume}}</td>
            <td>${latest.date}</td>
          </tr>`;
          }).join("")}
          <tr>
            <td><span class="badge-tag badge-blue">stock-market-realtime</span></td>
            <td>0</td>
            <td>—</td>
            <td colspan="3">Quote snapshots are not merged into the NSE daily book.</td>
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

/* ==========================================================================
   PLATFORM NAVIGATION CONTROLLER
   ========================================================================== */
function switchPlatformTab(tabId) {
  state.activePlatformTab = tabId;
  document.querySelectorAll(".nav-tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".tab-content-panel").forEach(p => p.classList.remove("active"));

  const targetBtn = document.getElementById(`pTab-${tabId}`);
  const targetPanel = document.getElementById(`panel-${tabId}`);
  if (targetBtn) targetBtn.classList.add("active");
  if (targetPanel) targetPanel.classList.add("active");

  if (tabId === "screener") {
    renderScreenerTable();
  } else if (tabId === "quality") {
    renderDataQualityDashboard();
  } else if (tabId === "alerts") {
    renderAlertsDashboard();
  } else if (tabId === "monitor") {
    drawChart();
  }
}

/* ==========================================================================
   FEATURE 1: STOCK SCREENER LOGIC
   ========================================================================== */
function renderScreenerTable() {
  const tbody = document.getElementById("screenerTableBody");
  if (!tbody) return;

  const searchVal = (document.getElementById("screenerSearch")?.value || "").trim().toLowerCase();
  const changeFilter = document.getElementById("screenerChangeFilter")?.value || "all";
  const maFilter = document.getElementById("screenerMaFilter")?.value || "all";
  const volFilter = document.getElementById("screenerVolFilter")?.value || "all";
  const sortBy = document.getElementById("screenerSortBy")?.value || "symbol";

  let list = Object.keys(state.stocks).map(sym => state.stocks[sym]);

  // Search filter (symbol, name, sector)
  if (searchVal) {
    list = list.filter(s => 
      s.symbol.toLowerCase().includes(searchVal) ||
      s.name.toLowerCase().includes(searchVal) ||
      s.sector.toLowerCase().includes(searchVal)
    );
  }

  // Daily % Change filter
  if (changeFilter === "gainers") {
    list = list.filter(s => s.latest.change > 0);
  } else if (changeFilter === "losers") {
    list = list.filter(s => s.latest.change < 0);
  }

  // MA15 filter
  if (maFilter === "above") {
    list = list.filter(s => s.latest.ma15 !== null && s.latest.close > s.latest.ma15);
  } else if (maFilter === "below") {
    list = list.filter(s => s.latest.ma15 !== null && s.latest.close < s.latest.ma15);
  } else if (maFilter === "missing") {
    list = list.filter(s => s.latest.ma15 === null);
  }

  // Volume Surge filter (>1.25x average)
  if (volFilter === "surge") {
    list = list.filter(s => s.latest.volume > (s.latest.avgVolume15 * 1.25));
  }

  // Sorting
  list.sort((a, b) => {
    if (sortBy === "price_desc") return b.latest.close - a.latest.close;
    if (sortBy === "price_asc") return a.latest.close - b.latest.close;
    if (sortBy === "change_desc") return b.latest.percentChange - a.latest.percentChange;
    if (sortBy === "change_asc") return a.latest.percentChange - b.latest.percentChange;
    if (sortBy === "volume_desc") return b.latest.volume - a.latest.volume;
    if (sortBy === "ma15_desc") return (b.latest.ma15 || 0) - (a.latest.ma15 || 0);
    return a.symbol.localeCompare(b.symbol);
  });

  // Update badge counter
  const badge = document.getElementById("screenerCountBadge");
  if (badge) badge.textContent = list.length;

  if (list.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" style="text-align:center; padding:2rem; color:var(--text-dim);">
          No stocks matched the active screener filters. Try resetting the criteria.
        </td>
      </tr>
    `;
    return;
  }

  let html = "";
  list.forEach(s => {
    const latest = s.latest;
    const isPos = latest.change >= 0;
    const isSurge = latest.volume > (latest.avgVolume15 * 1.25);
    
    let maDisplay = `<span style="color:#fb923c;">${inr(latest.ma15)}</span>`;
    let signalBadge = `<span class="badge-tag badge-blue">Neutral</span>`;

    if (latest.ma15 === null) {
      maDisplay = `<span style="color:var(--text-dim); font-size:0.75rem;">N/A (Insufficient Data)</span>`;
      signalBadge = `<span class="badge-tag" style="background:rgba(255,255,255,0.1); color:var(--text-dim);">No MA15 Baseline</span>`;
    } else if (latest.close > latest.ma15) {
      signalBadge = `<span class="badge-tag badge-green">&uarr; Bullish (Above MA15)</span>`;
    } else {
      signalBadge = `<span class="badge-tag" style="background:rgba(244,63,94,0.15); color:#fb7185;">&darr; Bearish (Below MA15)</span>`;
    }

    if (isSurge) {
      signalBadge += ` <span class="badge-tag" style="background:rgba(245,158,11,0.15); color:#fbbf24;">⚡ Volume Surge</span>`;
    }

    html += `
      <tr>
        <td>
          <strong style="color:var(--accent-cyan); cursor:pointer;" onclick="selectSymbol('${s.symbol}'); switchPlatformTab('monitor');">${s.symbol}</strong>
        </td>
        <td>
          <div style="font-weight:600;">${s.name}</div>
          <div style="font-size:0.72rem; color:var(--text-dim);">${s.sector}</div>
        </td>
        <td style="font-weight:700;">${inr(latest.close)}</td>
        <td style="color:${isPos ? '#34d399' : '#fb7185'}; font-weight:700;">
          ${isPos ? '+' : ''}${inr(latest.change)} (${isPos ? '+' : ''}${latest.percentChange.toFixed(2)}%)
        </td>
        <td>
          <div>${Number(latest.volume).toLocaleString()}</div>
          <div style="font-size:0.7rem; color:var(--text-dim);">Avg15: ${Number(latest.avgVolume15).toLocaleString()}</div>
        </td>
        <td>${maDisplay}</td>
        <td>${signalBadge}</td>
        <td><span class="badge-tag badge-blue">${s.dataSource || "Browser fallback series"}</span></td>
      </tr>
    `;
  });

  tbody.innerHTML = html;
}

function sortScreenerByCol(col) {
  const select = document.getElementById("screenerSortBy");
  if (!select) return;
  if (col === "symbol") select.value = "symbol";
  else if (col === "price") select.value = select.value === "price_desc" ? "price_asc" : "price_desc";
  else if (col === "change") select.value = select.value === "change_desc" ? "change_asc" : "change_desc";
  else if (col === "volume") select.value = "volume_desc";
  else if (col === "ma15") select.value = "ma15_desc";
  renderScreenerTable();
}

function resetScreenerFilters() {
  const search = document.getElementById("screenerSearch");
  const chg = document.getElementById("screenerChangeFilter");
  const ma = document.getElementById("screenerMaFilter");
  const vol = document.getElementById("screenerVolFilter");
  const sort = document.getElementById("screenerSortBy");
  if (search) search.value = "";
  if (chg) chg.value = "all";
  if (ma) ma.value = "all";
  if (vol) vol.value = "all";
  if (sort) sort.value = "symbol";
  renderScreenerTable();
}

/* ==========================================================================
   FEATURE 2: DATA QUALITY DASHBOARD AUDIT ENGINE
   ========================================================================== */
function executeDataQualityAudit() {
  const records = state.warehouseRecords;
  let totalAssertions = 0;
  const failures = [];

  const seenKeys = new Set();
  let duplicateCount = 0;
  let missingValueCount = 0;
  let schemaErrorCount = 0;

  records.forEach((row, idx) => {
    // 1. Primary Key Uniqueness Check (symbol, date)
    totalAssertions++;
    const pk = `${row.symbol}_${row.date}`;
    if (seenKeys.has(pk)) {
      duplicateCount++;
      failures.push({
        severity: "CRITICAL",
        ruleId: "ASSERT_PK_UNIQUE",
        entity: row.symbol,
        violation: `Duplicate primary key (${row.symbol}, ${row.date}) detected in warehouse load`,
        remediation: "Deduplicated via Window ROW_NUMBER partition in PySpark Silver layer; merged atomically in Snowflake",
        timestamp: new Date().toLocaleTimeString()
      });
    } else {
      seenKeys.add(pk);
    }

    // 2. Not Null Checks on Core Metrics
    const requiredCols = ["symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume"];
    requiredCols.forEach(col => {
      totalAssertions++;
      if (row[col] === null || row[col] === undefined || row[col] === "") {
        missingValueCount++;
        failures.push({
          severity: "WARNING",
          ruleId: "ASSERT_NOT_NULL",
          entity: `${row.symbol}::${col}`,
          violation: `Column ${col} contained missing or unpopulated value on date ${row.date}`,
          remediation: "Forward-fill or default imputation applied; quarantined in raw audit logs",
          timestamp: new Date().toLocaleTimeString()
        });
      }
    });

    // 3. Price Sanity Check (> 0)
    totalAssertions++;
    if (row.daily_close <= 0) {
      failures.push({
        severity: "CRITICAL",
        ruleId: "ASSERT_NON_NEGATIVE_PRICE",
        entity: row.symbol,
        violation: `Invalid daily_close value (${inr(row.daily_close)}) <= 0`,
        remediation: "Rejected from Gold warehouse; ingestion alert dispatched",
        timestamp: new Date().toLocaleTimeString()
      });
    }

    // 4. Spread Integrity Check (High >= Low)
    totalAssertions++;
    if (row.daily_high < row.daily_low) {
      failures.push({
        severity: "CRITICAL",
        ruleId: "ASSERT_HIGH_GE_LOW",
        entity: row.symbol,
        violation: `Spread inversion: High (${inr(row.daily_high)}) < Low (${inr(row.daily_low)})`,
        remediation: "Quarantined in MinIO dead-letter bucket; flagged for provider re-fetch",
        timestamp: new Date().toLocaleTimeString()
      });
    }

    // 5. Volume Sanity Check (volume >= 0)
    totalAssertions++;
    if (row.daily_volume < 0) {
      failures.push({
        severity: "WARNING",
        ruleId: "ASSERT_NON_NEGATIVE_VOL",
        entity: row.symbol,
        violation: `Negative trading volume (${row.daily_volume}) observed`,
        remediation: "Absolute value transformation or null replacement in Silver stage",
        timestamp: new Date().toLocaleTimeString()
      });
    }
  });

  const passRate = totalAssertions === 0
    ? 0
    : Number((((totalAssertions - failures.length) / totalAssertions) * 100).toFixed(1));

  return {
    scope: "browser_demonstration_series",
    totalRecords: records.length,
    totalAssertions,
    passRate,
    missingRecords: missingValueCount,
    duplicateRecords: duplicateCount,
    schemaErrors: schemaErrorCount,
    failures
  };
}

function renderDataQualityDashboard() {
  const audit = executeDataQualityAudit();
  state.dataQualityAudit = audit;

  const totalEl = document.getElementById("dqTotalRecords");
  const passRateEl = document.getElementById("dqPassRate");
  const passBadge = document.getElementById("dqPassRateBadge");
  const missingEl = document.getElementById("dqMissingRecords");
  const dupEl = document.getElementById("dqDuplicateRecords");
  const schemaEl = document.getElementById("dqSchemaErrors");
  const freshnessEl = document.getElementById("dqFreshness");
  const tbody = document.getElementById("dqFailedChecksBody");

  if (totalEl) totalEl.textContent = Number(audit.totalRecords).toLocaleString();
  if (passRateEl) passRateEl.textContent = `${audit.passRate}%`;
  if (passBadge) passBadge.textContent = `${audit.passRate}%`;
  if (missingEl) missingEl.textContent = audit.missingRecords;
  if (dupEl) dupEl.textContent = audit.duplicateRecords;
  if (schemaEl) schemaEl.textContent = audit.schemaErrors;
  if (freshnessEl) {
    const manifest = state.pipelineManifest;
    freshnessEl.textContent = manifest
      ? manifest.execution_date
      : "No manifest";
  }
  const sla = document.getElementById("dqSlaStatus");
  if (sla) {
    sla.textContent = state.pipelineManifest
      ? `Last local run ${state.pipelineManifest.status} · ${state.pipelineManifest.finished_at}`
      : "Run python3 run_pipeline_local.py to write a manifest";
  }

  if (tbody) {
    if (audit.failures.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="6" style="text-align:center; padding:2rem; color:#34d399;">
            ✔ All automated data quality checks passed with 100% integrity! No violations detected.
          </td>
        </tr>
      `;
      return;
    }

    let html = "";
    audit.failures.forEach(f => {
      const sevClass = f.severity === "CRITICAL" ? "dq-badge-crit" : "dq-badge-warn";
      html += `
        <tr>
          <td><span class="${sevClass}">${f.severity}</span></td>
          <td><code style="color:var(--accent-cyan);">${f.ruleId}</code></td>
          <td><strong>${f.entity}</strong></td>
          <td style="color:#e2e8f0;">${f.violation}</td>
          <td style="font-size:0.78rem; color:var(--text-muted);">${f.remediation}</td>
          <td style="font-size:0.75rem; color:var(--text-dim); font-family:var(--font-mono);">${f.timestamp}</td>
        </tr>
      `;
    });
    tbody.innerHTML = html;
  }
}

function runLiveQualityAudit() {
  const badge = document.getElementById("dqAuditStatusBadge");
  if (badge) {
    badge.textContent = "Re-checking browser series…";
    badge.style.color = "#fbbf24";
  }

  setTimeout(() => {
    renderDataQualityDashboard();
    loadPipelineManifest();
    if (badge) {
      badge.textContent = "Browser series audit";
      badge.style.color = "#fbbf24";
    }
  }, 400);
}

function shortStamp(value) {
  if (!value) return "";
  return value.replace("T", " ").replace(/\.\d+/, "").replace("+00:00", " UTC");
}

function renderPipelineKpis(manifest) {
  const grid = document.getElementById("pipelineKpiGrid");
  const source = document.getElementById("pipelineKpiSource");
  if (!grid) return;

  if (!manifest) {
    if (source) source.textContent = "No artifacts/latest_run_manifest.json";
    grid.innerHTML = `
      <div class="dq-kpi-card">
        <span class="dq-kpi-label">Last run</span>
        <span class="dq-kpi-val" style="font-size:1.1rem;">Not loaded</span>
        <span class="dq-kpi-footer">Run python3 run_pipeline_local.py --execution-date 2026-10-13</span>
      </div>`;
    return;
  }

  state.pipelineManifest = manifest;
  const counts = manifest.row_counts || {};
  const completeness = manifest.completeness || {};
  const quality = manifest.quality || {};
  const ratio = completeness.completeness_ratio == null
    ? "—"
    : `${Math.round(completeness.completeness_ratio * 1000) / 10}%`;
  if (source) {
    source.textContent = `${manifest.data_source} · ${manifest.runtime_status} · ${manifest.run_id}`;
  }

  const cards = [
    ["Last status", manifest.status, manifest.execution_date || ""],
    ["Freshness", manifest.execution_date || "—", shortStamp(manifest.finished_at) || "Gold time unavailable"],
    ["Completeness", ratio, `${completeness.received_combinations ?? "—"} / ${completeness.expected_combinations ?? "—"} symbol-sessions`],
    ["Validation", quality.pass_rate_pct != null ? `${quality.pass_rate_pct}%` : "—", quality.processing_status || ""],
    ["Duplicate keys", String(manifest.duplicate_key_count ?? 0), "Canonical key is (symbol, date)"],
    ["Rejected rows", String(counts.rejected ?? manifest.invalid_record_count ?? 0), "Conflicts are not loaded"],
    ["Gold rows", String(counts.gold_total ?? "—"), `${counts.gold_for_execution_date ?? "—"} on the session`],
    ["Duration", manifest.duration_seconds != null ? `${manifest.duration_seconds}s` : "—", manifest.data_source === "yahoo_finance_chart" ? "Live NSE daily fetch, not a cluster benchmark" : "Local fixture run, not a cluster benchmark"],
    ["API calls", String(manifest.quota_calls_this_run ?? 0), manifest.quota_note || ""],
    ["Recovery", manifest.recovered ? "Recovered" : (manifest.failure_injected || "No injected failure"), "At-least-once replay is covered by unit tests"]
  ];

  grid.innerHTML = cards.map(([label, value, footer]) => `
    <div class="dq-kpi-card">
      <span class="dq-kpi-label">${label}</span>
      <span class="dq-kpi-val" style="font-size:1.15rem;">${value}</span>
      <span class="dq-kpi-footer">${footer}</span>
    </div>
  `).join("");
}

async function loadPipelineManifest() {
  try {
    const response = await fetch("artifacts/latest_run_manifest.json", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const manifest = await response.json();
    renderPipelineKpis(manifest);
    const freshnessEl = document.getElementById("dqFreshness");
    if (freshnessEl) freshnessEl.textContent = manifest.execution_date;
    const sla = document.getElementById("dqSlaStatus");
    if (sla) sla.textContent = `Last run ${manifest.status} · ${manifest.finished_at}`;
    const health = document.getElementById("healthBatch");
    if (health) {
      health.textContent = manifest.data_source === "yahoo_finance_chart"
        ? `NSE daily: ${manifest.execution_date}`
        : `Fixture batch: ${manifest.execution_date}`;
    }
  } catch (error) {
    state.pipelineManifest = null;
    renderPipelineKpis(null);
  }
}

/* ==========================================================================
   FEATURE 3: MARKET ACTIVITY ALERTS ENGINE
   ========================================================================== */
function computeMarketAlerts() {
  const volThresh = parseFloat(document.getElementById("threshVolatility")?.value || state.alertsConfig.volatilityThreshold);
  const surgeThresh = parseFloat(document.getElementById("threshVolume")?.value || state.alertsConfig.volumeSurgeMultiplier);
  const maThresh = parseFloat(document.getElementById("threshMaDivergence")?.value || state.alertsConfig.maDivergenceLimit);

  const alerts = [];

  Object.keys(state.stocks).forEach(sym => {
    const s = state.stocks[sym];
    const latest = s.latest;

    // Rule 1: Intraday Volatility Swing
    const absChange = Math.abs(latest.percentChange);
    if (absChange >= volThresh) {
      alerts.push({
        stock: s.symbol,
        name: s.name,
        type: "PRICE_VOLATILITY_SWING",
        title: "Intraday Volatility Threshold Exceeded",
        severity: absChange >= volThresh * 1.5 ? "CRITICAL" : "WARNING",
        observed: `${latest.percentChange >= 0 ? '+' : ''}${latest.percentChange.toFixed(2)}%`,
        threshold: `|Δ%| ≥ ${volThresh.toFixed(1)}%`,
        reason: `Daily price shift of ${latest.percentChange.toFixed(2)}% exceeded configurable volatility rule (${volThresh.toFixed(1)}%).`,
        ruleType: "Statistical Price Delta",
        timestamp: new Date().toLocaleTimeString()
      });
    }

    // Rule 2: Volume Surge
    if (latest.avgVolume15 && latest.volume >= (latest.avgVolume15 * surgeThresh)) {
      const multiple = (latest.volume / latest.avgVolume15).toFixed(2);
      alerts.push({
        stock: s.symbol,
        name: s.name,
        type: "UNUSUAL_VOLUME_SURGE",
        title: "Unusual Trading Liquidity Spike",
        severity: "WARNING",
        observed: `${Number(latest.volume).toLocaleString()} (${multiple}x)`,
        threshold: `≥ ${surgeThresh.toFixed(2)}x Avg (${Number(latest.avgVolume15).toLocaleString()})`,
        reason: `Session volume is ${multiple}x of trailing 15-day average, signaling abnormal liquidity inflow/outflow.`,
        ruleType: "Volume Multiplier Heuristic",
        timestamp: new Date().toLocaleTimeString()
      });
    }

    // Rule 3: Moving Average Divergence
    if (latest.ma15 !== null) {
      const maDiv = Math.abs(((latest.close - latest.ma15) / latest.ma15) * 100);
      if (maDiv >= maThresh) {
        alerts.push({
          stock: s.symbol,
          name: s.name,
          type: "MA_DIVERGENCE_ANOMALY",
          title: "15-Day Moving Average Gap Exceeded",
          severity: "INFO",
          observed: `${latest.close > latest.ma15 ? '+' : '-'}${maDiv.toFixed(2)}% (${inr(latest.close)} vs ${inr(latest.ma15)})`,
          threshold: `|MA15 Gap| ≥ ${maThresh.toFixed(1)}%`,
          reason: `Current price has separated by ${maDiv.toFixed(2)}% from its 15-day moving average, signaling high mean-reversion tension.`,
          ruleType: "Statistical MA Spread",
          timestamp: new Date().toLocaleTimeString()
        });
      }
    }
  });

  return alerts;
}

function renderAlertsDashboard() {
  const alerts = computeMarketAlerts();
  const grid = document.getElementById("alertsGrid");
  const countDisp = document.getElementById("alertCountDisplay");
  const badge = document.getElementById("alertsCountBadge");

  if (countDisp) countDisp.textContent = alerts.length;
  if (badge) badge.textContent = alerts.length;

  if (!grid) return;

  if (alerts.length === 0) {
    grid.innerHTML = `
      <div style="grid-column: 1 / -1; text-align: center; padding: 2.5rem; background: rgba(15,23,42,0.6); border-radius: 12px; border: 1px dashed var(--border-color); color: var(--text-dim);">
        <p style="font-size:1rem; color:var(--text-main);">No alerts currently triggered.</p>
        <p style="font-size:0.8rem; margin-top:0.35rem;">Try lowering the volatility threshold or volume multiplier sliders above to inspect active triggers.</p>
      </div>
    `;
    return;
  }

  let html = "";
  alerts.forEach(a => {
    let sevBadge = `<span class="badge-tag badge-blue">Informational</span>`;
    if (a.severity === "CRITICAL") sevBadge = `<span class="badge-tag" style="background:rgba(244,63,94,0.2); color:#fb7185;">Critical Alert</span>`;
    else if (a.severity === "WARNING") sevBadge = `<span class="badge-tag" style="background:rgba(245,158,11,0.2); color:#fbbf24;">Warning</span>`;

    html += `
      <div class="alert-card-item">
        <div class="alert-card-top">
          <div style="display:flex; align-items:center; gap:0.6rem;">
            <span class="alert-stock-badge">${a.stock}</span>
            <span style="font-size:0.8rem; color:var(--text-dim);">${a.name}</span>
          </div>
          ${sevBadge}
        </div>

        <div style="font-size:0.9rem; font-weight:700; color:var(--text-main); margin-top:0.25rem;">
          ${a.title}
        </div>

        <div class="alert-card-reason">
          ${a.reason}
        </div>

        <div style="display:grid; grid-template-columns:1fr 1fr; gap:0.5rem; background:rgba(0,0,0,0.3); padding:0.6rem; border-radius:6px; font-size:0.75rem; font-family:var(--font-mono); margin-top:0.25rem;">
          <div>
            <span style="color:var(--text-dim); display:block; font-size:0.68rem;">OBSERVED VALUE</span>
            <strong style="color:#38bdf8;">${a.observed}</strong>
          </div>
          <div>
            <span style="color:var(--text-dim); display:block; font-size:0.68rem;">CONFIGURED RULE</span>
            <strong style="color:var(--accent-amber);">${a.threshold}</strong>
          </div>
        </div>

        <div class="alert-card-meta">
          <span>Engine: ${a.ruleType}</span>
          <span>Triggered: ${a.timestamp}</span>
        </div>
      </div>
    `;
  });

  grid.innerHTML = html;
}

function updateAlertThresholds() {
  const vol = document.getElementById("threshVolatility")?.value;
  const surge = document.getElementById("threshVolume")?.value;
  const ma = document.getElementById("threshMaDivergence")?.value;

  if (vol) document.getElementById("valVolatility").textContent = `≥ ${parseFloat(vol).toFixed(1)}%`;
  if (surge) document.getElementById("valVolume").textContent = `≥ ${parseFloat(surge).toFixed(2)}x`;
  if (ma) document.getElementById("valMaDivergence").textContent = `≥ ${parseFloat(ma).toFixed(1)}%`;

  renderAlertsDashboard();
}

function resetAlertThresholds() {
  const vol = document.getElementById("threshVolatility");
  const surge = document.getElementById("threshVolume");
  const ma = document.getElementById("threshMaDivergence");

  if (vol) vol.value = 2.5;
  if (surge) surge.value = 1.25;
  if (ma) ma.value = 3.0;

  updateAlertThresholds();
}

function refreshMarketViews() {
  renderTickerRibbon();
  updateStockHero();
  drawChart();
  renderExplorerTable();
  renderScreenerTable();
  renderDataQualityDashboard();
  renderAlertsDashboard();
}

function applyMarketBook(book) {
  const entries = Array.isArray(book.symbols) ? book.symbols : [];
  if (!entries.length) return false;
  state.warehouseRecords = [];
  state.stocks = {};
  entries.forEach(entry => {
    const candles = (entry.candles || []).map(candle => ({
      date: candle.date,
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      volume: candle.volume,
      change: candle.change,
      percentChange: candle.percentChange,
      ma15: candle.ma15 == null ? null : candle.ma15,
      ma30: candle.ma30 == null ? null : candle.ma30
    }));
    if (!candles.length) return;
    const latest = candles[candles.length - 1];
    const trailing = candles.slice(-15);
    latest.avgVolume15 = Math.round(trailing.reduce((sum, candle) => sum + Number(candle.volume || 0), 0) / trailing.length);
    state.stocks[entry.symbol] = {
      name: entry.name,
      sector: entry.sector,
      symbol: entry.symbol,
      candles,
      latest,
      dataSource: "NSE daily candle"
    };
    state.warehouseRecords.push({
      symbol: entry.symbol,
      date: latest.date,
      daily_open: latest.open,
      daily_high: latest.high,
      daily_low: latest.low,
      daily_close: latest.close,
      daily_volume: latest.volume,
      daily_change: latest.percentChange,
      ma_15: latest.ma15,
      ma_30: latest.ma30,
      last_updated: book.as_of
    });
  });
  if (!state.stocks[state.selectedSymbol]) {
    state.selectedSymbol = entries[0].symbol;
  }
  const label = document.getElementById("marketDataSourceLabel");
  if (label) label.textContent = `NSE · ${book.as_of} · ${book.currency || "INR"}`;
  const note = document.getElementById("chartDataNote");
  if (note) note.textContent = `Daily candles through ${book.as_of}. ${book.source}`;
  const screenerBlurb = document.getElementById("screenerBlurb");
  if (screenerBlurb) screenerBlurb.textContent = `NSE cash equities through ${book.as_of}: daily move, MA15, and volume versus a trailing baseline.`;
  return Object.keys(state.stocks).length > 0;
}

async function loadLiveMarket() {
  try {
    const response = await fetch("artifacts/nse_market.json", { cache: "no-store" });
    if (!response.ok) return false;
    return applyMarketBook(await response.json());
  } catch (error) {
    return false;
  }
}

// Initialize on DOM load
document.addEventListener("DOMContentLoaded", () => {
  initStockData();
  refreshMarketViews();
  loadPipelineManifest();
  loadLiveMarket().then(loaded => {
    if (loaded) refreshMarketViews();
  });

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

  // Search input in explorer
  const searchInput = document.getElementById("tableSearch");
  if (searchInput) {
    searchInput.oninput = (e) => {
      state.searchTerm = e.target.value;
      renderExplorerTable();
    };
  }

  // Screener controls listeners
  const scSearch = document.getElementById("screenerSearch");
  if (scSearch) scSearch.oninput = () => renderScreenerTable();

  const scChange = document.getElementById("screenerChangeFilter");
  if (scChange) scChange.onchange = () => renderScreenerTable();

  const scMa = document.getElementById("screenerMaFilter");
  if (scMa) scMa.onchange = () => renderScreenerTable();

  const scVol = document.getElementById("screenerVolFilter");
  if (scVol) scVol.onchange = () => renderScreenerTable();

  const scSort = document.getElementById("screenerSortBy");
  if (scSort) scSort.onchange = () => renderScreenerTable();
});

