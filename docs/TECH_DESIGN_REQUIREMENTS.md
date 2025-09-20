# Trading System — Requirements & Workflow Contract (v1)

## 0) Scope

Build one mid/long-horizon strategy with:
- **Daily risk alerts** after market close.
- **Weekly/Monthly rebalancing proposals.**
- **Manual execution** in v1 (no auto-trading).
- Deterministic backtests and paper/live parity.

Out of scope (v1): multi-strategy portfolio, advanced sizing (Kelly/vol-targeting), auto-execution.

---

## 1) Expected Usage

### Primary flows
1) **Daily (post-close)**  
   User receives a report containing: updated holdings, risk alerts, market filter state, and any exit recommendations.

2) **Weekly/Monthly (rebalance day)**  
   User receives a proposal with target weights and a draft order list to enact next session.

3) **Ad hoc (research/backtest)**  
   User runs backtests, inspects metrics, and iterates parameters in code/config.

### Personas
- **Owner/Operator**: runs scheduled jobs, reviews reports, decides actions, executes orders manually at broker.

---

## 2) Modes & Assumptions

- **Mode A: Research/Backtest**  
  Offline; uses historical data; produces metrics, charts, logs. No alerts/orders.

- **Mode B: Paper/Live-Alerts**  
  Uses latest EOD data; produces reports, alerts, proposals. **No order placement**.

- **Assumptions**
  - Data source is reachable and returns expected schema.
  - Universe config and strategy params are valid.
  - Prior steps produced correct inputs (no re-validation loops).

---

## 3) High-Level Workflow

```
Data Pull → Preprocess → Signals → Risk Rules → Rebalance Decision → Proposal & Report → Notification
```

Manual steps:
- Approve/reject proposals.
- Place/cancel/modify orders at broker.
- Update parameters on cadence (weekly/monthly).

---

## 4) Detailed Steps (Inputs → Process → Outputs)

### 4.1 Data Pull (EOD)
**Input**
- `config.yml`: data provider config, universe (tickers), date range, calendar.
- Optional `holdings.json`: current positions snapshot (symbol, qty, cost_basis, as_of_date).

**Process**
- Fetch OHLCV bars for `universe` for required horizon (e.g., 400 trading days).
- Include benchmark (e.g., SPY).
- Persist raw pulls to `/data/raw/YYYY-MM-DD/*.parquet`.

**Output**
- `/data/raw/...` files.
- `meta_run.json` with run timestamp, symbols fetched, last bar date.

### 4.2 Preprocess
**Input**
- Raw bars (parquet), `config.yml` preprocessing settings (split/dividend adjust policy, missing data policy).

**Process**
- Adjust (if configured), forward-fill allowed gaps within tolerance.
- Align to trading calendar.
- Compute base indicators used downstream (e.g., SMA100, SMA200, returns, rolling highs).

**Output**
- `/data/curated/YYYY-MM-DD/*.parquet` with canonical columns:  
  `date, symbol, open, high, low, close, volume, adj_close, sma_100, sma_200, ret_1d, ret_20d, drawdown_lookback_n,...`

### 4.3 Signal Engine
**Input**
- Curated bars per symbol.
- `strategy.yml`: rule parameters (e.g., trend filter: price > SMA100; exit: close < SMA100; rank metric).

**Process**
- Evaluate entry/exit conditions for each symbol at `t_close`.
- Compute ranking features (e.g., 63d momentum, Sortino on trailing window).
- Produce discrete signals (`BUY`, `HOLD`, `EXIT`) and a numeric rank score.

**Output**
- `signals.parquet` with:  
  `date, symbol, signal, rank_score, features{...}`

### 4.4 Risk Rules (Daily Defense)
**Input**
- Curated bars, `holdings.json`, `risk.yml`:
  - Single-day crash threshold (e.g., −8%).
  - Drawdown from rolling peak (e.g., −20%).
  - Market filter (e.g., SPY < 200-DMA).

**Process**
- For each holding:
  - Compute daily return vs threshold.
  - Compute drawdown vs peak (lookback window).
- Evaluate benchmark filter.
- Tag holdings as `ALERT_{CRASH|DRAWDOWN}` and global `MARKET_RISK_ON/OFF`.

**Output**
- `risk_alerts.json` with per-symbol alerts and market flag.

### 4.5 Rebalance Decision (Weekly/Monthly)
**Input**
- `signals.parquet` (latest date).
- `holdings.json` (current qty).
- `rebalance.yml`: cadence, max positions N, min weight, equal-weight toggle, turnover cap, cash buffer.

**Process**
- If today is a rebalance day:
  - Select top N candidates by `rank_score` that pass entry rules and any market filters.
  - Construct target portfolio:
    - Equal weights among selected, enforce min weight and cash buffer.
    - Drop positions that violate exit rule.
  - Compute diff vs current positions → order intents.

**Output**
- `rebalance_proposal.json`:  
  - `targets`: `[ {symbol, target_weight} ]`  
  - `orders`: `[ {symbol, side, qty, notional_hint} ]`  
  - `rationale`: summary including rules triggered and rank cutoff.

### 4.6 Proposal & Report Generation
**Input**
- `risk_alerts.json`, `rebalance_proposal.json`, curated metrics, `holdings.json`.

**Process**
- Compile a human-readable report:
  - Portfolio snapshot: holdings, weights, P&L since cost.
  - Risk: per-symbol alerts, market filter state.
  - Actions:
    - Daily: exits due to rule breaks (if any).
    - Rebalance day: target weights and order list.
  - Backtest-expectation deltas (optional in v1).

**Output**
- `/reports/YYYY-MM-DD/daily_report.html` and `.pdf`
- Machine-readable `/reports/YYYY-MM-DD/daily_report.json`

### 4.7 Notification
**Input**
- `daily_report.*`, `config.yml` notification settings (email/Slack webhook).

**Process**
- Send link or attach report; include summary text and orders block.

**Output**
- User receives the report. End of automated pipeline.

---

## 5) Data Contracts (Schemas)

### 5.1 Bars (curated)
```json
{
  "date": "YYYY-MM-DD",
  "symbol": "AAPL",
  "open": 191.2,
  "high": 193.0,
  "low": 190.4,
  "close": 192.5,
  "volume": 53200123,
  "adj_close": 192.5,
  "sma_100": 184.3,
  "sma_200": 176.9,
  "ret_1d": 0.0042
}
```

### 5.2 Holdings
```json
{
  "as_of_date": "YYYY-MM-DD",
  "positions": [
    {"symbol": "AAPL", "qty": 100, "cost_basis": 172.1},
    {"symbol": "MSFT", "qty": 50, "cost_basis": 315.0}
  ],
  "cash": 12000.00,
  "base_ccy": "USD"
}
```

### 5.3 Signals
```json
{
  "date": "YYYY-MM-DD",
  "symbol": "AAPL",
  "signal": "BUY|HOLD|EXIT",
  "rank_score": 0.78,
  "features": {"mom_63d": 0.12, "sortino_63d": 1.6}
}
```

### 5.4 Risk Alerts
```json
{
  "date": "YYYY-MM-DD",
  "market_filter": "ON|OFF",
  "alerts": [
    {"symbol": "AAPL", "type": "CRASH|-8%", "value": -0.085},
    {"symbol": "MSFT", "type": "DRAWDOWN|-20%", "value": -0.22}
  ]
}
```

### 5.5 Rebalance Proposal
```json
{
  "date": "YYYY-MM-DD",
  "universe_size": 25,
  "selected": 8,
  "targets": [
    {"symbol": "AAPL", "target_weight": 0.125},
    {"symbol": "MSFT", "target_weight": 0.125}
  ],
  "orders": [
    {"symbol": "AAPL", "side": "BUY", "qty": 25, "notional": 4800.00},
    {"symbol": "TSLA", "side": "SELL", "qty": 10, "notional": 2500.00}
  ],
  "rationale": "Top-8 rank; market_filter=ON; exits: TSLA < SMA100"
}
```

---

## 6) Configuration (example)

`config.yml`
```yaml
base_ccy: USD
calendar: NYSE
data:
  provider: eodhd
  adjust: splits_dividends
  lookback_days: 420
universe:
  tickers: [AAPL, MSFT, NVDA, GOOGL, META, TSLA, SPY]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
  rank: "momentum_63d"
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.20
  market_filter:
    benchmark: SPY
    rule: "close > sma_200"
rebalance:
  cadence: monthly   # or weekly
  max_positions: 8
  equal_weight: true
  min_weight: 0.05
  cash_buffer: 0.05
  turnover_cap_pct: 0.35
notify:
  email: you@example.com
  slack_webhook: "https://..."
paths:
  data_raw: ./data/raw
  data_curated: ./data/curated
  reports: ./reports
```

---

## 7) Module Boundaries (SOLID-aligned)

- **DataProvider** (interface)  
  `get_bars(universe, start, end) -> DataFrame`  
  Implementations: `YahooProvider`, `EODHDProvider`, `FinnhubProvider`.

- **Preprocessor**  
  `transform(raw_df, settings) -> curated_df`  
  Single responsibility: adjustments, alignment, indicator precompute.

- **StrategyEngine** (Open/Closed for new strategies)  
  `generate_signals(curated_df, params) -> signals_df`  
  Encapsulate rules and ranking; pure, deterministic.

- **RiskEngine**  
  `evaluate(holdings, curated_df, rules) -> risk_alerts`  
  No I/O; pure evaluation.

- **Rebalancer**  
  `propose(signals_df, holdings, rules) -> proposal`  
  Strategy-agnostic capital allocator; respects turnover, weights.

- **ReportBuilder**  
  `render(inputs) -> html/pdf/json`  
  Only presentation/summary logic.

- **Notifier**  
  `send(report, channel_cfg)`.

Each class has one reason to change; external dependencies are injected via interfaces (Dependency Inversion).

---

## 8) Interfaces & CLI

### CLI commands
- `ts pull --asof 2025-09-19`
- `ts signals --asof 2025-09-19`
- `ts risk --asof 2025-09-19`
- `ts rebalance --asof 2025-09-30`
- `ts report --asof 2025-09-30 --daily|--rebalance`
- `ts notify --asof 2025-09-30`
- `ts backtest --start 2015-01-01 --end 2025-09-01`

Each command reads `config.yml`, writes artifacts to `paths.*`.

### Scheduler
- Cron or GitHub Actions run:
  - Daily: `pull → preprocess → signals → risk → report → notify`
  - Rebalance days: add `rebalance` before `report`.

---

## 9) Non-Functional Requirements

- **Determinism & Reproducibility**: Same inputs → same outputs; pin library versions.
- **Performance**: Daily pipeline completes < 2 minutes for 200 symbols on a laptop.
- **Observability**: Structured logs per step; timings; artifact manifest.
- **Failure Visibility**: Fail fast with clear error; do not auto-retry inside a step.
- **Idempotency**: Re-running creates identical outputs for same `--asof`.
- **Config-Driven**: No hardcoded parameters; strategy lives in config + code.

---

## 10) Testing

- **Unit**:  
  - Preprocessor transforms with golden inputs → golden outputs.  
  - Strategy rules on synthetic series (known crosses, known exits).  
  - Risk thresholds on synthetic shocks.

- **Integration**:  
  - End-to-end daily pipeline over a short historical window.  
  - Backtest runs with fixed seed produce stable metrics.

- **Regression**:  
  - Lock expected metrics for a fixture universe; alert on drift.

---

## 11) Manual Responsibilities (v1)

- Review daily report and risk alerts.
- Approve/adjust weekly/monthly proposals.
- Place trades at broker; update `holdings.json` snapshot if desired.
- Parameter review on monthly cadence; update `config.yml`.

---

## 12) Roadmap (not in v1)

- Auto-execution adapter behind `BrokerAdapter` interface (Alpaca/IBKR).
- Position sizing beyond equal-weight (vol targeting).
- Multi-strategy portfolio and correlation-aware allocator.
- Better data (point-in-time constituents, survivorship-free validation).

---
