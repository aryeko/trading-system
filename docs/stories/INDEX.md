| ID | Title | Description | Dependencies |
|----|-------|-------------|--------------|
| S-00 | Repository Setup and Tooling | Create GitHub repo, Poetry env, CI, and baseline structure. | — |
| S-01 | Project Scaffolding, Config Loader, and Paths | Load config, create canonical directories, expose Config object. | — |
| S-02 | DataProvider Interface + Yahoo/EODHD Adapter (Mockable) | Interface and at least one adapter; persist raw pulls and meta. | S-01 |
| S-03 | Preprocessor Module | Transform raw bars to curated dataset with indicators. | S-01 |
| S-04 | StrategyEngine (Rules + Ranking) | Evaluate entry/exit and rank candidates deterministically. | S-01,S-03 |
| S-05 | RiskEngine (Daily Defense) | Crash/drawdown alerts and market filter. | S-01,S-03 |
| S-06 | Rebalancer (Portfolio Construction) | Build target weights, apply turnover and cash buffer, output orders. | S-01,S-04 |
| S-07 | ReportBuilder (HTML/PDF/JSON) | Compile daily report and optional rebalance section. | S-01,S-05,S-06 |
| S-08 | Notifier (Email/Slack) | Deliver report link or attachment. | S-07 |
| S-09 | CLI Orchestrator | Step commands and composed daily pipeline. | S-01, wraps S-02..S-08 |
| S-10 | Scheduler Integration (Cron/GitHub Actions) | Automate daily and rebalance-day runs. | S-09 |
| S-11 | Backtest Engine (Deterministic) | Historical backtests using same engines; simple cost model. | S-03,S-04,S-06 |
| S-12 | Observability & Artifact Manifest | Structured logs, timings, manifest per run. | S-01 |
| S-13 | Golden Fixtures & Regression Tests | Fixtures and expected outputs; CI regression. | S-03..S-07 |
| S-14 | Holdings Snapshot Tooling | Import CSV to holdings.json per schema. | S-01 |
