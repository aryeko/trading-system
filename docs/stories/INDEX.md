| ID | Title | Description | Dependencies |
|----|-------|-------------|--------------|
| S-00 | Repository Setup and Tooling | Bootstrap Poetry project, automation, and CLI skeleton with doctor command. | — |
| S-01 | Configuration Loader and Path Contracts | Typed config models, directory preparation, and config inspection CLI. | S-00 |
| S-02 | Data Provider Layer and Raw Persistence | Provider interface, Yahoo adapter, raw data CLI, and atomic persistence. | S-01 |
| S-03 | Preprocessing Pipeline | Curated dataset builder with indicators and CLI preprocessing command. | S-01,S-02 |
| S-04 | Strategy Engine (Signals + Ranking) | Rule evaluation, ranking engine, and signals CLI. | S-01,S-03 |
| S-05 | Risk Engine (Daily Defense) | Crash/drawdown alerts, market filter, and risk CLI. | S-01,S-03 |
| S-06 | Rebalancer (Portfolio Construction) | Target weights, orders, and rebalance CLI. | S-01,S-03,S-04,S-05 |
| S-07 | Report Builder (HTML/PDF/JSON) | Multi-format daily report generation CLI. | S-01,S-05,S-06 |
| S-08 | Notification Channels (Email/Slack) | Deliver reports through configured channels via CLI. | S-01,S-07 |
| S-09 | CLI Orchestrator | Unified pipeline commands and composite daily/rebalance runs. | S-02,S-03,S-04,S-05,S-06,S-07,S-08 |
| S-10 | Scheduler Integration (Cron / GitHub Actions) | Declarative schedules for automated runs with planner CLI. | S-09 |
| S-11 | Backtest Engine (Deterministic) | Reuse live logic for historical evaluation with CLI tooling. | S-03,S-04,S-06 |
| S-12 | Observability and Artifact Manifest | Structured logs, manifest generation, and observability CLI. | S-09 |
| S-13 | Golden Fixtures and Regression Suite | Deterministic fixtures, diff tooling, and regression CLI. | S-03,S-04,S-05,S-06,S-07,S-08,S-09,S-12 |
| S-14 | Holdings Snapshot Toolkit | Import broker CSVs into holdings.json with validation CLI. | S-01 |
