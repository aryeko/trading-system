# Trading System

[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Research and alerting toolkit for a mid/long-horizon discretionary trading workflow. The
project follows the process and requirements described in [`docs/WORKFLOW.md`](docs/WORKFLOW.md)
and [`docs/TECH_DESIGN_REQUIREMENTS.md`](docs/TECH_DESIGN_REQUIREMENTS.md).

## Project Layout

```
.
├── configs/              # Environment, universe, and strategy configuration assets
├── data/                 # Raw and curated market data (gitignored, see .gitkeep markers)
│   ├── raw/
│   └── curated/
├── docs/                 # Design docs, requirements, and story backlog
├── reports/              # Generated HTML/PDF reports (gitignored)
├── scripts/              # Operational scripts and automation entry points
├── src/trading_system/   # Application source code
├── stories/              # Convenience pointers back to docs/stories
└── tests/                # Pytest suite
```

## Getting Started

1. Install dependencies using Poetry (Python 3.13+ required):
   ```bash
   poetry install
   ```
2. Activate the in-project virtual environment:
   ```bash
   source .venv/bin/activate
   ```
3. Run the command line interface:
   ```bash
   poetry run ts --help
   ```
4. Execute the automated tests:
   ```bash
   poetry run tests
   ```

## CLI Highlights

Commands default the as-of option (pass via `--asof`/`--as-of` or `TS_ASOF`) to today's date, so you can omit it for
"run it now" workflows.

- `poetry run ts signals build --config configs/sample-config.yml --as-of YYYY-MM-DD`
  generates a `signals.parquet` artifact with rule outcomes and rank scores.
- `poetry run ts signals explain --config configs/sample-config.yml --symbol AAPL --as-of YYYY-MM-DD`
  prints the entry/exit evaluation details and key indicator values for a symbol.
- `poetry run ts risk evaluate --config configs/sample-config.yml --holdings data/holdings.json --as-of YYYY-MM-DD`
  emits `risk_alerts.json` summarizing crash/drawdown alerts and market filter state.
- `poetry run ts risk explain --config configs/sample-config.yml --holdings data/holdings.json --symbol AAPL --as-of YYYY-MM-DD`
  surfaces detailed metrics supporting each alert for the selected holding.
- `poetry run ts report build --config configs/sample-config.yml --holdings data/holdings.json --as-of YYYY-MM-DD`
  compiles HTML/JSON daily operator reports (add `--include-pdf` to request PDF output).
- `poetry run ts report preview --config configs/sample-config.yml --holdings data/holdings.json --as-of YYYY-MM-DD --open`
  regenerates the report and optionally opens the HTML for manual QA.
- `poetry run ts notify preview --config configs/sample-config.yml --as-of YYYY-MM-DD --channel slack`
  prints the Slack payload for the existing daily report without sending it.
- `poetry run ts notify send --config configs/sample-config.yml --as-of YYYY-MM-DD --channel all`
  delivers email and Slack notifications (use `--dry-run` to render without sending).
- `poetry run ts steps`
  enumerates the pipeline stages with artifact expectations.
- `poetry run ts run daily --config configs/sample-config.yml --holdings data/holdings.json --asof YYYY-MM-DD`
  orchestrates pull → preprocess → risk → report → notify with structured logging.
- `poetry run ts run rebalance --config configs/sample-config.yml --holdings data/holdings.json --asof YYYY-MM-DD --force`
  runs the full pipeline including signals/rebalance even when cadence is overriden.
- `poetry run ts observability manifest --run reports/YYYY-MM-DD`
  validates artifact hashes, sizes, and row counts while printing a summary table.
- `poetry run ts observability tail --run reports/YYYY-MM-DD`
  streams the structured JSON logs captured during the pipeline run.
- `poetry run ts backtest run --config configs/sample-config.yml --start YYYY-MM-DD --end YYYY-MM-DD --output reports/backtests/demo --label smoke`
  executes the deterministic backtest engine and writes metrics, equity curve, trade log, and optional Plotly HTML chart.
- `poetry run ts backtest compare --baseline reports/backtests/base --candidate reports/backtests/experiment`
  highlights metric deltas across backtest scenarios.

### Handy Automation Commands

All local automation lives under Poetry scripts:

- `poetry run lint` — Ruff lint + Black check
- `poetry run fmt` — apply Black formatting
- `poetry run typecheck` — mypy static analysis
- `poetry run tests` — pytest suite
- `poetry run ci` — run lint, typecheck, and tests sequentially

## Tooling

- Formatting: `black`
- Linting: `ruff`
- Static typing: `mypy`
- Testing: `pytest` & `pytest-cov`

CI workflows execute the same toolchain to keep `main` green.

### Notification Setup

Email delivery expects the following environment variables:

- `TS_EMAIL_SENDER` — From address used in the outgoing message.
- `TS_SMTP_HOST` — SMTP server hostname.
- `TS_SMTP_PORT` — SMTP port (default 587).
- `TS_SMTP_USERNAME`/`TS_SMTP_PASSWORD` — Optional credentials for authenticated servers.
- `TS_SMTP_STARTTLS` — Set to `false` to disable STARTTLS (enabled by default).

Slack notifications require `config.notify.slack_webhook` to point at an incoming webhook URL. Run
`poetry run ts notify send --dry-run ...` during setup to validate formatting without hitting external services.

## License

Released under the MIT License. See [LICENSE](./LICENSE) for details.
