# Trading System

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

1. Install dependencies using Poetry (Python 3.11+ required):
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
