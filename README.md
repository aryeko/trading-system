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
   poetry run pytest
   ```

## Tooling

- Formatting: `black`
- Linting: `ruff`
- Static typing: `mypy`
- Testing: `pytest` & `pytest-cov`

CI workflows execute the same toolchain to keep `main` green.
