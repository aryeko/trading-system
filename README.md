# Trading System
*Automated portfolio operations to keep capital protected and opportunities in play.*

[![CI](https://github.com/aryeko/trading-system/actions/workflows/ci.yml/badge.svg)](https://github.com/aryeko/trading-system/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Python Version](https://img.shields.io/badge/Python-3.13-blue.svg)
![Code Style: Black](https://img.shields.io/badge/Code%20Style-Black-000000.svg)

## Overview
Guard portfolio capital daily. Propose improvements weekly/monthly.

## Features
- **Capital protection** with daily crash and drawdown checks.
- **Portfolio reports** in HTML and JSON formats.
- **Signals & rebalance proposals** for proactive adjustments.
- **Backtests** to validate strategies before deployment.
- **Point-in-time data** for reproducible analysis.
- **CLI orchestration** to coordinate daily and scheduled runs.
- **Notifications** through Slack and email channels.

## Quickstart

```bash
git clone https://github.com/aryeko/trading-system
cd trading-system
poetry install

poetry run ts run daily --config data/sample.yaml --holdings data/sample-holdings.json --dry-run
```

Sample onboarding assets live under `data/`. Update the universe, holdings snapshot, and notification settings before running against live capital.

## Usage Examples

```bash
# Evaluate portfolio risk with point-in-time context
poetry run ts risk evaluate --config data/sample.yaml --holdings data/sample-holdings.json

# Execute the full daily pipeline with rebalance proposals
poetry run ts run rebalance --config data/sample.yaml --holdings data/sample-holdings.json --dry-run

# Run a historical backtest across a custom period
poetry run ts backtest run --config data/sample.yaml --start 2024-01-01 --end 2024-06-30 --output reports/backtests/demo
```

## Documentation
Comprehensive setup guides, operational workflows, and configuration references live in the [Wiki](https://github.com/aryeko/trading-system/wiki). For the original workflow contract and design requirements, see [`docs/WORKFLOW.md`](docs/WORKFLOW.md) and [`docs/TECH_DESIGN_REQUIREMENTS.md`](docs/TECH_DESIGN_REQUIREMENTS.md).

## Roadmap
- ✅ Daily capital protection checks
- ✅ Automated HTML/JSON reporting
- ✅ CLI orchestration workflows
- 🔲 Expanded broker integrations
- 🔲 Enhanced scenario backtesting library
- 🔲 Additional notification channels and templates

Track ongoing initiatives on the [GitHub Project board](https://github.com/users/aryeko/projects/2).

## Contributing
We welcome collaboration! Please review [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request.

## Disclaimer

This project is for **educational use only** and does not provide financial advice.  
See [DISCLAIMER.md](DISCLAIMER.md) for full details.

## License
Released under the [MIT License](LICENSE).

## Acknowledgements
- Built with a focus on operator experience and repeatable portfolio governance.
