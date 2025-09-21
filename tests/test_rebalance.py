import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from trading_system.cli import app
from trading_system.config import load_config
from trading_system.rebalance import RebalanceEngine
from trading_system.risk import load_holdings

runner = CliRunner()

CONFIG_TEMPLATE = """
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 30
universe:
  tickers: [{tickers}]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
  rank: momentum_63d
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.20
rebalance:
  cadence: monthly
  max_positions: 3
  equal_weight: true
  min_weight: 0.1
  cash_buffer: 0.1
  turnover_cap_pct: 0.40
notify:
  email: ops@example.com
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


REBALANCE_CONFIG_TURNOVER = """
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 30
universe:
  tickers: [{tickers}]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
  rank: momentum_63d
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.20
rebalance:
  cadence: monthly
  max_positions: 2
  equal_weight: false
  min_weight: 0.05
  cash_buffer: 0.1
  turnover_cap_pct: 0.05
notify:
  email: ops@example.com
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


SYMBOLS = ["AAPL", "MSFT", "NVDA"]


def _write_config(tmp_path: Path, *, template: str = CONFIG_TEMPLATE) -> Path:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        template.format(tickers=", ".join(SYMBOLS)), encoding="utf-8"
    )
    return config_path


def _write_curated(
    config_path: Path, as_of: pd.Timestamp, prices: dict[str, float]
) -> None:
    config = load_config(config_path)
    curated_dir = config.paths.data_curated / as_of.strftime("%Y-%m-%d")
    curated_dir.mkdir(parents=True, exist_ok=True)
    for symbol, price in prices.items():
        frame = pd.DataFrame(
            {
                "date": [as_of],
                "symbol": [symbol],
                "close": [price],
            }
        )
        frame.to_parquet(curated_dir / f"{symbol}.parquet", index=False)


def _write_holdings(
    tmp_path: Path, positions: list[dict[str, object]], *, cash: float = 0.0
) -> Path:
    holdings_path = tmp_path / "holdings.json"
    payload = {
        "as_of_date": "2024-05-31",
        "positions": positions,
        "cash": cash,
        "base_ccy": "USD",
    }
    holdings_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return holdings_path


def _make_signals(
    as_of: pd.Timestamp, rows: list[tuple[str, str, float]]
) -> pd.DataFrame:
    data = {
        "date": [as_of for _ in rows],
        "symbol": [symbol for symbol, _, _ in rows],
        "signal": [signal for _, signal, _ in rows],
        "rank_score": [score for _, _, score in rows],
    }
    return pd.DataFrame(data)


def test_rebalance_engine_generates_targets_and_orders(tmp_path: Path) -> None:
    as_of = pd.Timestamp("2024-05-31")
    config_path = _write_config(tmp_path)
    _write_curated(config_path, as_of, {"AAPL": 150.0, "MSFT": 200.0, "NVDA": 300.0})
    holdings_path = _write_holdings(
        tmp_path,
        [
            {"symbol": "AAPL", "qty": 10, "cost_basis": 120.0},
            {"symbol": "MSFT", "qty": 5, "cost_basis": 180.0},
        ],
        cash=1000.0,
    )

    config = load_config(config_path)
    holdings = load_holdings(holdings_path)
    signals = _make_signals(
        as_of,
        [
            ("AAPL", "HOLD", 0.6),
            ("MSFT", "EXIT", 0.2),
            ("NVDA", "BUY", 0.9),
        ],
    )

    engine = RebalanceEngine(config)
    result = engine.evaluate(as_of, holdings=holdings, signals=signals)

    assert result.status == "REBALANCE"
    weights = {target.symbol: target.target_weight for target in result.targets}
    assert pytest.approx(weights.get("AAPL", 0.0), rel=1e-6) == 0.45
    assert pytest.approx(weights.get("NVDA", 0.0), rel=1e-6) == 0.45
    assert weights.get("MSFT") == 0.0

    orders = {order.symbol: order for order in result.orders}
    assert orders["MSFT"].side == "SELL"
    assert orders["NVDA"].side == "BUY"
    assert result.turnover <= 0.400001


def test_rebalance_engine_enforces_cadence(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    holdings_path = _write_holdings(tmp_path, [])
    config = load_config(config_path)
    holdings = load_holdings(holdings_path)
    as_of = pd.Timestamp("2024-05-30")
    signals = _make_signals(
        as_of,
        [
            ("AAPL", "HOLD", 0.5),
        ],
    )

    engine = RebalanceEngine(config)
    result = engine.evaluate(as_of, holdings=holdings, signals=signals)

    assert result.status == "NO_REBALANCE"
    assert not result.orders
    assert result.notes and "Cadence" in result.notes[0]


def test_rebalance_engine_turnover_cap_limits_new_positions(tmp_path: Path) -> None:
    as_of = pd.Timestamp("2024-05-31")
    config_path = _write_config(tmp_path, template=REBALANCE_CONFIG_TURNOVER)
    _write_curated(config_path, as_of, {"AAPL": 100.0, "NVDA": 300.0})
    holdings_path = _write_holdings(
        tmp_path,
        [
            {"symbol": "AAPL", "qty": 10, "cost_basis": 80.0},
        ],
    )
    config = load_config(config_path)
    holdings = load_holdings(holdings_path)
    signals = _make_signals(
        as_of,
        [
            ("AAPL", "HOLD", 0.2),
            ("NVDA", "BUY", 0.9),
        ],
    )

    engine = RebalanceEngine(config)
    result = engine.evaluate(as_of, holdings=holdings, signals=signals)

    assert result.status == "REBALANCE"
    assert result.turnover <= 0.050001
    symbols = [target.symbol for target in result.targets if target.target_weight > 0]
    assert symbols == ["AAPL"]
    assert any("turnover" in note.lower() for note in result.notes)


def test_rebalance_cli_propose_writes_artifact(tmp_path: Path) -> None:
    as_of = pd.Timestamp("2024-05-31")
    config_path = _write_config(tmp_path)
    _write_curated(config_path, as_of, {"AAPL": 150.0, "MSFT": 200.0})
    holdings_path = _write_holdings(
        tmp_path,
        [
            {"symbol": "AAPL", "qty": 10, "cost_basis": 120.0},
            {"symbol": "MSFT", "qty": 5, "cost_basis": 180.0},
        ],
    )
    signals = _make_signals(
        as_of,
        [
            ("AAPL", "HOLD", 0.6),
            ("MSFT", "EXIT", 0.2),
        ],
    )
    signals_path = tmp_path / "signals.parquet"
    signals.to_parquet(signals_path, index=False)

    result = runner.invoke(
        app,
        [
            "rebalance",
            "propose",
            "--config",
            str(config_path),
            "--holdings",
            str(holdings_path),
            "--as-of",
            as_of.strftime("%Y-%m-%d"),
            "--signals",
            str(signals_path),
        ],
    )

    assert result.exit_code == 0
    proposal_path = (
        tmp_path / "reports" / as_of.strftime("%Y-%m-%d") / "rebalance_proposal.json"
    )
    assert proposal_path.exists()
    payload = json.loads(proposal_path.read_text(encoding="utf-8"))
    assert payload["status"] == "REBALANCE"
    assert payload["orders"]


def test_rebalance_cli_dry_run_reports_status(tmp_path: Path) -> None:
    as_of = pd.Timestamp("2024-05-30")
    config_path = _write_config(tmp_path)
    holdings_path = _write_holdings(tmp_path, [])
    signals = _make_signals(
        as_of,
        [
            ("AAPL", "HOLD", 0.5),
        ],
    )
    signals_path = tmp_path / "signals.parquet"
    signals.to_parquet(signals_path, index=False)

    result = runner.invoke(
        app,
        [
            "rebalance",
            "dry-run",
            "--config",
            str(config_path),
            "--holdings",
            str(holdings_path),
            "--as-of",
            as_of.strftime("%Y-%m-%d"),
            "--signals",
            str(signals_path),
            "--max-candidates",
            "0",
        ],
    )

    assert result.exit_code == 0
    assert "NO_REBALANCE" in result.stdout
