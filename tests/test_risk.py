import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pandas as pd
from typer.testing import CliRunner

from trading_system.cli import app
from trading_system.config import load_config
from trading_system.risk import RiskEngine, load_holdings

runner = CliRunner()

RISK_CONFIG = """
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
  market_filter:
    benchmark: SPY
    rule: "close > sma_200"
rebalance:
  cadence: monthly
  max_positions: 5
notify:
  email: ops@example.com
paths:
  data_raw: data/raw
  data_curated: data/curated
  reports: reports
"""


def _write_config(tmp_path: Path, tickers: list[str]) -> Path:
    config_text = RISK_CONFIG.format(tickers=", ".join(tickers))
    config_path = tmp_path / "config.yml"
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


def _make_curated_frame(
    dates: pd.DatetimeIndex,
    symbol: str,
    closes: npt.NDArray[np.float_],
) -> pd.DataFrame:
    series = pd.Series(closes, index=dates, dtype=float)
    values = series.to_numpy(copy=True)
    frame = pd.DataFrame(
        {
            "date": dates,
            "symbol": symbol,
            "open": values,
            "high": values,
            "low": values,
            "close": values,
            "volume": np.full(len(series), 1_000, dtype=float),
            "adj_close": values,
            "sma_100": series.rolling(100, min_periods=1).mean().to_numpy(),
            "sma_200": series.rolling(200, min_periods=1).mean().to_numpy(),
            "ret_1d": series.pct_change().fillna(0.0).to_numpy(),
            "ret_20d": series.pct_change(20).fillna(0.0).to_numpy(),
            "rolling_peak": series.cummax().to_numpy(),
        }
    )
    return frame


def _write_curated(
    config_path: Path, as_of: pd.Timestamp, frames: dict[str, pd.DataFrame]
) -> Path:
    config = load_config(config_path)
    curated_dir = config.paths.data_curated / as_of.strftime("%Y-%m-%d")
    curated_dir.mkdir(parents=True, exist_ok=True)
    for symbol, frame in frames.items():
        frame.to_parquet(curated_dir / f"{symbol}.parquet", index=False)
    return curated_dir


def _write_holdings(tmp_path: Path, positions: list[dict[str, object]]) -> Path:
    holdings_path = tmp_path / "holdings.json"
    payload = {
        "as_of_date": "2024-05-02",
        "positions": positions,
        "cash": 10000.0,
        "base_ccy": "USD",
    }
    holdings_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return holdings_path


def test_risk_engine_generates_alerts_and_json(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, ["AAPL", "MSFT", "SPY"])
    as_of = pd.Timestamp("2024-05-02")
    dates = pd.bdate_range(end=as_of, periods=10)

    aapl_closes = np.array([100, 105, 110, 108, 102, 97, 95, 90, 88, 80], dtype=float)
    msft_closes = np.array([50, 51, 52, 53, 54, 55, 56, 57, 58, 59], dtype=float)
    spy_closes = np.array(
        [400, 401, 402, 399, 398, 397, 396, 395, 394, 393], dtype=float
    )

    aapl_frame = _make_curated_frame(dates, "AAPL", aapl_closes)
    msft_frame = _make_curated_frame(dates, "MSFT", msft_closes)
    spy_frame = _make_curated_frame(dates, "SPY", spy_closes)
    spy_frame["sma_200"] = 405.0  # ensure market filter fails

    _write_curated(
        config_path,
        as_of,
        {"AAPL": aapl_frame, "MSFT": msft_frame, "SPY": spy_frame},
    )

    holdings_path = _write_holdings(
        tmp_path,
        [
            {"symbol": "AAPL", "qty": 10, "cost_basis": 120.0},
            {"symbol": "MSFT", "qty": 5, "cost_basis": 45.0},
        ],
    )

    config = load_config(config_path)
    holdings = load_holdings(holdings_path)

    def fixed_clock() -> datetime:
        return datetime(2024, 5, 2, 22, tzinfo=UTC)

    engine = RiskEngine(config, clock=fixed_clock)

    result = engine.build(as_of, holdings, dry_run=False)

    assert result.market_state == "RISK_OFF"
    assert result.output_path is not None
    assert result.output_path.exists()

    alerts = {(alert.symbol, alert.alert_type) for alert in result.alerts}
    assert ("AAPL", "CRASH") in alerts
    assert ("AAPL", "DRAWDOWN") in alerts

    payload = json.loads(result.output_path.read_text(encoding="utf-8"))
    assert payload["market_state"] == "RISK_OFF"
    assert len(payload["alerts"]) == 2
    assert payload["alerts"][0]["symbol"] == "AAPL"

    aapl_eval = result.evaluations["AAPL"]
    assert aapl_eval.crash_triggered is True
    assert aapl_eval.drawdown_triggered is True
    assert aapl_eval.drawdown <= -0.2


def test_risk_cli_commands_run(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, ["AAPL", "SPY"])
    as_of = pd.Timestamp("2024-05-02")
    dates = pd.bdate_range(end=as_of, periods=6)

    aapl_closes = np.array([90, 92, 91, 85, 83, 75], dtype=float)
    spy_closes = np.array([300, 299, 298, 297, 296, 295], dtype=float)

    aapl_frame = _make_curated_frame(dates, "AAPL", aapl_closes)
    spy_frame = _make_curated_frame(dates, "SPY", spy_closes)
    spy_frame["sma_200"] = 305.0

    _write_curated(config_path, as_of, {"AAPL": aapl_frame, "SPY": spy_frame})

    holdings_path = _write_holdings(
        tmp_path, [{"symbol": "AAPL", "qty": 12, "cost_basis": 95.0}]
    )

    result = runner.invoke(
        app,
        [
            "risk",
            "evaluate",
            "--config",
            str(config_path),
            "--holdings",
            str(holdings_path),
            "--as-of",
            "2024-05-02",
        ],
    )

    assert result.exit_code == 0
    assert "Risk alerts written" in result.stdout

    alerts_path = tmp_path / "reports" / "2024-05-02" / "risk_alerts.json"
    assert alerts_path.exists()

    explain = runner.invoke(
        app,
        [
            "risk",
            "explain",
            "--config",
            str(config_path),
            "--holdings",
            str(holdings_path),
            "--symbol",
            "AAPL",
            "--as-of",
            "2024-05-02",
        ],
    )

    assert explain.exit_code == 0
    assert "daily_return" in explain.stdout
