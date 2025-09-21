import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from trading_system.cli import app
from trading_system.config import load_config
from trading_system.report import ReportBuilder
from trading_system.risk import load_holdings

runner = CliRunner()


CONFIG_TEXT = """
base_ccy: USD
calendar: NYSE
data:
  provider: yahoo
  lookback_days: 30
universe:
  tickers: [AAPL, MSFT]
strategy:
  type: trend_follow
  entry: "close > sma_100"
  exit: "close < sma_100"
risk:
  crash_threshold_pct: -0.08
  drawdown_threshold_pct: -0.20
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


RISK_PAYLOAD = {
    "date": "2024-05-31",
    "market_state": "RISK_ON",
    "alerts": [
        {
            "symbol": "MSFT",
            "type": "CRASH",
            "value": -0.09,
            "threshold": -0.08,
            "reason": "Daily return breached crash threshold",
        }
    ],
    "market_filter": {
        "benchmark": "SPY",
        "passed": True,
        "rule": "close > sma_200",
    },
}


PROPOSAL_PAYLOAD = {
    "date": "2024-05-31",
    "status": "REBALANCE",
    "turnover": 0.12,
    "cash_buffer": 0.1,
    "targets": [
        {"symbol": "AAPL", "target_weight": 0.5},
        {"symbol": "MSFT", "target_weight": 0.5},
    ],
    "orders": [
        {"symbol": "AAPL", "side": "BUY", "quantity": 5, "notional": 750.0},
    ],
    "notes": ["Sample note"],
}


AS_OF = pd.Timestamp("2024-05-31")


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yml"
    config_path.write_text(CONFIG_TEXT, encoding="utf-8")
    return config_path


def _write_curated(config_path: Path, prices: dict[str, float]) -> None:
    config = load_config(config_path)
    curated_dir = config.paths.data_curated / AS_OF.strftime("%Y-%m-%d")
    curated_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.bdate_range(AS_OF - pd.tseries.offsets.BDay(80), AS_OF)
    for symbol, close_price in prices.items():
        start_value = close_price - len(dates) + 1
        base = pd.Series([start_value + idx for idx in range(len(dates))], index=dates)
        frame = pd.DataFrame(
            {
                "date": dates,
                "symbol": symbol,
                "close": base.values,
            }
        )
        frame["ret_1d"] = frame["close"].pct_change().fillna(0.0)
        frame["ret_20d"] = frame["close"].pct_change(20).fillna(0.0)
        frame["rolling_peak"] = frame["close"].cummax()
        frame.to_parquet(curated_dir / f"{symbol}.parquet", index=False)


def _write_holdings(tmp_path: Path) -> Path:
    holdings_path = tmp_path / "holdings.json"
    payload = {
        "as_of_date": AS_OF.strftime("%Y-%m-%d"),
        "positions": [
            {"symbol": "AAPL", "qty": 10, "cost_basis": 120.0},
            {"symbol": "MSFT", "qty": 5, "cost_basis": 150.0},
        ],
        "cash": 500.0,
        "base_ccy": "USD",
    }
    holdings_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return holdings_path


def _write_risk(tmp_path: Path) -> Path:
    risk_path = tmp_path / "risk_alerts.json"
    risk_path.write_text(json.dumps(RISK_PAYLOAD, indent=2), encoding="utf-8")
    return risk_path


def _write_proposal(tmp_path: Path) -> Path:
    proposal_path = tmp_path / "rebalance_proposal.json"
    proposal_path.write_text(json.dumps(PROPOSAL_PAYLOAD, indent=2), encoding="utf-8")
    return proposal_path


def _write_signals(tmp_path: Path) -> Path:
    signals = pd.DataFrame(
        {
            "date": [AS_OF, AS_OF],
            "symbol": ["AAPL", "MSFT"],
            "signal": ["BUY", "HOLD"],
            "rank_score": [0.9, 0.6],
        }
    )
    path = tmp_path / "signals.parquet"
    signals.to_parquet(path, index=False)
    return path


def test_report_builder_writes_artifacts(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    _write_curated(config_path, {"AAPL": 175.0, "MSFT": 320.0})
    risk_path = _write_risk(tmp_path)
    proposal_path = _write_proposal(tmp_path)
    signals_path = _write_signals(tmp_path)
    holdings_path = _write_holdings(tmp_path)

    config = load_config(config_path)
    holdings = load_holdings(holdings_path)

    def fake_renderer(content: str, output_path: Path) -> tuple[bool, str | None]:
        output_path.write_text("pdf", encoding="utf-8")
        return True, None

    builder = ReportBuilder(config, pdf_renderer=fake_renderer)
    result = builder.build(
        AS_OF,
        holdings=holdings,
        holdings_path=holdings_path,
        risk_payload=RISK_PAYLOAD,
        risk_path=risk_path,
        proposal_payload=PROPOSAL_PAYLOAD,
        proposal_path=proposal_path,
        signals=pd.read_parquet(signals_path),
        signals_path=signals_path,
        include_pdf=True,
    )

    assert result.html_path and result.html_path.exists()
    assert result.json_path and result.json_path.exists()
    assert result.pdf_path and result.pdf_path.exists()
    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["actions"]["status"] == "REBALANCE"
    assert "curated::AAPL" in payload["manifest"]
    html = result.html_path.read_text(encoding="utf-8")
    assert "Daily Operations Report" in html


def test_report_cli_build_handles_missing_artifacts(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    _write_curated(config_path, {"AAPL": 170.0, "MSFT": 260.0})
    holdings_path = _write_holdings(tmp_path)
    signals_path = _write_signals(tmp_path)

    result = runner.invoke(
        app,
        [
            "report",
            "build",
            "--config",
            str(config_path),
            "--holdings",
            str(holdings_path),
            "--as-of",
            AS_OF.strftime("%Y-%m-%d"),
            "--signals",
            str(signals_path),
            "--include-pdf",
        ],
    )

    assert result.exit_code == 0
    report_dir = tmp_path / "reports" / AS_OF.strftime("%Y-%m-%d")
    html_path = report_dir / "daily_report.html"
    json_path = report_dir / "daily_report.json"
    assert html_path.exists()
    assert json_path.exists()
    assert "Risk alerts artifact missing" in result.stdout
    assert "PDF not generated" in result.stdout


def test_report_cli_preview_opens_browser(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    opened: list[str] = []

    def fake_open(url: str) -> bool:
        opened.append(url)
        return True

    monkeypatch.setattr("trading_system.cli.webbrowser.open", fake_open)

    config_path = _write_config(tmp_path)
    _write_curated(config_path, {"AAPL": 165.0, "MSFT": 255.0})
    holdings_path = _write_holdings(tmp_path)
    signals_path = _write_signals(tmp_path)

    result = runner.invoke(
        app,
        [
            "report",
            "preview",
            "--config",
            str(config_path),
            "--holdings",
            str(holdings_path),
            "--as-of",
            AS_OF.strftime("%Y-%m-%d"),
            "--signals",
            str(signals_path),
            "--open",
        ],
    )

    assert result.exit_code == 0
    assert opened and opened[0].startswith("file:")
