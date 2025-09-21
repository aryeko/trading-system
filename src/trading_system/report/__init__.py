"""Report builder producing daily operator artifacts."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from jinja2 import DictLoader, Environment, Template, select_autoescape

from trading_system.config import Config
from trading_system.risk import HoldingsSnapshot

logger = logging.getLogger(__name__)


DEFAULT_TEMPLATE = """<!DOCTYPE html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <title>Daily Report - {{ as_of }}</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 1.5rem; color: #222; }
      h1, h2, h3 { color: #0b3d91; }
      table { border-collapse: collapse; width: 100%; margin-bottom: 1.5rem; }
      th, td { border: 1px solid #ddd; padding: 0.5rem; text-align: left; }
      th { background-color: #f5f5f5; }
      .muted { color: #666; }
      .tag { display: inline-block; padding: 0.2rem 0.4rem; border-radius: 0.3rem; font-size: 0.85rem; }
      .tag.on { background-color: #e0f7ec; color: #137333; }
      .tag.off { background-color: #fdecea; color: #b00020; }
      .notes { margin-top: 1rem; font-size: 0.9rem; }
    </style>
  </head>
  <body>
    <header>
      <h1>Daily Operations Report</h1>
      <p class=\"muted\">As of {{ as_of }} &mdash; generated at {{ generated_at }} ({{ base_currency }} base)</p>
    </header>

    <section>
      <h2>Portfolio Snapshot</h2>
      <p>Total value: <strong>{{ portfolio.value | currency(base_currency) }}</strong> &mdash; Cash: {{ portfolio.cash | currency(base_currency) }} &mdash; Invested: {{ portfolio.invested | currency(base_currency) }}</p>
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Quantity</th>
            <th>Price</th>
            <th>Value</th>
            <th>Weight</th>
            <th>Cost Basis</th>
            <th>Unrealized</th>
            <th>Return</th>
            <th>20d Return</th>
          </tr>
        </thead>
        <tbody>
          {% if portfolio.positions %}
          {% for position in portfolio.positions %}
          <tr>
            <td>{{ position.symbol }}</td>
            <td>{{ position.quantity | number }}</td>
            <td>{{ position.price | currency(base_currency) }}</td>
            <td>{{ position.value | currency(base_currency) }}</td>
            <td>{{ position.weight | percent }}</td>
            <td>{{ position.cost_basis | currency(base_currency) }}</td>
            <td>{{ position.unrealized | currency(base_currency) }}</td>
            <td>{{ position.unrealized_pct | percent }}</td>
            <td>{{ position.ret_20d | percent }}</td>
          </tr>
          {% endfor %}
          {% else %}
          <tr><td colspan=\"9\" class=\"muted\">No open positions.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </section>

    <section>
      <h2>Risk Summary</h2>
      {% if risk %}
      <p>Market state: <span class=\"tag {{ 'on' if risk.market_state == 'RISK_ON' else 'off' }}\">{{ risk.market_state }}</span></p>
      {% if risk.benchmark %}
      <p class=\"muted\">Benchmark {{ risk.benchmark }} rule: {{ risk.rule or 'n/a' }} &mdash; Passed: {{ risk.passed }}</p>
      {% endif %}
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Type</th>
            <th>Value</th>
            <th>Threshold</th>
            <th>Reason</th>
          </tr>
        </thead>
        <tbody>
          {% if risk.alerts %}
          {% for alert in risk.alerts %}
          <tr>
            <td>{{ alert.symbol }}</td>
            <td>{{ alert.type }}</td>
            <td>{{ alert.value | number }}</td>
            <td>{{ alert.threshold | number }}</td>
            <td>{{ alert.reason }}</td>
          </tr>
          {% endfor %}
          {% else %}
          <tr><td colspan=\"5\" class=\"muted\">No alerts triggered.</td></tr>
          {% endif %}
        </tbody>
      </table>
      {% else %}
      <p class=\"muted\">Risk evaluation artifact not available.</p>
      {% endif %}
    </section>

    <section>
      <h2>Actions &amp; Orders</h2>
      {% if actions.orders %}
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Side</th>
            <th>Quantity</th>
            <th>Notional</th>
          </tr>
        </thead>
        <tbody>
          {% for order in actions.orders %}
          <tr>
            <td>{{ order.symbol }}</td>
            <td>{{ order.side }}</td>
            <td>{{ order.quantity | number }}</td>
            <td>{{ order.notional | currency(base_currency) }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <p class=\"muted\">No orders generated.</p>
      {% endif %}
      {% if actions.exits %}
      <p>Exit recommendations: {{ actions.exits | join(', ') }}</p>
      {% endif %}
      <p class=\"muted\">Proposal status: {{ actions.status }} &mdash; Turnover: {{ actions.turnover | percent }}</p>
    </section>

    <section>
      <h2>Signals Overview</h2>
      {% if signals.records %}
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Signal</th>
            <th>Rank Score</th>
          </tr>
        </thead>
        <tbody>
          {% for signal in signals.records %}
          <tr>
            <td>{{ signal.symbol }}</td>
            <td>{{ signal.signal }}</td>
            <td>{{ signal.rank_score | number }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <p class=\"muted\">No signal artifacts were provided.</p>
      {% endif %}
    </section>

    <section>
      <h2>Performance Metrics</h2>
      <table>
        <tbody>
          <tr>
            <th>63d Sharpe</th>
            <td>{{ performance.sharpe_63d | number }}</td>
          </tr>
          <tr>
            <th>20d Portfolio Return</th>
            <td>{{ performance.return_20d | percent }}</td>
          </tr>
          <tr>
            <th>Holdings Count</th>
            <td>{{ portfolio.positions | length }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>Artifact Manifest</h2>
      <table>
        <thead>
          <tr>
            <th>Artifact</th>
            <th>Path</th>
            <th>SHA256</th>
          </tr>
        </thead>
        <tbody>
          {% for name, entry in manifest.items() %}
          <tr>
            <td>{{ name }}</td>
            <td>{{ entry.path }}</td>
            <td>{{ entry.sha256 or '—' }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </section>

    {% if notes %}
    <section class=\"notes\">
      <h2>Notes</h2>
      <ul>
        {% for note in notes %}
        <li>{{ note }}</li>
        {% endfor %}
      </ul>
    </section>
    {% endif %}
  </body>
</html>
"""


@dataclass(slots=True)
class ManifestEntry:
    """Record describing an artifact referenced by the report."""

    path: str
    sha256: str | None


@dataclass(slots=True)
class ReportResult:
    """Result of building a daily report."""

    as_of: date
    generated_at: datetime
    html_path: Path | None
    json_path: Path | None
    pdf_path: Path | None
    payload: dict[str, Any]
    manifest: dict[str, ManifestEntry]
    notes: tuple[str, ...]


class ReportBuilder:
    """Render daily operator reports summarizing system outputs."""

    def __init__(
        self,
        config: Config,
        *,
        template: str | None = None,
        clock: Callable[[], datetime] | None = None,
        pdf_renderer: Callable[[str, Path], tuple[bool, str | None]] | None = None,
    ) -> None:
        self._config = config
        self._curated_base = config.paths.data_curated
        self._reports_base = config.paths.reports
        self._clock = clock or (lambda: datetime.now(UTC))
        self._pdf_renderer = pdf_renderer
        env = Environment(
            loader=DictLoader({"daily_report.html": template or DEFAULT_TEMPLATE}),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        env.filters["currency"] = _currency_filter
        env.filters["percent"] = _percent_filter
        env.filters["number"] = _number_filter
        env.globals.update({"len": len})
        self._template: Template = env.get_template("daily_report.html")

    def build(
        self,
        as_of: date | str | pd.Timestamp,
        *,
        holdings: HoldingsSnapshot,
        holdings_path: Path | None = None,
        risk_payload: Mapping[str, Any] | None = None,
        risk_path: Path | None = None,
        proposal_payload: Mapping[str, Any] | None = None,
        proposal_path: Path | None = None,
        signals: pd.DataFrame | None = None,
        signals_path: Path | None = None,
        include_pdf: bool = False,
        dry_run: bool = False,
    ) -> ReportResult:
        """Build a report for ``as_of`` and persist artifacts unless ``dry_run``."""

        as_of_ts = _normalize_timestamp(as_of)
        as_of_date = as_of_ts.date()
        curated_dir = self._curated_base / as_of_ts.strftime("%Y-%m-%d")
        if not curated_dir.is_dir():
            raise FileNotFoundError(f"Curated data directory not found: {curated_dir}")

        generated_at = self._clock()
        notes: list[str] = []

        position_frames: dict[str, pd.DataFrame] = {}
        portfolio_section, value_map = _build_portfolio_section(
            holdings, curated_dir, position_frames
        )

        risk_section = _build_risk_section(risk_payload)
        if risk_payload is None:
            notes.append(
                "Risk alerts artifact missing; section rendered with placeholder."
            )

        actions_section = _build_actions_section(proposal_payload)
        if proposal_payload is None:
            notes.append("Rebalance proposal artifact missing; orders section empty.")

        signals_section = _build_signals_section(signals, as_of_ts)
        if signals is None:
            notes.append("Signals parquet not supplied; signal table omitted.")

        performance_section = _build_performance_section(
            position_frames, value_map, holdings.cash or 0.0
        )

        manifest = _build_manifest(
            holdings_path=holdings_path,
            risk_path=risk_path,
            proposal_path=proposal_path,
            signals_path=signals_path,
            curated_dir=curated_dir,
            symbols=tuple(value_map),
        )

        context = {
            "as_of": as_of_date.isoformat(),
            "generated_at": generated_at.isoformat(),
            "base_currency": holdings.base_ccy or self._config.base_ccy,
            "portfolio": portfolio_section,
            "risk": risk_section,
            "actions": actions_section,
            "signals": signals_section,
            "performance": performance_section,
            "manifest": {name: asdict(entry) for name, entry in manifest.items()},
            "notes": notes,
        }

        html_content = self._template.render(**context)
        json_payload = json.loads(json.dumps(context))

        html_path: Path | None = None
        json_path: Path | None = None
        pdf_path: Path | None = None

        if not dry_run:
            output_dir = self._reports_base / as_of_date.strftime("%Y-%m-%d")
            output_dir.mkdir(parents=True, exist_ok=True)

            html_path = output_dir / "daily_report.html"
            html_path.write_text(html_content, encoding="utf-8")

            json_path = output_dir / "daily_report.json"
            json_path.write_text(
                json.dumps(json_payload, indent=2, sort_keys=True), encoding="utf-8"
            )

            if include_pdf:
                renderer = self._pdf_renderer or _default_pdf_renderer
                candidate_path = output_dir / "daily_report.pdf"
                success, message = renderer(html_content, candidate_path)
                if success:
                    pdf_path = candidate_path
                else:
                    notes.append(f"PDF generation skipped: {message}")
                    if candidate_path.exists():
                        candidate_path.unlink()

        return ReportResult(
            as_of=as_of_date,
            generated_at=generated_at,
            html_path=html_path,
            json_path=json_path,
            pdf_path=pdf_path,
            payload=json_payload,
            manifest=manifest,
            notes=tuple(notes),
        )


def _normalize_timestamp(value: date | str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def _build_portfolio_section(
    holdings: HoldingsSnapshot,
    curated_dir: Path,
    position_frames: dict[str, pd.DataFrame],
) -> tuple[dict[str, Any], dict[str, float]]:
    positions: list[dict[str, Any]] = []
    values: dict[str, float] = {}
    total_value = float(holdings.cash or 0.0)
    invested_value = 0.0

    for position in holdings.positions:
        frame = _load_symbol_frame(curated_dir, position.symbol)
        position_frames[position.symbol] = frame
        latest = frame.iloc[-1]
        price = float(latest.get("close", 0.0))
        value = price * position.qty
        invested_value += value
        total_value += value
        values[position.symbol] = value
        cost_basis = position.cost_basis
        unrealized = None
        unrealized_pct = None
        if cost_basis is not None:
            unrealized = (price - cost_basis) * position.qty
            if cost_basis != 0:
                unrealized_pct = (price / cost_basis) - 1.0
        ret_20d_raw = latest.get("ret_20d")
        ret_20d_value = None
        if ret_20d_raw is not None and not pd.isna(ret_20d_raw):
            ret_20d_value = float(ret_20d_raw)
        positions.append(
            {
                "symbol": position.symbol,
                "quantity": position.qty,
                "price": price,
                "value": value,
                "weight": 0.0,  # placeholder; compute below
                "cost_basis": cost_basis,
                "unrealized": unrealized,
                "unrealized_pct": unrealized_pct,
                "ret_20d": ret_20d_value,
            }
        )

    weight_divisor = total_value if total_value else 0.0
    for entry in positions:
        if weight_divisor:
            entry["weight"] = entry["value"] / weight_divisor
        else:
            entry["weight"] = 0.0

    positions.sort(key=_position_sort_key)

    return (
        {
            "positions": positions,
            "value": total_value,
            "cash": float(holdings.cash or 0.0),
            "invested": invested_value,
        },
        values,
    )


def _position_sort_key(entry: dict[str, Any]) -> tuple[float, str]:
    value_raw = entry.get("value", 0.0)
    try:
        value = float(value_raw)
    except (TypeError, ValueError):
        value = 0.0
    symbol = str(entry.get("symbol", ""))
    return (-abs(value), symbol)


def _build_risk_section(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    alerts = [
        {
            "symbol": str(alert.get("symbol", "")),
            "type": str(alert.get("type", "")),
            "value": float(alert.get("value", 0.0)),
            "threshold": float(alert.get("threshold", 0.0)),
            "reason": str(alert.get("reason", "")),
        }
        for alert in payload.get("alerts", [])
        if isinstance(alert, Mapping)
    ]
    alerts.sort(key=lambda item: (item["symbol"], item["type"]))
    market_filter = payload.get("market_filter")
    benchmark = None
    passed = None
    rule = None
    if isinstance(market_filter, Mapping):
        benchmark = (
            str(market_filter.get("benchmark"))
            if market_filter.get("benchmark")
            else None
        )
        passed = market_filter.get("passed")
        rule = market_filter.get("rule")
    return {
        "market_state": str(payload.get("market_state", "")),
        "alerts": alerts,
        "benchmark": benchmark,
        "passed": passed,
        "rule": rule,
    }


def _build_actions_section(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if payload is None:
        return {"orders": [], "exits": [], "status": "UNKNOWN", "turnover": None}
    orders = [
        {
            "symbol": str(order.get("symbol", "")),
            "side": str(order.get("side", "")),
            "quantity": float(order.get("quantity", 0.0)),
            "notional": float(order.get("notional", 0.0)),
        }
        for order in payload.get("orders", [])
        if isinstance(order, Mapping)
    ]
    orders.sort(key=lambda item: str(item.get("symbol", "")))

    exits = [
        str(target.get("symbol", ""))
        for target in payload.get("targets", [])
        if isinstance(target, Mapping)
        and float(target.get("target_weight", 0.0)) == 0.0
    ]
    exits.sort()

    turnover = payload.get("turnover")
    turnover_value = float(turnover) if turnover is not None else None

    return {
        "orders": orders,
        "exits": exits,
        "status": str(payload.get("status", "")),
        "turnover": turnover_value,
    }


def _build_signals_section(
    frame: pd.DataFrame | None, as_of: pd.Timestamp
) -> dict[str, Any]:
    if frame is None:
        return {"records": []}
    working = frame.copy()
    if "date" in working.columns:
        working["date"] = pd.to_datetime(working["date"]).dt.normalize()
        working = working[working["date"] == as_of]
    if "rank_score" in working.columns:
        working = working.sort_values(["rank_score", "symbol"], ascending=[False, True])
    else:
        working = working.sort_values(["symbol"])
    working = working.head(15)
    items = []
    for _, row in working.iterrows():
        items.append(
            {
                "symbol": str(row.get("symbol", "")),
                "signal": str(row.get("signal", "")),
                "rank_score": float(row.get("rank_score", 0.0)),
            }
        )
    return {"records": items}


def _build_performance_section(
    frames: Mapping[str, pd.DataFrame],
    values: Mapping[str, float],
    cash: float,
) -> dict[str, Any]:
    if not frames:
        return {"sharpe_63d": None, "return_20d": None}

    invested = sum(values.values())
    weights: dict[str, float] = {}
    if invested:
        for symbol, value in values.items():
            weights[symbol] = value / invested
    else:
        weights = {symbol: 0.0 for symbol in frames}

    combined = None
    for symbol, frame in frames.items():
        returns = frame.get("ret_1d")
        if returns is None:
            continue
        series = returns.dropna().tail(63)
        if series.empty:
            continue
        weighted = series * weights.get(symbol, 0.0)
        combined = (
            weighted if combined is None else combined.add(weighted, fill_value=0.0)
        )

    sharpe = None
    if combined is not None and not combined.empty:
        mean = combined.mean()
        std = combined.std(ddof=0)
        if std > 0:
            sharpe = (mean / std) * (252**0.5)

    ret_20d = None
    if invested:
        accumulator = 0.0
        weight_sum = 0.0
        for symbol, frame in frames.items():
            last_row = frame.iloc[-1]
            ret_value = last_row.get("ret_20d")
            if ret_value is None or pd.isna(ret_value):
                continue
            weight = weights.get(symbol, 0.0)
            accumulator += weight * float(ret_value)
            weight_sum += weight
        if weight_sum > 0:
            ret_20d = accumulator

    return {"sharpe_63d": sharpe, "return_20d": ret_20d}


def _build_manifest(
    *,
    holdings_path: Path | None,
    risk_path: Path | None,
    proposal_path: Path | None,
    signals_path: Path | None,
    curated_dir: Path,
    symbols: Sequence[str],
) -> dict[str, ManifestEntry]:
    manifest: dict[str, ManifestEntry] = {}

    if holdings_path is not None:
        manifest["holdings"] = ManifestEntry(
            path=str(holdings_path), sha256=_sha256_or_none(holdings_path)
        )
    if risk_path is not None:
        manifest["risk_alerts"] = ManifestEntry(
            path=str(risk_path), sha256=_sha256_or_none(risk_path)
        )
    if proposal_path is not None:
        manifest["rebalance_proposal"] = ManifestEntry(
            path=str(proposal_path), sha256=_sha256_or_none(proposal_path)
        )
    if signals_path is not None:
        manifest["signals"] = ManifestEntry(
            path=str(signals_path), sha256=_sha256_or_none(signals_path)
        )

    for symbol in symbols:
        path = curated_dir / f"{symbol}.parquet"
        if path.is_file():
            manifest[f"curated::{symbol}"] = ManifestEntry(
                path=str(path), sha256=_sha256_or_none(path)
            )

    if not manifest:
        manifest["reports_dir"] = ManifestEntry(path=str(curated_dir), sha256=None)

    return manifest


def _load_symbol_frame(curated_dir: Path, symbol: str) -> pd.DataFrame:
    path = curated_dir / f"{symbol}.parquet"
    if not path.is_file():
        raise FileNotFoundError(
            f"Curated dataset missing for {symbol} in {curated_dir}"
        )
    frame = pd.read_parquet(path)
    if frame.empty:
        raise ValueError(f"Curated dataset for {symbol} is empty")
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    frame = frame.sort_values("date")
    return frame


def _sha256_or_none(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:  # pragma: no cover - defensive
        logger.warning("Unable to compute SHA256 for %s: %s", path, exc)
        return None


def _default_pdf_renderer(content: str, output_path: Path) -> tuple[bool, str | None]:
    try:
        import pdfkit  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        return False, f"pdfkit unavailable: {exc}"

    try:
        pdfkit.from_string(content, str(output_path))
        return True, None
    except Exception as exc:  # pragma: no cover - runtime dependency
        return False, str(exc)


def _currency_filter(value: Any, currency: str) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{currency} {numeric:,.2f}"


def _percent_filter(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{numeric * 100:.2f}%"


def _number_filter(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{numeric:.4f}"


__all__ = ["ManifestEntry", "ReportBuilder", "ReportResult"]
