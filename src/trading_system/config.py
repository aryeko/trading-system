"""Configuration loader for the trading system."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, MutableMapping

import yaml
from pydantic import BaseModel, ConfigDict


class DataConfig(BaseModel):
    """Settings for data acquisition."""

    model_config = ConfigDict(extra="allow")

    provider: str
    adjust: str | None = None
    lookback_days: int | None = None


class UniverseConfig(BaseModel):
    """Universe selection settings."""

    model_config = ConfigDict(extra="allow")

    tickers: list[str]


class StrategyConfig(BaseModel):
    """Strategy rule parameters."""

    model_config = ConfigDict(extra="allow")

    type: str
    entry: str
    exit: str
    rank: str | None = None


class MarketFilterConfig(BaseModel):
    """Market filter rule settings."""

    model_config = ConfigDict(extra="allow")

    benchmark: str
    rule: str


class RiskConfig(BaseModel):
    """Risk management thresholds."""

    model_config = ConfigDict(extra="allow")

    crash_threshold_pct: float
    drawdown_threshold_pct: float
    market_filter: MarketFilterConfig | None = None


class RebalanceConfig(BaseModel):
    """Rebalance cadence and constraints."""

    model_config = ConfigDict(extra="allow")

    cadence: str
    max_positions: int
    equal_weight: bool | None = None
    min_weight: float | None = None
    cash_buffer: float | None = None
    turnover_cap_pct: float | None = None


class NotifyConfig(BaseModel):
    """Notification channels."""

    model_config = ConfigDict(extra="allow")

    email: str | None = None
    slack_webhook: str | None = None


class PathsConfig(BaseModel):
    """Canonical project paths."""

    model_config = ConfigDict(extra="ignore")

    data_raw: Path
    data_curated: Path
    reports: Path

    @property
    def directories(self) -> tuple[Path, Path, Path]:
        """Return the managed directories."""

        return (self.data_raw, self.data_curated, self.reports)


class Config(BaseModel):
    """Top-level configuration contract for the trading system."""

    model_config = ConfigDict(extra="allow")

    base_ccy: str
    calendar: str
    data: DataConfig
    universe: UniverseConfig
    strategy: StrategyConfig
    risk: RiskConfig
    rebalance: RebalanceConfig
    notify: NotifyConfig
    paths: PathsConfig


def _resolve_directories(
    *, config_path: Path, paths_section: Mapping[str, object]
) -> dict[str, Path]:
    """Resolve the configured directories relative to the config file location."""

    resolved: dict[str, Path] = {}
    base_dir = config_path.parent
    for key, raw_value in paths_section.items():
        path_value = Path(str(raw_value)).expanduser()
        if not path_value.is_absolute():
            path_value = base_dir / path_value
        resolved[key] = path_value.resolve()
    return resolved


def load_config(path: str | Path) -> Config:
    """Load configuration from ``path`` and ensure project directories exist."""

    config_path = Path(path).expanduser()
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        payload_raw = yaml.safe_load(handle) or {}

    if not isinstance(payload_raw, MutableMapping):
        raise ValueError("Configuration file must contain a mapping at the top level.")

    payload: dict[str, Any] = dict(payload_raw)

    raw_paths = payload.get("paths")
    if not isinstance(raw_paths, Mapping):
        raise ValueError("Configuration missing 'paths' mapping.")

    payload["paths"] = _resolve_directories(
        config_path=config_path, paths_section=raw_paths
    )

    config = Config.model_validate(payload)

    for directory in config.paths.directories:
        directory.mkdir(parents=True, exist_ok=True)

    return config


__all__ = [
    "Config",
    "DataConfig",
    "UniverseConfig",
    "StrategyConfig",
    "RiskConfig",
    "RebalanceConfig",
    "NotifyConfig",
    "PathsConfig",
    "load_config",
]
