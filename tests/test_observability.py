"""Tests for observability manifest helpers."""

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from trading_system.observability.manifest import (
    ArtifactSpec,
    ManifestBuilder,
    load_manifest,
    validate_manifest,
)


def test_manifest_builder_captures_artifacts(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports" / "2024-05-02"
    config_path = tmp_path / "config.yml"
    config_path.write_text("config", encoding="utf-8")
    holdings_path = tmp_path / "holdings.json"
    holdings_path.write_text("{}", encoding="utf-8")
    log_path = reports_dir / "run.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("log", encoding="utf-8")

    builder = ManifestBuilder(
        pipeline="daily",
        as_of=date(2024, 5, 2),
        reports_dir=reports_dir,
        config_path=config_path,
        holdings_path=holdings_path,
        log_path=log_path,
    )

    raw_dir = tmp_path / "data" / "raw" / "2024-05-02"
    raw_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame({"value": [1, 2, 3]})
    raw_path = raw_dir / "sample.parquet"
    frame.to_parquet(raw_path, index=False)

    start = datetime.now(UTC)
    builder.add_step(
        name="data_pull",
        status="completed",
        started_at=start,
        completed_at=start,
        duration_seconds=0.0,
        details=None,
        artifacts=(ArtifactSpec(key="data_raw", path=raw_dir, kind="directory"),),
    )

    result = builder.finalize(
        started_at=start,
        completed_at=start,
        success=True,
    )

    manifest = load_manifest(result.path)
    assert any(
        artifact.key == "data_raw"
        for step in manifest.steps
        for artifact in step.artifacts
    )
    assert manifest.run.log_path == str(log_path)
    assert "manifest_json" in result.summary
    assert validate_manifest(manifest) == []


def test_validate_manifest_flags_hash_mismatch(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports" / "2024-05-02"
    config_path = tmp_path / "config.yml"
    config_path.write_text("config", encoding="utf-8")
    holdings_path = tmp_path / "holdings.json"
    holdings_path.write_text("{}", encoding="utf-8")
    log_path = reports_dir / "run.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("log", encoding="utf-8")

    builder = ManifestBuilder(
        pipeline="daily",
        as_of=date(2024, 5, 2),
        reports_dir=reports_dir,
        config_path=config_path,
        holdings_path=holdings_path,
        log_path=log_path,
    )

    start = datetime.now(UTC)
    artifact_path = reports_dir / "report.json"
    artifact_path.write_text("{}", encoding="utf-8")

    builder.add_step(
        name="report_build",
        status="completed",
        started_at=start,
        completed_at=start,
        duration_seconds=0.0,
        details=None,
        artifacts=(ArtifactSpec(key="report_json", path=artifact_path, kind="file"),),
    )

    result = builder.finalize(
        started_at=start,
        completed_at=start,
        success=True,
    )

    manifest = load_manifest(result.path)
    log_path.write_text("corrupted", encoding="utf-8")

    errors = validate_manifest(manifest)
    assert any("run_log" in message for message in errors)
