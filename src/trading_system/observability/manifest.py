# Copyright (c) 2025 Arye Kogan
# SPDX-License-Identifier: MIT

"""Artifact manifest models and helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq  # type: ignore[import-untyped]
from pydantic import BaseModel, Field

MANIFEST_VERSION = "1.0.0"
ArtifactKind = Literal["file", "directory", "missing"]


@dataclass(frozen=True, slots=True)
class ArtifactSpec:
    """Lightweight descriptor for an artifact produced during a step."""

    key: str
    path: Path
    kind: ArtifactKind | None = None
    row_count: int | None = None
    description: str | None = None


@dataclass(slots=True)
class _PendingStep:
    name: str
    status: str
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    details: str | None
    artifacts: tuple[ArtifactSpec, ...]


@dataclass(slots=True)
class _PathMetadata:
    kind: ArtifactKind
    sha256: str | None
    size_bytes: int | None
    row_count: int | None


class ManifestArtifact(BaseModel):
    key: str
    path: str
    kind: ArtifactKind
    sha256: str | None = None
    size_bytes: int | None = None
    row_count: int | None = Field(default=None, ge=0)
    description: str | None = None


class ManifestStep(BaseModel):
    name: str
    status: str
    started_at: datetime
    completed_at: datetime
    duration_seconds: float = Field(ge=0)
    details: str | None = None
    artifacts: list[ManifestArtifact] = Field(default_factory=list)


class ManifestRun(BaseModel):
    pipeline: str
    as_of: date
    started_at: datetime
    completed_at: datetime
    duration_seconds: float = Field(ge=0)
    success: bool
    log_path: str | None = None
    config_path: str | None = None
    holdings_path: str | None = None
    artifacts: list[ManifestArtifact] = Field(default_factory=list)


class PipelineManifest(BaseModel):
    version: str = Field(default=MANIFEST_VERSION)
    run: ManifestRun
    steps: list[ManifestStep] = Field(default_factory=list)


@dataclass(slots=True)
class ManifestWriteResult:
    """Return value from :class:`ManifestBuilder` finalization."""

    manifest: PipelineManifest
    path: Path
    summary: dict[str, str]


class ManifestBuilder:
    """Collect step metadata and materialize a manifest file."""

    def __init__(
        self,
        *,
        pipeline: str,
        as_of: date,
        reports_dir: Path,
        config_path: Path | None,
        holdings_path: Path | None,
        log_path: Path | None,
    ) -> None:
        self._pipeline = pipeline
        self._as_of = as_of
        self._reports_dir = reports_dir
        self._config_path = config_path
        self._holdings_path = holdings_path
        self._log_path = log_path
        self._steps: list[_PendingStep] = []
        self._globals: list[ArtifactSpec] = []
        self._manifest_path = reports_dir / "manifest.json"
        self._summary: dict[str, str] = {}
        self._cache: dict[Path, _PathMetadata] = {}

    def add_global_artifact(self, spec: ArtifactSpec) -> None:
        self._globals.append(spec)
        self._summary[spec.key] = str(spec.path)

    def add_step(
        self,
        *,
        name: str,
        status: str,
        started_at: datetime,
        completed_at: datetime,
        duration_seconds: float,
        details: str | None,
        artifacts: Sequence[ArtifactSpec],
    ) -> None:
        self._steps.append(
            _PendingStep(
                name=name,
                status=status,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration_seconds,
                details=details,
                artifacts=tuple(artifacts),
            )
        )
        for spec in artifacts:
            self._summary.setdefault(spec.key, str(spec.path))

    def finalize(
        self,
        *,
        started_at: datetime,
        completed_at: datetime,
        success: bool,
    ) -> ManifestWriteResult:
        run_artifacts = [self._materialize(spec) for spec in self._run_specs()]
        step_models: list[ManifestStep] = []
        for step in self._steps:
            step_models.append(
                ManifestStep(
                    name=step.name,
                    status=step.status,
                    started_at=step.started_at,
                    completed_at=step.completed_at,
                    duration_seconds=step.duration_seconds,
                    details=step.details,
                    artifacts=[self._materialize(spec) for spec in step.artifacts],
                )
            )

        manifest = PipelineManifest(
            run=ManifestRun(
                pipeline=self._pipeline,
                as_of=self._as_of,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=(completed_at - started_at).total_seconds(),
                success=success,
                log_path=str(self._log_path) if self._log_path else None,
                config_path=str(self._config_path) if self._config_path else None,
                holdings_path=str(self._holdings_path) if self._holdings_path else None,
                artifacts=run_artifacts,
            ),
            steps=step_models,
        )

        text = manifest.model_dump_json(indent=2, by_alias=True)
        self._manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self._manifest_path.write_text(text, encoding="utf-8")

        summary = self._summary.copy()
        summary.setdefault("manifest_json", str(self._manifest_path))

        return ManifestWriteResult(
            manifest=manifest,
            path=self._manifest_path,
            summary=summary,
        )

    def _run_specs(self) -> Iterator[ArtifactSpec]:
        if self._config_path is not None:
            yield ArtifactSpec(key="config", path=self._config_path, kind="file")
        if self._holdings_path is not None:
            yield ArtifactSpec(key="holdings", path=self._holdings_path, kind="file")
        if self._log_path is not None:
            yield ArtifactSpec(key="run_log", path=self._log_path, kind="file")
        yield from self._globals

    def _materialize(self, spec: ArtifactSpec) -> ManifestArtifact:
        path = spec.path
        metadata = self._describe(path)
        return ManifestArtifact(
            key=spec.key,
            path=str(path),
            kind=metadata.kind,
            sha256=metadata.sha256,
            size_bytes=metadata.size_bytes,
            row_count=metadata.row_count if spec.row_count is None else spec.row_count,
            description=spec.description,
        )

    def _describe(self, path: Path) -> _PathMetadata:
        if path in self._cache:
            return self._cache[path]

        if not path.exists():
            metadata = _PathMetadata(
                kind="missing",
                sha256=None,
                size_bytes=None,
                row_count=None,
            )
        elif path.is_file():
            file_hash, size, row_count = _hash_file(path)
            metadata = _PathMetadata(
                kind="file",
                sha256=file_hash,
                size_bytes=size,
                row_count=row_count,
            )
        elif path.is_dir():
            dir_hash, size, row_count = _hash_directory(path)
            metadata = _PathMetadata(
                kind="directory",
                sha256=dir_hash,
                size_bytes=size,
                row_count=row_count,
            )
        else:
            metadata = _PathMetadata(
                kind="missing", sha256=None, size_bytes=None, row_count=None
            )

        self._cache[path] = metadata
        return metadata


def _hash_file(path: Path) -> tuple[str, int, int | None]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(131072), b""):
            digest.update(chunk)
            size += len(chunk)
    row_count = _row_count_for_file(path)
    return digest.hexdigest(), size, row_count


def _hash_directory(path: Path) -> tuple[str, int, int | None]:
    digest = hashlib.sha256()
    total_size = 0
    total_rows = 0
    have_rows = False
    for file_path in sorted(p for p in path.rglob("*") if p.is_file()):
        rel = file_path.relative_to(path).as_posix().encode("utf-8")
        file_hash, size, row_count = _hash_file(file_path)
        digest.update(rel)
        digest.update(b"|")
        digest.update(file_hash.encode("utf-8"))
        digest.update(b"|")
        digest.update(str(size).encode("utf-8"))
        total_size += size
        if row_count is not None:
            have_rows = True
            total_rows += row_count
    return digest.hexdigest(), total_size, total_rows if have_rows else None


def _row_count_for_file(path: Path) -> int | None:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        try:
            parquet = pq.ParquetFile(path)
        except Exception:  # pragma: no cover - defensive for partial files
            return None
        metadata = parquet.metadata
        return metadata.num_rows if metadata is not None else None
    if suffix in {".json", ".jsonl"}:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return None
        if isinstance(payload, list):
            return len(payload)
        if isinstance(payload, dict):
            for key in ("alerts", "targets", "orders", "positions"):
                value = payload.get(key)
                if isinstance(value, list):
                    return len(value)
        return None
    return None


def load_manifest(path: Path) -> PipelineManifest:
    """Load and validate a manifest JSON payload."""

    content = path.read_text(encoding="utf-8")
    data = json.loads(content)
    return PipelineManifest.model_validate(data)


def validate_manifest(manifest: PipelineManifest) -> list[str]:
    """Recompute metadata and return human-readable mismatches."""

    errors: list[str] = []
    cache: dict[Path, _PathMetadata] = {}

    def _validate_artifact(record: ManifestArtifact) -> None:
        path = Path(record.path)
        if path not in cache:
            cache[path] = _collect_metadata(path)

        meta = cache[path]
        if record.kind != meta.kind:
            errors.append(
                f"{record.key}: expected kind {record.kind}, observed {meta.kind}"
            )
        if record.kind != "missing":
            if record.sha256 and meta.sha256 and record.sha256 != meta.sha256:
                errors.append(
                    f"{record.key}: SHA mismatch {record.sha256} != {meta.sha256}"
                )
            if record.size_bytes is not None and meta.size_bytes is not None:
                if record.size_bytes != meta.size_bytes:
                    errors.append(
                        f"{record.key}: size mismatch {record.size_bytes} != {meta.size_bytes}"
                    )
            if record.row_count is not None and meta.row_count is not None:
                if record.row_count != meta.row_count:
                    errors.append(
                        f"{record.key}: row count mismatch {record.row_count} != {meta.row_count}"
                    )
            if not path.exists():
                errors.append(f"{record.key}: path missing at {record.path}")

    for artifact in manifest.run.artifacts:
        _validate_artifact(artifact)
    for step in manifest.steps:
        for artifact in step.artifacts:
            _validate_artifact(artifact)

    return errors


def _collect_metadata(path: Path) -> _PathMetadata:
    if not path.exists():
        return _PathMetadata(
            kind="missing", sha256=None, size_bytes=None, row_count=None
        )
    if path.is_file():
        file_hash, size, row_count = _hash_file(path)
        return _PathMetadata(
            kind="file", sha256=file_hash, size_bytes=size, row_count=row_count
        )
    if path.is_dir():
        dir_hash, size, row_count = _hash_directory(path)
        return _PathMetadata(
            kind="directory",
            sha256=dir_hash,
            size_bytes=size,
            row_count=row_count,
        )
    return _PathMetadata(kind="missing", sha256=None, size_bytes=None, row_count=None)


__all__ = [
    "ArtifactSpec",
    "ManifestArtifact",
    "ManifestBuilder",
    "ManifestWriteResult",
    "PipelineManifest",
    "load_manifest",
    "validate_manifest",
]
