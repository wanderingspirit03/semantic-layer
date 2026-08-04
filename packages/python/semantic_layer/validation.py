"""Validation for the public semantic trace v1 bundle."""

from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from jsonschema import Draft202012Validator, FormatChecker

from .capture_v1 import _Scanner
from .permissions import read_regular_file, reject_symlink_path_components

RequiredEvidence = Literal["root", "model", "tool", "delivery"]
_CONTEXT_REFERENCE_KINDS = {"message", "model.response", "tool.result"}


def _schema_validator(filename: str, *, version: str = "v1") -> Draft202012Validator:
    """Load the canonical contract in a checkout or a packaged schema copy."""
    candidates = [Path(__file__).with_name("schemas") / filename]
    candidates.extend(
        parent / "contracts" / "trace" / version / filename
        for parent in Path(__file__).resolve().parents
    )
    for candidate in candidates:
        if candidate.is_file():
            return Draft202012Validator(
                json.loads(candidate.read_text()),
                format_checker=FormatChecker(),
            )
    raise RuntimeError(f"semantic trace contract is unavailable: {filename}")


_RECORD_VALIDATOR = _schema_validator("semantic-trace-record.schema.json")
_MANIFEST_VALIDATORS = {
    "semantic_trace_manifest_v1": _schema_validator("semantic-trace-manifest.schema.json"),
    "semantic_trace_manifest_v2": _schema_validator(
        "semantic-trace-manifest-v2.schema.json", version="v2"
    ),
}


@dataclass(frozen=True)
class SourceActivity:
    source_id: str
    name: str
    records: int


@dataclass(frozen=True)
class ArtifactValidationReport:
    valid: bool
    profile: str
    issues: tuple[str, ...]
    rows: int
    secret_matches: int
    source_activity: tuple[SourceActivity, ...]


def validate_artifact(
    artifact_path: str | Path,
    *,
    injected: dict[str, Any] | None = None,
    secret_values: list[str] | tuple[str, ...] | None = None,
    profile: str = "structural",
    required_evidence: tuple[RequiredEvidence, ...] | list[RequiredEvidence] = (),
    required_source_activity: tuple[str, ...] | list[str] = (),
) -> ArtifactValidationReport:
    """Validate one on-disk bundle or injected manifest and record values."""
    if profile not in {"structural", "rich-agent"}:
        raise ValueError("profile must be structural or rich-agent")

    root = Path(artifact_path)
    manifest_bytes = b""
    trace_bytes = b""
    if injected is None:
        try:
            reject_symlink_path_components(root)
            manifest_bytes = read_regular_file(root / "manifest.json")
            trace_bytes = read_regular_file(root / "trace.jsonl")
            manifest = json.loads(manifest_bytes)
            rows = [json.loads(line) for line in trace_bytes.splitlines() if line]
        except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return ArtifactValidationReport(
                False, profile, ("ARTIFACT_UNREADABLE",), 0, 0, ()
            )
    else:
        try:
            manifest = injected["manifest"]
            rows = injected["rows"]
        except (KeyError, TypeError):
            return ArtifactValidationReport(
                False, profile, ("ARTIFACT_UNREADABLE",), 0, 0, ()
            )
        if not isinstance(rows, list):
            return ArtifactValidationReport(
                False, profile, ("ARTIFACT_UNREADABLE",), 0, 0, ()
            )
        try:
            trace_bytes = _encode_rows(rows)
        except (TypeError, ValueError):
            trace_bytes = b""

    issues: list[str] = []
    manifest_schema = manifest.get("schema") if isinstance(manifest, dict) else None
    manifest_validator = (
        _MANIFEST_VALIDATORS.get(manifest_schema)
        if isinstance(manifest_schema, str)
        else None
    )
    if manifest_validator is None or list(manifest_validator.iter_errors(manifest)):
        issues.append("MANIFEST_SCHEMA_INVALID")

    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict) or list(_RECORD_VALIDATOR.iter_errors(row)):
            issues.append("RECORD_SCHEMA_INVALID")
        if not isinstance(row, dict) or row.get("seq") != index:
            issues.append("SEQUENCE_INVALID")

    if isinstance(manifest, dict):
        _validate_references(manifest, rows, issues)
        _validate_manifest_accounting(manifest, rows, trace_bytes, issues)
    if profile == "rich-agent":
        _validate_rich_agent_profile(rows, issues)
    _validate_required_evidence(rows, required_evidence, issues)
    source_activity = _source_activity(manifest, rows)
    _validate_required_source_activity(
        source_activity, required_source_activity, issues
    )

    scanner = _Scanner(secret_values)
    secret_matches = 0
    if injected is None and isinstance(manifest, dict):
        if manifest.get("state") not in {"sealed", "recovered"}:
            issues.append("ARTIFACT_NOT_SEALED")
        secret_matches += _validate_disk_bundle(
            root,
            manifest_bytes,
            trace_bytes,
            manifest,
            rows,
            scanner,
            issues,
        )
    elif injected is not None:
        secret_matches += _scan_injected(manifest, trace_bytes, scanner)

    if secret_matches:
        issues.append("SECRET_MATCH")
    unique_issues = tuple(sorted(set(issues)))
    return ArtifactValidationReport(
        not unique_issues,
        profile,
        unique_issues,
        len(rows),
        secret_matches,
        source_activity,
    )


def _validate_references(
    manifest: dict[str, Any], rows: list[Any], issues: list[str]
) -> None:
    declared_sources: set[str] = set()
    for source in manifest.get("sources", []):
        if not isinstance(source, dict) or not isinstance(source.get("id"), str):
            continue
        source_id = source["id"]
        if source_id in declared_sources:
            issues.append("SOURCE_DECLARATION_INVALID")
        declared_sources.add(source_id)

    seen: dict[str, dict[str, Any]] = {}
    containment_roots: dict[str, str] = {}
    expandable_requests: set[str] = set()
    outcome_counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        record_id = row.get("id")
        if isinstance(record_id, str) and record_id in seen:
            issues.append("RECORD_ID_DUPLICATE")
        source = row.get("source")
        if isinstance(source, str) and source not in declared_sources:
            issues.append("SOURCE_UNDECLARED")

        parent_id = row.get("parent")
        if parent_id is not None and parent_id not in seen:
            issues.append("PARENT_REFERENCE_INVALID")
        root_id = (
            record_id
            if row.get("kind") == "run.start" and isinstance(record_id, str)
            else containment_roots.get(parent_id)
            if isinstance(parent_id, str)
            else None
        )

        links = row.get("links")
        if isinstance(links, list):
            for link in links:
                if not isinstance(link, dict) or not isinstance(
                    link.get("record"), str
                ):
                    continue
                if link["record"] not in seen:
                    issues.append("LINK_REFERENCE_INVALID")

        if row.get("kind") == "run.start" and parent_id is not None:
            issues.append("ROOT_START_INVALID")
        if row.get("kind") == "run.outcome":
            outcome_root_id = parent_id if isinstance(parent_id, str) else None
            root = (
                seen.get(outcome_root_id)
                if outcome_root_id is not None
                else None
            )
            if (
                outcome_root_id is None
                or root is None
                or root.get("kind") != "run.start"
            ):
                issues.append("ROOT_OUTCOME_INVALID")
            else:
                outcome_counts[outcome_root_id] = (
                    outcome_counts.get(outcome_root_id, 0) + 1
                )
                if outcome_counts[outcome_root_id] > 1:
                    issues.append("ROOT_OUTCOME_DUPLICATE")

        if isinstance(record_id, str):
            if root_id is not None:
                containment_roots[record_id] = root_id
            if row.get("kind") == "model.request":
                data = row.get("data")
                refs = data.get("context_refs") if isinstance(data, dict) else None
                refs_valid = True
                if (
                    isinstance(data, dict)
                    and "context_refs" in data
                    and not isinstance(refs, list)
                ):
                    issues.append("CONTEXT_REFERENCE_INVALID")
                    refs_valid = False
                if isinstance(refs, list):
                    for ref in refs:
                        target = seen.get(ref) if isinstance(ref, str) else None
                        if (
                            target is None
                            or target.get("kind") not in _CONTEXT_REFERENCE_KINDS
                        ):
                            issues.append("CONTEXT_REFERENCE_INVALID")
                            refs_valid = False
                base = data.get("context_base_ref") if isinstance(data, dict) else None
                base_valid = base is None
                if base is not None:
                    target = seen.get(base) if isinstance(base, str) else None
                    if (
                        target is None
                        or target.get("kind") != "model.request"
                        or root_id is None
                        or containment_roots.get(base) != root_id
                        or base not in expandable_requests
                        or not isinstance(refs, list)
                    ):
                        issues.append("CONTEXT_BASE_REFERENCE_INVALID")
                    else:
                        base_valid = True
                if base_valid and refs_valid and isinstance(refs, list):
                    expandable_requests.add(record_id)
            seen[record_id] = row


def _validate_manifest_accounting(
    manifest: dict[str, Any],
    rows: list[Any],
    trace_bytes: bytes,
    issues: list[str],
) -> None:
    trace = manifest.get("trace")
    if not isinstance(trace, dict):
        return
    if trace.get("records") != len(rows) or trace.get("last_seq") != len(rows):
        issues.append("MANIFEST_COUNT_MISMATCH")
    if trace.get("bytes") != len(trace_bytes):
        issues.append("MANIFEST_BYTE_COUNT_MISMATCH")
    losses = sum(
        isinstance(row, dict) and row.get("kind") == "loss" for row in rows
    )
    if trace.get("losses") != losses:
        issues.append("LOSS_COUNT_MISMATCH")
    if trace.get("sha256") != hashlib.sha256(trace_bytes).hexdigest():
        issues.append("TRACE_DIGEST_MISMATCH")


def _validate_rich_agent_profile(rows: list[Any], issues: list[str]) -> None:
    evidence = _evidence_roots(rows)
    if not evidence["root_pair"]:
        issues.append("PROFILE_PAIR_MISSING:root")
    if not evidence["model_pair"]:
        issues.append("PROFILE_PAIR_MISSING:model")
    if not evidence["tool_pair"]:
        issues.append("PROFILE_PAIR_MISSING:tool")
    if (
        evidence["root_pair"]
        and evidence["model_pair"]
        and evidence["tool_pair"]
        and evidence["outcome_roots"].isdisjoint(
            evidence["model_roots"] & evidence["tool_roots"]
        )
    ):
        issues.append("PROFILE_PAIR_ROOT_MISMATCH")


def _evidence_roots(rows: list[Any]) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    containment_roots: dict[str, str] = {}
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("id"), str):
            record_id = row["id"]
            records[record_id] = row
            parent = row.get("parent")
            root = (
                record_id
                if row.get("kind") == "run.start"
                else containment_roots.get(parent)
                if isinstance(parent, str)
                else None
            )
            if root is not None:
                containment_roots[record_id] = root
    outcome_roots: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or row.get("kind") != "run.outcome":
            continue
        parent = row.get("parent")
        if isinstance(parent, str) and records.get(parent, {}).get("kind") == "run.start":
            outcome_roots.add(parent)
    model_roots = _result_pair_roots(
        rows,
        records,
        containment_roots,
        "model.response",
        "model.request",
        "completed",
    )
    tool_roots = _result_pair_roots(
        rows,
        records,
        containment_roots,
        "tool.result",
        "tool.call",
        "succeeded",
    )
    root_pair = bool(outcome_roots)
    model_pair = bool(model_roots)
    tool_pair = bool(tool_roots)
    delivery = any(
        isinstance(row, dict)
        and row.get("kind") == "run.outcome"
        and isinstance(row.get("data"), dict)
        and row["data"].get("status") == "completed"
        and isinstance(row.get("parent"), str)
        and (
            "output" in row["data"]
            or any(
                isinstance(link, dict)
                and link.get("type") == "derived_from"
                and isinstance(link.get("record"), str)
                and _successful_delivery_target(records.get(link["record"]))
                and containment_roots.get(link["record"]) == row["parent"]
                for link in row.get("links", [])
                if isinstance(row.get("links"), list)
            )
        )
        for row in rows
    )
    return {
        "root_pair": root_pair,
        "model_pair": model_pair,
        "tool_pair": tool_pair,
        "delivery": delivery,
        "outcome_roots": outcome_roots,
        "model_roots": model_roots,
        "tool_roots": tool_roots,
    }


def _successful_delivery_target(record: dict[str, Any] | None) -> bool:
    if record is None:
        return False
    kind = record.get("kind")
    if kind in {"message", "state"}:
        return True
    data = record.get("data")
    if not isinstance(data, dict):
        return False
    if kind == "model.response":
        return data.get("status") == "completed"
    if kind == "tool.result":
        return data.get("status") == "succeeded"
    return False


def _validate_required_evidence(
    rows: list[Any],
    required: tuple[RequiredEvidence, ...] | list[RequiredEvidence],
    issues: list[str],
) -> None:
    evidence = _evidence_roots(rows)
    available = {
        "root": evidence["root_pair"],
        "model": evidence["model_pair"],
        "tool": evidence["tool_pair"],
        "delivery": evidence["delivery"],
    }
    for requirement in dict.fromkeys(required):
        if requirement not in available:
            raise ValueError(
                f"required_evidence contains unknown value: {requirement}"
            )
        if not available[requirement]:
            issues.append(f"REQUIRED_EVIDENCE_MISSING:{requirement}")


def _source_activity(
    manifest: Any, rows: list[Any]
) -> tuple[SourceActivity, ...]:
    counts: dict[str, int] = {}
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("source"), str):
            source_id = row["source"]
            counts[source_id] = counts.get(source_id, 0) + 1
    sources = manifest.get("sources", []) if isinstance(manifest, dict) else []
    return tuple(
        SourceActivity(
            source_id=source["id"],
            name=source["name"],
            records=counts.get(source["id"], 0),
        )
        for source in sources
        if isinstance(source, dict)
        and isinstance(source.get("id"), str)
        and isinstance(source.get("name"), str)
    )


def _validate_required_source_activity(
    activity: tuple[SourceActivity, ...],
    required: tuple[str, ...] | list[str],
    issues: list[str],
) -> None:
    for name in dict.fromkeys(required):
        if not any(item.name == name and item.records > 0 for item in activity):
            issues.append(f"REQUIRED_SOURCE_ACTIVITY_MISSING:{name}")


def _result_pair_roots(
    rows: list[Any],
    records: dict[str, dict[str, Any]],
    containment_roots: dict[str, str],
    result_kind: str,
    request_kind: str,
    result_status: str,
) -> set[str]:
    roots: set[str] = set()
    for row in rows:
        if (
            not isinstance(row, dict)
            or row.get("kind") != result_kind
            or (
                not isinstance(row.get("data"), dict)
                or row["data"].get("status") != result_status
            )
        ):
            continue
        result_id = row.get("id")
        result_root = (
            containment_roots.get(result_id)
            if isinstance(result_id, str)
            else None
        )
        links = row.get("links")
        if not isinstance(links, list):
            continue
        for link in links:
            if not isinstance(link, dict) or link.get("type") != "result_of":
                continue
            request_id = link.get("record")
            if not isinstance(request_id, str):
                continue
            request = records.get(request_id)
            if (
                request is not None
                and request.get("kind") == request_kind
                and result_root is not None
                and containment_roots.get(request_id) == result_root
            ):
                roots.add(result_root)
    return roots


def _validate_disk_bundle(
    root: Path,
    manifest_bytes: bytes,
    trace_bytes: bytes,
    manifest: dict[str, Any],
    rows: list[Any],
    scanner: _Scanner,
    issues: list[str],
) -> int:
    blob_refs = _blob_references(rows, issues)
    expected_files = {"manifest.json", "trace.jsonl", *blob_refs}
    expected_directories = {"blobs"}
    for relative in blob_refs:
        parent = Path(relative).parent
        while parent.as_posix() not in {".", ""}:
            expected_directories.add(parent.as_posix())
            parent = parent.parent

    secret_matches = int(not scanner.clean_json(manifest_bytes)) + int(
        not scanner.clean_json(trace_bytes)
    )
    try:
        if stat.S_IMODE(root.stat().st_mode) & 0o077:
            issues.append("DIRECTORY_PERMISSION_INVALID")
        for path in root.rglob("*"):
            relative = path.relative_to(root).as_posix()
            if path.is_symlink():
                issues.append("FILE_TYPE_INVALID")
                continue
            if path.is_dir():
                if relative not in expected_directories:
                    issues.append("UNDECLARED_ARTIFACT_FILE")
                if stat.S_IMODE(path.stat().st_mode) & 0o077:
                    issues.append("DIRECTORY_PERMISSION_INVALID")
                continue
            if not path.is_file():
                issues.append("UNDECLARED_ARTIFACT_FILE")
                continue
            if relative not in expected_files:
                issues.append("UNDECLARED_ARTIFACT_FILE")
            if stat.S_IMODE(path.stat().st_mode) & 0o077:
                issues.append("FILE_PERMISSION_INVALID")
            if relative not in {"manifest.json", "trace.jsonl"}:
                if not scanner.clean(read_regular_file(path)):
                    secret_matches += 1
    except (OSError, ValueError):
        issues.append("ARTIFACT_UNREADABLE")

    blob_bytes = 0
    for relative, blob in blob_refs.items():
        path = root / relative
        try:
            data = read_regular_file(path)
        except (OSError, ValueError):
            issues.append("BLOB_MISSING")
            continue
        blob_bytes += len(data)
        if len(data) != blob.get("bytes"):
            issues.append("BLOB_LENGTH_MISMATCH")
        if hashlib.sha256(data).hexdigest() != blob.get("sha256"):
            issues.append("BLOB_DIGEST_MISMATCH")

    blob_manifest = manifest.get("blobs")
    if isinstance(blob_manifest, dict):
        if blob_manifest.get("count") != len(blob_refs):
            issues.append("MANIFEST_BLOB_COUNT_MISMATCH")
        if blob_manifest.get("bytes") != blob_bytes:
            issues.append("MANIFEST_BLOB_BYTE_COUNT_MISMATCH")
    return secret_matches


def _blob_references(
    rows: list[Any], issues: list[str]
) -> dict[str, dict[str, Any]]:
    references: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("blob_refs"), list):
            continue
        for blob in row["blob_refs"]:
            if not isinstance(blob, dict) or not isinstance(blob.get("path"), str):
                continue
            relative = blob["path"]
            path = Path(relative)
            if (
                path.is_absolute()
                or ".." in path.parts
                or not path.parts
                or path.parts[0] != "blobs"
            ):
                continue
            prior = references.get(relative)
            if prior is not None and (
                prior.get("sha256") != blob.get("sha256")
                or prior.get("bytes") != blob.get("bytes")
            ):
                issues.append("BLOB_REFERENCE_MISMATCH")
            else:
                references[relative] = blob
    return references


def _scan_injected(
    manifest: Any, trace_bytes: bytes, scanner: _Scanner
) -> int:
    try:
        manifest_bytes = json.dumps(
            manifest, separators=(",", ":"), ensure_ascii=False
        ).encode()
    except (TypeError, ValueError):
        manifest_bytes = b""
    return int(not scanner.clean_json(manifest_bytes)) + int(
        not scanner.clean_json(trace_bytes)
    )


def _encode_rows(rows: list[Any]) -> bytes:
    if not rows:
        return b""
    return "".join(
        json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n"
        for row in rows
    ).encode()
