"""Shared primitives used by more than one official framework adapter."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import version as distribution_version
from typing import Any

from .capture_v1 import AdmissionReceipt, CaptureSource, _trust_official_source


class _Lifecycle:
    def __init__(self, deactivate: Any) -> None:
        self._deactivate = deactivate

    def deactivate(self) -> None:
        self._deactivate()

    def drain(self) -> None:
        return None


@dataclass
class _OpenTrace:
    identity: dict[str, str]
    name: str


@dataclass
class _OpenSpan:
    trace: dict[str, str]
    name: str
    start: AdmissionReceipt
    operation_name: str | None = None
    operation_id: str | None = None


class _FrameworkAdapter:
    """Small public factory shared by framework-specific source implementations."""

    def __init__(self, version: str, source_type: type[CaptureSource]) -> None:
        self.version = version
        self._source_type = source_type

    def create_source(self, client: object) -> CaptureSource:
        source = self._source_type(client, self.version)  # type: ignore[call-arg]
        return _trust_official_source(source, "deep")


def _installed_version(distribution: str, expected: str | None) -> str:
    installed = distribution_version(distribution)
    if expected is not None and expected != installed:
        raise ValueError(
            f"requested version {expected} does not match installed "
            f"{distribution} distribution {installed}"
        )
    return installed


def _explicit_turn_identity(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {}
    if isinstance(value.get("turn_id"), str) and value["turn_id"]:
        result["turn_id"] = value["turn_id"]
    turn_index = value.get("turn_index")
    if isinstance(turn_index, int) and not isinstance(turn_index, bool) and turn_index >= 0:
        result["turn_index"] = turn_index
    if isinstance(value.get("previous_turn_id"), str) and value["previous_turn_id"]:
        result["previous_turn_id"] = value["previous_turn_id"]
    return result


def _framework_metadata(name: str, seam: str, domain: str, version: str) -> dict[str, Any]:
    return {
        "name": f"official:{name}",
        "seam": seam,
        "identity_domain": domain,
        "version": version,
        "official": True,
        "coverage": [{"operation": "agent-run", "domain": domain, "role": "owner"}],
    }


def _source_qualification(
    version: str,
    *,
    exact_versions: frozenset[str],
    profile: str,
) -> dict[str, str]:
    """Describe exercised versions without turning compatibility into a range claim."""

    if version in exact_versions:
        return {"status": "exact_qualified"}
    return {
        "status": "capability_checked_unqualified",
        "profile": profile,
    }


def _record_unavailable_reasoning_gap(
    sink: Any,
    trace: dict[str, str],
    *,
    framework: str,
    affected: AdmissionReceipt,
    count: int,
    detail: str,
) -> None:
    """Record one bounded, connected gap for explicitly unavailable reasoning."""

    if count < 1:
        return
    bounded_count = min(count, 1_000_000)
    value: dict[str, Any] = {
        "kind": "unknown",
        "phase": "gap",
        "name": f"{framework}.model.reasoning.gap",
        "trace": trace,
        "native": {
            "reasoning_unavailable": True,
            "count": bounded_count,
        },
        "semantic": {
            "type": "capture.gap",
            "framework": framework,
            "reason": "unsupported_native_value",
            "count": bounded_count,
            "detail": detail[:4096],
        },
    }
    if affected.accepted and affected.record_id is not None:
        value["parent_record_id"] = affected.record_id
    sink.record(value)


def _provider_error_native(error: BaseException) -> dict[str, Any]:
    """Keep the exact exception and expose an owned provider envelope for navigation."""

    native = {"error": _raw_native(error)}
    structured = _owned_field(error, "structured_error")
    if (
        type(structured) is dict
        and type(structured.get("custom")) is dict
        and structured["custom"].get("code") == "SEMANTIC_LAYER_CAPTURE_FAILURE_V1"
    ):
        native["structured_error"] = _raw_native(structured)
    return native


def _raw_native(value: Any) -> Any:
    """Forward native evidence untouched to the SourceSink serialization owner."""

    return value


def _owned_field(value: Any, name: str, default: Any = None) -> Any:
    """Read an already-owned instance field without invoking a descriptor."""

    if value is None:
        return default
    try:
        members = object.__getattribute__(value, "__dict__")
    except BaseException:
        return default
    if not isinstance(members, dict) or name not in members:
        return default
    return dict.__getitem__(members, name)
