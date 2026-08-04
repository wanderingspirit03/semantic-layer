"""Small W3C/active-OpenTelemetry parent-context resolver with no global takeover."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class ResolvedParentContext:
    traceparent: str | None = None
    gap: Literal[
        "required_parent_context_missing",
        "invalid_traceparent",
        "parent_context_unreadable",
        "parent_context_conflict",
    ] | None = None
    error: BaseException | None = None


def resolve_parent_context(
    explicit: object | None, inherited: str | None = None
) -> ResolvedParentContext:
    try:
        if isinstance(explicit, Mapping):
            value = explicit.get("traceparent")
            required = explicit.get("required") is True
        elif explicit is None:
            value = None
            required = False
        else:
            value = explicit
            required = False
    except BaseException as error:
        return ResolvedParentContext(
            traceparent=inherited, gap="parent_context_unreadable", error=error
        )
    if value is not None:
        if not is_valid_traceparent(value):
            return ResolvedParentContext(traceparent=inherited, gap="invalid_traceparent")
        if inherited is not None and value != inherited:
            return ResolvedParentContext(
                traceparent=inherited, gap="parent_context_conflict"
            )
        return ResolvedParentContext(traceparent=inherited or value)
    if inherited is not None:
        return ResolvedParentContext(traceparent=inherited)
    active = _active_otel_traceparent()
    if active is not None:
        return ResolvedParentContext(traceparent=active)
    return (
        ResolvedParentContext(gap="required_parent_context_missing")
        if required
        else ResolvedParentContext()
    )


def is_valid_traceparent(value: object) -> bool:
    if not isinstance(value, str) or len(value) > 512:
        return False
    match = re.fullmatch(
        r"([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(.*)",
        value,
    )
    if match is None:
        return False
    version, trace_id, parent_id, _flags, future = match.groups()
    if version == "ff":
        return False
    if set(trace_id) == {"0"}:
        return False
    if set(parent_id) == {"0"}:
        return False
    if version == "00":
        return not future
    return not future or future.startswith("-")


def _active_otel_traceparent() -> str | None:
    try:
        from opentelemetry.trace import get_current_span

        context: Any = get_current_span().get_span_context()
        if not bool(getattr(context, "is_valid", False)):
            return None
        trace_id = int(context.trace_id)
        span_id = int(context.span_id)
        flags = int(context.trace_flags) & 0xFF
        candidate = f"00-{trace_id:032x}-{span_id:016x}-{flags:02x}"
        return candidate if is_valid_traceparent(candidate) else None
    except Exception:
        return None
