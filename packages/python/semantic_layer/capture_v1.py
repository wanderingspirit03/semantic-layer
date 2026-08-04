"""One deep local capture module for the internal semantic_capture_*_v1 contract."""

from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import hmac
import inspect
import json
import os
import queue
import re
import secrets
import sys
import threading
import time
import traceback
import weakref
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Mapping
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from types import GetSetDescriptorType, MappingProxyType, MemberDescriptorType, TracebackType
from typing import Any, Generic, Literal, NoReturn, Protocol, TypeAlias, TypedDict, TypeVar, cast
from urllib.parse import quote, unquote

from .parent_context import ResolvedParentContext, resolve_parent_context
from .permissions import (
    ensure_owner_only_directory,
    read_regular_file,
    reject_symlink_path_components,
    secure_owner_only,
)
from .trace import SemanticProjector

SDK_NAME = "semantic-layer-capture"
SDK_VERSION = "0.2.0b0"
QUEUE_CAPACITY = 64 * 1024 * 1024
CONTROL_RESERVE = 64 * 1024
MAX_SERIALIZATION_RETAINED_BYTES = 8 * 1024 * 1024
MAX_SERIALIZATION_NODES = 20_000
MAX_SERIALIZABLE_INT_BITS = 262_144
RETAINED_NODE_BYTES = 32
RESOURCE_LIMIT_SENTINEL_BYTES = 64
MAX_BOUNDED_CHARS = 512
MAX_COVERAGE_CLAIMS = 64
MAX_OWNERSHIP_RULES = 256
MAX_INSTALLED_SOURCES = 256
MAX_OWNERSHIP_GROUPS = 4096
MAX_OWNERSHIP_OBSERVATIONS = 512
MAX_RUNTIME_CORRELATION_ENTRIES = 4096
MAX_RUNTIME_ERROR_IDENTITIES = 4096
MAX_OPEN_SOURCE_TRACES = 4096
MAX_ERROR_MESSAGE_BYTES = 64 * 1024
MAX_ERROR_TRACEBACK_FRAMES = 256
LOSS_REASONS = (
    "credential_redaction",
    "configured_redaction",
    "scrubber_failure_payload_omitted",
    "serialization_failure",
    "unsafe_getter_avoided",
    "unsafe_helper_avoided",
    "size_overflow_blobbed",
    "size_overflow_discarded",
    "blob_scan_blocked",
    "queue_backpressure_drop",
    "persistence_failure",
    "unsupported_native_value",
    "source_rejection",
    "filter_limit_exclusion",
    "missing_parent_context",
    "parser_error_malformed_bytes",
    "crash_recovery",
    "uncertain_tail",
    "shutdown_timeout",
    "turn_order_ambiguous",
)

SourceEventKind = Literal[
    "lifecycle",
    "model",
    "tool",
    "state",
    "log",
    "error",
    "stream",
    "correlation",
    "unknown",
]
SourceEventPhase = Literal["start", "event", "end", "error", "cancelled", "gap"]


# Immutable mapping with required run_id/trace_id and the runtime-issued operation_id.
TraceIdentity: TypeAlias = Mapping[str, str]


class _SourceRecordOptional(TypedDict, total=False):
    coverage: dict[str, str]
    error_identity: BaseException
    native_identity: str
    parent_record_id: str
    semantic: dict[str, Any]


class SourceRecord(_SourceRecordOptional):
    """Public custom-source observation shape; loss rows are runtime-owned."""

    kind: SourceEventKind
    phase: SourceEventPhase
    name: str
    trace: TraceIdentity
    native: Any


class _OpenTraceRecordOptional(TypedDict, total=False):
    coverage: dict[str, str]
    native_identity: str
    conversation_id: str
    turn_id: str
    turn_index: int
    previous_turn_id: str
    parent_context: dict[str, Any]
    native: Any
    semantic: dict[str, Any]


class OpenTraceRecord(_OpenTraceRecordOptional):
    """Public custom-source lifecycle-root input shape."""

    name: str


_SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "proxyauthorization",
        "cookie",
        "setcookie",
        "apikey",
        "xapikey",
        "xgoogapikey",
        "accesstoken",
        "refreshtoken",
        "authtoken",
        "clientsecret",
        "password",
        "passwd",
        "privatekey",
        "connectionstring",
        "secretaccesskey",
    }
)
_SIGNED_QUERY_INLINE = re.compile(
    r"([?&](?:token|key|api_key|apikey|access_token|refresh_token|client_secret|"
    r"signature|sig|x-amz-signature|x-amz-security-token|x-amz-credential|"
    r"x-goog-signature|x-goog-credential|credential)=)([^&#\s\"'\\]+)",
    re.I,
)
_CONNECTION_CREDENTIAL = re.compile(
    r"\b((?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp)://)"
    r"([^@/?#\s\"'\\]+)@([^/?#\s\"'\\]+)",
    re.I,
)
_BEARER_CREDENTIAL = re.compile(
    r"((?:authorization|proxy-authorization)\s*:\s*)"
    r"(Bearer\s+)([A-Za-z0-9._~+/-]{8,}=*)",
    re.I | re.M,
)
_PROVIDER_FORMATS = (
    re.compile(r"AIza[0-9A-Za-z_-]{20,}"),
    re.compile(r"sk-or-v1-[0-9A-Fa-f]{64}(?![0-9A-Za-z_-])"),
    re.compile(r"sk-proj-[0-9A-Za-z_-]{32,}"),
    re.compile(r"sk-admin-[0-9A-Za-z_-]{32,}"),
    re.compile(r"sk-ant-api\d{2}-[0-9A-Za-z_-]{32,}"),
    re.compile(
        r"-----BEGIN ((?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY)-----"
        r"[\s\S]*?-----END \1-----"
    ),
)
_STRICT_JSON_DECODE = json.JSONDecoder().decode


def _sensitive_key(value: str) -> bool:
    return value.lower().replace("-", "").replace("_", "") in _SENSITIVE_KEYS


class _Settled:
    def __init__(self, completed: bool = True) -> None:
        self._future: Future[None] = Future()
        if completed:
            self._future.set_result(None)

    def done(self) -> bool:
        return self._future.done()

    def result(self, timeout: float | None = None) -> None:
        return self._future.result(timeout)

    def resolve(self) -> None:
        self._future.set_result(None)

    def reject(self, error: BaseException) -> None:
        self._future.set_exception(error)

    def __await__(self) -> Generator[None, None, None]:
        return asyncio.wrap_future(self._future).__await__()

    @classmethod
    def combine(cls, values: list[_Settled]) -> _Settled:
        if not values:
            return cls()
        combined = cls(completed=False)
        remaining = len(values)
        guard = threading.Lock()

        def completed(_future: Future[None]) -> None:
            nonlocal remaining
            with guard:
                remaining -= 1
                if remaining == 0:
                    combined.resolve()

        for value in values:
            value._future.add_done_callback(completed)
        return combined


@dataclass(frozen=True)
class AdmissionReceipt:
    accepted: bool
    reason: str | None = None
    record_id: str | None = None
    settled: _Settled = field(default_factory=_Settled)


@dataclass(frozen=True)
class OpenTraceReceipt(AdmissionReceipt):
    identity: TraceIdentity | None = None


@dataclass(frozen=True)
class CaptureStatus:
    state: str
    run_id: str
    artifact_path: str
    admitted: int
    persisted: int
    rejected: int
    losses: dict[str, int]
    active_sources: list[dict[str, Any]]
    queue_capacity_bytes: int
    control_reserve_bytes: int
    pending_bytes: int
    pending_control_bytes: int
    high_water_bytes: int
    coalesced_gaps: int
    last_error: str | None


class CaptureSourceSink(Protocol):
    def open_trace(self, value: OpenTraceRecord) -> OpenTraceReceipt: ...

    def record(self, value: SourceRecord) -> AdmissionReceipt: ...


class CaptureSourceLifecycle(Protocol):
    def deactivate(self) -> Any: ...

    def drain(self) -> Any: ...


class CaptureSource(ABC):
    metadata: dict[str, Any]

    @abstractmethod
    def install(self, sink: CaptureSourceSink) -> CaptureSourceLifecycle:
        raise NotImplementedError


_TRUSTED_OFFICIAL_SOURCES: dict[int, tuple[weakref.ReferenceType[CaptureSource], str]] = {}
_TRUSTED_OFFICIAL_SOURCES_LOCK = threading.RLock()


def _trust_official_source(source: CaptureSource, source_class: str) -> CaptureSource:
    if source_class not in {"deep", "provider", "otel"}:
        raise ValueError("official source class must be deep, provider, or otel")
    identity = id(source)

    def release(reference: weakref.ReferenceType[CaptureSource]) -> None:
        with _TRUSTED_OFFICIAL_SOURCES_LOCK:
            current = _TRUSTED_OFFICIAL_SOURCES.get(identity)
            if current is not None and current[0] is reference:
                _TRUSTED_OFFICIAL_SOURCES.pop(identity, None)

    reference = weakref.ref(source, release)
    with _TRUSTED_OFFICIAL_SOURCES_LOCK:
        _TRUSTED_OFFICIAL_SOURCES[identity] = (reference, source_class)
    return source


def _official_source_class(source: CaptureSource) -> str:
    with _TRUSTED_OFFICIAL_SOURCES_LOCK:
        trusted = _TRUSTED_OFFICIAL_SOURCES.get(id(source))
        if trusted is None or trusted[0]() is not source:
            return "custom"
        return trusted[1]


class _FrozenDict(dict[str, Any]):
    def _immutable(self) -> NoReturn:
        raise TypeError("installed source metadata is immutable")

    def __setitem__(self, key: str, value: Any) -> NoReturn:
        del key, value
        self._immutable()

    def __delitem__(self, key: str) -> NoReturn:
        del key
        self._immutable()

    def clear(self) -> NoReturn:
        self._immutable()

    def pop(self, key: str, default: Any = None) -> NoReturn:
        del key, default
        self._immutable()

    def popitem(self) -> NoReturn:
        self._immutable()

    def setdefault(self, key: str, default: Any = None) -> NoReturn:
        del key, default
        self._immutable()

    def update(self, *args: Any, **kwargs: Any) -> NoReturn:
        del args, kwargs
        self._immutable()


def _validate_secret_values(values: list[str] | tuple[str, ...] | None) -> None:
    for value in values or ():
        if value and len(value.encode()) < 8:
            raise ValueError("configured secret values must contain at least 8 bytes")


class _Scanner:
    detector_digest = hashlib.sha256(b"semantic-layer-credential-detectors-v4").hexdigest()

    def __init__(self, values: list[str] | tuple[str, ...] | None) -> None:
        _validate_secret_values(values)
        exact: set[str] = set()
        for value in values or ():
            if not value:
                continue
            raw = value.encode()
            json_encoded = json.dumps(value, ensure_ascii=False)[1:-1]
            percent_encoded = quote(value, safe="-._~")
            base64_encoded = base64.b64encode(raw).decode()
            base64url_encoded = base64.urlsafe_b64encode(raw).decode()
            exact.update(
                (
                    value,
                    json_encoded,
                    json_encoded.replace("/", r"\/"),
                    json.dumps(value)[1:-1],
                    percent_encoded,
                    re.sub(
                        r"%[0-9A-F]{2}",
                        lambda match: match.group(0).lower(),
                        percent_encoded,
                    ),
                    base64_encoded,
                    base64_encoded.rstrip("="),
                    base64url_encoded,
                    base64url_encoded.rstrip("="),
                )
            )
        self.exact = sorted((value for value in exact if value), key=len, reverse=True)
        self.exact_bytes = [value.encode() for value in self.exact]
        self.redaction_marker = "[REDACTED_CREDENTIAL]"
        counter = 0
        while any(exact in self.redaction_marker for exact in self.exact):
            self.redaction_marker = f"[SL:{counter:016x}]"
            counter += 1

    def scrub(self, value: Any, key: str | None = None) -> tuple[Any, int]:
        if key and _sensitive_key(key) and value is not None:
            return self.redaction_marker, 1
        if isinstance(value, str):
            result = value
            count = 0
            for exact in self.exact:
                found = result.count(exact)
                if found:
                    result = result.replace(exact, self.redaction_marker)
                    count += found
            for pattern in _PROVIDER_FORMATS:
                result, found = pattern.subn(self.redaction_marker, result)
                count += found

            def scrub_bearer(match: re.Match[str]) -> str:
                nonlocal count
                credential = match.group(3)
                if credential == self.redaction_marker:
                    return match.group(0)
                count += 1
                return f"{match.group(1)}{match.group(2)}{self.redaction_marker}"

            result = _BEARER_CREDENTIAL.sub(scrub_bearer, result)

            def scrub_connection(match: re.Match[str]) -> str:
                nonlocal count
                user_info = match.group(2)
                separator = user_info.find(":")
                credential = user_info[separator + 1 :] if separator >= 0 else user_info
                if _decoded(credential) == self.redaction_marker:
                    return match.group(0)
                count += 1
                safe_user_info = (
                    f"{user_info[:separator]}:{self.redaction_marker}"
                    if separator >= 0
                    else self.redaction_marker
                )
                return f"{match.group(1)}{safe_user_info}@{match.group(3)}"

            result = _CONNECTION_CREDENTIAL.sub(scrub_connection, result)

            def scrub_query(match: re.Match[str]) -> str:
                nonlocal count
                if _decoded(match.group(2)) == self.redaction_marker:
                    return match.group(0)
                count += 1
                return f"{match.group(1)}{self.redaction_marker}"

            result = _SIGNED_QUERY_INLINE.sub(scrub_query, result)
            return result, count
        if isinstance(value, list):
            output = []
            total = 0
            for item in value:
                clean, count = self.scrub(item)
                output.append(clean)
                total += count
            return output, total
        if isinstance(value, dict):
            output_dict: dict[str, Any] = {}
            total = 0
            for name, item in value.items():
                clean, count = self.scrub(item, str(name))
                output_dict[str(name)] = clean
                total += count
            return output_dict, total
        return value, 0

    def clean(self, value: bytes) -> bool:
        if any(exact in value for exact in self.exact_bytes):
            return False
        text = value.decode("utf-8", errors="replace")
        records = _parse_json_or_json_lines(text)
        if records is not None:
            return all(self._structured_clean(record) for record in records)
        return self._opaque_clean(text)

    def clean_json(self, value: bytes) -> bool:
        """Scan serialized JSON without treating short secrets as arbitrary substrings."""
        return self.clean(value)

    def _structured_clean(self, value: Any, key: str | None = None) -> bool:
        if key and _sensitive_key(key) and value is not None:
            return isinstance(value, str) and value == self.redaction_marker
        if isinstance(value, str):
            return self._opaque_clean(value)
        if isinstance(value, list):
            return all(self._structured_clean(item) for item in value)
        if isinstance(value, dict):
            return all(self._structured_clean(item, str(name)) for name, item in value.items())
        return True

    def _opaque_clean(self, text: str) -> bool:
        if any(exact in text for exact in self.exact):
            return False
        if any(pattern.search(text) for pattern in _PROVIDER_FORMATS):
            return False
        for match in _BEARER_CREDENTIAL.finditer(text):
            if match.group(3) != self.redaction_marker:
                return False
        for match in _CONNECTION_CREDENTIAL.finditer(text):
            user_info = match.group(2)
            separator = user_info.find(":")
            credential = user_info[separator + 1 :] if separator >= 0 else user_info
            if _decoded(credential) != self.redaction_marker:
                return False
        return all(
            _decoded(match.group(2)) == self.redaction_marker
            for match in _SIGNED_QUERY_INLINE.finditer(text)
        )


def _decoded(value: str) -> str:
    try:
        return unquote(value)
    except (UnicodeDecodeError, ValueError):
        return value


def _parse_json_or_json_lines(text: str) -> list[Any] | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return [_STRICT_JSON_DECODE(stripped)]
    except (json.JSONDecodeError, RecursionError):
        lines = [line for line in text.splitlines() if line.strip()]
        if len(lines) < 2:
            return None
        try:
            return [_STRICT_JSON_DECODE(line) for line in lines]
        except (json.JSONDecodeError, RecursionError):
            return None


@dataclass(frozen=True)
class _UnsafeAccessorOmission:
    field: str


def unsafe_accessor_omission(field: str) -> _UnsafeAccessorOmission:
    """Mark a specifically requested descriptor as intentionally unread."""

    return _UnsafeAccessorOmission(field)


def is_unsafe_accessor_omission(value: Any) -> bool:
    """Return whether a selected native field was intentionally left unread."""

    return type(value) is _UnsafeAccessorOmission


def _bounded_json_string_bytes(value: str, ceiling: int) -> int:
    """Measure only until a JSON string is known to exceed the remaining budget."""

    ceiling = max(0, ceiling)
    size = 2
    for character in value:
        code = ord(character)
        if code in {0x22, 0x5C}:
            size += 2
        elif code <= 0x1F or 0xD800 <= code <= 0xDFFF:
            size += 6
        elif code <= 0x7F:
            size += 1
        elif code <= 0x7FF:
            size += 2
        elif code <= 0xFFFF:
            size += 3
        else:
            size += 4
        if size > ceiling:
            return ceiling + 1
    return size


def _bounded_int_json_bytes(value: int, ceiling: int) -> int | None:
    """Estimate decimal JSON bytes without first allocating the decimal string."""

    bits = int.bit_length(value)
    if bits > MAX_SERIALIZABLE_INT_BITS:
        return None
    digits = 1 if bits == 0 else (bits * 30_103) // 100_000 + 1
    max_digits = sys.get_int_max_str_digits()
    if max_digits and digits >= max_digits:
        return None
    size = digits + int(value < 0)
    return size if size <= ceiling else None


def _safe(value: Any) -> tuple[Any, list[tuple[str, str]], list[dict[str, Any]]]:
    losses: list[tuple[str, str]] = []
    blobs: list[dict[str, Any]] = []
    active: dict[int, tuple[Any, str]] = {}
    nodes = 0
    retained_bytes = 0
    resource_limit_reached = False

    def resource_limit(path: str) -> dict[str, str]:
        nonlocal resource_limit_reached
        if not resource_limit_reached:
            resource_limit_reached = True
            losses.append(("serialization_failure", path))
        return {"$semantic_layer_omitted": "resource_limit"}

    def retain(size: int, path: str) -> bool:
        nonlocal retained_bytes
        if resource_limit_reached:
            return False
        working_limit = MAX_SERIALIZATION_RETAINED_BYTES - RESOURCE_LIMIT_SENTINEL_BYTES
        if size < 0 or size > working_limit - retained_bytes:
            resource_limit(path)
            return False
        retained_bytes += size
        return True

    def retain_string(value: str, path: str) -> Any:
        remaining = (
            MAX_SERIALIZATION_RETAINED_BYTES - RESOURCE_LIMIT_SENTINEL_BYTES - retained_bytes
        )
        size = _bounded_json_string_bytes(value, remaining)
        return value if retain(size, path) else resource_limit(path)

    def native_key(item: Any, index: int, path: str) -> str | None:
        item_type = type(item)
        if item_type is str:
            return cast(str, item)
        if item_type is bool:
            return str(item)
        if item_type is int:
            remaining = (
                MAX_SERIALIZATION_RETAINED_BYTES - RESOURCE_LIMIT_SENTINEL_BYTES - retained_bytes
            )
            if _bounded_int_json_bytes(item, remaining) is None:
                resource_limit(f"{path}/<key:{index}>")
                return None
            return str(item)
        if item_type is float:
            return str(item)
        if isinstance(item, Enum):
            name = inspect.getattr_static(item, "_name_", None)
            if type(name) is str:
                type_name = _safe_type_name(item, path, losses)
                if not retain(
                    _bounded_json_string_bytes(type_name, MAX_SERIALIZATION_RETAINED_BYTES),
                    path,
                ):
                    return None
                return f"{type_name}.{name}"
        losses.append(("unsupported_native_value", f"{path}/<key:{index}>"))
        return f"$semantic_layer_key_{index}"

    def binary(content: bytes, binary_path: str) -> Any:
        digest = hashlib.sha256(content).hexdigest()
        blobs.append(
            {
                "bytes": content,
                "digest": digest,
                "path": binary_path,
                "mime_type": "application/octet-stream",
            }
        )
        return {
            "$semantic_layer_binary": {
                "byte_length": len(content),
                "digest": digest,
                "inline_omitted": True,
            }
        }

    def visit(item: Any, path: str = "", depth: int = 0) -> Any:
        nonlocal nodes
        if resource_limit_reached:
            return resource_limit(path)
        nodes += 1
        if nodes > MAX_SERIALIZATION_NODES or depth > 48:
            return resource_limit(path)
        if not retain(RETAINED_NODE_BYTES, path):
            return resource_limit(path)
        item_type = type(item)
        if (
            item_type is dict
            and set(dict.keys(item)) == {"native_type", "omitted"}
            and dict.get(item, "omitted") == "resource_limit"
            and type(dict.get(item, "native_type")) is str
        ):
            return resource_limit(path)
        if item_type is _UnsafeAccessorOmission:
            losses.append(("unsafe_getter_avoided", path))
            return {"$semantic_layer_omitted": "accessor", "field": item.field}
        if item is None or item_type is bool:
            return item
        if item_type is str:
            return retain_string(item, path)
        if item_type is int:
            remaining = (
                MAX_SERIALIZATION_RETAINED_BYTES - RESOURCE_LIMIT_SENTINEL_BYTES - retained_bytes
            )
            size = _bounded_int_json_bytes(item, remaining)
            return item if size is not None and retain(size, path) else resource_limit(path)
        if item_type is float:
            if item == item and item not in (float("inf"), float("-inf")):
                return item if retain(32, path) else resource_limit(path)
            losses.append(("unsupported_native_value", path))
            return {"$semantic_layer_value": str(item)}
        if isinstance(item, bytes):
            if not retain(bytes.__len__(item) + 256, path):
                return resource_limit(path)
            content = bytes.__getitem__(item, slice(None))
            return binary(content, path)
        if isinstance(item, bytearray):
            if not retain(bytearray.__len__(item) + 256, path):
                return resource_limit(path)
            content = bytes(bytearray.__getitem__(item, slice(None)))
            return binary(content, path)
        if item_type is memoryview:
            if not retain(item.nbytes + 256, path):
                return resource_limit(path)
            content = item.tobytes()
            return binary(content, path)
        if isinstance(item, Enum):
            own = _safe_own_dict(item, path, losses)
            name = dict.get(own, "_name_")
            raw_value = dict.get(own, "_value_")
            if type(name) is not str or type(raw_value) not in {str, bool, int, float}:
                losses.append(("unsupported_native_value", path))
                return {"$semantic_layer_omitted": _safe_type_name(item, path, losses)}
            if type(raw_value) is float and (
                raw_value != raw_value or raw_value in (float("inf"), float("-inf"))
            ):
                losses.append(("unsupported_native_value", path))
                return {"$semantic_layer_omitted": _safe_type_name(item, path, losses)}
            type_name = _safe_type_name(item, path, losses)
            enum_name = retain_string(name, f"{path}/$semantic_layer_enum/name")
            enum_type = retain_string(type_name, f"{path}/$semantic_layer_enum/type")
            enum_value = visit(raw_value, f"{path}/$semantic_layer_enum/value", depth + 1)
            if resource_limit_reached:
                return resource_limit(path)
            return {
                "$semantic_layer_enum": {
                    "type": enum_type,
                    "name": enum_name,
                    "value": enum_value,
                }
            }

        identity = id(item)
        prior = active.get(identity)
        if prior is not None and prior[0] is item:
            losses.append(("unsupported_native_value", path))
            return {"$semantic_layer_ref": prior[1]}
        active[identity] = (item, path)
        try:
            if isinstance(item, BaseException):
                return _safe_error(
                    item,
                    path,
                    depth,
                    visit,
                    losses,
                    lambda: resource_limit_reached,
                )
            if isinstance(item, dict):
                output_mapping: dict[str, Any] = {}
                for index, (key, child) in enumerate(dict.items(item)):
                    if resource_limit_reached:
                        break
                    name = native_key(key, index, path)
                    if name is None:
                        break
                    key_size = _bounded_json_string_bytes(
                        name,
                        MAX_SERIALIZATION_RETAINED_BYTES - retained_bytes,
                    )
                    if not retain(key_size + 8, path):
                        break
                    output_mapping[name] = visit(child, f"{path}/{name}", depth + 1)
                return output_mapping
            if item_type in {set, frozenset}:
                values: list[Any] = []
                iterator = set.__iter__(item) if item_type is set else frozenset.__iter__(item)
                for index, child in enumerate(iterator):
                    if resource_limit_reached or not retain(1, path):
                        break
                    values.append(visit(child, f"{path}/$semantic_layer_set/{index}", depth + 1))
                return {"$semantic_layer_set": values}
            if item_type is list:
                values = []
                for index in range(list.__len__(item)):
                    if resource_limit_reached or not retain(1, path):
                        break
                    values.append(
                        visit(list.__getitem__(item, index), f"{path}/{index}", depth + 1)
                    )
                return values
            if item_type is tuple:
                values = []
                for index in range(tuple.__len__(item)):
                    if resource_limit_reached or not retain(1, path):
                        break
                    values.append(
                        visit(tuple.__getitem__(item, index), f"{path}/{index}", depth + 1)
                    )
                return values
            if callable(item):
                losses.append(("unsupported_native_value", path))
                return {"$semantic_layer_omitted": "callable"}
            own_values = _safe_own_dict(item, path, losses)
            slot_values = _safe_dataclass_slot_values(item, path, losses)
            if slot_values is not None:
                own_values.update(slot_values)
            output: dict[str, Any] = {}
            for name, child in dict.items(own_values):
                if resource_limit_reached:
                    break
                if type(name) is not str or name.startswith("_") or callable(child):
                    continue
                key_size = _bounded_json_string_bytes(
                    name,
                    MAX_SERIALIZATION_RETAINED_BYTES - retained_bytes,
                )
                if not retain(key_size + 8, path):
                    break
                output[name] = visit(child, f"{path}/{name}", depth + 1)
            if not output:
                losses.append(("unsupported_native_value", path))
                return {"$semantic_layer_omitted": _safe_type_name(item, path, losses)}
            return output
        finally:
            current = active.get(identity)
            if current is not None and current[0] is item:
                active.pop(identity, None)

    return visit(value), losses, blobs


def _safe_dataclass_slot_values(
    value: Any,
    path: str,
    losses: list[tuple[str, str]],
) -> dict[str, Any] | None:
    value_type = type(value)
    try:
        field_definitions = inspect.getattr_static(
            value_type,
            "__dataclass_fields__",
            None,
        )
    except BaseException:
        losses.append(("serialization_failure", path))
        return None
    if type(field_definitions) is not dict:
        return None

    output: dict[str, Any] = {}
    for name in dict.keys(field_definitions):
        if type(name) is not str or name.startswith("_"):
            continue
        try:
            descriptor = inspect.getattr_static(value_type, name, None)
        except BaseException:
            losses.append(("serialization_failure", f"{path}/{name}"))
            continue
        if type(descriptor) is not MemberDescriptorType:
            continue
        try:
            output[name] = descriptor.__get__(value, value_type)
        except BaseException:
            losses.append(("serialization_failure", f"{path}/{name}"))
    return output


def _safe_own_dict(value: Any, path: str, losses: list[tuple[str, str]]) -> dict[str, Any]:
    try:
        descriptor = inspect.getattr_static(value, "__dict__", None)
    except BaseException:
        losses.append(("serialization_failure", path))
        return {}
    if type(descriptor) is dict:
        return descriptor
    if type(descriptor) in {GetSetDescriptorType, MemberDescriptorType}:
        try:
            own = descriptor.__get__(value, type(value))
            return own if type(own) is dict else {}
        except BaseException:
            losses.append(("serialization_failure", path))
    return {}


def _safe_error(
    error: BaseException,
    path: str,
    depth: int,
    visit: Any,
    losses: list[tuple[str, str]],
    resource_limit_reached: Callable[[], bool],
) -> dict[str, Any]:
    own_attributes = _safe_own_dict(error, path, losses)

    def attribute(name: str, default: Any) -> Any:
        try:
            descriptor = vars(BaseException).get(name)
            if descriptor is None:
                descriptor = inspect.getattr_static(error, name, None)
            if type(descriptor) not in {GetSetDescriptorType, MemberDescriptorType}:
                return dict.get(own_attributes, name, default)
            return descriptor.__get__(error, type(error))
        except BaseException:
            losses.append(("serialization_failure", f"{path}/{name}"))
            return default

    stack = _safe_traceback_text(
        attribute("__traceback__", None),
        path,
        losses,
    )
    args = attribute("args", ())
    group_children = attribute("exceptions", None)
    is_group = (
        type(group_children) is tuple
        and type(args) is tuple
        and len(args) >= 1
        and type(tuple.__getitem__(args, 0)) is str
        and all(isinstance(child, BaseException) for child in group_children)
    )
    message: Any
    if is_group:
        count = tuple.__len__(group_children)
        suffix = "sub-exception" if count == 1 else "sub-exceptions"
        group_message = tuple.__getitem__(args, 0)
        addition = f" ({count} {suffix})"
        if (
            _bounded_json_string_bytes(group_message, MAX_ERROR_MESSAGE_BYTES)
            + len(addition.encode())
            > MAX_ERROR_MESSAGE_BYTES
        ):
            losses.append(("serialization_failure", f"{path}/message"))
            message = {"$semantic_layer_omitted": "resource_limit"}
        else:
            message = f"{group_message}{addition}"
    elif (
        type(error) is KeyError
        and type(args) is tuple
        and tuple.__len__(args) == 1
        and type(tuple.__getitem__(args, 0)) in {str, bool, int, float}
    ):
        message = _safe_error_primitive_repr(
            tuple.__getitem__(args, 0),
            path,
            losses,
        )
    else:
        message = _safe_error_message(args, path, losses)
    serialized_args = (tuple.__getitem__(args, 0),) if is_group else args
    notes = attribute("__notes__", None)
    output: dict[str, Any] = {
        "type": _safe_type_name(error, path, losses),
        "message": visit(message, f"{path}/message", depth + 1),
        "traceback": visit(stack, f"{path}/traceback", depth + 1),
        "args": visit(serialized_args, f"{path}/args", depth + 1),
        "suppress_context": bool(attribute("__suppress_context__", False)),
        "notes": visit(notes, f"{path}/notes", depth + 1) if type(notes) in {list, tuple} else [],
    }
    cause = attribute("__cause__", None)
    context = attribute("__context__", None)
    if cause is not None:
        output["cause"] = visit(cause, f"{path}/cause", depth + 1)
    if context is not None:
        output["context"] = visit(context, f"{path}/context", depth + 1)
    if is_group:
        output["exceptions"] = visit(group_children, f"{path}/exceptions", depth + 1)
    for name, child in dict.items(own_attributes):
        if resource_limit_reached():
            break
        if type(name) is str and not name.startswith("_") and not callable(child):
            output[name] = visit(child, f"{path}/{name}", depth + 1)
    return output


def _safe_error_message(args: Any, path: str, losses: list[tuple[str, str]]) -> Any:
    if type(args) is not tuple:
        losses.append(("unsafe_helper_avoided", f"{path}/message"))
        return {"$semantic_layer_omitted": "non_primitive_error_args"}
    if not args:
        return ""
    if len(args) == 1:
        value = tuple.__getitem__(args, 0)
        if type(value) is str:
            return value
        if value is None or type(value) in {bool, int, float}:
            rendered = _safe_error_primitive_text(value)
            if rendered is not None:
                return rendered
            losses.append(("serialization_failure", f"{path}/message"))
            return {"$semantic_layer_omitted": "resource_limit"}
        losses.append(("unsafe_helper_avoided", f"{path}/message"))
        return {"$semantic_layer_omitted": "non_primitive_error_args"}
    safe_values: list[str] = []
    retained = 2
    for value in args:
        if type(value) is str or value is None or type(value) in {bool, int, float}:
            rendered = _safe_error_primitive_repr_text(value)
            if rendered is None:
                losses.append(("serialization_failure", f"{path}/message"))
                return {"$semantic_layer_omitted": "resource_limit"}
            size = len(rendered.encode()) + (2 if safe_values else 0)
            if size > MAX_ERROR_MESSAGE_BYTES - retained:
                losses.append(("serialization_failure", f"{path}/message"))
                return {"$semantic_layer_omitted": "resource_limit"}
            safe_values.append(rendered)
            retained += size
        else:
            losses.append(("unsafe_helper_avoided", f"{path}/message"))
            return {"$semantic_layer_omitted": "non_primitive_error_args"}
    return f"({', '.join(safe_values)})"


def _safe_error_primitive_text(value: Any) -> str | None:
    if type(value) is int and _bounded_int_json_bytes(
        value, MAX_ERROR_MESSAGE_BYTES
    ) is None:
        return None
    try:
        rendered = str(value)
    except BaseException:
        return None
    return (
        rendered
        if _bounded_json_string_bytes(rendered, MAX_ERROR_MESSAGE_BYTES)
        <= MAX_ERROR_MESSAGE_BYTES
        else None
    )


def _safe_error_primitive_repr_text(value: Any) -> str | None:
    if type(value) is int and _bounded_int_json_bytes(
        value, MAX_ERROR_MESSAGE_BYTES
    ) is None:
        return None
    if type(value) is str and _bounded_json_string_bytes(
        value, MAX_ERROR_MESSAGE_BYTES
    ) > MAX_ERROR_MESSAGE_BYTES:
        return None
    try:
        rendered = repr(value)
    except BaseException:
        return None
    return (
        rendered
        if len(rendered.encode()) <= MAX_ERROR_MESSAGE_BYTES
        else None
    )


def _safe_error_primitive_repr(
    value: Any,
    path: str,
    losses: list[tuple[str, str]],
) -> Any:
    rendered = _safe_error_primitive_repr_text(value)
    if rendered is not None:
        return rendered
    losses.append(("serialization_failure", f"{path}/message"))
    return {"$semantic_layer_omitted": "resource_limit"}


def _safe_traceback_text(
    value: Any,
    path: str,
    losses: list[tuple[str, str]],
) -> str:
    try:
        frames = traceback.extract_tb(value, limit=MAX_ERROR_TRACEBACK_FRAMES)
    except BaseException:
        losses.append(("serialization_failure", f"{path}/traceback"))
        return ""
    lines: list[str] = []
    retained = 0
    for frame in frames:
        filename = frame.filename[:MAX_BOUNDED_CHARS]
        name = frame.name[:MAX_BOUNDED_CHARS]
        line = f'  File "{filename}", line {frame.lineno}, in {name}\n'
        encoded = len(line.encode())
        if encoded > MAX_ERROR_MESSAGE_BYTES - retained:
            losses.append(("serialization_failure", f"{path}/traceback"))
            break
        lines.append(line)
        retained += encoded
    return "".join(lines)


def _safe_type_name(value: Any, path: str, losses: list[tuple[str, str]]) -> str:
    try:
        return str(type.__getattribute__(type(value), "__name__"))
    except BaseException:
        losses.append(("serialization_failure", path))
        return "unknown"


def _normalize_manual_semantic(value: dict[str, Any]) -> dict[str, Any]:
    """Finish manual error semantics from the one already-safe error snapshot."""
    error = value.get("error")
    if not isinstance(error, dict):
        return value
    raw_type = error.get("type")
    type_name = raw_type.lower() if isinstance(raw_type, str) else "unknown"
    type_name = re.sub(r"[^a-z0-9._-]+", "_", type_name).strip("._-") or "unknown"
    message = error.get("message")
    normalized: dict[str, Any] = {
        "type": f"error.{type_name}"[:127],
        "message": message if isinstance(message, str) else "error details unavailable",
        "recoverable": False,
        "details": error,
    }
    code = error.get("code")
    if isinstance(code, str) and code:
        normalized["code"] = code[:256]
    return {**value, "error": normalized}


class _Artifact:
    def __init__(
        self,
        output: Path,
        scanner: _Scanner,
        installation_id: str | None = None,
    ) -> None:
        reject_symlink_path_components(output)
        output = output.resolve()
        ensure_owner_only_directory(output)
        self.recovery_findings = _find_stale_runs(output)
        self.run_id = _id("run")
        self.path = output / f"run-{self.run_id[4:]}"
        ensure_owner_only_directory(self.path)
        ensure_owner_only_directory(self.path / "blobs")
        self.trace_path = self.path / "trace.jsonl"
        self.trace_path.write_bytes(b"")
        secure_owner_only(self.trace_path, directory=False)
        self.manifest_path = self.path / "manifest.json"
        self.lock_path = self.path / ".writer.lock"
        descriptor = os.open(self.lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump({"pid": os.getpid(), "run_id": self.run_id, "created_at": _now()}, handle)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        secure_owner_only(self.lock_path, directory=False)
        _sync_directory(self.path)
        self.scanner = scanner
        self.installation_id = installation_id
        self.started_at = _now()
        self.state = "accepting"
        self.seq = 0
        self.admitted = 0
        self.persisted = 0
        self.rejected = 0
        self.rejection_by_reason: dict[str, int] = {}
        self.byte_count = 0
        self.high_water = 0
        self.pending_bytes = 0
        self.pending_control_bytes = 0
        self.coalesced_gap_count = 0
        self.coalesced: dict[str, dict[str, Any]] = {}
        self.losses: dict[str, int] = {}
        self.persisted_losses: dict[str, int] = {}
        self.projected_losses = 0
        self.traces: set[str] = set()
        self.sources: dict[str, dict[str, Any]] = {}
        self.activated_sources: set[str] = set()
        self.last_error: str | None = None
        self.persistence_failed = False
        self.revision = 0
        self.projector = SemanticProjector()
        self._guard = threading.RLock()
        self._write_queue: queue.Queue[
            tuple[
                bytes,
                int,
                int,
                dict[str, int],
                _Settled,
                list[dict[str, Any]],
                str,
                bool,
                int,
            ]
            | None
        ] = queue.Queue()
        self._writer = threading.Thread(
            target=self._writer_loop,
            name=f"semantic-layer-writer-{self.run_id[-8:]}",
            daemon=True,
        )
        self._writer.start()

    def register_source(
        self,
        metadata: dict[str, Any],
        source_id: str,
        source_class: str,
        *,
        active: bool = False,
    ) -> None:
        builtin = source_class in {"builtin_manual", "builtin_runtime"}
        self.sources[source_id] = {
            **metadata,
            "source_id": source_id,
            "source_class": source_class,
            "coverage": [dict(claim) for claim in metadata["coverage"]],
            "lifecycle": (
                {
                    "activation": "active",
                    "deactivation": "not_applicable",
                    "drain": "not_applicable",
                }
                if builtin
                else {
                    "activation": "active" if active else "pending",
                    "deactivation": "pending",
                    "drain": "pending",
                }
            ),
        }
        if active or builtin:
            self.activated_sources.add(source_id)
        self.write_manifest("open" if self.state == "accepting" else "closing")

    def activate_source(self, source_id: str) -> None:
        source = self.sources.get(source_id)
        if source is not None:
            source["lifecycle"]["activation"] = "active"
            self.activated_sources.add(source_id)

    def fail_source_activation(self, source_id: str) -> None:
        if source_id in self.activated_sources:
            return
        source = self.sources.get(source_id)
        if source is not None:
            source["lifecycle"]["activation"] = "failed"
            source["lifecycle"]["deactivation"] = "not_applicable"
            source["lifecycle"]["drain"] = "not_applicable"
            self.activated_sources.discard(source_id)

    def source_deactivated(self, source_id: str, ok: bool) -> None:
        source = self.sources.get(source_id)
        if source is not None:
            if not ok or source["lifecycle"]["deactivation"] != "failed":
                source["lifecycle"]["deactivation"] = "complete" if ok else "failed"
            self.activated_sources.discard(source_id)

    def source_drained(self, source_id: str, ok: bool) -> None:
        source = self.sources.get(source_id)
        if source is not None:
            if not ok or source["lifecycle"]["drain"] != "failed":
                source["lifecycle"]["drain"] = "complete" if ok else "failed"

    def degrade(self, message: str) -> None:
        self.last_error = message[:1024]

    def prepare_blobs(
        self, blobs: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        refs: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        staged: list[dict[str, Any]] = []
        seen: set[str] = set()
        for blob in blobs:
            digest = str(blob["digest"])
            if digest in seen:
                continue
            seen.add(digest)
            content = bytes(blob["bytes"])
            clean = self.scanner.clean(content)
            refs.append(
                {
                    "digest": digest,
                    "algorithm": "sha256",
                    "mime_type": str(blob["mime_type"]),
                    "byte_length": len(content),
                    "scan": "clean" if clean else "blocked",
                    "inline_omitted": True,
                    "source_path": str(blob["path"] or "/"),
                }
            )
            if not clean:
                blocked.append(blob)
                continue
            staged.append(blob)
        return refs, blocked, staged

    def admit(
        self,
        draft: dict[str, Any],
        *,
        control: bool = False,
        staged_blobs: list[dict[str, Any]] | None = None,
        allow_during_closing: bool = False,
        strict_control: bool = False,
    ) -> AdmissionReceipt:
        with self._guard:
            return self._admit_locked(
                draft,
                control=control,
                staged_blobs=staged_blobs or [],
                allow_during_closing=allow_during_closing,
                strict_control=strict_control,
            )

    def _admit_locked(
        self,
        draft: dict[str, Any],
        *,
        control: bool = False,
        staged_blobs: list[dict[str, Any]],
        allow_during_closing: bool,
        strict_control: bool,
    ) -> AdmissionReceipt:
        if self.state != "accepting" and not (
            (control or allow_during_closing) and self.state == "closing"
        ):
            return AdmissionReceipt(False, "runtime_closed")
        row = {
            **draft,
            "schema": "semantic_capture_event_v1",
            "run_id": self.run_id,
            "record_id": _id("record"),
            "seq": self.seq + 1,
            "observed_at": _now(),
            "monotonic_ns": time.monotonic_ns(),
            "provenance": {
                "language": "python",
                "sdk_name": SDK_NAME,
                "sdk_version": SDK_VERSION,
                "capture_policy": "rich_local_credential_scrubbed",
            },
        }
        data = (
            json.dumps(row, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n"
        ).encode()
        record_id = str(row["record_id"])
        if not self.scanner.clean_json(data):
            self.mark_rejected("scrubber_failure_payload_omitted")
            fallback = self.admit(
                {
                    "trace_id": draft["trace_id"],
                    "source": {
                        "source_id": "builtin/semantic-layer-runtime",
                        "name": "semantic-layer-runtime",
                        "seam": "capture-runtime",
                        "identity_domain": "semantic-layer",
                        "official": True,
                    },
                    "event_kind": "unknown",
                    "phase": "event",
                    "name": "semantic_layer.payload_omitted",
                    "native": None,
                    "semantic": {"payload_omitted": True},
                    "correlation": {},
                    "loss_refs": [],
                    "blob_refs": [],
                },
                control=True,
            )
            loss = self.record_loss(
                "scrubber_failure_payload_omitted",
                str(draft["trace_id"]),
                affected_record_id=fallback.record_id,
            )
            reason = "final_secret_scan_blocked" if fallback.accepted else "fallback_failed"
            return AdmissionReceipt(
                False,
                reason,
                settled=_Settled.combine([fallback.settled, loss.settled]),
            )
        # One capture row can project to a terminal plus a child error. Reserve a
        # conservative upper bound before mutating the stateful projector.
        staged_blob_bytes = sum(len(blob["bytes"]) for blob in staged_blobs)
        reservation_bytes = len(data) * 2 + 16 * 1024 + staged_blob_bytes
        data_pending = self.pending_bytes - self.pending_control_bytes
        if not control and data_pending + reservation_bytes > QUEUE_CAPACITY:
            self.mark_rejected("queue_backpressure_drop")
            loss = self.record_loss(
                "queue_backpressure_drop",
                str(draft["trace_id"]),
                bytes_count=reservation_bytes,
            )
            return AdmissionReceipt(False, "queue_backpressure", settled=loss.settled)
        if control and self.pending_control_bytes + reservation_bytes > CONTROL_RESERVE:
            if not strict_control:
                return self.coalesce_control(draft, reservation_bytes)
        projected = self.projector.project(row)
        projected_data = b"".join(
            (
                json.dumps(
                    record,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                + "\n"
            ).encode()
            for record in projected
        )
        projected_loss_counts: dict[str, int] = {}
        for record in projected:
            if record.get("kind") != "loss":
                continue
            loss_data = record.get("data")
            if not isinstance(loss_data, dict):
                continue
            projected_reason = loss_data.get("reason")
            count = loss_data.get("count")
            if isinstance(projected_reason, str) and type(count) is int and count > 0:
                projected_loss_counts[projected_reason] = (
                    projected_loss_counts.get(projected_reason, 0) + count
                )
        self.seq += 1
        self.admitted += 1
        self.traces.add(str(row["trace_id"]))
        if projected_data and not self.scanner.clean_json(projected_data):
            self.last_error = "projected trace final secret scan failed"
            self.persistence_failed = True
            self.mark_rejected("persistence_failure")
            return AdmissionReceipt(True, record_id=record_id)
        if not projected:
            return AdmissionReceipt(True, record_id=record_id)
        queued_bytes = len(projected_data) + staged_blob_bytes
        self.pending_bytes += queued_bytes
        if control:
            self.pending_control_bytes += queued_bytes
        self.high_water = max(self.high_water, self.pending_bytes)
        settled = _Settled(completed=False)
        self._write_queue.put(
            (
                projected_data,
                len(projected),
                sum(record["kind"] == "loss" for record in projected),
                projected_loss_counts,
                settled,
                staged_blobs,
                record_id,
                control,
                queued_bytes,
            )
        )
        return AdmissionReceipt(True, record_id=record_id, settled=settled)

    def _writer_loop(self) -> None:
        while True:
            item = self._write_queue.get()
            try:
                if item is None:
                    return
                (
                    data,
                    record_count,
                    loss_count,
                    loss_counts,
                    settled,
                    staged_blobs,
                    record_id,
                    control,
                    queued_bytes,
                ) = item
                created_blobs: list[Path] = []
                temporary_blob: Path | None = None
                try:
                    with self._guard:
                        discard = self.persistence_failed
                        if discard:
                            self.mark_rejected("persistence_failure")
                    if discard:
                        settled.resolve()
                        continue
                    for blob in staged_blobs:
                        destination = self.path / "blobs" / f"{blob['digest']}.blob"
                        if destination.exists():
                            continue
                        temporary_blob = destination.with_suffix(f".{record_id}.new")
                        descriptor = os.open(
                            temporary_blob, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                        )
                        with os.fdopen(descriptor, "wb") as handle:
                            handle.write(bytes(blob["bytes"]))
                            handle.flush()
                            os.fsync(handle.fileno())
                        os.replace(temporary_blob, destination)
                        temporary_blob = None
                        secure_owner_only(destination, directory=False)
                        created_blobs.append(destination)
                    if created_blobs:
                        _sync_directory(self.path / "blobs")
                    with self.trace_path.open("ab") as handle:
                        handle.write(data)
                        handle.flush()
                        os.fsync(handle.fileno())
                    with self._guard:
                        self.persisted += record_count
                        self.projected_losses += loss_count
                        for reason, count in loss_counts.items():
                            self.persisted_losses[reason] = (
                                self.persisted_losses.get(reason, 0) + count
                            )
                        self.byte_count += len(data)
                    settled.resolve()
                except BaseException:
                    if temporary_blob is not None:
                        try:
                            temporary_blob.unlink()
                        except OSError:
                            pass
                    for path in created_blobs:
                        try:
                            path.unlink()
                        except OSError:
                            pass
                    with self._guard:
                        self.last_error = "trace record persistence failed"
                        self.persistence_failed = True
                        self.mark_rejected("persistence_failure")
                    settled.resolve()
                finally:
                    with self._guard:
                        self.pending_bytes -= queued_bytes
                        if control:
                            self.pending_control_bytes -= queued_bytes
            finally:
                self._write_queue.task_done()

    def flush_rows(self) -> None:
        self._write_queue.join()

    def mark_rejected(self, reason: str) -> None:
        self.rejected += 1
        self.rejection_by_reason[reason] = self.rejection_by_reason.get(reason, 0) + 1

    def flush_all(self) -> None:
        self.flush_rows()
        self.drain_coalesced()
        self.flush_rows()

    def coalesce_control(self, draft: dict[str, Any], size: int) -> AdmissionReceipt:
        reason = str(draft.get("loss", {}).get("reason", "source_rejection"))
        path = draft.get("loss", {}).get("affected_path")
        ownership_marker = path if path == "/coverage/ownership/group_limit" else None
        key = json.dumps([reason, ownership_marker], separators=(",", ":"), ensure_ascii=False)
        entry = self.coalesced.get(key)
        if entry is None:
            entry = {
                "draft": draft,
                "count": 0,
                "bytes": 0,
                "settled": _Settled(completed=False),
            }
            self.coalesced[key] = entry
        entry["count"] += 1
        entry["bytes"] += size
        self.coalesced_gap_count += 1
        return AdmissionReceipt(False, "control_gap_coalesced", settled=entry["settled"])

    def drain_coalesced(self) -> None:
        if not self.coalesced:
            return
        entries = list(self.coalesced.values())
        self.coalesced.clear()
        for entry in entries:
            draft = dict(entry["draft"])
            loss = dict(draft.get("loss", {}))
            loss.update(
                {
                    "count": entry["count"],
                    "bytes": entry["bytes"],
                    "detail": "coalesced bounded control-lane gaps",
                }
            )
            draft["loss"] = loss
            receipt = self.admit(draft, control=True)
            receipt.settled.result()
            entry["settled"].resolve()

    def record_loss(
        self,
        reason: str,
        trace_id: str,
        path: str | None = None,
        affected_record_id: str | None = None,
        bytes_count: int | None = None,
        count: int = 1,
    ) -> AdmissionReceipt:
        self.losses[reason] = self.losses.get(reason, 0) + count
        return self.admit(
            {
                "trace_id": trace_id,
                "source": {
                    "source_id": "builtin/semantic-layer-runtime",
                    "name": "semantic-layer-runtime",
                    "seam": "capture-runtime",
                    "identity_domain": "semantic-layer",
                    "official": True,
                },
                "event_kind": "loss",
                "phase": "gap",
                "name": f"semantic_layer.loss.{reason}",
                "native": None,
                "semantic": {},
                "correlation": {},
                "loss": {
                    "reason": reason,
                    "stage": _loss_stage(reason),
                    "recoverable": reason
                    in {"credential_redaction", "unsafe_getter_avoided", "unsafe_helper_avoided"},
                    **({"affected_path": path} if path else {}),
                    **({"affected_record_id": affected_record_id} if affected_record_id else {}),
                    **({"bytes": bytes_count} if bytes_count is not None else {}),
                    **({"count": count} if count != 1 else {}),
                },
                "loss_refs": [],
                "blob_refs": [],
            },
            control=True,
        )

    def begin_closing(self) -> None:
        if self.state == "accepting":
            self.state = "closing"
            self.write_manifest("closing")

    def prepare_ownership_finalization(self) -> bool:
        self.flush_all()
        if not self.persistence_failed:
            return False
        self.recover_from_persistence_failure()
        return True

    def close(self) -> CaptureStatus:
        if self.state == "closed":
            return self.status()
        self.begin_closing()
        if self.persistence_failed:
            self.recover_from_persistence_failure()
        lifecycle_repaired = False
        for source in self.sources.values():
            if source["source_class"] in {"builtin_manual", "builtin_runtime"}:
                continue
            lifecycle = source["lifecycle"]
            if lifecycle["activation"] == "pending":
                lifecycle.update(
                    {
                        "activation": "failed",
                        "deactivation": "not_applicable",
                        "drain": "not_applicable",
                    }
                )
                lifecycle_repaired = True
            elif lifecycle["activation"] == "active":
                if lifecycle["deactivation"] == "pending":
                    lifecycle["deactivation"] = "failed"
                    lifecycle_repaired = True
                if lifecycle["drain"] == "pending":
                    lifecycle["drain"] = "failed"
                    lifecycle_repaired = True
        if lifecycle_repaired:
            self.degrade("source lifecycle incomplete during seal")
            self.record_loss("source_rejection", _id("trace")).settled.result()
        self.state = "closed"
        try:
            self.write_manifest("sealed")
        except BaseException:
            self.last_error = "manifest persistence failed during seal"
            self.state = "closing"
            return self.status()
        self._write_queue.put(None)
        self._write_queue.join()
        self._writer.join(timeout=1)
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass
        return self.status()

    def recover_from_persistence_failure(self) -> None:
        reported_failures = self.losses.get("persistence_failure", 0)
        failure_count = max(
            1,
            self.rejection_by_reason.get("persistence_failure", 1) - reported_failures,
        )
        try:
            if not self.trace_path.is_file() or self.trace_path.stat().st_size < self.byte_count:
                raise OSError("durable trace prefix is unavailable")
            with self.trace_path.open("r+b") as handle:
                handle.truncate(self.byte_count)
                handle.flush()
                os.fsync(handle.fileno())
            secure_owner_only(self.trace_path, directory=False)
        except OSError as error:
            self.last_error = "trace persistence failed; durable prefix could not be recovered"
            raise RuntimeError(self.last_error) from error
        self.pending_bytes = 0
        self.pending_control_bytes = 0
        for entry in self.coalesced.values():
            entry["settled"].resolve()
        self.coalesced.clear()
        self.projector = SemanticProjector(initial_seq=self.persisted)
        self.last_error = (
            "trace persistence failed; durable prefix retained and gap evidence appended"
        )
        self.persistence_failed = False
        self.losses["persistence_failure"] = reported_failures + failure_count
        self.admit(
            {
                "trace_id": _id("trace"),
                "source": {
                    "source_id": "builtin/semantic-layer-runtime",
                    "name": "semantic-layer-runtime",
                    "seam": "capture-runtime",
                    "identity_domain": "semantic-layer",
                    "official": True,
                },
                "event_kind": "loss",
                "phase": "gap",
                "name": "semantic_layer.loss.persistence_failure",
                "native": None,
                "semantic": {},
                "correlation": {},
                "loss": {
                    "reason": "persistence_failure",
                    "stage": "persist",
                    "recoverable": False,
                    "affected_path": "/trace.jsonl",
                    "count": failure_count,
                },
                "loss_refs": [],
                "blob_refs": [],
            },
            control=True,
        )
        self.flush_rows()

    def status(self) -> CaptureStatus:
        return CaptureStatus(
            self.state,
            self.run_id,
            str(self.path),
            self.admitted,
            self.persisted,
            self.rejected,
            dict(self.persisted_losses),
            [
                {
                    "name": source["name"],
                    "seam": source["seam"],
                    "identity_domain": source["identity_domain"],
                    "coverage": [dict(claim) for claim in source["coverage"]],
                    "official": source["official"],
                    **({"version": source["version"]} if source.get("version") else {}),
                    **(
                        {"qualification": dict(source["qualification"])}
                        if source.get("qualification")
                        else {}
                    ),
                }
                for source_id, source in self.sources.items()
                if source_id in self.activated_sources
            ],
            QUEUE_CAPACITY,
            CONTROL_RESERVE,
            self.pending_bytes,
            self.pending_control_bytes,
            self.high_water,
            self.coalesced_gap_count,
            self.last_error,
        )

    def write_manifest(self, state_value: str) -> None:
        self.flush_all()
        self.revision += 1
        sealed = state_value == "sealed"
        digest = _sha256_path(self.trace_path) if sealed else None
        blob_path = self.path / "blobs"
        blob_files = list(blob_path.glob("*.blob"))
        if sealed and not blob_files:
            try:
                blob_path.rmdir()
                _sync_directory(self.path)
            except OSError:
                pass
        now = _now()
        manifest_v2 = self.installation_id is not None
        manifest = {
            "schema": (
                "semantic_trace_manifest_v2"
                if manifest_v2
                else "semantic_trace_manifest_v1"
            ),
            "record_schema": "semantic_trace_record_v1",
            "bundle_id": self.run_id,
            "state": state_value,
            "sdk": {"language": "python", "version": SDK_VERSION},
            "privacy_mode": "local-rich",
            **(
                {
                    "capture_policy": "rich-credential-scrubbed",
                    "installation_id": self.installation_id,
                }
                if manifest_v2
                else {}
            ),
            "started_at": self.started_at,
            "updated_at": now,
            **({"sealed_at": now} if sealed else {}),
            "sources": [
                {
                    "id": _projected_source_id(source["source_id"]),
                    "name": str(source["name"])[:256],
                    "seam": str(source["seam"])[:256],
                    **(
                        {"version": str(source["version"])[:256]}
                        if source.get("version")
                        else {"version": SDK_VERSION}
                        if manifest_v2
                        and source["source_class"]
                        in {"builtin_manual", "builtin_runtime"}
                        else {}
                    ),
                    **(
                        {
                            "qualification": dict(
                                source.get("qualification")
                                or _default_source_qualification(source["source_class"])
                            )
                        }
                        if manifest_v2
                        else {}
                    ),
                }
                for source in self.sources.values()
            ],
            "trace": {
                "path": "trace.jsonl",
                "records": self.persisted,
                "last_seq": self.persisted,
                "bytes": self.byte_count,
                "losses": self.projected_losses,
                "sha256": digest,
            },
            "blobs": {
                "path": "blobs",
                "count": len(blob_files),
                "bytes": sum(path.stat().st_size for path in blob_files),
            },
        }
        data = (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode()
        if not self.scanner.clean_json(data):
            raise RuntimeError("manifest final secret scan blocked persistence")
        temporary = self.manifest_path.with_suffix(".json.new")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.manifest_path)
        secure_owner_only(self.manifest_path, directory=False)
        _sync_directory(self.path)


class _SourceSink:
    def __init__(self, runtime: _Runtime, metadata: dict[str, Any]) -> None:
        self.runtime = runtime
        self.metadata = metadata

    def open_trace(self, value: OpenTraceRecord) -> OpenTraceReceipt:
        if not self.runtime.source_admission_open or self.runtime.state == "closed":
            return OpenTraceReceipt(False, "source_frozen", None)
        faults: list[tuple[str, BaseException]] = []
        captured = _snapshot_open_trace_input(value, faults)
        scope = _CURRENT_SCOPE.get()
        ambient = scope if scope is not None and scope.active else None
        parent_fault = next((error for field, error in faults if field == "parent_context"), None)
        parent_context = (
            ResolvedParentContext(
                traceparent=ambient.traceparent if ambient else None,
                gap="parent_context_unreadable",
                error=parent_fault,
            )
            if parent_fault is not None
            else resolve_parent_context(
                captured.get("parent_context"), ambient.traceparent if ambient else None
            )
        )
        identity = _trace_identity(
            self.runtime.artifact.run_id,
            ambient.trace_id if ambient else _id("trace"),
            _id("operation"),
        )
        source_identities = self.runtime.turn_identity(captured)
        identities = {**source_identities, **ambient.identities} if ambient else source_identities
        coverage_identity = None
        if not any(field_name.startswith("coverage") for field_name, _error in faults):
            coverage_identity = self.runtime.coverage_registry.coverage_identity(
                self.metadata,
                captured.get("native_identity"),
                captured.get("coverage"),
            )
        if not self.runtime.reserve_source_trace():
            loss = self.runtime.artifact.record_loss(
                "source_rejection",
                identity["trace_id"],
                "/correlation/open_source_traces/capacity",
            )
            return OpenTraceReceipt(
                False,
                "correlation_capacity",
                settled=loss.settled,
            )
        try:
            receipt = self.runtime.record(
                self.metadata,
                {
                    "kind": "lifecycle",
                    "phase": "start",
                    "name": captured["name"],
                    "trace": identity,
                    "native_identity": captured.get("native_identity"),
                    "coverage": captured.get("coverage"),
                    "_coverage_identity": coverage_identity,
                    "_coverage_invalid": any(
                        field_name.startswith("coverage") for field_name, _error in faults
                    ),
                    "native": captured.get("native"),
                    "semantic": captured.get("semantic"),
                    "parent_record_id": ambient.parent_record_id if ambient else None,
                    "traceparent": parent_context.traceparent,
                },
                identities,
                allow_during_closing=True,
            )
        except BaseException:
            self.runtime.release_source_trace()
            raise
        self.runtime.record_context_gap(parent_context, identity, identities, receipt.record_id)
        self.runtime.record_capture_input_faults(
            faults, identity, identities, receipt.record_id, parent_context.traceparent
        )
        if receipt.accepted:
            self.runtime.open_source_traces[_source_operation_key(identity)] = {
                "metadata": self.metadata,
                "identity": identity,
                "identities": identities,
                "name": captured["name"],
                "start_record_id": receipt.record_id,
                "traceparent": parent_context.traceparent,
                "coverage": coverage_identity,
            }
        else:
            self.runtime.release_source_trace()
        return OpenTraceReceipt(
            accepted=receipt.accepted,
            reason=receipt.reason,
            record_id=receipt.record_id,
            settled=receipt.settled,
            identity=identity if receipt.accepted else None,
        )

    def record(self, value: SourceRecord) -> AdmissionReceipt:
        if not self.runtime.source_admission_open:
            return AdmissionReceipt(False, "source_frozen")
        faults: list[tuple[str, BaseException]] = []
        (
            captured,
            snapshot_trace,
            snapshot_operation_id,
            identity_shape_valid,
            error_identity,
        ) = _snapshot_source_record_input(value, faults)
        if captured is None:
            evidence = self._identity_evidence(snapshot_trace, snapshot_operation_id)
            issued = (
                self.runtime.open_source_traces.get(_source_operation_key(snapshot_trace))
                if identity_shape_valid and snapshot_trace is not None
                else None
            )
            identity_accepted = (
                issued is not None
                and issued["metadata"] is self.metadata
                and snapshot_trace is not None
                and dict(issued["identity"]) == dict(snapshot_trace)
            )
            trace = (
                evidence["identity"]
                if evidence is not None
                else _trace_identity(self.runtime.artifact.run_id, _id("trace"))
            )
            settlements = [
                self.runtime.record_capture_input_faults(
                    faults,
                    trace,
                    evidence["identities"] if evidence else {},
                    evidence["start_record_id"] if evidence else None,
                    evidence.get("traceparent") if evidence else None,
                )
            ]
            if not identity_accepted:
                rejection_reason = (
                    "invalid_identity_shape"
                    if not identity_shape_valid or snapshot_trace is None
                    else (
                        "unissued_or_closed_trace_identity"
                        if snapshot_trace["run_id"] == self.runtime.artifact.run_id
                        else "foreign_run_identity"
                    )
                )
                settlements.append(
                    self._record_identity_rejection(
                        snapshot_trace,
                        snapshot_operation_id,
                        None,
                        rejection_reason,
                        evidence,
                        trace,
                    )
                )
            settled = _Settled.combine(settlements)
            return AdmissionReceipt(False, "invalid_record", settled=settled)
        key = _source_operation_key(captured["trace"])
        candidate = self.runtime.open_source_traces.get(key)
        opened = (
            candidate
            if identity_shape_valid
            and candidate is not None
            and candidate["metadata"] is self.metadata
            and dict(candidate["identity"]) == dict(captured["trace"])
            else None
        )
        if opened is None:
            evidence = self._identity_evidence(captured["trace"])
            reason = (
                "invalid_record"
                if captured["trace"]["run_id"] == self.runtime.artifact.run_id
                else "foreign_run_identity"
            )
            trace = (
                evidence["identity"]
                if evidence is not None
                else _trace_identity(self.runtime.artifact.run_id, _id("trace"))
            )
            rejection_reason = (
                "invalid_identity_shape"
                if not identity_shape_valid
                else (
                    "unissued_or_closed_trace_identity"
                    if captured["trace"]["run_id"] == self.runtime.artifact.run_id
                    else "foreign_run_identity"
                )
            )
            settled = _Settled.combine(
                [
                    self._record_identity_rejection(
                        captured["trace"],
                        captured["trace"].get("operation_id"),
                        captured,
                        rejection_reason,
                        evidence,
                        trace,
                    ),
                    self.runtime.record_capture_input_faults(
                        faults,
                        trace,
                        evidence["identities"] if evidence else {},
                        evidence["start_record_id"] if evidence else None,
                        evidence.get("traceparent") if evidence else None,
                    ),
                ]
            )
            return AdmissionReceipt(False, reason, settled=settled)
        coverage_invalid = any(field_name.startswith("coverage") for field_name, _error in faults)
        if coverage_invalid:
            captured["_coverage_invalid"] = True
        enriched = captured
        if opened and not enriched.get("parent_record_id"):
            enriched["parent_record_id"] = opened["start_record_id"]
        if opened and opened.get("traceparent"):
            enriched["traceparent"] = opened["traceparent"]
        if opened and not coverage_invalid:
            selected = enriched.get("coverage")
            native_identity = enriched.get("native_identity")
            if selected is None:
                enriched["_coverage_identity"] = opened.get("coverage")
            elif isinstance(native_identity, str) and native_identity:
                enriched["_coverage_identity"] = self.runtime.coverage_registry.coverage_identity(
                    self.metadata, native_identity, selected
                )
            elif opened.get("coverage"):
                selected_surface = self.runtime.coverage_registry.coverage_surface(
                    self.metadata, selected
                )
                if selected_surface and (
                    selected_surface["operation"] == opened["coverage"]["operation"]
                    and selected_surface["domain"] == opened["coverage"]["domain"]
                ):
                    enriched["_coverage_identity"] = opened["coverage"]
        try:
            receipt = self.runtime.record(
                self.metadata,
                enriched,
                opened["identities"] if opened else None,
                allow_during_closing=True,
            )
        except BaseException as error:
            faults.append(("record", error))
            self.runtime.record_capture_input_faults(
                faults,
                captured["trace"],
                opened["identities"] if opened else {},
                None,
                opened.get("traceparent") if opened else None,
            )
            return AdmissionReceipt(False, "invalid_record")
        self.runtime.record_capture_input_faults(
            faults,
            captured["trace"],
            opened["identities"] if opened else {},
            receipt.record_id,
            opened.get("traceparent") if opened else None,
        )
        semantic = captured.get("semantic")
        ambient = _CURRENT_SCOPE.get()
        if (
            receipt.accepted
            and error_identity is not None
            and (
                captured.get("kind") == "error"
                or (
                    captured.get("kind") == "lifecycle"
                    and captured.get("phase") == "error"
                )
            )
            and isinstance(semantic, dict)
            and "error" in semantic
            and ambient is not None
            and ambient.active
            and ambient.trace_id == captured["trace"]["trace_id"]
        ):
            self.runtime.remember_source_error_identity(
                captured["trace"]["trace_id"],
                error_identity,
            )
        if (
            receipt.accepted
            and captured.get("kind") == "lifecycle"
            and captured.get("phase") in {"end", "error", "cancelled"}
            and enriched.get("parent_record_id") == opened["start_record_id"]
        ):
            if self.runtime.open_source_traces.pop(key, None) is not None:
                self.runtime.release_source_trace()
        return receipt

    def _identity_evidence(
        self, submitted: TraceIdentity | None, operation_id: str | None = None
    ) -> dict[str, Any] | None:
        key = operation_id or (_source_operation_key(submitted) if submitted is not None else None)
        by_operation = self.runtime.open_source_traces.get(key) if key else None
        if by_operation is not None and by_operation["metadata"] is self.metadata:
            return by_operation
        if submitted is None:
            return None
        for opened in self.runtime.open_source_traces.values():
            if (
                opened["metadata"] is self.metadata
                and opened["identity"]["trace_id"] == submitted["trace_id"]
            ):
                return opened
        return None

    def _record_identity_rejection(
        self,
        submitted_trace: TraceIdentity | None,
        submitted_operation_id: str | None,
        submitted: dict[str, Any] | None,
        reason: str,
        evidence: dict[str, Any] | None,
        trace: TraceIdentity,
    ) -> _Settled:
        gap = self.runtime.record(
            _RUNTIME_SOURCE,
            {
                "kind": "correlation",
                "phase": "gap",
                "name": "semantic_layer.source_identity.gap",
                "trace": trace,
                "native": {
                    "reason": reason,
                    "submitted_identity": (
                        dict(submitted_trace) if submitted_trace is not None else None
                    ),
                    **(
                        {"submitted_operation_id": submitted_operation_id}
                        if submitted_operation_id
                        else {}
                    ),
                    **(
                        {
                            "submitted_record": {
                                "kind": submitted["kind"],
                                "phase": submitted["phase"],
                                "name": submitted["name"],
                                **(
                                    {"native_identity": submitted["native_identity"]}
                                    if submitted.get("native_identity")
                                    else {}
                                ),
                            }
                        }
                        if submitted is not None
                        else {}
                    ),
                },
                "parent_record_id": evidence["start_record_id"] if evidence else None,
                "traceparent": evidence.get("traceparent") if evidence else None,
            },
            evidence["identities"] if evidence else {},
            allow_during_closing=True,
        )
        loss = self.runtime.artifact.record_loss(
            "source_rejection",
            trace["trace_id"],
            "/trace",
            gap.record_id or (evidence["start_record_id"] if evidence else None),
        )
        return _Settled.combine([gap.settled, loss.settled])


def _source_value(
    value: object, key: str, faults: list[tuple[str, BaseException]]
) -> tuple[bool, Any]:
    try:
        if type(value) is dict:
            return True, dict.get(value, key)
        if type(value) is MappingProxyType:
            return True, cast(Mapping[str, Any], value).get(key)
        raise TypeError("capture source input must be an exact dict")
    except BaseException as error:
        faults.append((key, error))
        return False, None


def _snapshot_open_trace_input(
    value: object, faults: list[tuple[str, BaseException]]
) -> dict[str, Any]:
    captured: dict[str, Any] = {"name": "source.unreadable"}
    for key in (
        "name",
        "native_identity",
        "conversation_id",
        "turn_id",
        "turn_index",
        "previous_turn_id",
        "parent_context",
        "native",
        "semantic",
        "coverage",
    ):
        ok, child = _source_value(value, key, faults)
        if ok and child is not None:
            captured[key] = child
    if not isinstance(captured.get("name"), str) or not captured["name"].strip():
        faults.append(("name", TypeError("name must be a non-empty string")))
        captured["name"] = "source.unreadable"
    for key in ("native_identity", "conversation_id", "turn_id", "previous_turn_id"):
        if key in captured and (
            not isinstance(captured[key], str)
            or (
                key == "native_identity"
                and (not captured[key].strip() or len(captured[key]) > MAX_BOUNDED_CHARS)
            )
        ):
            faults.append((key, TypeError(f"{key} must be a bounded string")))
            captured.pop(key)
    if "turn_index" in captured and (
        not isinstance(captured["turn_index"], int) or captured["turn_index"] < 0
    ):
        faults.append(("turn_index", TypeError("turn_index must be non-negative")))
        captured.pop("turn_index")
    if "coverage" in captured:
        coverage = _snapshot_coverage(captured["coverage"], faults)
        if coverage is None:
            captured.pop("coverage")
        else:
            captured["coverage"] = coverage
    if "semantic" in captured and type(captured["semantic"]) is not dict:
        faults.append(("semantic", TypeError("semantic must be a mapping")))
        captured.pop("semantic")
    return captured


def _snapshot_source_record_input(
    value: object, faults: list[tuple[str, BaseException]]
) -> tuple[
    dict[str, Any] | None,
    TraceIdentity | None,
    str | None,
    bool,
    BaseException | None,
]:
    observed = {
        key: _source_value(value, key, faults)
        for key in (
            "trace",
            "kind",
            "phase",
            "name",
            "native",
            "native_identity",
            "parent_record_id",
            "semantic",
            "coverage",
            "error_identity",
        )
    }
    trace_ok, trace_value = observed["trace"]
    identity_shape_valid = trace_ok and _exact_source_trace_shape(trace_value)
    trace, operation_id = _snapshot_source_trace(trace_value, faults) if trace_ok else (None, None)
    kind = observed["kind"][1] if observed["kind"][0] else None
    phase = observed["phase"][1] if observed["phase"][0] else None
    name = observed["name"][1] if observed["name"][0] else None
    error_identity: BaseException | None = None
    error_identity_ok, error_identity_value = observed["error_identity"]
    if error_identity_ok and error_identity_value is not None:
        if isinstance(error_identity_value, BaseException):
            error_identity = error_identity_value
        else:
            faults.append(
                (
                    "error_identity",
                    TypeError("error_identity must be a BaseException"),
                )
            )
    supported_kinds = {
        "lifecycle",
        "model",
        "tool",
        "state",
        "log",
        "error",
        "stream",
        "correlation",
        "unknown",
    }
    if kind not in supported_kinds:
        faults.append(
            (
                "event_kind",
                TypeError("event_kind must be a source event kind; loss rows are runtime-owned"),
            )
        )
    if phase not in {"start", "event", "end", "error", "cancelled", "gap"}:
        faults.append(("phase", TypeError("phase must be a supported event phase")))
    if not isinstance(name, str) or not name.strip():
        faults.append(("name", TypeError("name must be a non-empty string")))
    if (
        trace is None
        or not isinstance(kind, str)
        or kind not in supported_kinds
        or not isinstance(phase, str)
        or not isinstance(name, str)
        or not name.strip()
    ):
        return None, trace, operation_id, identity_shape_valid, error_identity
    captured: dict[str, Any] = {
        "trace": trace,
        "kind": kind,
        "phase": phase,
        "name": name,
        "native": observed["native"][1] if observed["native"][0] else None,
    }
    for key in ("native_identity", "parent_record_id"):
        ok, child = observed[key]
        if ok and child is not None:
            if isinstance(child, str) and (
                key != "native_identity"
                or (bool(child.strip()) and len(child) <= MAX_BOUNDED_CHARS)
            ):
                captured[key] = child
            else:
                faults.append((key, TypeError(f"{key} must be a bounded string")))
    semantic_ok, semantic = observed["semantic"]
    if semantic_ok and semantic is not None:
        if type(semantic) is dict:
            captured["semantic"] = semantic
        else:
            faults.append(("semantic", TypeError("semantic must be a mapping")))
    coverage_ok, coverage = observed["coverage"]
    if coverage_ok and coverage is not None:
        selected = _snapshot_coverage(coverage, faults)
        if selected is not None:
            captured["coverage"] = selected
    return captured, trace, operation_id, identity_shape_valid, error_identity


def _exact_source_trace_shape(value: object) -> bool:
    expected = {"run_id", "trace_id", "operation_id"}
    try:
        if type(value) is dict:
            return set(dict.keys(value)) == expected
        if type(value) is MappingProxyType:
            return set(cast(Mapping[str, Any], value).keys()) == expected
        return False
    except BaseException:
        return False


def _snapshot_coverage(
    value: object, faults: list[tuple[str, BaseException]]
) -> dict[str, str] | None:
    operation_ok, operation = _source_value(value, "operation", faults)
    domain_ok, domain = _source_value(value, "domain", faults)
    if not operation_ok or not isinstance(operation, str) or not operation.strip():
        faults.append(("coverage.operation", TypeError("coverage.operation must be a string")))
    if not domain_ok or not isinstance(domain, str) or not domain.strip():
        faults.append(("coverage.domain", TypeError("coverage.domain must be a string")))
    if (
        not isinstance(operation, str)
        or not operation.strip()
        or not isinstance(domain, str)
        or not domain.strip()
    ):
        return None
    return {"operation": operation, "domain": domain}


def _snapshot_source_trace(
    value: object, faults: list[tuple[str, BaseException]]
) -> tuple[TraceIdentity | None, str | None]:
    run_ok, run_id = _source_value(value, "run_id", faults)
    trace_ok, trace_id = _source_value(value, "trace_id", faults)
    operation_ok, operation_id = _source_value(value, "operation_id", faults)
    if not run_ok or not isinstance(run_id, str):
        faults.append(("trace.run_id", TypeError("trace.run_id must be a string")))
    if not trace_ok or not isinstance(trace_id, str):
        faults.append(("trace.trace_id", TypeError("trace.trace_id must be a string")))
    accepted_operation_id: str | None = None
    if operation_ok and operation_id is not None:
        if isinstance(operation_id, str):
            accepted_operation_id = operation_id
        else:
            faults.append(("trace.operation_id", TypeError("trace.operation_id must be a string")))
    if not run_ok or not trace_ok or not isinstance(run_id, str) or not isinstance(trace_id, str):
        return None, accepted_operation_id
    return _trace_identity(run_id, trace_id, accepted_operation_id), accepted_operation_id


R = TypeVar("R")


class ObservationScope:
    def __init__(
        self,
        runtime: _Runtime,
        trace: dict[str, str],
        identities: dict[str, Any],
        traceparent: str | None = None,
        manual_call_ids: set[str] | None = None,
        manual_call_ids_lock: threading.RLock | None = None,
        supports_output: bool = True,
    ) -> None:
        self.runtime = runtime
        self.trace = trace
        self.identities = identities
        self.active = True
        self.parent_record_id: str | None = None
        self.traceparent = traceparent
        self.captured_error: BaseException | None = None
        self.output: Any = None
        self.output_observed = False
        self._supports_output = supports_output
        self._manual_call_ids = manual_call_ids if manual_call_ids is not None else set()
        self._manual_call_ids_lock = manual_call_ids_lock or threading.RLock()

    @property
    def trace_id(self) -> str:
        return self.trace["trace_id"]

    def set_output(self, value: R) -> R:
        if not self.active:
            raise RuntimeError("observation scope is closed")
        if not self._supports_output:
            raise RuntimeError("set_output is only supported on a root observation")
        self.output = value
        self.output_observed = True
        return value

    def emit(self, name: str, value: Any = None) -> AdmissionReceipt:
        if not self.active:
            self.runtime.artifact.record_loss("source_rejection", self.trace_id)
            return AdmissionReceipt(False, "scope_closed")
        semantic_type = name if name in {"stream.delta", "stream.terminal"} else None
        state_type = name if name.startswith("state.") or ".state" in name else None
        semantic: dict[str, Any] | None = None
        if state_type:
            semantic = {
                "type": "state.transition",
                "state_type": state_type,
                "value": value,
            }
        elif semantic_type:
            semantic = {"type": semantic_type}
        kind = (
            "stream"
            if semantic_type
            else "state"
            if state_type
            else "log"
            if name.startswith("log.")
            else "unknown"
        )
        return self.runtime.record(
            _MANUAL,
            {
                "kind": kind,
                "phase": "event",
                "name": name,
                "trace": self.trace,
                "native": None if state_type else value,
                **({"semantic": semantic} if semantic is not None else {}),
                "parent_record_id": self.parent_record_id,
                "traceparent": self.traceparent,
            },
            self.identities,
        )

    def turn(self, name: str, **options: Any) -> _ChildObservation[Any]:
        return _ChildObservation(
            self.runtime,
            name,
            options,
            self.trace,
            self.parent_record_id,
            self.traceparent,
            self._manual_call_ids,
            self._manual_call_ids_lock,
        )

    def tool(
        self,
        name: str,
        value: Any,
        run: Any,
        *,
        call_id: str | None = None,
    ) -> Any:
        if not self.active:
            self.runtime.artifact.record_loss("source_rejection", self.trace_id)
            return run(value)
        requested_native_identity = _manual_call_identity(call_id)
        with self._manual_call_ids_lock:
            duplicate_call_id = (
                requested_native_identity is not None
                and requested_native_identity in self._manual_call_ids
            )
            native_identity = None if duplicate_call_id else requested_native_identity
            if requested_native_identity is not None and not duplicate_call_id:
                self._manual_call_ids.add(requested_native_identity)
        if call_id is not None and native_identity is None:
            reason = "duplicate_call_id" if duplicate_call_id else "invalid_call_id"
            self.runtime.record(
                _MANUAL,
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "manual.tool.identity",
                    "trace": self.trace,
                    "native": {"call_id": call_id},
                    "semantic": {
                        "type": "capture.gap",
                        "reason": reason,
                        "detail": (
                            "The supplied manual tool call ID was reused; "
                            "local correlation was used."
                            if duplicate_call_id
                            else "The supplied manual tool call ID was invalid; "
                            "local correlation was used."
                        ),
                    },
                    "parent_record_id": self.parent_record_id,
                    "traceparent": self.traceparent,
                },
                self.identities,
            )
        start = self.runtime.record(
            _MANUAL,
            {
                "kind": "tool",
                "phase": "start",
                "name": name,
                "trace": self.trace,
                "native": None,
                "semantic": {
                    "type": "tool.execution",
                    "name": name,
                    "input": value,
                },
                **({"native_identity": native_identity} if native_identity is not None else {}),
                "parent_record_id": self.parent_record_id,
                "traceparent": self.traceparent,
            },
            self.identities,
        )
        try:
            result = run(value)
        except BaseException as error:
            self.captured_error = error
            self.runtime.record(
                _MANUAL,
                {
                    "kind": "tool",
                    "phase": "error",
                    "name": name,
                    "trace": self.trace,
                    "native": None,
                    "semantic": {
                        "type": "tool.error",
                        "status": "failed",
                        "error": error,
                    },
                    **({"native_identity": native_identity} if native_identity is not None else {}),
                    "parent_record_id": start.record_id,
                    "traceparent": self.traceparent,
                },
                self.identities,
                allow_during_closing=True,
            )
            raise
        if inspect.isawaitable(result):

            async def wait() -> Any:
                task = asyncio.current_task()
                self.runtime.track_async_task(task)
                try:
                    output = await result
                except BaseException as error:
                    self.captured_error = error
                    self.runtime.record(
                        _MANUAL,
                        {
                            "kind": "tool",
                            "phase": "error",
                            "name": name,
                            "trace": self.trace,
                            "native": None,
                            "semantic": {
                                "type": "tool.error",
                                "status": "failed",
                                "error": error,
                            },
                            **(
                                {"native_identity": native_identity}
                                if native_identity is not None
                                else {}
                            ),
                            "parent_record_id": start.record_id,
                            "traceparent": self.traceparent,
                        },
                        self.identities,
                        allow_during_closing=True,
                    )
                    raise
                else:
                    self.runtime.record(
                        _MANUAL,
                        {
                            "kind": "tool",
                            "phase": "end",
                            "name": name,
                            "trace": self.trace,
                            "native": None,
                            "semantic": {
                                "type": "tool.result",
                                "status": "succeeded",
                                "output": output,
                            },
                            **(
                                {"native_identity": native_identity}
                                if native_identity is not None
                                else {}
                            ),
                            "parent_record_id": start.record_id,
                            "traceparent": self.traceparent,
                        },
                        self.identities,
                        allow_during_closing=True,
                    )
                    return output
                finally:
                    self.runtime.untrack_async_task(task)

            return wait()
        self.runtime.record(
            _MANUAL,
            {
                "kind": "tool",
                "phase": "end",
                "name": name,
                "trace": self.trace,
                "native": None,
                "semantic": {
                    "type": "tool.result",
                    "status": "succeeded",
                    "output": result,
                },
                **({"native_identity": native_identity} if native_identity is not None else {}),
                "parent_record_id": start.record_id,
                "traceparent": self.traceparent,
            },
            self.identities,
        )
        return result


class _Observation(Generic[R]):
    def __init__(self, runtime: _Runtime, name: str, options: dict[str, Any]) -> None:
        self.runtime = runtime
        self.name = name
        self.options = options
        self.trace = {"run_id": runtime.artifact.run_id, "trace_id": _id("trace")}
        self.identities = runtime.turn_identity(options)
        self.scope = ObservationScope(runtime, self.trace, self.identities)
        self.parent_context = ResolvedParentContext()
        self.token: contextvars.Token[ObservationScope | None] | None = None

    def __enter__(self) -> ObservationScope:
        self.parent_context = resolve_parent_context(self.options.get("parent_context"))
        self.scope.traceparent = self.parent_context.traceparent
        receipt = self.runtime.record(
            _MANUAL,
            {
                "kind": "lifecycle",
                "phase": "start",
                "name": self.name,
                "trace": self.trace,
                "native": {
                    "metadata": self.options.get("metadata", {}),
                },
                "semantic": {
                    "type": "agent.run",
                    "name": self.name,
                    "input": self.options.get("input"),
                },
                "traceparent": self.parent_context.traceparent,
            },
            self.identities,
        )
        self.scope.parent_record_id = receipt.record_id
        self.runtime.record_context_gap(
            self.parent_context, self.trace, self.identities, receipt.record_id
        )
        self.runtime.record_turn_conflict(
            self.options, self.identities, self.trace["trace_id"], receipt.record_id
        )
        self.token = _CURRENT_SCOPE.set(self.scope)
        if self.options.get("previous_turn_id") and (
            self.options.get("turn_index") is None or self.options.get("turn_index") == 0
        ):
            self.runtime.artifact.record_loss("turn_order_ambiguous", self.trace["trace_id"])
        return self.scope

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        if self.token is not None:
            _CURRENT_SCOPE.reset(self.token)
        self.scope.active = False
        if (
            error is not None
            and self.scope.captured_error is not error
            and not self.runtime.source_error_identity_was_admitted(
                self.trace["trace_id"],
                error,
            )
        ):
            self.runtime.record(
                _MANUAL,
                {
                    "kind": "error",
                    "phase": "error",
                    "name": self.name,
                    "trace": self.trace,
                    "native": None,
                    "semantic": {"type": "agent.error", "error": error},
                    "parent_record_id": self.scope.parent_record_id,
                    "traceparent": self.scope.traceparent,
                },
                self.identities,
                allow_during_closing=True,
            )
        try:
            self.runtime.record(
                _MANUAL,
                {
                    "kind": "lifecycle",
                    "phase": "error" if error is not None else "end",
                    "name": self.name,
                    "trace": self.trace,
                    "native": None,
                    "semantic": {
                        "type": "agent.run",
                        "status": "failed" if error is not None else "succeeded",
                        **(
                            {"output": self.scope.output}
                            if error is None and self.scope.output_observed
                            else {}
                        ),
                    },
                    "parent_record_id": self.scope.parent_record_id,
                    "traceparent": self.scope.traceparent,
                },
                self.identities,
                allow_during_closing=True,
            )
        finally:
            self.runtime.clear_source_error_identities(self.trace["trace_id"])
        return False

    async def __aenter__(self) -> ObservationScope:
        scope = self.__enter__()
        self.runtime.track_async_task(asyncio.current_task())
        return scope

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        try:
            return self.__exit__(kind, error, tb)
        finally:
            self.runtime.untrack_async_task(asyncio.current_task())


class _ChildObservation(_Observation[R]):
    def __init__(
        self,
        runtime: _Runtime,
        name: str,
        options: dict[str, Any],
        trace: dict[str, str],
        parent_record_id: str | None,
        inherited_traceparent: str | None,
        manual_call_ids: set[str],
        manual_call_ids_lock: threading.RLock,
    ) -> None:
        super().__init__(runtime, name, options)
        self.trace = trace
        self.scope = ObservationScope(
            runtime,
            trace,
            self.identities,
            inherited_traceparent,
            manual_call_ids,
            manual_call_ids_lock,
            supports_output=False,
        )
        self.parent_record_id = parent_record_id
        self.inherited_traceparent = inherited_traceparent

    def __enter__(self) -> ObservationScope:
        self.parent_context = resolve_parent_context(
            self.options.get("parent_context"), self.inherited_traceparent
        )
        self.scope.traceparent = self.parent_context.traceparent
        receipt = self.runtime.record(
            _MANUAL,
            {
                "kind": "lifecycle",
                "phase": "start",
                "name": self.name,
                "trace": self.trace,
                "native": {
                    "metadata": self.options.get("metadata", {}),
                },
                "semantic": {
                    "type": "scope",
                    "scope_type": "turn",
                    "name": self.name,
                },
                "parent_record_id": self.parent_record_id,
                "traceparent": self.parent_context.traceparent,
            },
            self.identities,
        )
        self.scope.parent_record_id = receipt.record_id
        self.runtime.record_context_gap(
            self.parent_context, self.trace, self.identities, receipt.record_id
        )
        self.runtime.record_turn_conflict(
            self.options, self.identities, self.trace["trace_id"], receipt.record_id
        )
        self.token = _CURRENT_SCOPE.set(self.scope)
        return self.scope

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        if self.token is not None:
            _CURRENT_SCOPE.reset(self.token)
        self.scope.active = False
        if (
            error is not None
            and self.scope.captured_error is not error
            and not self.runtime.source_error_identity_was_admitted(
                self.trace["trace_id"],
                error,
            )
        ):
            self.runtime.record(
                _MANUAL,
                {
                    "kind": "error",
                    "phase": "error",
                    "name": self.name,
                    "trace": self.trace,
                    "native": None,
                    "semantic": {"type": "agent.error", "error": error},
                    "parent_record_id": self.scope.parent_record_id,
                    "traceparent": self.scope.traceparent,
                },
                self.identities,
                allow_during_closing=True,
            )
        if error is not None:
            parent_scope = _CURRENT_SCOPE.get()
            if parent_scope is not None and parent_scope.active:
                parent_scope.captured_error = error
        self.runtime.record(
            _MANUAL,
            {
                "kind": "lifecycle",
                "phase": "error" if error is not None else "end",
                "name": self.name,
                "trace": self.trace,
                "native": None,
                "semantic": {
                    "type": "scope",
                    "scope_type": "turn",
                    "name": self.name,
                    "status": "failed" if error is not None else "succeeded",
                },
                "parent_record_id": self.scope.parent_record_id,
                "traceparent": self.scope.traceparent,
            },
            self.identities,
            allow_during_closing=True,
        )
        return False


class CaptureHandle:
    def __init__(self, runtime: _Runtime) -> None:
        self._runtime = runtime

    def instrument(self, *, adapter: Any, client: object) -> CaptureSourceLifecycle:
        source = adapter if hasattr(adapter, "install") else adapter.create_source(client)
        return self.install_source(source)

    def install_source(self, source: CaptureSource) -> CaptureSourceLifecycle:
        if self._runtime.state != "accepting":
            raise RuntimeError("capture is not accepting sources")
        return self._runtime.install_source(source)

    def observe(self, name: str, **options: Any) -> _Observation[Any]:
        return _Observation(self._runtime, name, options)

    def tool(
        self,
        name: str,
        value: Any,
        run: Any,
        *,
        call_id: str | None = None,
    ) -> Any:
        scope = _CURRENT_SCOPE.get()
        return scope.tool(name, value, run, call_id=call_id) if scope else run(value)

    def emit(self, name: str, value: Any = None) -> AdmissionReceipt:
        scope = _CURRENT_SCOPE.get()
        return (
            scope.emit(name, value) if scope else AdmissionReceipt(False, "no_active_observation")
        )

    def status(self) -> CaptureStatus:
        return self._runtime.artifact.status()

    def flush(self) -> CaptureStatus:
        if self._runtime.state == "closed":
            return self.status()
        self._runtime.artifact.write_manifest(
            "open" if self._runtime.state == "accepting" else "closing"
        )
        return self.status()

    def shutdown(self) -> CaptureStatus:
        return self._runtime.shutdown()

    async def shutdown_async(self) -> CaptureStatus:
        return await self._runtime.shutdown_async()


class _CoverageReservation:
    def __init__(
        self,
        coverage: dict[str, str] | None = None,
        overflow: Literal["group_limit"] | None = None,
        rollback: Callable[[], None] | None = None,
    ) -> None:
        self.coverage = coverage
        self.overflow = overflow
        self._rollback = rollback
        self._settled = False

    def settle(self, accepted: bool) -> None:
        if self._settled:
            return
        self._settled = True
        if not accepted and self._rollback is not None:
            self._rollback()


class _CoverageRegistry:
    """Exact, run-local ownership policy hidden behind the existing source-kit seam."""

    def __init__(
        self, service_name: str, configured: dict[str, Any] | None, identity_key: bytes
    ) -> None:
        frozen = _freeze_source_ownership(service_name, configured)
        self.namespace: str = frozen["namespace"]
        self.rules: tuple[dict[str, str], ...] = tuple(frozen["rules"])
        self.identity_key = identity_key
        self.authorities: dict[str, str] = {}
        self.active_sources: set[str] = set()
        self.source_ids: set[str] = set()
        self.source_names = {"manual", "semantic-layer-runtime"}
        self.installed: dict[int, tuple[dict[str, Any], str]] = {}
        self.installed_by_source_id: dict[str, tuple[dict[str, Any], str]] = {}
        self._registration_guard = threading.RLock()
        self.groups: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.group_overflows = 0
        self.state: Literal["collecting", "frozen", "finalized"] = "collecting"
        self.finalized_at: str | None = None
        self.frozen_decisions: tuple[dict[str, Any], ...] | None = None
        self.configured_policy = {
            "namespace": self.namespace,
            "rules": [dict(rule) for rule in self.rules],
        }
        policy_json = json.dumps(self.configured_policy, separators=(",", ":"), ensure_ascii=False)
        self.compatibility_sha256 = hashlib.sha256(policy_json.encode()).hexdigest()

    @property
    def compatibility(self) -> str:
        return self.compatibility_sha256

    def register(
        self, source: CaptureSource, metadata: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        _validate_source_metadata(metadata)
        declared_name = str(metadata["name"])
        authority = _official_source_class(source)
        source_id = (
            f"official/{declared_name.removeprefix('official:')}"
            if authority != "custom"
            else f"{self.namespace}/{declared_name}"
        )
        _bounded("source metadata source_id", source_id)
        _validate_source_id(source_id)
        coverage = tuple(_FrozenDict(dict(claim)) for claim in metadata["coverage"])
        identity_domain = str(metadata["identity_domain"])
        if authority == "custom":
            identity_domain = f"{self.namespace}/{identity_domain}"
        _bounded("source metadata identity_domain", identity_domain)
        candidate: dict[str, Any] = _FrozenDict(
            {
                **metadata,
                "official": authority != "custom",
                "identity_domain": identity_domain,
                "coverage": coverage,
                **(
                    {"qualification": _FrozenDict(dict(metadata["qualification"]))}
                    if isinstance(metadata.get("qualification"), dict)
                    else {}
                ),
            }
        )
        with self._registration_guard:
            existing = self.installed_by_source_id.get(source_id)
            if declared_name in self.source_names:
                if (
                    authority != "custom"
                    and existing is not None
                    and existing[1] == authority
                    and existing[0] == candidate
                ):
                    return existing[0], True
                raise TypeError(f"duplicate or reserved source name: {declared_name}")
            if existing is not None or source_id in self.source_ids:
                raise TypeError(f"duplicate source identity: {source_id}")
            if len(self.source_ids) >= MAX_INSTALLED_SOURCES:
                raise TypeError(f"at most {MAX_INSTALLED_SOURCES} capture sources may be installed")
            self.authorities[source_id] = authority
            self.source_names.add(declared_name)
            self.source_ids.add(source_id)
            self.installed[id(candidate)] = (candidate, source_id)
            self.installed_by_source_id[source_id] = (candidate, authority)
            return candidate, False

    def source_id(self, metadata: dict[str, Any]) -> str | None:
        if metadata is _MANUAL:
            return "builtin/manual"
        if metadata is _RUNTIME_SOURCE:
            return "builtin/semantic-layer-runtime"
        installed = self.installed.get(id(metadata))
        return installed[1] if installed is not None and installed[0] is metadata else None

    def source_class(self, metadata: dict[str, Any]) -> str | None:
        source_id = self.source_id(metadata)
        return self.authorities.get(source_id) if source_id is not None else None

    def activate(self, metadata: dict[str, Any]) -> None:
        source_id = self.source_id(metadata)
        if source_id is not None:
            self.active_sources.add(source_id)

    def coverage_identity(
        self,
        metadata: dict[str, Any],
        native_identity: str | None,
        selected: dict[str, str] | None,
    ) -> dict[str, str] | None:
        if not native_identity or not native_identity.strip():
            return None
        coverage = self.coverage_surface(metadata, selected)
        if coverage is None:
            return None
        return {
            **coverage,
            "identity_token": hmac.new(
                self.identity_key, native_identity.encode(), hashlib.sha256
            ).hexdigest(),
        }

    def coverage_surface(
        self, metadata: dict[str, Any], selected: dict[str, str] | None
    ) -> dict[str, str] | None:
        source_id = self.source_id(metadata)
        if source_id is None:
            return None
        declared = selected or _single_coverage(metadata)
        if declared is None or not _declares_coverage(metadata, declared):
            return None
        operation = str(declared["operation"])
        domain = str(declared["domain"])
        if self.authorities.get(source_id) == "custom" and not self._promoted(
            source_id, operation, domain
        ):
            domain = f"{self.namespace}/{domain}"
        return {"operation": operation, "domain": domain}

    def reserve(
        self, metadata: dict[str, Any], coverage: dict[str, str] | None
    ) -> _CoverageReservation:
        if self.state != "collecting" or coverage is None:
            return _CoverageReservation()
        source_id = self.source_id(metadata)
        if source_id is None or source_id.startswith("builtin/"):
            return _CoverageReservation()
        key = (
            coverage["operation"],
            coverage["domain"],
            coverage["identity_token"],
        )
        group = self.groups.get(key)
        if group is None:
            if len(self.groups) >= MAX_OWNERSHIP_GROUPS:
                self.group_overflows += 1
                return _CoverageReservation(overflow="group_limit")
            group = {"coverage": dict(coverage), "sources": {}}
            self.groups[key] = group
        sources: dict[str, dict[str, Any]] = group["sources"]
        participant = sources.get(source_id)
        sources[source_id] = {
            "count": int(participant["count"]) + 1 if participant is not None else 1,
            "role": self._coverage_role(metadata, coverage),
        }

        def rollback() -> None:
            retained = self.groups.get(key)
            if retained is None:
                return
            retained_sources: dict[str, dict[str, Any]] = retained["sources"]
            retained_participant = retained_sources.get(source_id)
            count = (
                int(retained_participant["count"])
                if retained_participant is not None
                else 0
            )
            if count > 1:
                assert retained_participant is not None
                retained_sources[source_id] = {
                    **retained_participant,
                    "count": count - 1,
                }
            else:
                retained_sources.pop(source_id, None)
            if not retained_sources:
                self.groups.pop(key, None)

        return _CoverageReservation(dict(coverage), rollback=rollback)

    def freeze(self) -> tuple[dict[str, Any], ...]:
        if self.frozen_decisions is not None:
            return self.frozen_decisions
        self.assert_policy_resolved()
        self.state = "frozen"
        decisions: list[dict[str, Any]] = []
        groups = sorted(
            self.groups.values(),
            key=lambda group: (
                group["coverage"]["operation"].encode(),
                group["coverage"]["domain"].encode(),
                group["coverage"]["identity_token"].encode(),
            ),
        )
        for group in groups:
            eligible = sorted(
                (
                    source
                    for source in group["sources"]
                    if source in self.active_sources
                ),
                key=lambda item: item.encode(),
            )
            if len(eligible) < 2:
                continue
            coverage = group["coverage"]
            owners = [
                source
                for source in eligible
                if group["sources"][source]["role"] == "owner"
            ]
            primary = owners[0] if len(owners) == 1 else None
            status = "ambiguous" if len(owners) > 1 else "owned" if primary else "evidence_only"
            decisions.append(
                {
                    "operation": coverage["operation"],
                    "domain": coverage["domain"],
                    "identity_token": coverage["identity_token"],
                    "status": status,
                    **({"primary_source_id": primary} if primary else {}),
                    "participant_source_ids": eligible,
                    "secondary_source_ids": (
                        [source for source in eligible if source != primary] if primary else []
                    ),
                }
            )
        self.frozen_decisions = tuple(decisions)
        return self.frozen_decisions

    def reset_evidence(self) -> None:
        if self.state != "collecting":
            raise RuntimeError("coverage ownership evidence is already frozen")
        self.groups.clear()
        self.group_overflows = 0

    def reset_after_persistence_recovery(self) -> None:
        self.groups.clear()
        self.group_overflows = 0
        self.frozen_decisions = None
        self.state = "collecting"
        self.finalized_at = None

    def finalize(self) -> None:
        if self.state == "collecting":
            raise RuntimeError("coverage ownership must freeze before finalization")
        self.state = "finalized"
        self.finalized_at = self.finalized_at or _now()

    def assert_policy_resolved(self) -> None:
        unresolved = sorted(
            {rule["source_id"] for rule in self.rules if rule["source_id"] not in self.source_ids},
            key=lambda value: value.encode(),
        )
        if unresolved:
            raise TypeError(
                "source ownership policy references uninstalled source: " + ", ".join(unresolved)
            )

    def manifest(self) -> dict[str, Any]:
        decisions = self.frozen_decisions or ()
        policy = {
            "namespace": self.namespace,
            "rules": [dict(rule) for rule in self.rules if rule["source_id"] in self.source_ids],
        }
        policy_json = json.dumps(policy, separators=(",", ":"), ensure_ascii=False)
        finalization = (
            {"state": "finalized", "finalized_at": self.finalized_at}
            if self.state == "finalized"
            else {"state": self.state}
        )
        return {
            "policy": policy,
            "policy_sha256": hashlib.sha256(policy_json.encode()).hexdigest(),
            "token": {
                "algorithm": "hmac-sha256",
                "scope": "run_local",
                "key_persisted": False,
            },
            "finalization": finalization,
            "counters": {
                "groups": len(self.groups),
                "decisions": len(decisions),
                "ambiguities": sum(item["status"] == "ambiguous" for item in decisions),
                "evidence_only": sum(item["status"] == "evidence_only" for item in decisions),
                "group_overflows": self.group_overflows,
                "citation_overflows": 0,
            },
        }

    def _promoted(self, source_id: str, operation: str, domain: str) -> bool:
        return any(
            rule["action"] == "promote"
            and rule["source_id"] == source_id
            and rule["operation"] == operation
            and rule["domain"] == domain
            for rule in self.rules
        )

    def _coverage_role(
        self, metadata: dict[str, Any], coverage: dict[str, str]
    ) -> Literal["owner", "evidence"]:
        source_id = self.source_id(metadata)
        if source_id is None:
            return "owner"
        roles: list[str] = []
        for claim in metadata.get("coverage", []):
            if not isinstance(claim, dict):
                continue
            operation = claim.get("operation")
            domain = claim.get("domain")
            if not isinstance(operation, str) or not isinstance(domain, str):
                continue
            surface_domain = domain
            if self.authorities.get(source_id) == "custom" and not self._promoted(
                source_id, operation, domain
            ):
                surface_domain = f"{self.namespace}/{domain}"
            if operation == coverage["operation"] and surface_domain == coverage["domain"]:
                roles.append("evidence" if claim.get("role") == "evidence" else "owner")
        return "evidence" if roles and all(role == "evidence" for role in roles) else "owner"


class _Runtime:
    def __init__(
        self,
        output: Path,
        service_name: str,
        installation_id: str | None,
        secret_values: list[str] | tuple[str, ...] | None,
        identity_mode: str,
        identity_key: str | bytes | None,
        shutdown_deadline_ms: int,
        source_ownership: dict[str, Any] | None,
    ) -> None:
        self.scanner = _Scanner(secret_values)
        self.identity_key = _identity_key(identity_key)
        self.identity_mode = identity_mode
        self.shutdown_deadline_ms = shutdown_deadline_ms
        self.coverage_registry = _CoverageRegistry(
            service_name, source_ownership, self.identity_key
        )
        self.artifact = _Artifact(output, self.scanner, installation_id)
        self.compatibility = (
            str(output.resolve()),
            service_name,
            installation_id,
            identity_mode,
            _identity_key_digest(identity_key),
            shutdown_deadline_ms,
            _secret_digest(secret_values),
            self.coverage_registry.compatibility,
        )
        self.state = "accepting"
        self.source_admission_open = True
        self.lifecycles: list[tuple[CaptureSourceLifecycle, str]] = []
        self.active_async: dict[asyncio.Task[Any], int] = {}
        self.open_source_traces: dict[str, dict[str, Any]] = {}
        self._open_source_trace_slots = threading.BoundedSemaphore(MAX_OPEN_SOURCE_TRACES)
        self.source_error_identities: dict[str, list[BaseException]] = {}
        self._source_error_identity_count = 0
        self._source_error_identity_guard = threading.RLock()
        self.conversation_order: dict[str, tuple[str, int]] = {}
        self.turn_indexes: dict[str, int] = {}
        self._shutdown_guard = threading.RLock()
        self._shutdown_async_task: asyncio.Task[CaptureStatus] | None = None
        self._shutdown_status: CaptureStatus | None = None
        self._background_shutdown_tasks: set[asyncio.Future[Any]] = set()
        self.artifact.register_source(_MANUAL, "builtin/manual", "builtin_manual", active=True)
        self.artifact.register_source(
            _RUNTIME_SOURCE,
            "builtin/semantic-layer-runtime",
            "builtin_runtime",
            active=True,
        )
        for run, uncertain_tail in self.artifact.recovery_findings:
            trace_id = _id("trace")
            self.artifact.record_loss("crash_recovery", trace_id, f"/prior_runs/{run}")
            if uncertain_tail:
                self.artifact.record_loss(
                    "uncertain_tail", trace_id, f"/prior_runs/{run}/trace.jsonl"
                )

    def track_async_task(self, task: asyncio.Task[Any] | None) -> None:
        if task is not None:
            self.active_async[task] = self.active_async.get(task, 0) + 1

    def untrack_async_task(self, task: asyncio.Task[Any] | None) -> None:
        if task is None:
            return
        remaining = self.active_async.get(task, 0) - 1
        if remaining > 0:
            self.active_async[task] = remaining
        else:
            self.active_async.pop(task, None)

    def close_source_traces(self) -> None:
        for key, opened in list(self.open_source_traces.items()):
            self.record(
                opened["metadata"],
                {
                    "kind": "lifecycle",
                    "phase": "cancelled",
                    "name": opened["name"],
                    "trace": opened["identity"],
                    "native": {"shutdown_cancelled": True},
                    "parent_record_id": opened["start_record_id"],
                    "_coverage_identity": opened.get("coverage"),
                },
                opened["identities"],
                allow_during_closing=True,
            )
            if self.open_source_traces.pop(key, None) is not None:
                self.release_source_trace()

    def reserve_source_trace(self) -> bool:
        return self._open_source_trace_slots.acquire(blocking=False)

    def release_source_trace(self) -> None:
        self._open_source_trace_slots.release()

    def remember_source_error_identity(
        self,
        trace_id: str,
        error: BaseException,
    ) -> None:
        with self._source_error_identity_guard:
            identities = self.source_error_identities.setdefault(trace_id, [])
            if any(candidate is error for candidate in identities):
                return
            if self._source_error_identity_count >= MAX_RUNTIME_ERROR_IDENTITIES:
                if not identities:
                    self.source_error_identities.pop(trace_id, None)
                return
            identities.append(error)
            self._source_error_identity_count += 1

    def source_error_identity_was_admitted(
        self,
        trace_id: str,
        error: BaseException,
    ) -> bool:
        with self._source_error_identity_guard:
            return any(
                candidate is error
                for candidate in self.source_error_identities.get(trace_id, ())
            )

    def clear_source_error_identities(self, trace_id: str) -> None:
        with self._source_error_identity_guard:
            removed = self.source_error_identities.pop(trace_id, ())
            self._source_error_identity_count -= len(removed)

    def install_source(self, source: CaptureSource) -> CaptureSourceLifecycle:
        metadata = dict(source.metadata)
        required = {"name", "seam", "identity_domain", "coverage"}
        if not required.issubset(metadata) or not isinstance(metadata["coverage"], list):
            raise TypeError("source metadata requires name, seam, identity_domain, and coverage")
        metadata, reused = self.coverage_registry.register(source, metadata)
        source_id = self.coverage_registry.source_id(metadata)
        source_class = self.coverage_registry.source_class(metadata)
        assert source_id is not None and source_class is not None
        artifact_class = "framework_deep" if source_class == "deep" else source_class
        # Reserve the declaration before invoking user/framework code. Some official seams
        # synchronously replay their current state while a processor is being installed;
        # those rows must never precede their manifest declaration, even if installation
        # subsequently fails.
        if not reused:
            self.artifact.register_source(metadata, source_id, artifact_class, active=False)
        try:
            lifecycle = source.install(_SourceSink(self, metadata))
            if not callable(getattr(lifecycle, "deactivate", None)) or not callable(
                getattr(lifecycle, "drain", None)
            ):
                raise TypeError("source lifecycle requires deactivate and drain")
        except BaseException:
            self.artifact.fail_source_activation(source_id)
            self.artifact.degrade("source installation failed")
            self.artifact.record_loss("source_rejection", _id("trace"))
            raise
        self.coverage_registry.activate(metadata)
        self.artifact.activate_source(source_id)
        self.lifecycles.append((lifecycle, source_id))
        return lifecycle

    def record(
        self,
        metadata: dict[str, Any],
        value: dict[str, Any],
        identities: dict[str, Any] | None = None,
        *,
        allow_during_closing: bool = False,
    ) -> AdmissionReceipt:
        trace = value["trace"]
        if trace["run_id"] != self.artifact.run_id:
            return AdmissionReceipt(False, "foreign_run_identity")
        native, native_losses, native_blobs = _safe(value.get("native"))
        semantic, semantic_losses, semantic_blobs = _safe(value.get("semantic", {}))
        native, native_redactions = self.scanner.scrub(native)
        semantic, semantic_redactions = self.scanner.scrub(semantic)
        if metadata is _MANUAL and isinstance(semantic, dict):
            semantic = _normalize_manual_semantic(semantic)
        blob_refs, blocked_blobs, staged_blobs = self.artifact.prepare_blobs(
            native_blobs + semantic_blobs
        )
        source_id = self.coverage_registry.source_id(metadata)
        coverage_identity = None
        if not value.get("_coverage_invalid"):
            inherited = value.get("_coverage_identity")
            if isinstance(inherited, dict):
                coverage_identity = inherited
            else:
                native_identity = value.get("native_identity")
                selected = value.get("coverage")
                coverage_identity = self.coverage_registry.coverage_identity(
                    metadata,
                    native_identity if isinstance(native_identity, str) else None,
                    selected if isinstance(selected, dict) else None,
                )
        reservation = self.coverage_registry.reserve(metadata, coverage_identity)
        receipt = self.artifact.admit(
            {
                "trace_id": trace["trace_id"],
                **(identities or {}),
                "source": {
                    "name": metadata["name"],
                    "seam": metadata["seam"],
                    "identity_domain": metadata["identity_domain"],
                    "official": bool(metadata.get("official", False)),
                    **({"source_id": source_id} if source_id else {}),
                    **({"version": metadata["version"]} if metadata.get("version") else {}),
                },
                **({"coverage": reservation.coverage} if reservation.coverage else {}),
                **(
                    {"native_identity": value["native_identity"]}
                    if value.get("native_identity")
                    else {}
                ),
                "event_kind": value["kind"],
                "phase": value["phase"],
                "name": value["name"],
                "native": native,
                "semantic": semantic if isinstance(semantic, dict) else {"value": semantic},
                "correlation": (
                    {
                        **(
                            {"parent_record_id": value["parent_record_id"]}
                            if value.get("parent_record_id")
                            else {}
                        ),
                        **(
                            {"traceparent": value["traceparent"]}
                            if value.get("traceparent")
                            else {}
                        ),
                    }
                ),
                "loss_refs": [],
                "blob_refs": blob_refs,
            },
            control=False,
            staged_blobs=staged_blobs,
            allow_during_closing=allow_during_closing,
        )
        reservation.settle(receipt.accepted)
        if reservation.overflow is not None:
            self.artifact.degrade(f"coverage ownership {reservation.overflow}")
            self.artifact.record_loss(
                "source_rejection",
                str(trace["trace_id"]),
                "/coverage/ownership/group_limit",
            )
        if receipt.accepted:
            for reason, path in (*native_losses, *semantic_losses):
                self.artifact.record_loss(reason, trace["trace_id"], path, receipt.record_id)
            redaction_count = native_redactions + semantic_redactions
            if redaction_count:
                self.artifact.record_loss(
                    "credential_redaction",
                    trace["trace_id"],
                    affected_record_id=receipt.record_id,
                    count=redaction_count,
                )
            for blob in blocked_blobs:
                self.artifact.record_loss(
                    "blob_scan_blocked",
                    trace["trace_id"],
                    str(blob["path"]),
                    receipt.record_id,
                )
        return receipt

    def record_context_gap(
        self,
        context: ResolvedParentContext,
        trace: TraceIdentity,
        identities: dict[str, Any],
        affected_record_id: str | None,
    ) -> None:
        if context.gap is None:
            return
        gap = self.record(
            _RUNTIME_SOURCE,
            {
                "kind": "correlation",
                "phase": "gap",
                "name": "semantic_layer.context.gap",
                "trace": trace,
                "native": {
                    "reason": context.gap,
                    "boundary": "process",
                    "error": context.error,
                },
                "parent_record_id": affected_record_id,
                "traceparent": context.traceparent,
            },
            identities,
        )
        self.artifact.record_loss(
            "missing_parent_context",
            trace["trace_id"],
            affected_record_id=gap.record_id or affected_record_id,
        )

    def record_capture_input_faults(
        self,
        faults: list[tuple[str, BaseException]],
        trace: TraceIdentity,
        identities: dict[str, Any],
        affected_record_id: str | None,
        traceparent: str | None,
    ) -> _Settled:
        if not faults:
            return _Settled()
        gap = self.record(
            _RUNTIME_SOURCE,
            {
                "kind": "correlation",
                "phase": "gap",
                "name": "semantic_layer.capture_input.gap",
                "trace": trace,
                "native": {
                    "fields": [field for field, _error in faults],
                    "errors": [{"field": field, "error": error} for field, error in faults],
                },
                "parent_record_id": affected_record_id,
                "traceparent": traceparent,
            },
            identities,
        )
        settlements = [gap.settled]
        for fault_field, _error in faults:
            settlements.append(
                self.artifact.record_loss(
                    "serialization_failure",
                    trace["trace_id"],
                    "/event_kind"
                    if fault_field == "event_kind"
                    else f"/capture_input/{fault_field}",
                    gap.record_id or affected_record_id,
                ).settled
            )
        return _Settled.combine(settlements)

    def turn_identity(self, value: dict[str, Any]) -> dict[str, Any]:
        def identifier(prefix: str, raw: str) -> str:
            if self.identity_mode == "raw":
                start = f"{prefix}_"
                normalized = re.sub(r"[^a-z0-9._:-]", "_", raw.lower())
                candidate = f"{start}{normalized}"
                changed = normalized != raw or not 8 <= len(candidate) <= 128
                if not changed:
                    return candidate
                suffix = f"_{hashlib.sha256(raw.encode()).hexdigest()[:16]}"
                retained = normalized[: 128 - len(start) - len(suffix)]
                return f"{start}{retained}{suffix}"
            return (
                f"{prefix}_{hmac.new(self.identity_key, raw.encode(), hashlib.sha256).hexdigest()}"
            )

        return {
            **(
                {"conversation_id": identifier("conversation", str(value["conversation_id"]))}
                if value.get("conversation_id")
                else {}
            ),
            **(
                {"turn_id": identifier("turn", str(value["turn_id"]))}
                if value.get("turn_id")
                else {}
            ),
            **(
                {"turn_index": int(value["turn_index"])}
                if value.get("turn_index") is not None
                else {}
            ),
            **(
                {"previous_turn_id": identifier("turn", str(value["previous_turn_id"]))}
                if value.get("previous_turn_id")
                else {}
            ),
        }

    def record_turn_conflict(
        self,
        value: dict[str, Any],
        identities: dict[str, Any],
        trace_id: str,
        affected_record_id: str | None,
    ) -> None:
        turn_id = identities.get("turn_id")
        turn_index = value.get("turn_index")
        if not turn_id or turn_index is None:
            return
        index = int(turn_index)
        prior_index = self.turn_indexes.get(str(turn_id))
        previous = identities.get("previous_turn_id")
        conversation_id = identities.get("conversation_id")
        conversation = (
            self.conversation_order.get(str(conversation_id)) if conversation_id else None
        )
        conflict = (
            previous == turn_id
            or (prior_index is not None and prior_index != index)
            or (
                conversation is not None and conversation[0] != turn_id and index <= conversation[1]
            )
            or (conversation is not None and previous is not None and previous != conversation[0])
        )
        if conflict:
            self.artifact.record_loss(
                "turn_order_ambiguous", trace_id, affected_record_id=affected_record_id
            )
        turn_key = str(turn_id)
        self.turn_indexes.pop(turn_key, None)
        self.turn_indexes[turn_key] = index
        if len(self.turn_indexes) > MAX_RUNTIME_CORRELATION_ENTRIES:
            self.turn_indexes.pop(next(iter(self.turn_indexes)))
            self.artifact.record_loss(
                "turn_order_ambiguous",
                trace_id,
                "/correlation/turn_indexes/evicted",
                affected_record_id,
            )
        if conversation_id and (conversation is None or index >= conversation[1]):
            conversation_key = str(conversation_id)
            self.conversation_order.pop(conversation_key, None)
            self.conversation_order[conversation_key] = (str(turn_id), index)
            if len(self.conversation_order) > MAX_RUNTIME_CORRELATION_ENTRIES:
                self.conversation_order.pop(next(iter(self.conversation_order)))
                self.artifact.record_loss(
                    "turn_order_ambiguous",
                    trace_id,
                    "/correlation/conversation_order/evicted",
                    affected_record_id,
                )

    def shutdown(self) -> CaptureStatus:
        with self._shutdown_guard:
            if self._shutdown_status is not None:
                return self._shutdown_status
            if self.state == "closed":
                return self.artifact.status()
            if self.state == "closing" and not self.source_admission_open:
                return self._retry_seal()
            self.coverage_registry.assert_policy_resolved()
            self.state = "closing"
            self.artifact.begin_closing()
            if self.active_async:
                self.artifact.record_loss("shutdown_timeout", _id("trace"))
                self.artifact.degrade("shutdown called synchronously with active child operations")
            for lifecycle, source_id in reversed(self.lifecycles):
                ok = True
                try:
                    result = lifecycle.deactivate()
                    if inspect.isawaitable(result):
                        ok = False
                        self._teardown_failure()
                        close = getattr(result, "close", None)
                        if callable(close):
                            close()
                except BaseException:
                    ok = False
                    self._teardown_failure()
                self.artifact.source_deactivated(source_id, ok)
            for lifecycle, source_id in reversed(self.lifecycles):
                ok = True
                try:
                    result = lifecycle.drain()
                    if inspect.isawaitable(result):
                        ok = False
                        self._teardown_failure()
                        close = getattr(result, "close", None)
                        if callable(close):
                            close()
                except BaseException:
                    ok = False
                    self._teardown_failure()
                self.artifact.source_drained(source_id, ok)
            return self._finalize_shutdown()

    async def shutdown_async(self) -> CaptureStatus:
        if self._shutdown_status is not None:
            return self._shutdown_status
        if self.state == "closing" and not self.source_admission_open:
            return self._retry_seal()
        self.coverage_registry.assert_policy_resolved()
        if self._shutdown_async_task is None:
            self._shutdown_async_task = asyncio.create_task(self._run_shutdown_async())
        return await asyncio.shield(self._shutdown_async_task)

    async def _run_shutdown_async(self) -> CaptureStatus:
        if self.state == "closed":
            return self.artifact.status()
        self.state = "closing"
        self.artifact.begin_closing()
        deadline = time.monotonic() + self.shutdown_deadline_ms / 1000

        async def before_deadline(
            value: Any,
        ) -> Literal["complete", "failed", "timeout"]:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                if inspect.isawaitable(value):
                    task = asyncio.ensure_future(value)
                    done, _ = await asyncio.wait({task}, timeout=remaining)
                    if not done:
                        self._background_shutdown_tasks.add(task)
                        task.add_done_callback(self._settle_background_shutdown_task)
                        return "timeout"
                    task.result()
                return "complete"
            except BaseException:
                return "failed"

        for lifecycle, source_id in reversed(self.lifecycles):
            ok = True
            try:
                outcome = await before_deadline(lifecycle.deactivate())
                ok = outcome == "complete"
                if outcome == "timeout":
                    ok = False
                    self.artifact.record_loss("shutdown_timeout", _id("trace"))
                    self.artifact.degrade("shutdown deadline expired during source teardown")
                elif outcome == "failed":
                    self._teardown_failure()
            except BaseException:
                ok = False
                self._teardown_failure()
            self.artifact.source_deactivated(source_id, ok)
        for lifecycle, source_id in reversed(self.lifecycles):
            ok = True
            try:
                outcome = await before_deadline(lifecycle.drain())
                ok = outcome == "complete"
                if outcome == "timeout":
                    ok = False
                    self.artifact.record_loss("shutdown_timeout", _id("trace"))
                    self.artifact.degrade("shutdown deadline expired during source drain")
                elif outcome == "failed":
                    self._teardown_failure()
            except BaseException:
                ok = False
                self._teardown_failure()
            self.artifact.source_drained(source_id, ok)
        current = asyncio.current_task()
        pending = [task for task in self.active_async if task is not current]
        if pending:
            remaining = max(0.0, deadline - time.monotonic())
            done, still_pending = await asyncio.wait(pending, timeout=remaining)
            del done
            if still_pending:
                self.artifact.record_loss("shutdown_timeout", _id("trace"))
                self.artifact.degrade("shutdown deadline expired with active child operations")
        return self._finalize_shutdown()

    def _finalize_shutdown(self) -> CaptureStatus:
        def freeze_ownership() -> None:
            for decision in self.coverage_registry.freeze():
                if decision.get("status") == "ambiguous":
                    self.artifact.record_loss(
                        "source_rejection",
                        _id("trace"),
                        "/coverage/ownership/ambiguous",
                    )

        self.source_admission_open = False
        self.close_source_traces()
        if self.artifact.prepare_ownership_finalization():
            self.coverage_registry.reset_evidence()
        freeze_ownership()
        self.artifact.write_manifest("closing")
        if self.artifact.prepare_ownership_finalization():
            self.coverage_registry.reset_after_persistence_recovery()
            freeze_ownership()
            self.artifact.write_manifest("closing")
        self.coverage_registry.finalize()
        result = self.artifact.close()
        self.state = "closed" if result.state == "closed" else "closing"
        if self.state == "closed":
            self.conversation_order.clear()
            self.turn_indexes.clear()
            self.source_error_identities.clear()
            self._source_error_identity_count = 0
            self._shutdown_status = result
        return result

    def _retry_seal(self) -> CaptureStatus:
        with self._shutdown_guard:
            if self._shutdown_status is not None:
                return self._shutdown_status
            result = self.artifact.close()
            self.state = "closed" if result.state == "closed" else "closing"
            if self.state == "closed":
                self.conversation_order.clear()
                self.turn_indexes.clear()
                self.source_error_identities.clear()
                self._source_error_identity_count = 0
                self._shutdown_status = result
            return result

    def _settle_background_shutdown_task(self, task: asyncio.Future[Any]) -> None:
        self._background_shutdown_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.exception()
        except BaseException:
            pass

    def _teardown_failure(self) -> None:
        self.artifact.degrade("source teardown failed during shutdown")
        self.artifact.record_loss("source_rejection", _id("trace"))

    def accepts(
        self,
        output: Path,
        service_name: str,
        installation_id: str | None,
        secret_values: list[str] | tuple[str, ...] | None,
        identity_mode: str,
        identity_key: str | bytes | None,
        shutdown_deadline_ms: int,
        source_ownership: dict[str, Any] | None,
    ) -> bool:
        return self.compatibility == (
            str(output.resolve()),
            service_name,
            installation_id,
            identity_mode,
            _identity_key_digest(identity_key),
            shutdown_deadline_ms,
            _secret_digest(secret_values),
            _CoverageRegistry(
                service_name, source_ownership, _identity_key(identity_key)
            ).compatibility,
        )


_MANUAL = {
    "name": "manual",
    "seam": "observe/tool/emit",
    "identity_domain": "manual.operation",
    "coverage": [],
    "official": False,
    "qualification": {"status": "exact_qualified"},
}
_RUNTIME_SOURCE = {
    "name": "semantic-layer-runtime",
    "seam": "capture-runtime",
    "identity_domain": "semantic-layer",
    "coverage": [],
    "official": True,
    "qualification": {"status": "exact_qualified"},
}


_CURRENT_SCOPE: contextvars.ContextVar[ObservationScope | None] = contextvars.ContextVar(
    "semantic_layer_scope", default=None
)
_RUNTIME: _Runtime | None = None
_HANDLE: CaptureHandle | None = None


def initialize(
    *,
    output: str | os.PathLike[str] | None = None,
    service_name: str,
    installation_id: str | None = None,
    secret_values: list[str] | tuple[str, ...] | None = None,
    identity_mode: str = "hashed",
    identity_key: str | bytes | None = None,
    shutdown_deadline_ms: int = 10_000,
    queue_capacity_bytes: int = QUEUE_CAPACITY,
    source_ownership: dict[str, Any] | None = None,
) -> CaptureHandle:
    global _RUNTIME, _HANDLE
    output_path = Path(output or ".semantic-layer/traces")
    _validate_installation_id(installation_id)
    _validate_secret_values(secret_values)
    reject_symlink_path_components(output_path)
    if _HANDLE is not None:
        assert _RUNTIME is not None
        if not _RUNTIME.accepts(
            output_path,
            service_name,
            installation_id,
            secret_values,
            identity_mode,
            identity_key,
            shutdown_deadline_ms,
            source_ownership,
        ):
            raise RuntimeError(
                "initialize received options incompatible with the active capture runtime"
            )
        return _HANDLE
    if not service_name.strip():
        raise ValueError("service_name is required")
    if queue_capacity_bytes != QUEUE_CAPACITY:
        raise ValueError("queue capacity is fixed at 64 MiB")
    if identity_mode not in {"hashed", "raw"}:
        raise ValueError("identity_mode must be hashed or raw")
    if not isinstance(shutdown_deadline_ms, int) or not 1 <= shutdown_deadline_ms <= 60_000:
        raise ValueError("shutdown_deadline_ms must be between 1 and 60000")
    _RUNTIME = _Runtime(
        output_path,
        service_name,
        installation_id,
        secret_values,
        identity_mode,
        identity_key,
        shutdown_deadline_ms,
        source_ownership,
    )
    _HANDLE = CaptureHandle(_RUNTIME)
    return _HANDLE


class SemanticLayer:
    initialize = staticmethod(initialize)


def reset_capture_for_tests() -> None:
    global _RUNTIME, _HANDLE
    if _HANDLE is not None:
        _HANDLE.shutdown()
    _RUNTIME = None
    _HANDLE = None


def _freeze_source_ownership(service_name: str, value: dict[str, Any] | None) -> dict[str, Any]:
    default_namespace = f"app/{hashlib.sha256(service_name.encode()).hexdigest()[:16]}"
    if value is None:
        return {"namespace": default_namespace, "rules": []}
    if type(value) is not dict:
        raise TypeError("source_ownership must be an exact dict")
    namespace = dict.get(value, "namespace", default_namespace)
    if (
        not isinstance(namespace, str)
        or not namespace.strip()
        or ".." in namespace
        or re.fullmatch(r"(?!builtin(?:/|$)|official(?:/|$))[^\s/]+(?:/[^\s/]+)*", namespace)
        is None
    ):
        raise TypeError("source_ownership.namespace is invalid")
    _bounded("source_ownership.namespace", namespace)
    configured_rules = dict.get(value, "rules", [])
    if not isinstance(configured_rules, (list, tuple)):
        raise TypeError("source_ownership.rules must be a list or tuple")
    if len(configured_rules) > MAX_OWNERSHIP_RULES:
        raise TypeError(
            f"source_ownership.rules must contain at most {MAX_OWNERSHIP_RULES} entries"
        )
    rules: list[dict[str, str]] = []
    for index, rule in enumerate(configured_rules):
        if type(rule) is not dict:
            raise TypeError(f"source_ownership.rules[{index}] must be an exact dict")
        action = dict.get(rule, "action")
        source = dict.get(rule, "source")
        operation = dict.get(rule, "operation")
        domain = dict.get(rule, "domain")
        if action != "promote":
            raise TypeError(
                "source ownership rules require promote action, source, operation, "
                "and domain; prefer is unsupported"
            )
        if not all(isinstance(item, str) and item.strip() for item in (source, operation, domain)):
            raise TypeError(
                f"source_ownership.rules[{index}] requires source, operation, and domain"
            )
        assert isinstance(source, str)
        assert isinstance(operation, str)
        assert isinstance(domain, str)
        if not source.startswith("./") and not source.startswith("official/"):
            raise TypeError("source_ownership rule source must be ./<custom> or official/<source>")
        _bounded("source_ownership.rules[].source", source)
        _bounded("source_ownership.rules[].operation", operation)
        _bounded("source_ownership.rules[].domain", domain)
        source_id = f"{namespace}/{source[2:]}" if source.startswith("./") else source
        _validate_source_id(source_id)
        rules.append(
            {
                "action": action,
                "source_id": source_id,
                "operation": operation,
                "domain": domain,
            }
        )
    serialized_rules = [
        json.dumps(rule, separators=(",", ":"), ensure_ascii=False) for rule in rules
    ]
    if len(set(serialized_rules)) != len(serialized_rules):
        raise TypeError("source_ownership.rules must not contain duplicates")
    rules.sort(
        key=lambda rule: json.dumps(rule, separators=(",", ":"), ensure_ascii=False).encode()
    )
    return {"namespace": namespace, "rules": rules}


def _bounded(field: str, value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= MAX_BOUNDED_CHARS or not value.strip():
        raise TypeError(f"{field} must contain between 1 and {MAX_BOUNDED_CHARS} characters")
    return value


def _validate_source_metadata(metadata: dict[str, Any]) -> None:
    for metadata_field in ("name", "seam", "identity_domain"):
        _bounded(f"source metadata.{metadata_field}", metadata.get(metadata_field))
    if metadata.get("version") is not None:
        _bounded("source metadata.version", metadata["version"])
    qualification = metadata.get("qualification")
    if qualification is not None:
        if not isinstance(qualification, dict) or set(qualification) - {"status", "profile"}:
            raise TypeError(
                "source metadata.qualification must contain status and optional profile"
            )
        if qualification.get("status") not in {
            "exact_qualified",
            "capability_checked_unqualified",
            "unknown",
        }:
            raise TypeError("source metadata.qualification.status is invalid")
        if qualification.get("profile") is not None:
            _bounded("source metadata.qualification.profile", qualification["profile"])
        if qualification.get("status") == "exact_qualified" and metadata.get("version") is None:
            raise TypeError(
                "source metadata exact_qualified status requires an observed version"
            )
    if re.fullmatch(r"[^\s/]+", str(metadata.get("name", ""))) is None:
        raise TypeError("source metadata.name must not contain whitespace or slash")
    coverage = metadata.get("coverage")
    if not isinstance(coverage, list):
        raise TypeError("source metadata.coverage must be a list")
    if len(coverage) > MAX_COVERAGE_CLAIMS:
        raise TypeError(
            f"source metadata.coverage must contain at most {MAX_COVERAGE_CLAIMS} claims"
        )
    seen_claims: set[str] = set()
    for claim in coverage:
        if type(claim) is not dict:
            raise TypeError("source metadata.coverage[] must be an exact dict")
        if not set(claim).issubset({"operation", "domain", "role"}):
            raise TypeError("source metadata.coverage[] contains unknown fields")
        _bounded("source metadata.coverage[].operation", claim.get("operation"))
        _bounded("source metadata.coverage[].domain", claim.get("domain"))
        if claim.get("role") is not None and claim["role"] not in {"owner", "evidence"}:
            raise TypeError("source metadata.coverage[].role must be owner or evidence")
        serialized = json.dumps(claim, sort_keys=True, separators=(",", ":"))
        if serialized in seen_claims:
            raise TypeError("source metadata.coverage[] must not contain duplicates")
        seen_claims.add(serialized)


def _validate_source_id(source_id: str) -> None:
    if (
        re.fullmatch(
            r"(?:builtin/(?:manual|semantic-layer-runtime)|official/[^\s/]+|"
            r"(?!builtin/|official/)[^\s/]+(?:/[^\s/]+)+)",
            source_id,
        )
        is None
    ):
        raise TypeError("source identity is invalid")


def _single_coverage(metadata: dict[str, Any]) -> dict[str, str] | None:
    claims = metadata.get("coverage", [])
    if len(claims) != 1 or not isinstance(claims[0], dict):
        return None
    operation = claims[0].get("operation")
    domain = claims[0].get("domain")
    if not isinstance(operation, str) or not isinstance(domain, str):
        return None
    return {"operation": operation, "domain": domain}


def _declares_coverage(metadata: dict[str, Any], selected: dict[str, str]) -> bool:
    return any(
        isinstance(claim, dict)
        and claim.get("operation") == selected["operation"]
        and claim.get("domain") == selected["domain"]
        for claim in metadata.get("coverage", [])
    )


def _coverage_role(metadata: dict[str, Any], selected: dict[str, str]) -> str:
    for claim in metadata.get("coverage", []):
        if (
            isinstance(claim, dict)
            and claim.get("operation") == selected["operation"]
            and claim.get("domain") == selected["domain"]
        ):
            return "evidence" if claim.get("role") == "evidence" else "owner"
    return "evidence"


def _id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(16)}"


def _manual_call_identity(value: Any) -> str | None:
    return (
        value
        if (isinstance(value, str) and value.strip() and len(value) <= 256 and "\0" not in value)
        else None
    )


def _projected_source_id(source_id: str) -> str:
    return f"src_{hashlib.sha256(source_id.encode()).hexdigest()[:24]}"


def _source_operation_key(identity: Mapping[str, str]) -> str:
    return identity.get("operation_id", identity["trace_id"])


def _trace_identity(
    run_id: str,
    trace_id: str,
    operation_id: str | None = None,
) -> TraceIdentity:
    values = {"run_id": run_id, "trace_id": trace_id}
    if operation_id is not None:
        values["operation_id"] = operation_id
    return MappingProxyType(values)


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _loss_stage(reason: str) -> str:
    if "credential" in reason or "scrubber" in reason:
        return "scrub"
    if "getter" in reason or "helper" in reason or "serialization" in reason:
        return "serialize"
    if "queue" in reason:
        return "queue"
    if "persistence" in reason:
        return "persist"
    if "crash" in reason or "tail" in reason:
        return "recover"
    return "source"


def _secret_digest(values: list[str] | tuple[str, ...] | None) -> str:
    digest = hashlib.sha256()
    for value in values or ():
        encoded = value.encode()
        digest.update(str(len(encoded)).encode())
        digest.update(b":")
        digest.update(encoded)
        digest.update(b"\0")
    return digest.hexdigest()


def _validate_installation_id(value: str | None) -> None:
    if value is not None and re.fullmatch(r"install_[A-Za-z0-9_-]{22,128}", value) is None:
        raise ValueError(
            "installation_id must start with install_ and contain 22 to 128 opaque characters"
        )


def _default_source_qualification(source_class: str) -> dict[str, str]:
    return {
        "status": (
            "exact_qualified"
            if source_class in {"builtin_manual", "builtin_runtime"}
            else "unknown"
        )
    }


def _identity_key(value: str | bytes | None) -> bytes:
    if value is None:
        return secrets.token_bytes(32)
    encoded = value.encode() if isinstance(value, str) else bytes(value)
    if len(encoded) < 16:
        raise ValueError("identity_key must contain at least 16 bytes")
    return encoded


def _identity_key_digest(value: str | bytes | None) -> str:
    if value is None:
        return "ephemeral"
    encoded = value.encode() if isinstance(value, str) else bytes(value)
    return hashlib.sha256(encoded).hexdigest()


def _sync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _find_stale_runs(output: Path) -> list[tuple[str, bool]]:
    findings: list[tuple[str, bool]] = []
    for path in output.iterdir():
        if path.is_symlink() and path.name.startswith("run-"):
            findings.append((path.name, True))
            _quarantine_run(output, path)
            continue
        if not path.is_dir() or not path.name.startswith("run-"):
            continue
        lock_path = path / ".writer.lock"
        if lock_path.exists():
            try:
                lock = json.loads(read_regular_file(lock_path))
                pid = int(lock.get("pid", 0))
                if pid and _process_alive(pid):
                    continue
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                # An unreadable existing lock is conservatively treated as live.
                continue
        try:
            manifest = json.loads(read_regular_file(path / "manifest.json"))
            if manifest.get("state") not in {"open", "closing"}:
                continue
            trace = read_regular_file(path / "trace.jsonl")
            uncertain = bool(trace and not trace.endswith(b"\n"))
            if not uncertain:
                try:
                    for line in trace.splitlines():
                        if line:
                            json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    uncertain = True
            findings.append((path.name, uncertain))
            _quarantine_run(output, path)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            findings.append((path.name, True))
            _quarantine_run(output, path)
    return findings


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def _quarantine_run(output: Path, path: Path) -> None:
    try:
        if not path.is_symlink():
            (path / ".writer.lock").unlink(missing_ok=True)
        destination = output / f"quarantine-{path.name}"
        if destination.exists() or destination.is_symlink():
            destination = output / f"quarantine-{path.name}-{time.time_ns()}"
        os.replace(path, destination)
        _sync_directory(output)
    except OSError:
        # If quarantine is not durable, discovery repeats on the next run.
        pass
