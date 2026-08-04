"""Descriptor-safe native snapshots shared by optional framework adapters."""

from __future__ import annotations

import inspect
import math
import types
from collections.abc import Mapping
from datetime import date, datetime
from enum import Enum
from typing import Any

from .capture_v1 import unsafe_accessor_omission

MAX_SNAPSHOT_NODES = 20_000
MAX_SNAPSHOT_DEPTH = 48
MAX_SNAPSHOT_BINARY_BYTES = 8 * 1024 * 1024
MAX_SNAPSHOT_RETAINED_BYTES = 8 * 1024 * 1024
MAX_SNAPSHOT_PATH_CHARS = 512
MAX_SNAPSHOT_KEY_INT_BITS = 4096
SNAPSHOT_NODE_BYTES = 32
SNAPSHOT_RESOURCE_LIMIT_BYTES = 512


def native_snapshot(value: Any) -> Any:
    """Hand native evidence to the core sink without invoking framework helpers.

    Blob creation, omission markers, and loss accounting are owned by the SourceSink
    snapshot boundary.  Keeping this adapter boundary transparent prevents evidence
    from being silently reduced before that owner can account for it.
    """

    return _Snapshot().visit(value)


def native_field(value: Any, name: str, default: Any = None) -> Any:
    """Read an exact dict or instance-owned field without invoking descriptors."""

    if type(value) is dict:
        return dict.get(value, name, default)
    own = _own_dict(value)
    if name in own:
        return dict.get(own, name, default)
    try:
        descriptor = inspect.getattr_static(value, name, default)
    except BaseException:
        return unsafe_accessor_omission(name)
    if isinstance(descriptor, property) or type(descriptor) in {
        types.GetSetDescriptorType,
        types.MemberDescriptorType,
    }:
        return unsafe_accessor_omission(name)
    return default


def native_own_data(value: Any) -> dict[str, Any]:
    """Expose instance-owned data for semantic extraction without helper calls."""

    return value if type(value) is dict else _own_dict(value)


class _Snapshot:
    def __init__(self) -> None:
        self.seen: dict[int, str] = {}
        self.nodes = 0
        self.retained_bytes = 0
        self.exhausted = False

    def omitted(self, value: Any) -> dict[str, str]:
        return {
            "native_type": _type_name(value)[:256],
            "omitted": "resource_limit",
        }

    def retain(self, size: int) -> bool:
        working_limit = MAX_SNAPSHOT_RETAINED_BYTES - SNAPSHOT_RESOURCE_LIMIT_BYTES
        if size < 0 or size > working_limit - self.retained_bytes:
            self.exhausted = True
            return False
        self.retained_bytes += size
        return True

    def retain_text(self, value: str) -> bool:
        remaining = MAX_SNAPSHOT_RETAINED_BYTES - self.retained_bytes
        return self.retain(_bounded_json_string_bytes(value, remaining))

    def visit(self, value: Any, path: str = "$", depth: int = 0) -> Any:
        if self.exhausted:
            return self.omitted(value)
        self.nodes += 1
        if self.nodes > MAX_SNAPSHOT_NODES or depth > MAX_SNAPSHOT_DEPTH:
            self.exhausted = True
            return self.omitted(value)
        if not self.retain(SNAPSHOT_NODE_BYTES):
            return self.omitted(value)
        value_type = type(value)
        if value is None or value_type is bool:
            return value
        if value_type is int:
            if int.bit_length(value) > MAX_SNAPSHOT_KEY_INT_BITS:
                self.exhausted = True
                return self.omitted(value)
            return value
        if value_type is str:
            return value if self.retain_text(value) else self.omitted(value)
        if value_type is float:
            if math.isfinite(value):
                return value
            text = str(value)
            return (
                {"native_type": "float", "value": text}
                if self.retain_text(text)
                else self.omitted(value)
            )
        if isinstance(value, bytes):
            size = bytes.__len__(value)
            if size > MAX_SNAPSHOT_BINARY_BYTES or not self.retain(size):
                return self.omitted(value)
        elif isinstance(value, bytearray):
            size = bytearray.__len__(value)
            if size > MAX_SNAPSHOT_BINARY_BYTES or not self.retain(size):
                return self.omitted(value)
        if isinstance(value, bytes):
            return bytes.__getitem__(value, slice(None))
        if isinstance(value, bytearray):
            return bytearray.__getitem__(value, slice(None))
        if value_type is memoryview:
            if value.nbytes > MAX_SNAPSHOT_BINARY_BYTES or not self.retain(value.nbytes):
                return self.omitted(value)
            return value
        if value_type in {datetime, date}:
            text = value_type.isoformat(value)
            return text if self.retain_text(text) else self.omitted(value)
        if isinstance(value, Enum):
            enum_value = inspect.getattr_static(value, "_value_", None)
            type_name = _type_name(value)
            if not self.retain_text(type_name):
                return self.omitted(value)
            return {
                "native_type": type_name,
                "value": self.visit(enum_value, _child_path(path, "value"), depth + 1),
            }

        identity = id(value)
        prior = self.seen.get(identity)
        if prior is not None:
            type_name = _type_name(value)
            if not self.retain_text(type_name) or not self.retain_text(prior):
                return self.omitted(value)
            return {"native_type": type_name, "reference": prior}
        self.seen[identity] = path

        if isinstance(value, BaseException):
            return value
        if isinstance(value, Mapping) and not isinstance(value, dict):
            return value
        if isinstance(value, dict):
            result: dict[str, Any] = {}
            for index, (key, child) in enumerate(dict.items(value)):
                name = _safe_key(key, index)
                if not self.retain_text(name):
                    return self.omitted(value)
                result[name] = self.visit(child, _child_path(path, name), depth + 1)
                if self.exhausted:
                    break
            return result
        if value_type in {list, tuple}:
            result_list: list[Any] = []
            length = list.__len__(value) if value_type is list else tuple.__len__(value)
            for index in range(length):
                child = (
                    list.__getitem__(value, index)
                    if value_type is list
                    else tuple.__getitem__(value, index)
                )
                result_list.append(
                    self.visit(child, _child_path(path, str(index)), depth + 1)
                )
                if self.exhausted:
                    break
            return result_list
        if value_type in {set, frozenset}:
            result_items: list[Any] = []
            iterator = set.__iter__(value) if value_type is set else frozenset.__iter__(value)
            for index, child in enumerate(iterator):
                result_items.append(
                    self.visit(child, _child_path(path, str(index)), depth + 1)
                )
                if self.exhausted:
                    break
            type_name = value_type.__name__
            if not self.retain_text(type_name):
                return self.omitted(value)
            return {"native_type": type_name, "items": result_items}
        own = _own_dict(value)
        object_result: dict[str, Any] = {}
        for name, child in dict.items(own):
            if type(name) is str and not name.startswith("_") and not callable(child):
                if not self.retain_text(name):
                    return self.omitted(value)
                object_result[name] = self.visit(
                    child,
                    _child_path(path, name),
                    depth + 1,
                )
                if self.exhausted:
                    break
        type_name = _type_name(value)
        if not self.retain_text(type_name):
            return self.omitted(value)
        return {"native_type": type_name, **object_result}

def _own_dict(value: Any) -> dict[str, Any]:
    try:
        attributes = inspect.getattr_static(value, "__dict__", None)
    except BaseException:
        return {}
    if type(attributes) is dict:
        return attributes
    if type(attributes) in {types.GetSetDescriptorType, types.MemberDescriptorType}:
        try:
            own = attributes.__get__(value, type(value))
            return own if type(own) is dict else {}
        except BaseException:
            return {}
    return {}


def _safe_key(value: Any, index: int) -> str:
    if type(value) is str:
        return value
    if type(value) is bool or type(value) is float:
        return str(value)
    if type(value) is int:
        return (
            str(value)
            if int.bit_length(value) <= MAX_SNAPSHOT_KEY_INT_BITS
            else f"<int:{index}>"
        )
    if isinstance(value, Enum):
        name = inspect.getattr_static(value, "_name_", None)
        if type(name) is str:
            return f"{_type_name(value)[:255]}.{name[:255]}"
    return f"<{_type_name(value)[:480]}:{index}>"


def _child_path(path: str, component: str) -> str:
    available = MAX_SNAPSHOT_PATH_CHARS - len(path) - 1
    if available <= 0:
        return path[:MAX_SNAPSHOT_PATH_CHARS]
    return f"{path}/{component[:available]}"


def _bounded_json_string_bytes(value: str, ceiling: int) -> int:
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


def _type_name(value: Any) -> str:
    try:
        return str(type.__getattribute__(type(value), "__name__"))
    except BaseException:
        return "unknown"
