"""Repository-only verifier for source conformance."""

from __future__ import annotations

import inspect
import json
import struct
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from semantic_layer.capture_v1 import CaptureSource, initialize, reset_capture_for_tests
from semantic_layer.validation import validate_artifact

_CASES = ("lifecycle", "stream", "error", "unknown", "rejection", "shutdown")
_SECRET = "semantic-layer-conformance-secret-value"


async def verify_conformance(adapter_module: Any, driver_module: Any) -> dict[str, Any]:
    issues: list[str] = []
    try:
        with tempfile.TemporaryDirectory(prefix="semantic-layer-adapter-") as directory:
            try:
                control_subject = await _await_maybe(driver_module.create_subject())
                runners: dict[str, Any] = {}
                controls: dict[str, dict[str, Any]] = {}
                for case in _CASES:
                    run = getattr(driver_module, case, None)
                    expectations = getattr(driver_module, "expectations", {})
                    if not callable(run) or case not in expectations:
                        issues.append(f"CASE_CONTRACT_MISSING:{case}")
                        continue
                    runners[case] = run
                    controls[case] = await _observe_case(run, control_subject)
                subject = await _await_maybe(driver_module.create_subject())
                capture = initialize(
                    output=directory,
                    service_name="adapter-conformance",
                    secret_values=[_SECRET],
                )
                source = adapter_module.create_source(subject=subject)
                source_name = source.metadata.get("name")
                if not isinstance(source_name, str) or not source_name:
                    issues.append("SOURCE_METADATA_NAME_MISSING")
                capture.install_source(source)
                probe_sink: Any = None

                class _ProbeSource(CaptureSource):
                    metadata = {
                        "name": "conformance:source-input-probe",
                        "seam": "conformance.runtime-probe",
                        "identity_domain": "conformance.runtime-probe",
                        "coverage": [],
                    }

                    def install(self, sink: Any) -> Any:
                        nonlocal probe_sink
                        probe_sink = sink

                        class _Lifecycle:
                            def deactivate(self) -> None:
                                pass

                            def drain(self) -> None:
                                pass

                        return _Lifecycle()

                capture.install_source(_ProbeSource())
                probe_opened = probe_sink.open_trace(
                    {"name": "conformance.source-input-probe"}
                )
                if not probe_opened.accepted:
                    issues.append("SOURCE_INPUT_PROBE_OPEN_REJECTED")
                else:
                    identity = probe_opened.identity
                    identity_snapshot = dict(identity)
                    immutable = isinstance(identity, Mapping)
                    for key in ("run_id", "trace_id", "operation_id"):
                        try:
                            identity[key] = f"tampered-{key}"
                            immutable = False
                        except TypeError:
                            pass
                        except BaseException:
                            immutable = False
                    if not immutable or dict(identity) != identity_snapshot:
                        issues.append("SOURCE_TRACE_IDENTITY_MUTABLE")
                    forged_identity = probe_sink.record(
                        {
                            "kind": "log",
                            "phase": "event",
                            "name": "conformance.forged-identity",
                            "trace": {
                                **dict(probe_opened.identity),
                                "trace_id": "trace_conformance_forged",
                            },
                            "native": None,
                        }
                    )
                    if (
                        forged_identity.accepted
                        or forged_identity.reason != "invalid_record"
                    ):
                        issues.append("SOURCE_TRACE_IDENTITY_FORGERY_NOT_REJECTED")
                    await forged_identity.settled
                    forged_loss = probe_sink.record(
                        {
                            "kind": "loss",
                            "phase": "gap",
                            "name": "conformance.forged-loss",
                            "trace": probe_opened.identity,
                            "native": None,
                            "loss": {
                                "reason": "serialization_failure",
                                "stage": "source",
                                "recoverable": False,
                            },
                        }
                    )
                    if forged_loss.accepted or forged_loss.reason != "invalid_record":
                        issues.append("SOURCE_LOSS_KIND_NOT_REJECTED")
                    probe_ended = probe_sink.record(
                        {
                            "kind": "lifecycle",
                            "phase": "end",
                            "name": "conformance.source-input-probe",
                            "trace": probe_opened.identity,
                            "native": None,
                            "semantic": {
                                "type": "workflow.run",
                                "status": "succeeded",
                            },
                        }
                    )
                    if not probe_ended.accepted:
                        issues.append("SOURCE_INPUT_PROBE_END_REJECTED")
                for case in _CASES:
                    run = runners.get(case)
                    control = controls.get(case)
                    if run is None or control is None:
                        continue
                    expectations = getattr(driver_module, "expectations", {})
                    observed = await _observe_case(run, subject)
                    if (
                        control.get("status") == "unsupported"
                        or observed.get("status") == "unsupported"
                    ):
                        issues.append(f"CASE_OUTCOME_UNSUPPORTED:{case}")
                        continue
                    if observed != control:
                        issues.append(f"CASE_PARITY_MISMATCH:{case}")
                    if (
                        observed.get("status") == "returned"
                        and "protocol" not in observed
                        and observed.get("value") != _comparable(expectations[case])
                    ):
                        issues.append(f"CASE_RETURN_MISMATCH:{case}")
                status = capture.shutdown()
                artifact = validate_artifact(status.artifact_path)
                issues.extend(f"ARTIFACT:{issue}" for issue in artifact.issues)
                text = (Path(status.artifact_path) / "trace.jsonl").read_text()
                rows = [json.loads(line) for line in text.splitlines()]
                manifest = json.loads(
                    (Path(status.artifact_path) / "manifest.json").read_text()
                )
                source_id = next(
                    (
                        source["id"]
                        for source in manifest["sources"]
                        if source["name"] == source_name
                    ),
                    None,
                )
                if not any(
                    row.get("kind") == "loss"
                    and row.get("data", {}).get("reason") == "source_rejection"
                    and row.get("data", {}).get("path") == "/trace"
                    for row in rows
                ):
                    issues.append("SOURCE_TRACE_IDENTITY_REJECTION_EVIDENCE_MISSING")
                if not any(
                    row.get("kind") == "loss"
                    and row.get("data", {}).get("reason")
                    == "serialization_failure"
                    and row.get("data", {}).get("path") == "/event_kind"
                    for row in rows
                ):
                    issues.append("SOURCE_LOSS_REJECTION_EVIDENCE_MISSING")
                source_rows = [
                    row for row in rows if row.get("source") == source_id
                ]
                if not source_rows:
                    issues.append("SOURCE_EVIDENCE_MISSING")
                if not any(
                    row["kind"] == "run.start"
                    for row in source_rows
                ):
                    issues.append("LIFECYCLE_START_MISSING")
                if not any(
                    row["kind"] == "run.outcome"
                    and row.get("data", {}).get("status") == "completed"
                    for row in source_rows
                ):
                    issues.append("LIFECYCLE_END_MISSING")
                if not any(
                    row["kind"] == "state"
                    and row.get("data", {}).get("type") == "state.stream_observed"
                    for row in source_rows
                ):
                    issues.append("STREAM_EVIDENCE_MISSING")
                if not any(row["kind"] == "error" for row in source_rows):
                    issues.append("ERROR_EVIDENCE_MISSING")
                if not any(
                    row["kind"] == "state"
                    and row.get("data", {}).get("type") == "state.unknown_observed"
                    for row in source_rows
                ):
                    issues.append("UNKNOWN_EVIDENCE_MISSING")
                if not any(
                    row["kind"] == "run.outcome"
                    and row.get("data", {}).get("status") == "cancelled"
                    for row in source_rows
                ):
                    issues.append("ACTIVE_SHUTDOWN_MISSING")
                if (
                    status.pending_bytes != 0
                    or status.pending_control_bytes != 0
                ):
                    issues.append("ADMITTED_RECORDS_UNSETTLED")
                if not any(
                    row.get("kind") == "loss"
                    and row.get("data", {}).get("reason") == "credential_redaction"
                    for row in rows
                ):
                    issues.append("SECRET_REDACTION_LOSS_MISSING")
                if _SECRET in text:
                    issues.append("SECRET_PERSISTED")
            finally:
                reset_capture_for_tests()
    except Exception as error:
        issues = [f"ADAPTER_CONFORMANCE_FAILED:{type(error).__name__}:{error}"]
    return {"valid": not issues, "cases": len(_CASES), "issues": issues}


async def _observe_case(run: Any, subject: Any) -> dict[str, Any]:
    try:
        value = await _await_maybe(run(subject))
    except Exception as error:
        try:
            return {"status": "threw", "error": _error_value(error)}
        except _ConformanceObservationError as observation_error:
            return {"status": "unsupported", "error": str(observation_error)}
    try:
        iterator_factory = _special_method(value, "__aiter__")
        if iterator_factory is not None:
            return {
                "status": "returned",
                "protocol": await _observe_async_protocol(value, iterator_factory),
            }
        return {"status": "returned", "value": _comparable(value)}
    except _ConformanceObservationError as error:
        return {"status": "unsupported", "error": str(error)}


async def _observe_async_protocol(iterable: Any, iterator_factory: Any) -> dict[str, Any]:
    values: list[Any] = []
    iterator = iterator_factory(iterable)
    next_method = _required_special_method(iterator, "__anext__")
    for index in range(64):
        try:
            value = await next_method(iterator)
            values.append({"done": False, "value": _comparable(value)})
        except StopAsyncIteration:
            values.append({"done": True})
            break
        if index == 63:
            values.append({"bounded": True})
    return_iterator = iterator_factory(iterable)
    close = _special_method(return_iterator, "aclose")
    returned = (
        await _settle_protocol(close, return_iterator, None)
        if close is not None
        else {"supported": False}
    )
    throw_iterator = iterator_factory(iterable)
    throw = _special_method(throw_iterator, "athrow")
    sentinel = RuntimeError("conformance-throw-sentinel")
    thrown = (
        await _settle_protocol(throw, throw_iterator, sentinel, sentinel)
        if throw is not None
        else {"supported": False}
    )
    return {"values": values, "returned": returned, "thrown": thrown}


async def _settle_protocol(
    operation: Any, receiver: Any, argument: Any, sentinel: Any = None
) -> dict[str, Any]:
    try:
        result = (
            await operation(receiver, argument)
            if argument is not None
            else await operation(receiver)
        )
    except Exception as error:
        return {
            "supported": True,
            "status": "threw",
            "error": _error_value(error),
            "sentinel": error is sentinel,
        }
    return {
        "supported": True,
        "status": "returned",
        "result": _comparable(result),
        "sentinel": result is sentinel,
    }


async def _await_maybe(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _error_value(error: BaseException) -> dict[str, Any]:
    args = object.__getattribute__(error, "args")
    cause = object.__getattribute__(error, "__cause__")
    return {
        "type": type(error).__name__,
        "args": _comparable(args),
        "cause": _comparable(cause),
    }


def _comparable(value: Any, seen: set[int] | None = None) -> Any:
    value_type = type(value)
    if value is None:
        return {"type": "none"}
    if value_type is bool:
        return {"type": "bool", "value": value}
    if value_type is int:
        return {"type": "int", "value": value}
    if value_type is str:
        return {"type": "str", "value": value}
    if value_type is float:
        return {"type": "float", "ieee754": struct.pack(">d", value).hex()}
    if isinstance(value, (str, int, float, bool)):
        raise _ConformanceObservationError("primitive subclass return shape")
    if isinstance(value, BaseException):
        return _error_value(value)
    identities = seen if seen is not None else set()
    identity = id(value)
    if identity in identities:
        return {"circular": True}
    identities.add(identity)
    if type(value) in (list, tuple):
        return {
            "sequence": type(value).__name__,
            "values": [_comparable(item, identities) for item in value],
        }
    if type(value) is dict:
        return {
            "map": [
                [_comparable(key, identities), _comparable(item, identities)]
                for key, item in dict.items(value)
            ]
        }
    if type(value) is set:
        items = [_comparable(item, identities) for item in set.__iter__(value)]
        return {"set": sorted(items, key=lambda item: json.dumps(item, sort_keys=True))}
    for cls in type.__getattribute__(value_type, "__mro__"):
        namespace = type.__getattribute__(cls, "__dict__")
        if any(isinstance(descriptor, property) for descriptor in namespace.values()):
            raise _ConformanceObservationError("property return shape")
    try:
        attributes = object.__getattribute__(value, "__dict__")
    except AttributeError as error:
        raise _ConformanceObservationError("opaque return shape") from error
    if type(attributes) is not dict or not attributes:
        raise _ConformanceObservationError("unsupported empty return shape")
    return {
        "object": f"{value_type.__module__}.{value_type.__qualname__}",
        "attributes": _comparable(attributes, identities),
    }


class _ConformanceObservationError(Exception):
    pass


def _special_method(value: Any, name: str) -> Any:
    if value is None:
        return None
    value_type = type(value)
    for cls in type.__getattribute__(value_type, "__mro__"):
        namespace = type.__getattribute__(cls, "__dict__")
        if name not in namespace:
            continue
        descriptor = namespace[name]
        if isinstance(descriptor, property):
            raise _ConformanceObservationError(f"property protocol surface: {name}")
        if not callable(descriptor):
            raise _ConformanceObservationError(f"non-callable protocol surface: {name}")
        return descriptor
    return None


def _required_special_method(value: Any, name: str) -> Any:
    method = _special_method(value, name)
    if method is None:
        raise _ConformanceObservationError(f"missing protocol surface: {name}")
    return method
