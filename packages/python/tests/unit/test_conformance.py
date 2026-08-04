from __future__ import annotations

from typing import Any

import pytest

from semantic_layer.capture_v1 import CaptureSource
from tests import conformance_fixture as conformance
from tests.conformance_verify import verify_conformance


async def test_conformance_proves_evidence_parity_loss_privacy_and_shutdown() -> None:
    assert await verify_conformance(conformance, conformance) == {
        "valid": True,
        "cases": 6,
        "issues": [],
    }


async def test_conformance_rejects_a_validly_shaped_noop_source() -> None:
    class NoopSource(CaptureSource):
        metadata = {
            "name": "conformance:custom-source",
            "seam": "noop",
            "identity_domain": "conformance.operation",
            "coverage": [],
        }

        def install(self, _sink: Any) -> Any:
            class Lifecycle:
                def deactivate(self) -> None:
                    pass

                def drain(self) -> None:
                    pass

            return Lifecycle()

    class NoopModule:
        @staticmethod
        def create_source(*, subject: Any) -> CaptureSource:
            del subject
            return NoopSource()

    report = await verify_conformance(NoopModule, conformance)
    assert not report["valid"]
    assert {
        "SOURCE_EVIDENCE_MISSING",
        "STREAM_EVIDENCE_MISSING",
        "ERROR_EVIDENCE_MISSING",
        "UNKNOWN_EVIDENCE_MISSING",
        "ACTIVE_SHUTDOWN_MISSING",
        "SECRET_REDACTION_LOSS_MISSING",
    }.issubset(report["issues"])


async def test_conformance_derives_alternate_source_name() -> None:
    class AlternateModule:
        @staticmethod
        def create_source(*, subject: Any) -> CaptureSource:
            source = conformance.create_source(subject=subject)
            source.metadata = {**source.metadata, "name": "conformance:alternate-source"}
            return source

    assert await verify_conformance(AlternateModule, conformance) == {
        "valid": True,
        "cases": 6,
        "issues": [],
    }


async def test_conformance_rejects_stream_control_mutation_blessed_by_expectations() -> None:
    class Protocol:
        def __init__(self, value: str) -> None:
            self.value = value

        def __aiter__(self) -> Any:
            async def iterate() -> Any:
                try:
                    yield f"value:{self.value}"
                except Exception:
                    yield f"caught:{self.value}"

            return iterate()

    class Driver:
        expectations = {**conformance.expectations, "lifecycle": "mutated"}
        stream = staticmethod(conformance.stream)
        error = staticmethod(conformance.error)
        unknown = staticmethod(conformance.unknown)
        rejection = staticmethod(conformance.rejection)
        shutdown = staticmethod(conformance.shutdown)

        @staticmethod
        def create_subject() -> Any:
            subject = conformance.create_subject()
            setattr(subject, "parity_value", "control")
            return subject

        @staticmethod
        def lifecycle(subject: Any) -> Protocol:
            conformance.lifecycle(subject)
            return Protocol(str(getattr(subject, "parity_value")))

    class MutatingModule:
        @staticmethod
        def create_source(*, subject: Any) -> CaptureSource:
            setattr(subject, "parity_value", "mutated")
            return conformance.create_source(subject=subject)

    report = await verify_conformance(MutatingModule, Driver)
    assert not report["valid"]
    assert "CASE_PARITY_MISMATCH:lifecycle" in report["issues"]


async def test_conformance_rejects_properties_without_repr_or_getter_calls() -> None:
    class Hostile:
        getter_calls = 0
        repr_calls = 0

        @property
        def hostile(self) -> str:
            type(self).getter_calls += 1
            raise RuntimeError("getter must not run")

        def __repr__(self) -> str:
            type(self).repr_calls += 1
            raise RuntimeError("repr must not run")

    class Driver:
        expectations = conformance.expectations
        stream = staticmethod(conformance.stream)
        error = staticmethod(conformance.error)
        unknown = staticmethod(conformance.unknown)
        rejection = staticmethod(conformance.rejection)
        shutdown = staticmethod(conformance.shutdown)
        create_subject = staticmethod(conformance.create_subject)

        @staticmethod
        def lifecycle(subject: Any) -> Hostile:
            conformance.lifecycle(subject)
            return Hostile()

    report = await verify_conformance(conformance, Driver)
    assert not report["valid"]
    assert "CASE_OUTCOME_UNSUPPORTED:lifecycle" in report["issues"]
    assert Hostile.getter_calls == 0
    assert Hostile.repr_calls == 0


async def test_conformance_snapshots_control_before_shared_class_mutation() -> None:
    class SharedSubject(conformance.ConformanceSubject):
        behavior = "control"

    class Driver:
        expectations = {**conformance.expectations, "lifecycle": "observed"}
        stream = staticmethod(conformance.stream)
        error = staticmethod(conformance.error)
        unknown = staticmethod(conformance.unknown)
        rejection = staticmethod(conformance.rejection)
        shutdown = staticmethod(conformance.shutdown)

        @staticmethod
        def create_subject() -> SharedSubject:
            return SharedSubject()

        @staticmethod
        def lifecycle(subject: SharedSubject) -> str:
            conformance.lifecycle(subject)
            return str(subject.behavior)

    class MutatingModule:
        @staticmethod
        def create_source(*, subject: SharedSubject) -> CaptureSource:
            SharedSubject.behavior = "observed"
            return conformance.create_source(subject=subject)

    report = await verify_conformance(MutatingModule, Driver)
    assert not report["valid"]
    assert "CASE_PARITY_MISMATCH:lifecycle" in report["issues"]


@pytest.mark.parametrize(
    ("control_value", "observed_value"),
    [(1, True), (0, False), (0.0, -0.0)],
)
async def test_conformance_distinguishes_exact_python_primitive_types_and_bits(
    control_value: Any, observed_value: Any
) -> None:
    class Driver:
        expectations = {**conformance.expectations, "lifecycle": observed_value}
        stream = staticmethod(conformance.stream)
        error = staticmethod(conformance.error)
        unknown = staticmethod(conformance.unknown)
        rejection = staticmethod(conformance.rejection)
        shutdown = staticmethod(conformance.shutdown)

        @staticmethod
        def create_subject() -> Any:
            subject = conformance.create_subject()
            setattr(subject, "primitive_value", control_value)
            return subject

        @staticmethod
        def lifecycle(subject: Any) -> Any:
            conformance.lifecycle(subject)
            return getattr(subject, "primitive_value")

    class MutatingModule:
        @staticmethod
        def create_source(*, subject: Any) -> CaptureSource:
            setattr(subject, "primitive_value", observed_value)
            return conformance.create_source(subject=subject)

    report = await verify_conformance(MutatingModule, Driver)
    assert not report["valid"]
    assert "CASE_PARITY_MISMATCH:lifecycle" in report["issues"]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
async def test_conformance_canonicalizes_equivalent_special_floats(value: float) -> None:
    class Driver:
        expectations = {**conformance.expectations, "lifecycle": value}
        stream = staticmethod(conformance.stream)
        error = staticmethod(conformance.error)
        unknown = staticmethod(conformance.unknown)
        rejection = staticmethod(conformance.rejection)
        shutdown = staticmethod(conformance.shutdown)
        create_subject = staticmethod(conformance.create_subject)

        @staticmethod
        def lifecycle(subject: Any) -> float:
            conformance.lifecycle(subject)
            return float(value)

    assert await verify_conformance(conformance, Driver) == {
        "valid": True,
        "cases": 6,
        "issues": [],
    }


async def test_conformance_rejects_primitive_subclasses_without_repr_calls() -> None:
    class HostileInt(int):
        repr_calls = 0

        def __repr__(self) -> str:
            type(self).repr_calls += 1
            raise RuntimeError("repr must not run")

        @property
        def hostile(self) -> str:
            raise RuntimeError("property must not run")

    class Driver:
        expectations = {**conformance.expectations, "lifecycle": HostileInt(1)}
        stream = staticmethod(conformance.stream)
        error = staticmethod(conformance.error)
        unknown = staticmethod(conformance.unknown)
        rejection = staticmethod(conformance.rejection)
        shutdown = staticmethod(conformance.shutdown)
        create_subject = staticmethod(conformance.create_subject)

        @staticmethod
        def lifecycle(subject: Any) -> HostileInt:
            conformance.lifecycle(subject)
            return HostileInt(1)

    report = await verify_conformance(conformance, Driver)
    assert not report["valid"]
    assert "CASE_OUTCOME_UNSUPPORTED:lifecycle" in report["issues"]
    assert HostileInt.repr_calls == 0


async def test_conformance_awaits_async_subject_and_cases() -> None:
    class AsyncDriver:
        expectations = conformance.expectations

        @staticmethod
        async def create_subject() -> Any:
            return conformance.create_subject()

    for name in ("lifecycle", "stream", "error", "unknown", "rejection", "shutdown"):
        sync_case = getattr(conformance, name)

        async def async_case(subject: Any, case: Any = sync_case) -> Any:
            return case(subject)

        setattr(AsyncDriver, name, staticmethod(async_case))

    assert await verify_conformance(conformance, AsyncDriver) == {
        "valid": True,
        "cases": 6,
        "issues": [],
    }
