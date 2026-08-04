from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from semantic_layer import CaptureSource, initialize
from semantic_layer import capture_v1 as capture_module
from semantic_layer.capture_v1 import _CoverageRegistry, _trust_official_source
from semantic_layer.validation import validate_artifact


class _Lifecycle:
    def deactivate(self) -> None:
        return None

    def drain(self) -> None:
        return None


def _records(artifact: str | Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in (Path(artifact) / "trace.jsonl").read_text().splitlines()
    ]


class _SemanticSource(CaptureSource):
    def __init__(
        self,
        name: str,
        native_identity: str = "request-1",
        role: str | None = "owner",
    ) -> None:
        self.name = name
        self.native_identity = native_identity
        claim = {
            "operation": "model-call",
            "domain": "fixture.model",
        }
        if role is not None:
            claim["role"] = role
        self.metadata = {
            "name": name,
            "seam": f"fixture.{name}",
            "identity_domain": "fixture.request",
            "coverage": [claim],
        }

    def install(self, sink: Any) -> Any:
        coverage = {"operation": "model-call", "domain": "fixture.model"}
        opened = sink.open_trace(
            {
                "name": f"{self.name}.run",
                "native_identity": self.native_identity,
                "coverage": coverage,
                "semantic": {
                    "type": "workflow.run",
                    "name": f"{self.name}.run",
                },
            }
        )
        assert opened.accepted
        ended = sink.record(
            {
                "kind": "lifecycle",
                "phase": "end",
                "name": f"{self.name}.run",
                "trace": opened.identity,
                "native_identity": self.native_identity,
                "coverage": coverage,
                "semantic": {
                    "type": "workflow.run",
                    "status": "succeeded",
                },
                "native": None,
            }
        )
        assert ended.accepted
        return _Lifecycle()


def _official(name: str, source_class: str = "deep") -> CaptureSource:
    return _trust_official_source(
        _SemanticSource(f"official:{name}"),
        source_class,
    )


def test_exact_source_qualification_requires_observed_version(tmp_path: Path) -> None:
    class UnversionedExactSource(CaptureSource):
        metadata = {
            "name": "unversioned-exact-source",
            "seam": "fixture.callback",
            "identity_domain": "fixture",
            "coverage": [],
            "qualification": {"status": "exact_qualified"},
        }

        def install(self, _sink: Any) -> Any:
            return _Lifecycle()

    capture = initialize(output=tmp_path, service_name="source-qualification")
    with pytest.raises(TypeError, match="exact_qualified.*version"):
        capture.install_source(UnversionedExactSource())


def test_failed_source_install_rethrows_exact_error_and_rolls_back(
    tmp_path: Path,
) -> None:
    expected = RuntimeError("registration failed")

    class FailingSource(CaptureSource):
        metadata = {
            "name": "failing-source",
            "seam": "fixture.register",
            "identity_domain": "fixture",
            "coverage": [],
        }

        def install(self, _sink: Any) -> Any:
            raise expected

    capture = initialize(output=tmp_path, service_name="install-rollback")
    with pytest.raises(RuntimeError) as caught:
        capture.install_source(FailingSource())
    assert caught.value is expected
    assert "failing-source" not in {
        source["name"] for source in capture.status().active_sources
    }
    assert validate_artifact(capture.shutdown().artifact_path).valid


@pytest.mark.asyncio
async def test_source_identity_drain_and_shutdown_freeze_are_exact(
    tmp_path: Path,
) -> None:
    state: dict[str, Any] = {}

    class Source(CaptureSource):
        metadata = {
            "name": "identity-source",
            "seam": "fixture.callback",
            "identity_domain": "fixture",
            "coverage": [],
        }

        def install(self, sink: Any) -> Any:
            opened = sink.open_trace(
                {
                    "name": "fixture.run",
                    "native_identity": " exact-id ",
                    "semantic": {"type": "workflow.run", "name": "fixture.run"},
                }
            )
            assert opened.accepted and opened.identity is not None
            state.update(sink=sink, opened=opened)

            class Lifecycle:
                def deactivate(self) -> None:
                    return None

                async def drain(self) -> None:
                    await asyncio.sleep(0)
                    state["drained"] = sink.record(
                        {
                            "kind": "state",
                            "phase": "event",
                            "name": "fixture.drained",
                            "trace": opened.identity,
                            "semantic": {
                                "type": "state.drained",
                                "value": True,
                            },
                            "native": None,
                        }
                    )
                    state["ended"] = sink.record(
                        {
                            "kind": "lifecycle",
                            "phase": "end",
                            "name": "fixture.run",
                            "trace": opened.identity,
                            "native_identity": " exact-id ",
                            "semantic": {
                                "type": "workflow.run",
                                "status": "succeeded",
                            },
                            "native": None,
                        }
                    )
                    state["replayed"] = sink.record(
                        {
                            "kind": "state",
                            "phase": "event",
                            "name": "fixture.replayed",
                            "trace": opened.identity,
                            "semantic": {
                                "type": "state.replayed",
                                "value": False,
                            },
                            "native": None,
                        }
                    )

            return Lifecycle()

    capture = initialize(output=tmp_path, service_name="source-identity")
    capture.install_source(Source())
    identity = state["opened"].identity
    assert isinstance(identity, Mapping)
    snapshot = dict(identity)
    with pytest.raises(TypeError):
        cast(Any, identity)["trace_id"] = "forged"
    exact = state["sink"].record(
        {
            "kind": "state",
            "phase": "event",
            "name": "fixture.exact",
            "trace": dict(identity),
            "semantic": {"type": "state.exact", "value": True},
            "native": None,
        }
    )
    forged = state["sink"].record(
        {
            "kind": "state",
            "phase": "event",
            "name": "fixture.forged",
            "trace": {**snapshot, "trace_id": "trace_forged"},
            "semantic": {"type": "state.forged", "value": False},
            "native": None,
        }
    )
    foreign = state["sink"].record(
        {
            "kind": "state",
            "phase": "event",
            "name": "fixture.foreign",
            "trace": {**snapshot, "run_id": "run_foreign"},
            "semantic": {"type": "state.foreign", "value": False},
            "native": None,
        }
    )
    forged_loss = state["sink"].record(
        {
            "kind": "loss",
            "phase": "gap",
            "name": "fixture.forged-loss",
            "trace": identity,
            "native": None,
        }
    )
    assert exact.accepted
    assert not forged.accepted and forged.reason == "invalid_record"
    assert not foreign.accepted and foreign.reason == "foreign_run_identity"
    assert not forged_loss.accepted and forged_loss.reason == "invalid_record"

    first, second = await asyncio.gather(
        capture.shutdown_async(),
        capture.shutdown_async(),
    )
    assert first == second
    assert state["drained"].accepted
    assert state["ended"].accepted
    assert not state["replayed"].accepted
    assert state["replayed"].reason == "invalid_record"
    late = state["sink"].record(
        {
            "kind": "state",
            "phase": "event",
            "name": "fixture.late",
            "trace": identity,
            "semantic": {"type": "state.late", "value": True},
            "native": None,
        }
    )
    assert not late.accepted and late.reason == "source_frozen"
    losses = [
        record["data"]
        for record in _records(first.artifact_path)
        if record["kind"] == "loss"
    ]
    assert {
        (loss["reason"], loss.get("path"))
        for loss in losses
    } >= {
        ("source_rejection", "/trace"),
        ("serialization_failure", "/event_kind"),
    }
    assert validate_artifact(first.artifact_path).valid


def test_rejects_preference_and_deduplicates_source_declarations(
    tmp_path: Path,
) -> None:
    with pytest.raises(TypeError, match="prefer is unsupported"):
        initialize(
            output=tmp_path,
            service_name="ownership-preference",
            source_ownership={
                "rules": [
                    {
                        "action": "prefer",
                        "source": "official/preferred",
                        "operation": "model-call",
                        "domain": "fixture.model",
                    },
                ],
            },
        )

    capture = initialize(output=tmp_path / "captured", service_name="ownership-deduplication")
    capture.install_source(_official("preferred"))
    capture.install_source(_official("preferred"))
    status = capture.shutdown()
    manifest = json.loads(
        (Path(status.artifact_path) / "manifest.json").read_text()
    )
    assert [source["name"] for source in manifest["sources"]].count(
        "official:preferred"
    ) == 1
    assert validate_artifact(status.artifact_path).valid


def test_ambiguous_exact_overlap_emits_one_semantic_loss(tmp_path: Path) -> None:
    capture = initialize(
        output=tmp_path,
        service_name="ownership-ambiguity",
    )
    capture.install_source(_official("first"))
    capture.install_source(_official("second"))
    status = capture.shutdown()
    ambiguous = [
        record
        for record in _records(status.artifact_path)
        if record["kind"] == "loss"
        and record["data"].get("path") == "/coverage/ownership/ambiguous"
    ]
    assert len(ambiguous) == 1
    assert ambiguous[0]["data"]["reason"] == "source_rejection"
    assert validate_artifact(status.artifact_path).valid


@pytest.mark.parametrize(
    ("name", "roles", "expected"),
    [
        (
            "owner-with-evidence",
            ("owner", "evidence"),
            {
                "status": "owned",
                "primary_source_id": "official/owner-with-evidence-0",
                "secondary_source_ids": ["official/owner-with-evidence-1"],
            },
        ),
        (
            "multiple-owners",
            ("owner", "owner", "evidence"),
            {
                "status": "ambiguous",
                "secondary_source_ids": [],
            },
        ),
        (
            "evidence-only",
            ("evidence", "evidence"),
            {
                "status": "evidence_only",
                "secondary_source_ids": [],
            },
        ),
        (
            "omitted-role-is-owner",
            (None, "evidence"),
            {
                "status": "owned",
                "primary_source_id": "official/omitted-role-is-owner-0",
                "secondary_source_ids": ["official/omitted-role-is-owner-1"],
            },
        ),
    ],
)
def test_declared_coverage_roles_are_overlap_authority(
    name: str,
    roles: tuple[str | None, ...],
    expected: dict[str, Any],
) -> None:
    registry = _CoverageRegistry(name, None, bytes([7]) * 32)
    for index, role in enumerate(roles):
        source = _trust_official_source(
            _SemanticSource(f"{name}-{index}", role=role),
            "deep",
        )
        metadata, reused = registry.register(source, source.metadata)
        assert not reused
        registry.activate(metadata)
        identity = registry.coverage_identity(
            metadata,
            "shared-request",
            {"operation": "model-call", "domain": "fixture.model"},
        )
        registry.reserve(metadata, identity).settle(True)

    decisions = registry.freeze()
    assert len(decisions) == 1
    assert {key: decisions[0].get(key) for key in expected} == expected
    if expected["status"] != "owned":
        assert "primary_source_id" not in decisions[0]


def test_rejected_owner_reservation_does_not_claim_authority() -> None:
    registry = _CoverageRegistry("ownership-rollback", None, bytes([7]) * 32)

    def reserve(name: str, role: str, accepted: bool) -> None:
        source = _trust_official_source(_SemanticSource(name, role=role), "deep")
        metadata, reused = registry.register(source, source.metadata)
        assert not reused
        registry.activate(metadata)
        identity = registry.coverage_identity(
            metadata,
            "shared-request",
            {"operation": "model-call", "domain": "fixture.model"},
        )
        registry.reserve(metadata, identity).settle(accepted)

    reserve("rejected-owner", "owner", False)
    reserve("evidence-a", "evidence", True)
    reserve("evidence-b", "evidence", True)

    decisions = registry.freeze()
    assert len(decisions) == 1
    assert decisions[0]["status"] == "evidence_only"
    assert decisions[0]["participant_source_ids"] == [
        "official/evidence-a",
        "official/evidence-b",
    ]
    assert decisions[0]["secondary_source_ids"] == []


def test_ownership_group_bound_is_one_coalesced_semantic_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(capture_module, "MAX_OWNERSHIP_GROUPS", 2)

    class ManyGroups(CaptureSource):
        metadata = {
            "name": "many-groups",
            "seam": "fixture.callback",
            "identity_domain": "fixture.request",
            "coverage": [
                {
                    "operation": "model-call",
                    "domain": "fixture.model",
                    "role": "owner",
                }
            ],
        }

        def install(self, sink: Any) -> Any:
            opened = sink.open_trace(
                {
                    "name": "many-groups.run",
                    "semantic": {
                        "type": "workflow.run",
                        "name": "many-groups.run",
                    },
                }
            )
            for index in range(4):
                sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": "many-groups.state",
                        "trace": opened.identity,
                        "native_identity": f"request-{index}",
                        "coverage": {
                            "operation": "model-call",
                            "domain": "fixture.model",
                        },
                        "semantic": {
                            "type": "state.group",
                            "value": index,
                        },
                        "native": None,
                    }
                )
            sink.record(
                {
                    "kind": "lifecycle",
                    "phase": "end",
                    "name": "many-groups.run",
                    "trace": opened.identity,
                    "semantic": {
                        "type": "workflow.run",
                        "status": "succeeded",
                    },
                    "native": None,
                }
            )
            return _Lifecycle()

    capture = initialize(output=tmp_path, service_name="ownership-group-bound")
    capture.install_source(ManyGroups())
    status = capture.shutdown()
    losses = [
        record["data"]
        for record in _records(status.artifact_path)
        if record["kind"] == "loss"
        and record["data"].get("path") == "/coverage/ownership/group_limit"
    ]
    assert sum(loss["count"] for loss in losses) == 2
    assert status.losses["source_rejection"] == 2
    assert validate_artifact(status.artifact_path).valid
