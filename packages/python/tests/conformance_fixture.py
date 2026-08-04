"""Repository-only fixture used to verify adapter conformance."""

from __future__ import annotations

from typing import Any

from semantic_layer.capture_v1 import CaptureSource


class ConformanceSubject:
    def __init__(self) -> None:
        self.listeners: list[Any] = []

    def subscribe(self, listener: Any) -> Any:
        self.listeners.append(listener)

        def unsubscribe() -> None:
            if listener in self.listeners:
                self.listeners.remove(listener)

        return unsubscribe

    def emit(self, kind: str, value: Any = None) -> Any:
        event = {"type": kind, "value": value}
        for listener in list(self.listeners):
            listener(event)
        return value


class _Lifecycle:
    def __init__(self, unsubscribe: Any, close_active: Any) -> None:
        self.unsubscribe = unsubscribe
        self.close_active = close_active

    def deactivate(self) -> None:
        self.unsubscribe()
        self.close_active()

    def drain(self) -> None:
        return None


class _Source(CaptureSource):
    metadata = {
        "name": "conformance:custom-source",
        "seam": "subject.subscribe",
        "identity_domain": "conformance.operation",
        "coverage": [],
        "version": "1",
    }

    def __init__(self, subject: ConformanceSubject) -> None:
        self.subject = subject

    def install(self, sink: Any) -> _Lifecycle:
        current: dict[str, str] | None = None
        current_name: str | None = None

        def handle(event: dict[str, Any]) -> None:
            nonlocal current, current_name
            kind = str(event["type"])
            if kind.endswith(".start"):
                current_name = kind[:-6]
                opened = sink.open_trace(
                    {
                        "name": current_name,
                        "native": event,
                        "semantic": {
                            "type": "workflow.run",
                            "name": current_name,
                            "framework": "conformance",
                        },
                    }
                )
                current = opened.identity if opened.accepted else None
                return
            if current is None:
                return
            if kind == "stream.delta":
                sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": kind,
                        "trace": current,
                        "native": event,
                        "semantic": {
                            "type": "state.stream_observed",
                            "value": event["value"],
                        },
                    }
                )
            elif kind.endswith(".error"):
                error = {
                    "type": "conformance_error",
                    "message": "fixture",
                    "recoverable": True,
                }
                sink.record(
                    {
                        "kind": "error",
                        "phase": "event",
                        "name": kind,
                        "trace": current,
                        "native": event,
                        "semantic": {"type": "agent.error", "error": error},
                    }
                )
                sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "error",
                        "name": kind[:-6],
                        "trace": current,
                        "native": event,
                        "semantic": {
                            "type": "workflow.run",
                            "status": "failed",
                            "error": error,
                        },
                    }
                )
                current = None
                current_name = None
            elif kind.endswith(".end"):
                sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "end",
                        "name": kind[:-4],
                        "trace": current,
                        "native": event,
                        "semantic": {
                            "type": "workflow.run",
                            "status": "succeeded",
                        },
                    }
                )
                current = None
                current_name = None
            else:
                sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": kind,
                        "trace": current,
                        "native": event,
                        "semantic": {
                            "type": "state.unknown_observed",
                            "value": event["value"],
                        },
                    }
                )

        def close_active() -> None:
            nonlocal current, current_name
            if current is None:
                return
            sink.record(
                {
                    "kind": "lifecycle",
                    "phase": "cancelled",
                    "name": current_name or "shutdown",
                    "trace": current,
                    "native": {"active_at_shutdown": True},
                    "semantic": {
                        "type": "workflow.run",
                        "status": "cancelled",
                    },
                }
            )
            current = None
            current_name = None

        return _Lifecycle(self.subject.subscribe(handle), close_active)


expectations = {
    "lifecycle": "lifecycle-ok",
    "stream": "stream-ok",
    "error": "error-ok",
    "unknown": "unknown-ok",
    "rejection": "rejection-ok",
    "shutdown": "shutdown-ok",
}


def create_subject() -> ConformanceSubject:
    return ConformanceSubject()


def create_source(*, subject: ConformanceSubject) -> CaptureSource:
    return _Source(subject)


def lifecycle(subject: ConformanceSubject) -> str:
    subject.emit("lifecycle.start", {"credential": "semantic-layer-conformance-secret-value"})
    subject.emit("lifecycle.end")
    return "lifecycle-ok"


def unknown(subject: ConformanceSubject) -> str:
    subject.emit("unknown.start")
    subject.emit("unknown.event", {"future": True})
    subject.emit("unknown.end")
    return "unknown-ok"


def stream(subject: ConformanceSubject) -> str:
    subject.emit("stream.start")
    subject.emit("stream.delta", {"text": "one"})
    subject.emit("stream.end")
    return "stream-ok"


def error(subject: ConformanceSubject) -> str:
    subject.emit("failure.start")
    subject.emit("failure.error", {"cause": "fixture"})
    return "error-ok"


def rejection(subject: ConformanceSubject) -> str:
    subject.emit("rejection.start")
    subject.emit("rejection.event", {"payload": "bounded fixture"})
    subject.emit("rejection.end")
    return "rejection-ok"


def shutdown(subject: ConformanceSubject) -> str:
    subject.emit("shutdown.start", {"active_at_shutdown": True})
    return "shutdown-ok"
