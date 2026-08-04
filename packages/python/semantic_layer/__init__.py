"""Public Python interface for local semantic trace capture."""

from .capture_v1 import (
    LOSS_REASONS,
    AdmissionReceipt,
    CaptureHandle,
    CaptureSource,
    CaptureStatus,
    OpenTraceReceipt,
    OpenTraceRecord,
    SemanticLayer,
    SourceEventKind,
    SourceRecord,
    TraceIdentity,
    initialize,
    reset_capture_for_tests,
)
from .crewai_adapter import crewai_adapter
from .custom_agent import (
    CustomAgentBridge,
    CustomAgentError,
    CustomAgentEvent,
    create_custom_agent_source,
)
from .framework_adapters import (
    google_adk_adapter,
    langgraph_adapter,
    openai_agents_adapter,
    pydantic_ai_adapter,
    strands_adapter,
)
from .haystack_otel_adapter import haystack_otel_adapter
from .llamaindex_adapter import llamaindex_adapter
from .microsoft_agent_framework_adapter import microsoft_agent_framework_adapter
from .otel import OpenTelemetrySource, create_otel_source
from .provider_adapters import (
    anthropic_provider_adapter,
    gemini_provider_adapter,
    openai_provider_adapter,
    provider_capture_context,
)

__all__ = [
    "LOSS_REASONS",
    "AdmissionReceipt",
    "CaptureHandle",
    "CaptureSource",
    "CaptureStatus",
    "OpenTraceReceipt",
    "OpenTraceRecord",
    "SemanticLayer",
    "SourceEventKind",
    "SourceRecord",
    "TraceIdentity",
    "initialize",
    "reset_capture_for_tests",
    "openai_provider_adapter",
    "anthropic_provider_adapter",
    "gemini_provider_adapter",
    "provider_capture_context",
    "openai_agents_adapter",
    "langgraph_adapter",
    "strands_adapter",
    "pydantic_ai_adapter",
    "google_adk_adapter",
    "crewai_adapter",
    "CustomAgentBridge",
    "CustomAgentError",
    "CustomAgentEvent",
    "create_custom_agent_source",
    "microsoft_agent_framework_adapter",
    "llamaindex_adapter",
    "haystack_otel_adapter",
    "OpenTelemetrySource",
    "create_otel_source",
]
