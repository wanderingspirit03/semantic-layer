import { describe, expect, it } from 'vitest';
import type { OpenClawPluginApi } from 'openclaw/plugin-sdk/plugin-entry';
import {
  createOpenClawToolCorrelationBridge,
  registerOpenClawToolCorrelation,
} from './openclaw-tool-correlation.js';

describe('trusted OpenClaw tool correlation bridge', () => {
  it('registers against the exact OpenClaw 2026.5.5 hook contract', () => {
    const handlers: Array<Parameters<OpenClawPluginApi['on']>[1]> = [];
    const api = {
      on: ((name, handler) => {
        expect(name).toBe('before_tool_call');
        handlers.push(handler);
      }) as OpenClawPluginApi['on'],
    };
    const bridge = createOpenClawToolCorrelationBridge();
    registerOpenClawToolCorrelation(api, bridge);

    expect(handlers).toHaveLength(1);
  });

  it('moves the trusted native run ID through the exact tool call ID', () => {
    const bridge = createOpenClawToolCorrelationBridge();
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'call-1' },
      { runId: 'native-openclaw-run', toolCallId: 'call-1' },
    );

    expect(bridge.consume('call-1')).toBe('native-openclaw-run');
    expect(bridge.consume('call-1')).toBeUndefined();
  });

  it('does not read a model supplied research ID', () => {
    const bridge = createOpenClawToolCorrelationBridge();
    const modelParams = { client_request_id: 'model-controlled-value' };
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'call-safe' },
      { runId: 'trusted-runtime-run', toolCallId: 'call-safe' },
    );

    expect(modelParams.client_request_id).toBe('model-controlled-value');
    expect(bridge.consume('call-safe')).toBe('trusted-runtime-run');
  });

  it('fails closed on a conflicting tool call binding', () => {
    const bridge = createOpenClawToolCorrelationBridge();
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'call-conflict' },
      { runId: 'run-a' },
    );
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'call-conflict' },
      { runId: 'run-b' },
    );

    expect(bridge.consume('call-conflict')).toBeUndefined();
  });

  it('rejects mismatched host call IDs and altered native identities', () => {
    const bridge = createOpenClawToolCorrelationBridge();
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'event-call' },
      { runId: 'trusted-run', toolCallId: 'context-call' },
    );
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'whitespace-call' },
      { runId: ' trusted-run ' },
    );
    bridge.remember(
      {
        toolName: 'dispatch_search',
        toolCallId: 'run-mismatch',
        runId: 'event-run',
      },
      { runId: 'context-run', toolCallId: 'run-mismatch' },
    );

    expect(bridge.consume('event-call')).toBeUndefined();
    expect(bridge.consume('context-call')).toBeUndefined();
    expect(bridge.consume('whitespace-call')).toBeUndefined();
    expect(bridge.consume('run-mismatch')).toBeUndefined();
  });

  it('ignores unrelated tools, expired entries, and entries above capacity', () => {
    let time = 1_000;
    const bridge = createOpenClawToolCorrelationBridge({
      maxBindings: 1,
      ttlMs: 50,
      now: () => time,
    });
    bridge.remember(
      { toolName: 'read_file', toolCallId: 'unrelated' },
      { runId: 'run-unrelated' },
    );
    expect(bridge.consume('unrelated')).toBeUndefined();

    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'first' },
      { runId: 'run-first' },
    );
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'second' },
      { runId: 'run-second' },
    );
    expect(bridge.consume('second')).toBeUndefined();

    time += 51;
    expect(bridge.consume('first')).toBeUndefined();
  });
});
