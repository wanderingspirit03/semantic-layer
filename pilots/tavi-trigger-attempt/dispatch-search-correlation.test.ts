import { describe, expect, it, vi } from 'vitest';
import { dispatchSearchWithTrustedCorrelation } from './dispatch-search-correlation.js';
import { createOpenClawToolCorrelationBridge } from './openclaw-tool-correlation.js';

describe('dispatch_search trusted correlation', () => {
  it('replaces model correlation with the trusted native run ID', async () => {
    const bridge = createOpenClawToolCorrelationBridge();
    bridge.remember(
      { toolName: 'dispatch_search', toolCallId: 'call-1' },
      { runId: 'trusted-native-run', toolCallId: 'call-1' },
    );
    const dispatch = vi.fn(async () => 'complete');

    await expect(dispatchSearchWithTrustedCorrelation({
      bridge,
      toolCallId: 'call-1',
      modelParams: {
        query: 'safe synthetic query',
        client_request_id: 'model-controlled-value',
        researchId: 'another-model-value',
        clientRequestId: 'camel-case-model-value',
        taskId: 'model-task',
        runId: 'model-run',
        correlation: { taskId: 'nested-model-task' },
        metadata: {
          tenant_task_id: 'nested-task',
          traceparent: 'nested-traceparent',
          nested: [{ rootRunId: 'nested-run', keep: 'safe-value' }],
        },
      },
      dispatch,
    })).resolves.toBe('complete');

    expect(dispatch).toHaveBeenCalledWith(
      {
        query: 'safe synthetic query',
        metadata: { nested: [{ keep: 'safe-value' }] },
      },
      { client_request_id: 'trusted-native-run' },
    );
  });

  it('keeps research fail-open and sends no model correlation when a binding is missing', async () => {
    const bridge = createOpenClawToolCorrelationBridge();
    const dispatch = vi.fn(async () => ({ customerResult: true }));

    await expect(dispatchSearchWithTrustedCorrelation({
      bridge,
      toolCallId: 'missing-call',
      modelParams: {
        query: 'safe synthetic query',
        client_request_id: 'model-controlled-value',
      },
      dispatch,
    })).resolves.toEqual({ customerResult: true });

    expect(dispatch).toHaveBeenCalledWith(
      { query: 'safe synthetic query' },
      {},
    );
  });
});
