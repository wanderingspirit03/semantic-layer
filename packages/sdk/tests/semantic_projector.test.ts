import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { expect, it } from 'vitest';

import { initialize, resetCaptureForTests } from '../src/index.js';
import { SemanticProjector } from '../src/trace/semantic-projector.js';
import type { SemanticCaptureEventV1 } from '../src/v1/generated.js';

const source = {
  source_id: 'official/example-framework',
  name: 'example-framework',
  seam: 'fixture.callback',
  identity_domain: 'fixture.operation',
  official: true,
};

function capture(
  recordId: string,
  input: Partial<SemanticCaptureEventV1> & Pick<
    SemanticCaptureEventV1,
    'event_kind' | 'phase' | 'name'
  >,
): SemanticCaptureEventV1 {
  return {
    schema: 'semantic_capture_event_v1',
    run_id: 'run_fixture',
    record_id: recordId,
    seq: 1,
    observed_at: '2026-07-26T00:00:00.000Z',
    monotonic_ns: 1,
    trace_id: 'trace_fixture',
    source,
    native: null,
    semantic: {},
    correlation: {},
    loss_refs: [],
    blob_refs: [],
    provenance: {
      language: 'typescript',
      sdk_name: 'semantic-layer-capture',
      sdk_version: 'fixture',
      capture_policy: 'rich_local_credential_scrubbed',
    },
    ...input,
  };
}

async function expectContractRecords(records: unknown[]): Promise<void> {
  const schema = JSON.parse(await readFile(
    new URL('../../../contracts/trace/v1/semantic-trace-record.schema.json', import.meta.url),
    'utf8',
  ));
  const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
  addFormats(ajv);
  const validate = ajv.compile(schema);
  for (const record of records) {
    expect(validate(record), ajv.errorsText(validate.errors)).toBe(true);
  }
}

it('retires model, tool, and lifecycle correlation after a payload omission', () => {
  const projector = new SemanticProjector();
  projector.project(capture('root_start', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'run',
    semantic: { type: 'agent.run', name: 'run' },
  }));
  projector.project(capture('model_start_1', {
    event_kind: 'model',
    phase: 'start',
    name: 'model',
    native_identity: 'model_identity',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'model.request', model: 'fixture' },
  }));
  projector.retireOmitted(capture('model_end_omitted', {
    event_kind: 'model',
    phase: 'end',
    name: 'model',
    native_identity: 'model_identity',
    correlation: { parent_record_id: 'model_start_1' },
    semantic: { type: 'model.response', status: 'completed' },
  }));
  expect(projector.project(capture('model_start_2', {
    event_kind: 'model',
    phase: 'start',
    name: 'model',
    native_identity: 'model_identity',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'model.request', model: 'fixture' },
  }))).toEqual([expect.objectContaining({ kind: 'model.request' })]);

  projector.project(capture('tool_start_1', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup',
    native_identity: 'tool_identity',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'tool.execution', name: 'lookup', input: {} },
  }));
  projector.retireOmitted(capture('tool_end_omitted', {
    event_kind: 'tool',
    phase: 'end',
    name: 'lookup',
    native_identity: 'tool_identity',
    correlation: { parent_record_id: 'tool_start_1' },
    semantic: { type: 'tool.result', status: 'succeeded', output: null },
  }));
  expect(projector.project(capture('tool_start_2', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup',
    native_identity: 'tool_identity',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'tool.execution', name: 'lookup', input: {} },
  }))).toEqual([expect.objectContaining({ kind: 'tool.call' })]);

  projector.retireOmitted(capture('root_end_omitted', {
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'run',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'agent.run', status: 'succeeded' },
  }));
  expect(projector.project(capture('root_start_2', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'run two',
    semantic: { type: 'agent.run', name: 'run two' },
  }))).toEqual([expect.objectContaining({ kind: 'run.start' })]);
});

it('keeps ambiguous correlations and retires an omitted proposal transition', () => {
  const projector = new SemanticProjector();
  projector.project(capture('root_start', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'run',
    semantic: { type: 'agent.run', name: 'run' },
  }));
  for (const identity of ['model_a', 'model_b']) {
    projector.project(capture(`${identity}_start`, {
      event_kind: 'model',
      phase: 'start',
      name: 'model',
      native_identity: identity,
      correlation: { parent_record_id: 'root_start' },
      semantic: { type: 'model.request', model: 'fixture' },
    }));
  }
  projector.retireOmitted(capture('ambiguous_model_end', {
    event_kind: 'model',
    phase: 'end',
    name: 'model',
    native_identity: 'model_b',
    correlation: { parent_record_id: 'model_a_start' },
    semantic: { type: 'model.response', status: 'completed' },
  }));
  for (const identity of ['model_a', 'model_b']) {
    expect(projector.project(capture(`${identity}_duplicate`, {
      event_kind: 'model',
      phase: 'start',
      name: 'model',
      native_identity: identity,
      correlation: { parent_record_id: 'root_start' },
      semantic: { type: 'model.request', model: 'fixture' },
    }))).toEqual([
      expect.objectContaining({
        kind: 'loss',
        data: expect.objectContaining({ reason: 'duplicate_active_model_identity' }),
      }),
    ]);
  }

  for (const identity of ['tool_a', 'tool_b']) {
    projector.project(capture(`${identity}_start`, {
      event_kind: 'tool',
      phase: 'start',
      name: 'lookup',
      native_identity: identity,
      correlation: { parent_record_id: 'root_start' },
      semantic: { type: 'tool.execution', name: 'lookup', input: {} },
    }));
  }
  projector.retireOmitted(capture('ambiguous_tool_end', {
    event_kind: 'tool',
    phase: 'end',
    name: 'lookup',
    native_identity: 'tool_b',
    correlation: { parent_record_id: 'tool_a_start' },
    semantic: { type: 'tool.result', status: 'succeeded', output: null },
  }));
  for (const identity of ['tool_a', 'tool_b']) {
    expect(projector.project(capture(`${identity}_duplicate`, {
      event_kind: 'tool',
      phase: 'start',
      name: 'lookup',
      native_identity: identity,
      correlation: { parent_record_id: 'root_start' },
      semantic: { type: 'tool.execution', name: 'lookup', input: {} },
    }))).toEqual([
      expect.objectContaining({
        kind: 'loss',
        data: expect.objectContaining({ reason: 'duplicate_active_tool_identity' }),
      }),
    ]);
  }

  projector.project(capture('proposal_start', {
    event_kind: 'tool',
    phase: 'start',
    name: 'proposed',
    native_identity: 'proposal_identity',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'tool.proposal', name: 'proposed', input: {} },
  }));
  projector.retireOmitted(capture('proposal_execution_omitted', {
    event_kind: 'tool',
    phase: 'start',
    name: 'proposed',
    native_identity: 'proposal_identity',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'tool.execution', name: 'proposed', input: {} },
  }));
  expect(projector.project(capture('proposal_reused', {
    event_kind: 'tool',
    phase: 'start',
    name: 'proposed',
    native_identity: 'proposal_identity',
    correlation: { parent_record_id: 'root_start' },
    semantic: { type: 'tool.proposal', name: 'proposed', input: {} },
  }))).toEqual([expect.objectContaining({ kind: 'tool.proposal' })]);
});

it('projects multiple agent roots and a nested scope into one contiguous capture-session timeline', async () => {
  const projector = new SemanticProjector();
  const records = [
    ...projector.project(capture('record_root_a', {
      event_kind: 'lifecycle',
      phase: 'start',
      name: 'first agent run',
      turn_id: 'turn_a',
      native: { ignored: 'native payload is not parsed' },
      semantic: {
        type: 'agent.run',
        name: 'first agent run',
        input: { goal: 'first' },
      },
    })),
    ...projector.project(capture('record_scope_a', {
      event_kind: 'lifecycle',
      phase: 'start',
      name: 'first turn',
      correlation: { parent_record_id: 'record_root_a' },
      semantic: {
        type: 'scope',
        scope_id: 'scope_turn_a',
        scope_type: 'turn',
        name: 'first turn',
      },
    })),
    ...projector.project(capture('record_scope_a_end', {
      event_kind: 'lifecycle',
      phase: 'end',
      name: 'first turn',
      correlation: { parent_record_id: 'record_scope_a' },
      semantic: {
        type: 'scope',
        scope_id: 'scope_turn_a',
        scope_type: 'turn',
        status: 'succeeded',
      },
    })),
    ...projector.project(capture('record_root_a_end', {
      event_kind: 'lifecycle',
      phase: 'end',
      name: 'first agent run',
      correlation: { parent_record_id: 'record_root_a' },
      semantic: {
        type: 'agent.run',
        status: 'succeeded',
        summary: 'First run terminated normally.',
      },
    })),
    ...projector.project(capture('record_root_b', {
      event_kind: 'lifecycle',
      phase: 'start',
      name: 'second agent run',
      trace_id: 'trace_second',
      turn_id: 'turn_b',
      previous_turn_id: 'turn_a',
      semantic: {
        type: 'workflow.run',
        name: 'second agent run',
      },
    })),
    ...projector.project(capture('record_root_b_end', {
      event_kind: 'lifecycle',
      phase: 'cancelled',
      name: 'second agent run',
      trace_id: 'trace_second',
      correlation: { parent_record_id: 'record_root_b' },
      semantic: {
        type: 'workflow.run',
        status: 'cancelled',
      },
    })),
  ];

  expect(records.map((record) => record.seq)).toEqual([1, 2, 3, 4, 5, 6]);
  expect(records.map((record) => record.kind)).toEqual([
    'run.start',
    'scope',
    'scope',
    'run.outcome',
    'run.start',
    'run.outcome',
  ]);
  expect(records[1]).toMatchObject({
    parent: 'record_root_a',
    data: { scope_id: 'scope_turn_a', type: 'turn', phase: 'start' },
  });
  expect(records[3]).toMatchObject({
    parent: 'record_root_a',
    data: { status: 'completed', summary: 'First run terminated normally.' },
  });
  expect(records[4].links).toEqual([{ type: 'continues_from', record: 'record_root_a' }]);
  expect(records[5].data).not.toHaveProperty('summary');
  expect(records[0].source).toBe(records[5].source);
  expect(records[0].source).toMatch(/^src_[a-f0-9]{24}$/);
  await expectContractRecords(records);
});

it('keeps external continuation identity without inventing a cross-bundle loss', async () => {
  const projector = new SemanticProjector();
  const records = projector.project(capture('record_root_resumed', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'resumed agent run',
    trace_id: 'trace_resumed',
    conversation_id: 'conversation_shared',
    turn_id: 'turn_b',
    turn_index: 1,
    previous_turn_id: 'turn_a',
    semantic: {
      type: 'agent.run',
      name: 'resumed agent run',
    },
  }));

  expect(records).toHaveLength(1);
  expect(records[0].data).toEqual({
    name: 'resumed agent run',
    conversation_id: 'conversation_shared',
    turn_id: 'turn_b',
    turn_index: 1,
    previous_turn_id: 'turn_a',
  });
  expect(records[0].links).toBeUndefined();
  await expectContractRecords(records);
});

it('pairs out-of-order same-name tool results by exact call identity', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'tool run',
    semantic: { type: 'agent.run', name: 'tool run' },
  }));
  const proposalA = projector.project(capture('record_proposal_a', {
    event_kind: 'tool',
    phase: 'event',
    name: 'tool proposed',
    native_identity: 'native A',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'tool.proposal',
      call_id: 'call_a',
      native_call_id: 'native A',
      name: 'read_file',
      input: { path: 'a.ts' },
    },
  }))[0];
  const proposalB = projector.project(capture('record_proposal_b', {
    event_kind: 'tool',
    phase: 'event',
    name: 'tool proposed',
    native_identity: 'native B',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'tool.proposal',
      call_id: 'call_b',
      native_call_id: 'native B',
      name: 'read_file',
      input: { path: 'b.ts' },
    },
  }))[0];
  const callA = projector.project(capture('record_call_a', {
    event_kind: 'tool',
    phase: 'start',
    name: 'read_file',
    native_identity: 'native A',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'tool.execution',
      call_id: 'call_a',
      native_call_id: 'native A',
      name: 'read_file',
      input: { path: 'a.ts' },
    },
  }))[0];
  const callB = projector.project(capture('record_call_b', {
    event_kind: 'tool',
    phase: 'start',
    name: 'read_file',
    native_identity: 'native B',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'tool.execution',
      call_id: 'call_b',
      native_call_id: 'native B',
      name: 'read_file',
      input: { path: 'b.ts' },
    },
  }))[0];
  const resultB = projector.project(capture('record_result_b', {
    event_kind: 'tool',
    phase: 'end',
    name: 'read_file',
    native_identity: 'native B',
    correlation: { parent_record_id: 'record_call_b' },
    semantic: {
      type: 'tool.result',
      call_id: 'call_b',
      native_call_id: 'native B',
      status: 'succeeded',
      output: { lines: 20 },
    },
  }))[0];
  const resultA = projector.project(capture('record_result_a', {
    event_kind: 'tool',
    phase: 'end',
    name: 'read_file',
    native_identity: 'native A',
    correlation: { parent_record_id: 'record_call_a' },
    semantic: {
      type: 'tool.result',
      call_id: 'call_a',
      native_call_id: 'native A',
      status: 'succeeded',
      output: { lines: 10 },
    },
  }))[0];
  const reusedCallA = projector.project(capture('record_call_a_reused', {
    event_kind: 'tool',
    phase: 'start',
    name: 'read_file',
    native_identity: 'native A',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'tool.execution',
      call_id: 'call_a_reused',
      native_call_id: 'native A',
      name: 'read_file',
      input: { path: 'reused.ts' },
    },
  }))[0];
  const staleResultA = projector.project(capture('record_result_a_stale', {
    event_kind: 'tool',
    phase: 'end',
    name: 'read_file',
    native_identity: 'native A',
    correlation: { parent_record_id: 'record_call_a' },
    semantic: {
      type: 'tool.result',
      native_call_id: 'native A',
      status: 'succeeded',
      output: { lines: 999 },
    },
  }))[0];
  const reusedResultA = projector.project(capture('record_result_a_reused', {
    event_kind: 'tool',
    phase: 'end',
    name: 'read_file',
    native_identity: 'native A',
    correlation: { parent_record_id: 'record_call_a_reused' },
    semantic: {
      type: 'tool.result',
      native_call_id: 'native A',
      status: 'succeeded',
      output: { lines: 30 },
    },
  }))[0];

  const canonicalA = proposalA.data.call_id;
  const canonicalB = proposalB.data.call_id;
  expect(canonicalA).toMatch(/^call_[a-f0-9]{24}$/);
  expect(canonicalB).toMatch(/^call_[a-f0-9]{24}$/);
  expect(canonicalA).not.toBe(canonicalB);
  expect(proposalA).toMatchObject({
    kind: 'tool.proposal',
    data: { call_id: canonicalA, native_call_id: 'native A', input: { path: 'a.ts' } },
  });
  expect(proposalB.data).toMatchObject({ call_id: canonicalB, input: { path: 'b.ts' } });
  expect(callA).toMatchObject({
    kind: 'tool.call',
    data: { call_id: canonicalA, input: { path: 'a.ts' } },
    links: [{ type: 'derived_from', record: 'record_proposal_a' }],
  });
  expect(callB.links).toEqual([{ type: 'derived_from', record: 'record_proposal_b' }]);
  expect(resultB).toMatchObject({
    kind: 'tool.result',
    parent: 'record_root',
    data: { call_id: canonicalB, output: { lines: 20 } },
    links: [{ type: 'result_of', record: 'record_call_b' }],
  });
  expect(resultA).toMatchObject({
    kind: 'tool.result',
    parent: 'record_root',
    data: { call_id: canonicalA, output: { lines: 10 } },
    links: [{ type: 'result_of', record: 'record_call_a' }],
  });
  expect(reusedCallA).toMatchObject({
    kind: 'tool.call',
    data: { call_id: canonicalA, input: { path: 'reused.ts' } },
  });
  expect(reusedCallA.links).toBeUndefined();
  expect(staleResultA).toMatchObject({
    kind: 'loss',
    parent: 'record_root',
    data: { reason: 'unmatched_tool_result' },
  });
  expect(reusedResultA).toMatchObject({
    kind: 'tool.result',
    data: { call_id: canonicalA, output: { lines: 30 } },
    links: [{ type: 'result_of', record: 'record_call_a_reused' }],
  });
  projector.project(capture('record_other_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'other run',
    trace_id: 'trace_other',
    semantic: { type: 'agent.run' },
  }));
  const sameNativeIdInOtherRoot = projector.project(capture('record_other_proposal', {
    event_kind: 'tool',
    phase: 'event',
    name: 'tool proposed',
    trace_id: 'trace_other',
    correlation: { parent_record_id: 'record_other_root' },
    semantic: {
      type: 'tool.proposal',
      call_id: 'call_a',
      native_call_id: 'native A',
      name: 'read_file',
      input: { path: 'other.ts' },
    },
  }))[0];
  expect(sameNativeIdInOtherRoot.data.call_id).not.toBe(canonicalA);
  const sameNativeIdInOtherSource = projector.project(capture('record_other_source_proposal', {
    event_kind: 'tool',
    phase: 'event',
    name: 'tool proposed',
    source: {
      ...source,
      source_id: 'official/other-framework',
      identity_domain: 'other.operation',
    },
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'tool.proposal',
      call_id: 'different_normalized_id',
      native_call_id: 'native A',
      name: 'read_file',
      input: { path: 'other-source.ts' },
    },
  }))[0];
  expect(sameNativeIdInOtherSource.data.call_id).not.toBe(canonicalA);
  expect(sameNativeIdInOtherSource.data.native_call_id).toBe('native A');
  await expectContractRecords([
    proposalA,
    proposalB,
    callA,
    callB,
    resultB,
    resultA,
    reusedCallA,
    staleResultA,
    reusedResultA,
    sameNativeIdInOtherRoot,
    sameNativeIdInOtherSource,
  ]);
});

it('links a provider proposal to a custom call by exact cross-source native identity', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('cross_source_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'cross-source tool run',
    semantic: { type: 'agent.run', name: 'cross-source tool run' },
  }));
  const [proposal] = projector.project(capture('provider_proposal', {
    event_kind: 'tool',
    phase: 'event',
    name: 'lookup proposed',
    source: {
      ...source,
      source_id: 'provider/openrouter',
      name: 'openrouter',
      identity_domain: 'openai.operation',
    },
    correlation: { parent_record_id: 'cross_source_root' },
    semantic: {
      type: 'tool.proposal',
      native_call_id: 'shared-native-call',
      name: 'lookup',
      input: { query: 'alpha' },
    },
  }));
  const [call] = projector.project(capture('custom_call', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup',
    source: {
      ...source,
      source_id: 'custom/agent',
      name: 'custom agent',
      identity_domain: 'custom.operation',
      official: false,
    },
    correlation: { parent_record_id: 'cross_source_root' },
    semantic: {
      type: 'tool.execution',
      native_call_id: 'shared-native-call',
      name: 'lookup',
      input: { query: 'alpha' },
    },
  }));
  const [result] = projector.project(capture('custom_result', {
    event_kind: 'tool',
    phase: 'end',
    name: 'lookup result',
    source: {
      ...source,
      source_id: 'custom/agent',
      name: 'custom agent',
      identity_domain: 'custom.operation',
      official: false,
    },
    correlation: { parent_record_id: 'custom_call' },
    semantic: {
      type: 'tool.result',
      native_call_id: 'shared-native-call',
      status: 'succeeded',
      output: { answer: 42 },
    },
  }));

  expect(call).toMatchObject({
    kind: 'tool.call',
    data: { call_id: proposal.data.call_id, native_call_id: 'shared-native-call' },
    links: [{ type: 'derived_from', record: proposal.id }],
  });
  expect(result).toMatchObject({
    kind: 'tool.result',
    data: { call_id: proposal.data.call_id, output: { answer: 42 } },
    links: [{ type: 'result_of', record: call.id }],
  });
  await expectContractRecords([proposal, call, result]);
});

it('keeps different cross-source native tool identities independent', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('independent_source_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'independent cross-source tool run',
    semantic: { type: 'agent.run', name: 'independent cross-source tool run' },
  }));
  const [proposal] = projector.project(capture('independent_provider_proposal', {
    event_kind: 'tool',
    phase: 'event',
    name: 'lookup proposed',
    source: {
      ...source,
      source_id: 'provider/openrouter',
      name: 'openrouter',
      identity_domain: 'openai.operation',
    },
    correlation: { parent_record_id: 'independent_source_root' },
    semantic: {
      type: 'tool.proposal',
      native_call_id: 'provider-call',
      name: 'lookup',
      input: { query: 'alpha' },
    },
  }));
  const [call] = projector.project(capture('independent_custom_call', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup',
    source: {
      ...source,
      source_id: 'custom/agent',
      name: 'custom agent',
      identity_domain: 'custom.operation',
      official: false,
    },
    correlation: { parent_record_id: 'independent_source_root' },
    semantic: {
      type: 'tool.execution',
      native_call_id: 'custom-call',
      name: 'lookup',
      input: { query: 'alpha' },
    },
  }));

  expect(call.kind).toBe('tool.call');
  expect(call.data.call_id).not.toBe(proposal.data.call_id);
  expect(call.links).toBeUndefined();
  await expectContractRecords([proposal, call]);
});

it('reports duplicate cross-source proposal identity instead of guessing', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('ambiguous_source_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'ambiguous cross-source tool run',
    semantic: { type: 'agent.run', name: 'ambiguous cross-source tool run' },
  }));
  const [proposal] = projector.project(capture('ambiguous_provider_proposal', {
    event_kind: 'tool',
    phase: 'event',
    name: 'lookup proposed',
    source: {
      ...source,
      source_id: 'provider/openrouter',
      name: 'openrouter',
      identity_domain: 'openai.operation',
    },
    correlation: { parent_record_id: 'ambiguous_source_root' },
    semantic: {
      type: 'tool.proposal',
      native_call_id: 'ambiguous-call',
      name: 'lookup',
      input: { query: 'alpha' },
    },
  }));
  const [duplicate] = projector.project(capture('ambiguous_custom_proposal', {
    event_kind: 'tool',
    phase: 'event',
    name: 'lookup proposed',
    source: {
      ...source,
      source_id: 'custom/agent',
      name: 'custom agent',
      identity_domain: 'custom.operation',
      official: false,
    },
    correlation: { parent_record_id: 'ambiguous_source_root' },
    semantic: {
      type: 'tool.proposal',
      native_call_id: 'ambiguous-call',
      name: 'lookup',
      input: { query: 'alpha' },
    },
  }));
  const [call] = projector.project(capture('ambiguous_custom_call', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup',
    source: {
      ...source,
      source_id: 'custom/agent',
      name: 'custom agent',
      identity_domain: 'custom.operation',
      official: false,
    },
    correlation: { parent_record_id: 'ambiguous_source_root' },
    semantic: {
      type: 'tool.execution',
      native_call_id: 'ambiguous-call',
      name: 'lookup',
      input: { query: 'alpha' },
    },
  }));

  expect(duplicate).toMatchObject({
    kind: 'loss',
    data: {
      reason: 'duplicate_active_tool_identity',
      stage: 'source',
      count: 1,
      recoverable: false,
    },
  });
  expect(call).toMatchObject({
    kind: 'tool.call',
    data: { call_id: proposal.data.call_id },
    links: [{ type: 'derived_from', record: proposal.id }],
  });
  await expectContractRecords([proposal, duplicate, call]);
});

it('does not fabricate a tool result when no exact call can be found', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'tool run',
    semantic: { type: 'agent.run', name: 'tool run' },
  }));

  const [unmatched] = projector.project(capture('record_unmatched_result', {
    event_kind: 'tool',
    phase: 'end',
    name: 'read_file',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'tool.result',
      call_id: 'call_missing',
      status: 'succeeded',
      output: { lines: 10 },
    },
  }));

  expect(unmatched).toMatchObject({
    kind: 'loss',
    parent: 'record_root',
    data: {
      reason: 'unmatched_tool_result',
      stage: 'source',
      count: 1,
      recoverable: false,
    },
  });
  expect(JSON.stringify(unmatched)).not.toContain('"lines":10');
  await expectContractRecords([unmatched]);
});

it('lets terminal error and cancellation phases override stale success statuses', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'terminal precedence',
    semantic: { type: 'agent.run', name: 'terminal precedence' },
  }));
  const model = projector.project(capture('record_model_failed', {
    event_kind: 'model',
    phase: 'error',
    name: 'failed response',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.response', status: 'completed' },
  }))[0];
  projector.project(capture('record_tool_start', {
    event_kind: 'tool',
    phase: 'start',
    name: 'cancelled tool',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'tool.execution', name: 'cancelled tool', input: {} },
  }));
  const tool = projector.project(capture('record_tool_cancelled', {
    event_kind: 'tool',
    phase: 'cancelled',
    name: 'cancelled tool',
    correlation: { parent_record_id: 'record_tool_start' },
    semantic: { type: 'tool.result', status: 'succeeded' },
  }))[0];

  expect(model).toMatchObject({ kind: 'model.response', data: { status: 'failed' } });
  expect(tool).toMatchObject({ kind: 'tool.result', data: { status: 'cancelled' } });
  await expectContractRecords([model, tool]);
});

it('pairs concurrent model responses only by exact native identity or request parent', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'model run',
    semantic: { type: 'agent.run', name: 'model run' },
  }));
  const requestA = projector.project(capture('record_request_a', {
    event_kind: 'model',
    phase: 'event',
    name: 'request A',
    native_identity: 'native-request-a',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.request', model: 'fixture-model', context_refs: [] },
  }))[0];
  const requestB = projector.project(capture('record_request_b', {
    event_kind: 'model',
    phase: 'event',
    name: 'request B',
    native_identity: 'native-request-b',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.request', model: 'fixture-model', context_refs: [] },
  }))[0];
  const responseB = projector.project(capture('record_response_b', {
    event_kind: 'model',
    phase: 'event',
    name: 'response B',
    native_identity: 'native-request-b',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.response', status: 'completed', content: 'B' },
  }))[0];
  const responseA = projector.project(capture('record_response_a', {
    event_kind: 'model',
    phase: 'event',
    name: 'response A',
    native_identity: 'native-request-a',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.response', status: 'completed', content: 'A' },
  }))[0];
  const requestC = projector.project(capture('record_request_c', {
    event_kind: 'model',
    phase: 'event',
    name: 'request C',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.request', context_refs: [] },
  }))[0];
  const responseC = projector.project(capture('record_response_c', {
    event_kind: 'model',
    phase: 'event',
    name: 'response C',
    correlation: { parent_record_id: 'record_request_c' },
    semantic: { type: 'model.response', status: 'completed', content: 'C' },
  }))[0];
  const unpaired = projector.project(capture('record_response_ambiguous', {
    event_kind: 'model',
    phase: 'event',
    name: 'unpaired response',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.response', status: 'completed', content: '?' },
  }))[0];
  const reusedRequestA = projector.project(capture('record_request_a_reused', {
    event_kind: 'model',
    phase: 'event',
    name: 'request A reused',
    native_identity: 'native-request-a',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'model.request', model: 'fixture-model', context_refs: [] },
  }))[0];
  const staleResponseA = projector.project(capture('record_response_a_stale', {
    event_kind: 'model',
    phase: 'event',
    name: 'stale response A',
    native_identity: 'native-request-a',
    correlation: { parent_record_id: 'record_request_a' },
    semantic: { type: 'model.response', status: 'completed', content: 'stale' },
  }))[0];
  const reusedResponseA = projector.project(capture('record_response_a_reused', {
    event_kind: 'model',
    phase: 'event',
    name: 'response A reused',
    native_identity: 'native-request-a',
    correlation: { parent_record_id: 'record_request_a_reused' },
    semantic: { type: 'model.response', status: 'completed', content: 'reused' },
  }))[0];

  expect(responseB.links).toEqual([{ type: 'result_of', record: requestB.id }]);
  expect(responseA.links).toEqual([{ type: 'result_of', record: requestA.id }]);
  expect(responseC.links).toEqual([{ type: 'result_of', record: requestC.id }]);
  expect(responseC.parent).toBe('record_root');
  expect(unpaired.links).toBeUndefined();
  expect(staleResponseA).toMatchObject({
    kind: 'loss',
    data: { reason: 'unmatched_model_response' },
  });
  expect(staleResponseA.parent).toBe('record_root');
  expect(reusedResponseA).toMatchObject({
    parent: 'record_root',
    links: [{ type: 'result_of', record: reusedRequestA.id }],
  });
  await expectContractRecords([
    requestA,
    requestB,
    responseB,
    responseA,
    requestC,
    responseC,
    unpaired,
    reusedRequestA,
    staleResponseA,
    reusedResponseA,
  ]);
});

it('preserves repeated exposed reasoning in source order and separate from content', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'reasoning run',
    semantic: { type: 'agent.run', name: 'reasoning run' },
  }));
  const [response] = projector.project(capture('record_reasoning_response', {
    event_kind: 'model',
    phase: 'end',
    name: 'model response',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'model.response',
      status: 'completed',
      content: 'The answer is 42.',
      reasoning: [
        { type: 'summary', text: 'Checked the available evidence.' },
        { type: 'summary', text: 'Checked the available evidence.' },
        { type: 'text', text: 'The provider exposed this reasoning.' },
      ],
    },
  }));

  expect(response.data).toEqual({
    status: 'completed',
    content: 'The answer is 42.',
    reasoning: [
      { type: 'summary', text: 'Checked the available evidence.' },
      { type: 'summary', text: 'Checked the available evidence.' },
      { type: 'text', text: 'The provider exposed this reasoning.' },
    ],
  });
  await expectContractRecords([response]);
});

it('contains unparented records under the exact trace root without guessing root outcomes', async () => {
  const projector = new SemanticProjector();
  const [root] = projector.project(capture('record_fallback_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'fallback root',
    semantic: { type: 'agent.run', name: 'fallback root' },
  }));
  const [message] = projector.project(capture('record_fallback_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'user message',
    semantic: { type: 'message', role: 'user', content: 'Continue.' },
  }));
  const [state] = projector.project(capture('record_fallback_state', {
    event_kind: 'state',
    phase: 'event',
    name: 'state',
    semantic: {
      type: 'state.transition',
      state_type: 'state.ready',
      value: true,
    },
  }));
  const [request] = projector.project(capture('record_fallback_request', {
    event_kind: 'model',
    phase: 'event',
    name: 'request',
    native_identity: 'fallback-request',
    semantic: {
      type: 'model.request',
      context_refs: ['record_fallback_message'],
    },
  }));
  const [response] = projector.project(capture('record_fallback_response', {
    event_kind: 'model',
    phase: 'event',
    name: 'response',
    native_identity: 'fallback-request',
    semantic: {
      type: 'model.response',
      status: 'completed',
      content: 'Done.',
    },
  }));
  for (const record of [message, state, request, response]) {
    expect(record.parent).toBe(root.id);
  }
  expect(response.links).toEqual([{ type: 'result_of', record: request.id }]);

  const [uncorrelatedTerminal] = projector.project(capture('record_uncorrelated_terminal', {
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'uncorrelated terminal',
    semantic: { type: 'agent.run', status: 'succeeded' },
  }));
  expect(uncorrelatedTerminal).toMatchObject({
    kind: 'loss',
    parent: root.id,
    data: { reason: 'unsupported_semantic_projection' },
  });
  const [outcome] = projector.project(capture('record_fallback_outcome', {
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'fallback root',
    correlation: { parent_record_id: 'record_fallback_root' },
    semantic: { type: 'agent.run', status: 'succeeded' },
  }));
  expect(outcome).toMatchObject({
    kind: 'run.outcome',
    parent: root.id,
    data: { status: 'completed' },
  });
  await expectContractRecords([
    root,
    message,
    state,
    request,
    response,
    uncorrelatedTerminal,
    outcome,
  ]);
});

it('records unresolved parents and failed lifecycle terminals without inventing structure', async () => {
  const projector = new SemanticProjector();
  const [orphan] = projector.project(capture('record_orphan', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'nested run with missing parent',
    correlation: { parent_record_id: 'record_missing' },
    semantic: {
      type: 'agent.run',
      name: 'nested run with missing parent',
      input: { goal: 'must remain nested' },
    },
  }));
  expect(orphan).toMatchObject({
    kind: 'loss',
    data: {
      reason: 'unresolved_parent',
      stage: 'source',
      count: 1,
      recoverable: false,
    },
  });
  expect(orphan).not.toHaveProperty('parent');
  const [orphanChild] = projector.project(capture('record_orphan_child', {
    event_kind: 'log',
    phase: 'event',
    name: 'orphan child',
    correlation: { parent_record_id: 'record_orphan' },
    semantic: {
      type: 'message',
      role: 'assistant',
      content: 'Must not be placed under the orphan loss.',
    },
  }));
  const [orphanGrandchild] = projector.project(capture('record_orphan_grandchild', {
    event_kind: 'log',
    phase: 'event',
    name: 'orphan grandchild',
    correlation: { parent_record_id: 'record_orphan_child' },
    semantic: {
      type: 'message',
      role: 'assistant',
      content: 'Must remain unresolved too.',
    },
  }));
  for (const descendant of [orphanChild, orphanGrandchild]) {
    expect(descendant).toMatchObject({
      kind: 'loss',
      data: { reason: 'unresolved_parent' },
    });
    expect(descendant).not.toHaveProperty('parent');
  }
  const [sparseModelOrphan] = projector.project(capture('record_sparse_model_orphan', {
    event_kind: 'model',
    phase: 'event',
    name: 'sparse model request',
    correlation: { parent_record_id: 'record_orphan' },
    semantic: { type: 'model.request' },
  }));
  const [sparseLifecycleOrphan] = projector.project(capture('record_sparse_scope_orphan', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'sparse nested scope',
    correlation: { parent_record_id: 'record_sparse_model_orphan' },
    semantic: { type: 'scope' },
  }));
  for (const sparseOrphan of [sparseModelOrphan, sparseLifecycleOrphan]) {
    expect(sparseOrphan).toMatchObject({
      kind: 'loss',
      data: { reason: 'unresolved_parent' },
    });
    expect(sparseOrphan).not.toHaveProperty('parent');
  }

  projector.project(capture('record_failed_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'failed root',
    semantic: { type: 'agent.run', name: 'failed root' },
  }));
  const [failedRoot] = projector.project(capture('record_failed_root_end', {
    event_kind: 'lifecycle',
    phase: 'error',
    name: 'failed root',
    correlation: { parent_record_id: 'record_failed_root' },
    semantic: {
      type: 'agent.run',
      status: 'succeeded',
      error: {
        type: 'model_error',
        message: 'The model request failed.',
        recoverable: false,
      },
    },
  }));
  expect(failedRoot).toMatchObject({
    kind: 'run.outcome',
    parent: 'record_failed_root',
    data: {
      status: 'failed',
      error: {
        type: 'model_error',
        message: 'The model request failed.',
        recoverable: false,
      },
    },
  });

  projector.project(capture('record_scope_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'scope root',
    semantic: { type: 'agent.run', name: 'scope root' },
  }));
  projector.project(capture('record_failed_scope', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'failed step',
    correlation: { parent_record_id: 'record_scope_root' },
    semantic: {
      type: 'scope',
      scope_id: 'scope_failed_step',
      scope_type: 'step',
      name: 'failed step',
    },
  }));
  const failedScope = projector.project(capture('record_failed_scope_end', {
    event_kind: 'lifecycle',
    phase: 'error',
    name: 'failed step',
    correlation: { parent_record_id: 'record_failed_scope' },
    semantic: {
      type: 'scope',
      status: 'succeeded',
      error: {
        type: 'tool_error',
        message: 'The step tool failed.',
        recoverable: true,
      },
    },
  }));
  expect(failedScope).toHaveLength(2);
  expect(failedScope[0]).toMatchObject({
    id: 'record_failed_scope_end',
    kind: 'scope',
    parent: 'record_failed_scope',
    data: {
      scope_id: 'scope_failed_step',
      type: 'step',
      phase: 'end',
      status: 'failed',
    },
  });
  expect(failedScope[1]).toMatchObject({
    kind: 'error',
    parent: 'record_failed_scope_end',
    data: {
      type: 'tool_error',
      message: 'The step tool failed.',
      recoverable: true,
    },
  });
  expect(failedScope[1].id).toMatch(/^error_[a-f0-9]{24}$/);
  const [duplicateScopeTerminal] = projector.project(capture('record_failed_scope_duplicate', {
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'duplicate failed step end',
    correlation: { parent_record_id: 'record_failed_scope' },
    semantic: { type: 'scope', status: 'succeeded' },
  }));
  expect(duplicateScopeTerminal).toMatchObject({
    kind: 'loss',
    data: { reason: 'unsupported_semantic_projection' },
  });
  await expectContractRecords([
    orphan,
    orphanChild,
    orphanGrandchild,
    sparseModelOrphan,
    sparseLifecycleOrphan,
    failedRoot,
    ...failedScope,
    duplicateScopeTerminal,
  ]);
});

it('keeps only proven context-bearing records and reports dropped references', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_context_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'context run',
    semantic: { type: 'agent.run', name: 'context run' },
  }));
  const [message] = projector.project(capture('record_context_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'user message',
    correlation: { parent_record_id: 'record_context_root' },
    semantic: { type: 'message', role: 'user', content: 'Do the work.' },
  }));
  projector.project(capture('record_context_state', {
    event_kind: 'state',
    phase: 'event',
    name: 'state',
    correlation: { parent_record_id: 'record_context_root' },
    semantic: {
      type: 'state.transition',
      state_type: 'state.ready',
      value: true,
    },
  }));

  const projected = projector.project(capture('record_context_request', {
    event_kind: 'model',
    phase: 'event',
    name: 'model request',
    correlation: { parent_record_id: 'record_context_root' },
    semantic: {
      type: 'model.request',
      context_refs: [
        'record_context_message',
        'record_context_state',
        'record_context_missing',
        42,
      ],
    },
  }));

  expect(projected).toHaveLength(2);
  expect(projected[0]).toMatchObject({
    kind: 'model.request',
    parent: 'record_context_root',
    data: { context_refs: [message.id] },
  });
  expect(projected[1]).toMatchObject({
    kind: 'loss',
    parent: 'record_context_root',
    data: {
      reason: 'unresolved_context_ref',
      stage: 'source',
      count: 3,
      recoverable: false,
    },
    links: [{ type: 'affects', record: 'record_context_request' }],
  });
  await expectContractRecords(projected);
});

it('projects an exact earlier same-trace model request as a context base', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_base_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'base run',
    semantic: { type: 'agent.run', name: 'base run' },
  }));
  const [firstMessage] = projector.project(capture('record_base_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'first message',
    correlation: { parent_record_id: 'record_base_root' },
    semantic: { type: 'message', role: 'user', content: 'Start.' },
  }));
  const [firstRequest] = projector.project(capture('record_base_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'first request',
    native_identity: 'base-request',
    correlation: { parent_record_id: 'record_base_root' },
    semantic: {
      type: 'model.request',
      context_refs: ['record_base_message', 'record_base_message'],
    },
  }));
  const [suffixMessage] = projector.project(capture('record_suffix_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'suffix message',
    correlation: { parent_record_id: 'record_base_root' },
    semantic: { type: 'message', role: 'assistant', content: 'Continue.' },
  }));

  const projected = projector.project(capture('record_appended_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'appended request',
    native_identity: 'appended-request',
    correlation: { parent_record_id: 'record_base_root' },
    semantic: {
      type: 'model.request',
      context_base_ref: 'record_base_request',
      context_refs: ['record_suffix_message'],
    },
  }));

  expect(projected).toEqual([
    expect.objectContaining({
      id: 'record_appended_request',
      kind: 'model.request',
      data: {
        context_base_ref: firstRequest.id,
        context_refs: [suffixMessage.id],
      },
    }),
  ]);
  expect(firstRequest.data.context_refs).toEqual([firstMessage.id, firstMessage.id]);
  await expectContractRecords(projected);
});

it('preserves run-root identity through transparent aliases without crossing roots', () => {
  const projector = new SemanticProjector();
  const otelSource = {
    ...source,
    source_id: 'generic/otel',
    name: 'generic:otel',
  };
  projector.project(capture('record_alias_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'alias run',
    trace_id: 'trace_alias',
    semantic: { type: 'agent.run', name: 'alias run' },
  }));
  projector.project(capture('record_alias_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'base message',
    trace_id: 'trace_alias',
    correlation: { parent_record_id: 'record_alias_root' },
    semantic: { type: 'message', role: 'user', content: 'Start.' },
  }));
  projector.project(capture('record_alias_base_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'base request',
    trace_id: 'trace_alias',
    native_identity: 'alias-base-request',
    correlation: { parent_record_id: 'record_alias_root' },
    semantic: {
      type: 'model.request',
      context_refs: ['record_alias_message'],
    },
  }));
  expect(projector.project(capture('record_redundant_alias', {
    event_kind: 'correlation',
    phase: 'event',
    name: 'redundant alias',
    trace_id: 'trace_alias',
    correlation: { parent_record_id: 'record_alias_root' },
    semantic: { type: 'agent.trace' },
  }))).toEqual([]);
  expect(projector.project(capture('record_transparent_alias', {
    event_kind: 'correlation',
    phase: 'event',
    name: 'transparent OTel alias',
    trace_id: 'trace_alias',
    source: otelSource,
    correlation: { parent_record_id: 'record_redundant_alias' },
    semantic: { type: 'capture.redundant', route: 'otel' },
  }))).toEqual([]);
  const [child] = projector.project(capture('record_alias_child', {
    event_kind: 'log',
    phase: 'event',
    name: 'suffix message',
    trace_id: 'trace_alias',
    correlation: { parent_record_id: 'record_transparent_alias' },
    semantic: { type: 'message', role: 'assistant', content: 'Continue.' },
  }));

  expect(projector.project(capture('record_alias_appended_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'appended request',
    trace_id: 'trace_alias',
    native_identity: 'alias-appended-request',
    correlation: { parent_record_id: 'record_alias_child' },
    semantic: {
      type: 'model.request',
      context_base_ref: 'record_alias_base_request',
      context_refs: ['record_alias_child'],
    },
  }))).toEqual([
    expect.objectContaining({
      parent: child.id,
      data: {
        context_base_ref: 'record_alias_base_request',
        context_refs: [child.id],
      },
    }),
  ]);

  projector.project(capture('record_other_alias_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'other alias run',
    trace_id: 'trace_other',
    semantic: { type: 'agent.run', name: 'other alias run' },
  }));
  projector.project(capture('record_other_transparent_alias', {
    event_kind: 'correlation',
    phase: 'event',
    name: 'other transparent OTel alias',
    trace_id: 'trace_alias',
    source: otelSource,
    correlation: { parent_record_id: 'record_other_alias_root' },
    semantic: { type: 'capture.redundant', route: 'otel' },
  }));
  const crossed = projector.project(capture('record_cross_root_alias_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'cross-root alias request',
    trace_id: 'trace_alias',
    native_identity: 'cross-root-alias-request',
    correlation: { parent_record_id: 'record_other_transparent_alias' },
    semantic: {
      type: 'model.request',
      context_base_ref: 'record_alias_base_request',
      context_refs: [],
    },
  }));
  expect(crossed).toHaveLength(2);
  expect(crossed[0].data).toEqual({});
  expect(crossed[1]).toMatchObject({
    kind: 'loss',
    data: { reason: 'unresolved_context_base_ref', count: 1 },
  });
});

it('keeps completed model requests as bases until bounded history evicts them', () => {
  const retained = new SemanticProjector();
  retained.project(capture('record_retained_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'retained run',
    semantic: { type: 'agent.run', name: 'retained run' },
  }));
  retained.project(capture('record_retained_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'retained request',
    native_identity: 'retained-model',
    correlation: { parent_record_id: 'record_retained_root' },
    semantic: { type: 'model.request', context_refs: [] },
  }));
  retained.project(capture('record_retained_response', {
    event_kind: 'model',
    phase: 'end',
    name: 'retained response',
    native_identity: 'retained-model',
    correlation: { parent_record_id: 'record_retained_request' },
    semantic: { type: 'model.response', status: 'completed', content: 'done' },
  }));
  expect(retained.project(capture('record_retained_append', {
    event_kind: 'model',
    phase: 'start',
    name: 'retained append',
    native_identity: 'retained-append',
    correlation: { parent_record_id: 'record_retained_root' },
    semantic: {
      type: 'model.request',
      context_base_ref: 'record_retained_request',
      context_refs: [],
    },
  }))).toEqual([
    expect.objectContaining({
      kind: 'model.request',
      data: {
        context_base_ref: 'record_retained_request',
        context_refs: [],
      },
    }),
  ]);

  const evicted = new SemanticProjector(0, 0);
  evicted.project(capture('record_evicted_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'evicted run',
    semantic: { type: 'agent.run', name: 'evicted run' },
  }));
  evicted.project(capture('record_evicted_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'evicted request',
    native_identity: 'evicted-model',
    correlation: { parent_record_id: 'record_evicted_root' },
    semantic: { type: 'model.request', context_refs: [] },
  }));
  evicted.project(capture('record_evicted_response', {
    event_kind: 'model',
    phase: 'end',
    name: 'evicted response',
    native_identity: 'evicted-model',
    correlation: { parent_record_id: 'record_evicted_request' },
    semantic: { type: 'model.response', status: 'completed', content: 'done' },
  }));
  const afterEviction = evicted.project(capture('record_evicted_append', {
    event_kind: 'model',
    phase: 'start',
    name: 'evicted append',
    native_identity: 'evicted-append',
    correlation: { parent_record_id: 'record_evicted_root' },
    semantic: {
      type: 'model.request',
      context_base_ref: 'record_evicted_request',
      context_refs: [],
    },
  }));
  expect(afterEviction).toHaveLength(2);
  expect(afterEviction[0].data).toEqual({});
  expect(afterEviction[1]).toMatchObject({
    kind: 'loss',
    data: { reason: 'unresolved_context_base_ref', count: 1 },
  });
});

it('omits invalid, forward, non-request, and cross-trace context bases with one loss', () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_trace_a_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'trace A',
    trace_id: 'trace_a',
    semantic: { type: 'agent.run', name: 'trace A' },
  }));
  projector.project(capture('record_trace_a_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'message A',
    trace_id: 'trace_a',
    correlation: { parent_record_id: 'record_trace_a_root' },
    semantic: { type: 'message', role: 'user', content: 'A' },
  }));
  projector.project(capture('record_trace_a_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'request A',
    trace_id: 'trace_a',
    native_identity: 'request-a',
    correlation: { parent_record_id: 'record_trace_a_root' },
    semantic: { type: 'model.request', context_refs: ['record_trace_a_message'] },
  }));
  projector.project(capture('record_trace_b_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'trace B',
    trace_id: 'trace_b',
    semantic: { type: 'agent.run', name: 'trace B' },
  }));
  projector.project(capture('record_trace_b_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'request B',
    trace_id: 'trace_b',
    native_identity: 'request-b',
    correlation: { parent_record_id: 'record_trace_b_root' },
    semantic: { type: 'model.request', context_refs: [] },
  }));

  const invalidBases = [
    [
      'record_non_request_base', 'record_trace_a_message',
      'trace_a', 'record_trace_a_root', true,
    ],
    [
      'record_forward_base', 'record_not_seen_yet',
      'trace_a', 'record_trace_a_root', true,
    ],
    [
      'record_cross_trace_base', 'record_trace_a_request',
      'trace_b', 'record_trace_b_root', true,
    ],
    [
      'record_crossed_parent_base', 'record_trace_b_request',
      'trace_b', 'record_trace_a_root', true,
    ],
    [
      'record_missing_suffix', 'record_trace_a_request',
      'trace_a', 'record_trace_a_root', false,
    ],
  ] as const;
  for (const [recordId, base, traceId, parentRecordId, hasSuffix] of invalidBases) {
    const projected = projector.project(capture(recordId, {
      event_kind: 'model',
      phase: 'start',
      name: recordId,
      trace_id: traceId,
      native_identity: recordId,
      correlation: { parent_record_id: parentRecordId },
      semantic: {
        type: 'model.request',
        context_base_ref: base,
        ...(hasSuffix ? { context_refs: [] } : {}),
      },
    }));
    expect(projected).toHaveLength(2);
    expect(projected[0].data).toEqual({});
    expect(projected[1]).toMatchObject({
      kind: 'loss',
      data: { reason: 'unresolved_context_base_ref', count: 1 },
    });
  }

  projector.project(capture('record_crossed_parent_plain', {
    event_kind: 'model',
    phase: 'start',
    name: 'crossed parent plain request',
    trace_id: 'trace_b',
    native_identity: 'crossed-parent-plain',
    correlation: { parent_record_id: 'record_trace_a_root' },
    semantic: { type: 'model.request', context_refs: [] },
  }));
  expect(projector.project(capture('record_crossed_parent_append', {
    event_kind: 'model',
    phase: 'start',
    name: 'crossed parent append request',
    trace_id: 'trace_b',
    native_identity: 'crossed-parent-append',
    correlation: { parent_record_id: 'record_trace_a_root' },
    semantic: {
      type: 'model.request',
      context_base_ref: 'record_crossed_parent_plain',
      context_refs: [],
    },
  }))).toEqual([
    expect.objectContaining({
      parent: 'record_trace_a_root',
      data: {
        context_base_ref: 'record_crossed_parent_plain',
        context_refs: [],
      },
    }),
  ]);
});

it('rejects a context base from an earlier root that reused the trace identity', () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_old_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'old root',
    trace_id: 'trace_reused',
    semantic: { type: 'agent.run', name: 'old root' },
  }));
  projector.project(capture('record_old_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'old request',
    trace_id: 'trace_reused',
    native_identity: 'old-model',
    correlation: { parent_record_id: 'record_old_root' },
    semantic: { type: 'model.request', context_refs: [] },
  }));
  projector.project(capture('record_old_response', {
    event_kind: 'model',
    phase: 'end',
    name: 'old response',
    trace_id: 'trace_reused',
    native_identity: 'old-model',
    correlation: { parent_record_id: 'record_old_request' },
    semantic: { type: 'model.response', status: 'completed', content: 'done' },
  }));
  projector.project(capture('record_old_outcome', {
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'old root',
    trace_id: 'trace_reused',
    correlation: { parent_record_id: 'record_old_root' },
    semantic: { type: 'agent.run', status: 'succeeded' },
  }));
  projector.project(capture('record_new_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'new root',
    trace_id: 'trace_reused',
    semantic: { type: 'agent.run', name: 'new root' },
  }));

  const projected = projector.project(capture('record_cross_root_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'cross-root request',
    trace_id: 'trace_reused',
    native_identity: 'new-model',
    correlation: { parent_record_id: 'record_new_root' },
    semantic: {
      type: 'model.request',
      context_base_ref: 'record_old_request',
      context_refs: [],
    },
  }));

  expect(projected).toHaveLength(2);
  expect(projected[0].data).toEqual({});
  expect(projected[1]).toMatchObject({
    kind: 'loss',
    data: { reason: 'unresolved_context_base_ref', count: 1 },
  });
});

it('projects normalized evidence and accounts for unsupported material without native parsing', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('record_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'normalized run',
    semantic: { type: 'agent.run', name: 'normalized run' },
  }));
  const message = projector.project(capture('record_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'history message',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'message',
      origin: 'context',
      role: 'tool',
      call_id: 'call_historical',
      name: 'read_file',
      content: { lines: 2 },
    },
  }))[0];
  const request = projector.project(capture('record_model_request', {
    event_kind: 'model',
    phase: 'event',
    name: 'model request',
    native_identity: 'request-native',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'model.request',
      model: 'fixture-model',
      context_refs: ['record_message'],
      tools: ['read_file'],
    },
  }))[0];
  const response = projector.project(capture('record_model_response', {
    event_kind: 'model',
    phase: 'event',
    name: 'model response',
    native_identity: 'request-native',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'model.response',
      status: 'completed',
      model: 'fixture-model',
      content: 'done',
      finish_reason: 'stop',
      usage: { input_tokens: 20, output_tokens: 3 },
    },
  }))[0];
  const state = projector.project(capture('record_state', {
    event_kind: 'state',
    phase: 'event',
    name: 'state changed',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'state.transition',
      state_type: 'state.delta',
      version: 2,
      value: { ready: true },
    },
  }))[0];
  const interrupt = projector.project(capture('record_interrupt', {
    event_kind: 'state',
    phase: 'event',
    name: 'interrupted',
    correlation: { parent_record_id: 'record_root' },
    semantic: { type: 'state.interrupt' },
  }))[0];
  const verification = projector.project(capture('record_verification', {
    event_kind: 'state',
    phase: 'event',
    name: 'delivery check',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'verification',
      subject: 'delivery',
      status: 'failed',
      records: ['record_model_response'],
      summary: 'Delivery was cancelled.',
    },
  }))[0];
  const error = projector.project(capture('record_error', {
    event_kind: 'error',
    phase: 'error',
    name: 'tool failed',
    correlation: { parent_record_id: 'record_root' },
    semantic: {
      type: 'agent.error',
      error: {
        type: 'tool_error',
        message: 'The tool failed.',
        recoverable: true,
        code: 'E_TOOL',
      },
    },
  }))[0];
  const structuredLoss = projector.project(capture('record_loss', {
    event_kind: 'loss',
    phase: 'gap',
    name: 'serialization loss',
    correlation: {},
    loss: {
      reason: 'serialization_failure',
      stage: 'snapshot',
      affected_record_id: 'record_message',
      affected_path: '/content',
      count: 2,
      recoverable: false,
      detail: 'Two values could not be represented.',
    },
  }))[0];
  const unsupported = projector.project(capture('record_future', {
    event_kind: 'unknown',
    phase: 'event',
    name: 'future material event',
    native: { must_not_be_parsed: 'secret shape' },
    semantic: { type: 'future.event' },
    correlation: { parent_record_id: 'record_root' },
  }))[0];
  const redundant = projector.project(capture('record_control', {
    event_kind: 'correlation',
    phase: 'event',
    name: 'native trace marker',
    native: null,
    semantic: { type: 'agent.trace' },
    correlation: { parent_record_id: 'record_root' },
  }));
  const duplicate = projector.project(capture('record_duplicate_model', {
    event_kind: 'model',
    phase: 'end',
    name: 'duplicate model callback',
    native: { output: 'already retained elsewhere' },
    semantic: { type: 'capture.redundant' },
    correlation: { parent_record_id: 'record_root' },
  }));

  expect(message).toMatchObject({
    kind: 'message',
    origin: 'context',
    data: {
      role: 'tool',
      call_id: 'call_historical',
      name: 'read_file',
      content: { lines: 2 },
    },
  });
  expect(request).toMatchObject({
    kind: 'model.request',
    data: {
      model: 'fixture-model',
      context_refs: ['record_message'],
      tools: ['read_file'],
    },
  });
  expect(response).toMatchObject({
    kind: 'model.response',
    data: {
      status: 'completed',
      content: 'done',
      usage: { input_tokens: 20, output_tokens: 3 },
    },
    links: [{ type: 'result_of', record: 'record_model_request' }],
  });
  expect(state).toMatchObject({
    kind: 'state',
    data: { type: 'state.delta', version: 2, value: { ready: true } },
  });
  expect(interrupt).toMatchObject({
    kind: 'state',
    data: { type: 'state.interrupt' },
  });
  expect(interrupt.data).not.toHaveProperty('value');
  expect(verification).toMatchObject({
    kind: 'verification',
    data: { subject: 'delivery', status: 'failed' },
    links: [{ type: 'verifies', record: 'record_model_response' }],
  });
  expect(error).toMatchObject({
    kind: 'error',
    data: { type: 'tool_error', message: 'The tool failed.', recoverable: true },
  });
  expect(structuredLoss).toMatchObject({
    kind: 'loss',
    data: {
      reason: 'serialization_failure',
      stage: 'serialize',
      count: 2,
      recoverable: false,
      path: '/content',
    },
    links: [{ type: 'affects', record: 'record_message' }],
  });
  expect(unsupported).toMatchObject({
    kind: 'loss',
    data: {
      reason: 'unsupported_semantic_projection',
      stage: 'source',
      count: 1,
      recoverable: false,
    },
  });
  expect(JSON.stringify(unsupported)).not.toContain('secret shape');
  expect(redundant).toEqual([]);
  expect(duplicate).toEqual([]);
  await expectContractRecords([
    message,
    request,
    response,
    state,
    interrupt,
    verification,
    error,
    structuredLoss,
    unsupported,
  ]);
});

it('bounds completed correlation history while preserving active correlations', async () => {
  const projector = new SemanticProjector(0, 1);
  const root = projector.project(capture('bounded_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'bounded run',
    turn_id: 'bounded_turn_old',
    semantic: { type: 'agent.run', name: 'bounded run' },
  }))[0];
  const scope = projector.project(capture('bounded_scope', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'bounded scope',
    correlation: { parent_record_id: 'bounded_root' },
    semantic: { type: 'scope', scope_type: 'step', name: 'bounded scope' },
  }))[0];
  const call = projector.project(capture('bounded_call', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup',
    native_identity: 'bounded-call',
    correlation: { parent_record_id: 'bounded_scope' },
    semantic: {
      type: 'tool.execution',
      name: 'lookup',
      input: { query: 'bounded' },
    },
  }))[0];
  const request = projector.project(capture('bounded_request', {
    event_kind: 'model',
    phase: 'start',
    name: 'bounded model request',
    native_identity: 'bounded-request',
    correlation: { parent_record_id: 'bounded_scope' },
    semantic: { type: 'model.request', model: 'fixture-model' },
  }))[0];
  projector.project(capture('bounded_old_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'old message',
    correlation: { parent_record_id: 'bounded_scope' },
    semantic: { type: 'message', role: 'user', content: 'old' },
  }));
  projector.project(capture('bounded_new_message', {
    event_kind: 'log',
    phase: 'event',
    name: 'new message',
    correlation: { parent_record_id: 'bounded_scope' },
    semantic: { type: 'message', role: 'user', content: 'new' },
  }));

  const evictedParent = projector.project(capture('bounded_orphan', {
    event_kind: 'log',
    phase: 'event',
    name: 'orphaned message',
    correlation: { parent_record_id: 'bounded_old_message' },
    semantic: { type: 'message', role: 'assistant', content: 'orphaned' },
  }))[0];
  const result = projector.project(capture('bounded_result', {
    event_kind: 'tool',
    phase: 'end',
    name: 'lookup result',
    native_identity: 'bounded-call',
    correlation: { parent_record_id: 'bounded_call' },
    semantic: { type: 'tool.result', status: 'succeeded', output: { ok: true } },
  }))[0];
  const response = projector.project(capture('bounded_response', {
    event_kind: 'model',
    phase: 'end',
    name: 'bounded model response',
    native_identity: 'bounded-request',
    correlation: { parent_record_id: 'bounded_request' },
    semantic: { type: 'model.response', status: 'completed', content: 'done' },
  }))[0];
  const scopeEnd = projector.project(capture('bounded_scope_end', {
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'bounded scope',
    correlation: { parent_record_id: 'bounded_scope' },
    semantic: { type: 'scope', status: 'succeeded' },
  }))[0];
  const outcome = projector.project(capture('bounded_outcome', {
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'bounded run',
    correlation: { parent_record_id: 'bounded_root' },
    semantic: { type: 'agent.run', status: 'succeeded' },
  }))[0];
  projector.project(capture('bounded_second_root', {
    trace_id: 'bounded_second_trace',
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'second bounded run',
    turn_id: 'bounded_turn_new',
    semantic: { type: 'agent.run', name: 'second bounded run' },
  }));
  projector.project(capture('bounded_second_outcome', {
    trace_id: 'bounded_second_trace',
    event_kind: 'lifecycle',
    phase: 'end',
    name: 'second bounded run',
    correlation: { parent_record_id: 'bounded_second_root' },
    semantic: { type: 'agent.run', status: 'succeeded' },
  }));
  const unresolvedContinuation = projector.project(capture('bounded_resumed_root', {
    trace_id: 'bounded_resumed_trace',
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'resumed bounded run',
    turn_id: 'bounded_turn_resumed',
    previous_turn_id: 'bounded_turn_old',
    semantic: { type: 'agent.run', name: 'resumed bounded run' },
  }));
  projector.project(capture('bounded_reference_old', {
    trace_id: 'bounded_resumed_trace',
    event_kind: 'log',
    phase: 'event',
    name: 'old reference',
    correlation: { parent_record_id: 'bounded_resumed_root' },
    semantic: { type: 'message', role: 'user', content: 'old reference' },
  }));
  projector.project(capture('bounded_reference_new', {
    trace_id: 'bounded_resumed_trace',
    event_kind: 'log',
    phase: 'event',
    name: 'new reference',
    correlation: { parent_record_id: 'bounded_resumed_root' },
    semantic: { type: 'message', role: 'assistant', content: 'new reference' },
  }));
  const partialVerification = projector.project(capture('bounded_verification', {
    trace_id: 'bounded_resumed_trace',
    event_kind: 'state',
    phase: 'event',
    name: 'bounded verification',
    correlation: { parent_record_id: 'bounded_resumed_root' },
    semantic: {
      type: 'verification',
      subject: 'delivery',
      status: 'passed',
      records: ['bounded_reference_new', 'bounded_reference_old'],
    },
  }));
  const unresolvedAffected = projector.project(capture('bounded_runtime_loss', {
    trace_id: 'bounded_resumed_trace',
    event_kind: 'loss',
    phase: 'gap',
    name: 'bounded runtime loss',
    correlation: { parent_record_id: 'bounded_resumed_root' },
    loss: {
      reason: 'serialization_failure',
      stage: 'serialize',
      affected_record_id: 'bounded_reference_old',
      count: 1,
      recoverable: false,
    },
  }));

  expect(evictedParent).toMatchObject({
    kind: 'loss',
    data: { reason: 'unresolved_parent' },
  });
  expect(evictedParent).not.toHaveProperty('parent');
  expect(result).toMatchObject({
    kind: 'tool.result',
    parent: scope.id,
    links: [{ type: 'result_of', record: call.id }],
  });
  expect(response).toMatchObject({
    kind: 'model.response',
    parent: scope.id,
    links: [{ type: 'result_of', record: request.id }],
  });
  expect(scopeEnd).toMatchObject({ kind: 'scope', parent: scope.id });
  expect(outcome).toMatchObject({ kind: 'run.outcome', parent: root.id });
  expect(unresolvedContinuation).toHaveLength(2);
  expect(unresolvedContinuation[0]).toMatchObject({ kind: 'run.start' });
  expect(unresolvedContinuation[0].links).toBeUndefined();
  expect(unresolvedContinuation[1]).toMatchObject({
    kind: 'loss',
    data: { reason: 'unresolved_previous_turn' },
    links: [{ type: 'affects', record: 'bounded_resumed_root' }],
  });
  expect(partialVerification).toHaveLength(2);
  expect(partialVerification[0]).toMatchObject({
    kind: 'verification',
    links: [{ type: 'verifies', record: 'bounded_reference_new' }],
  });
  expect(partialVerification[1]).toMatchObject({
    kind: 'loss',
    data: { reason: 'unresolved_verification_ref', count: 1 },
    links: [{ type: 'affects', record: 'bounded_verification' }],
  });
  expect(unresolvedAffected).toHaveLength(2);
  expect(unresolvedAffected[0]).toMatchObject({
    kind: 'loss',
    data: { reason: 'serialization_failure' },
  });
  expect(unresolvedAffected[0].links).toBeUndefined();
  expect(unresolvedAffected[1]).toMatchObject({
    kind: 'loss',
    data: { reason: 'unresolved_affected_ref' },
    links: [{ type: 'affects', record: 'bounded_runtime_loss' }],
  });
  await expectContractRecords([
    evictedParent,
    result,
    response,
    scopeEnd,
    outcome,
    ...unresolvedContinuation,
    ...partialVerification,
    ...unresolvedAffected,
  ]);
  const turnHistory = projector as unknown as { evictedTurns: Set<string> };
  expect(turnHistory.evictedTurns.size).toBeLessThanOrEqual(1);
  expect(turnHistory.evictedTurns.has('bounded_turn_old')).toBe(true);
  projector.project(capture('bounded_reintroduced_root', {
    trace_id: 'bounded_reintroduced_trace',
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'reintroduced bounded run',
    turn_id: 'bounded_turn_old',
    semantic: { type: 'agent.run', name: 'reintroduced bounded run' },
  }));
  expect(turnHistory.evictedTurns.has('bounded_turn_old')).toBe(false);
});

it('rejects duplicate active identities without overwriting exact correlations', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('duplicate_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'duplicate run',
    semantic: { type: 'agent.run', name: 'duplicate run' },
  }));
  const proposal = projector.project(capture('duplicate_proposal_original', {
    event_kind: 'tool',
    phase: 'event',
    name: 'lookup proposal',
    native_identity: 'duplicate-tool',
    correlation: { parent_record_id: 'duplicate_root' },
    semantic: { type: 'tool.proposal', name: 'lookup', input: { value: 1 } },
  }))[0];
  const reusedProposalRecord = projector.project(capture('duplicate_proposal_original', {
    event_kind: 'tool',
    phase: 'event',
    name: 'reused proposal record',
    native_identity: 'different-tool',
    correlation: { parent_record_id: 'duplicate_root' },
    semantic: { type: 'tool.proposal', name: 'lookup', input: { value: 3 } },
  }))[0];
  const duplicateProposal = projector.project(capture('duplicate_proposal_rejected', {
    event_kind: 'tool',
    phase: 'event',
    name: 'duplicate lookup proposal',
    native_identity: 'duplicate-tool',
    correlation: { parent_record_id: 'duplicate_root' },
    semantic: { type: 'tool.proposal', name: 'lookup', input: { value: 2 } },
  }))[0];
  const call = projector.project(capture('duplicate_call_original', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup call',
    native_identity: 'duplicate-tool',
    correlation: { parent_record_id: 'duplicate_root' },
    semantic: { type: 'tool.execution', name: 'lookup', input: { value: 1 } },
  }))[0];
  const duplicateCall = projector.project(capture('duplicate_call_rejected', {
    event_kind: 'tool',
    phase: 'start',
    name: 'duplicate lookup call',
    native_identity: 'duplicate-tool',
    correlation: { parent_record_id: 'duplicate_root' },
    semantic: { type: 'tool.execution', name: 'lookup', input: { value: 2 } },
  }))[0];
  const result = projector.project(capture('duplicate_result', {
    event_kind: 'tool',
    phase: 'end',
    name: 'lookup result',
    native_identity: 'duplicate-tool',
    correlation: { parent_record_id: 'duplicate_call_original' },
    semantic: { type: 'tool.result', status: 'succeeded', output: { value: 1 } },
  }))[0];
  const request = projector.project(capture('duplicate_request_original', {
    event_kind: 'model',
    phase: 'start',
    name: 'model request',
    native_identity: 'duplicate-model',
    correlation: { parent_record_id: 'duplicate_root' },
    semantic: { type: 'model.request', model: 'fixture-model' },
  }))[0];
  const duplicateRequest = projector.project(capture('duplicate_request_rejected', {
    event_kind: 'model',
    phase: 'start',
    name: 'duplicate model request',
    native_identity: 'duplicate-model',
    correlation: { parent_record_id: 'duplicate_root' },
    semantic: { type: 'model.request', model: 'other-model' },
  }))[0];
  const response = projector.project(capture('duplicate_response', {
    event_kind: 'model',
    phase: 'end',
    name: 'model response',
    native_identity: 'duplicate-model',
    correlation: { parent_record_id: 'duplicate_request_original' },
    semantic: { type: 'model.response', status: 'completed', content: 'original' },
  }))[0];

  expect(duplicateProposal).toMatchObject({
    kind: 'loss',
    data: { reason: 'duplicate_active_tool_identity' },
  });
  expect(reusedProposalRecord).toMatchObject({
    kind: 'loss',
    data: { reason: 'duplicate_active_tool_identity' },
  });
  expect(reusedProposalRecord.id).not.toBe(proposal.id);
  expect(call.links).toEqual([{ type: 'derived_from', record: proposal.id }]);
  expect(duplicateCall).toMatchObject({
    kind: 'loss',
    data: { reason: 'duplicate_active_tool_identity' },
  });
  expect(result.links).toEqual([{ type: 'result_of', record: call.id }]);
  expect(duplicateRequest).toMatchObject({
    kind: 'loss',
    data: { reason: 'duplicate_active_model_identity' },
  });
  expect(response.links).toEqual([{ type: 'result_of', record: request.id }]);
  await expectContractRecords([
    proposal,
    reusedProposalRecord,
    duplicateProposal,
    call,
    duplicateCall,
    result,
    request,
    duplicateRequest,
    response,
  ]);
});

it('keeps sibling tool executions distinct when they share a semantic call id', async () => {
  const projector = new SemanticProjector();
  projector.project(capture('sibling_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'sibling run',
    semantic: { type: 'agent.run', name: 'sibling run' },
  }));
  const firstCall = projector.project(capture('sibling_call_first', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup call',
    native_identity: 'execution-first',
    correlation: { parent_record_id: 'sibling_root' },
    semantic: {
      type: 'tool.execution',
      call_id: 'shared-semantic-call',
      name: 'lookup',
      input: { branch: 'first' },
    },
  }))[0];
  const secondCall = projector.project(capture('sibling_call_second', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup call',
    native_identity: 'execution-second',
    correlation: { parent_record_id: 'sibling_root' },
    semantic: {
      type: 'tool.execution',
      call_id: 'shared-semantic-call',
      name: 'lookup',
      input: { branch: 'second' },
    },
  }))[0];
  const firstResult = projector.project(capture('sibling_result_first', {
    event_kind: 'tool',
    phase: 'end',
    name: 'lookup result',
    correlation: { parent_record_id: 'sibling_call_first' },
    semantic: {
      type: 'tool.result',
      call_id: 'shared-semantic-call',
      status: 'succeeded',
      output: { branch: 'first' },
    },
  }))[0];
  const secondResult = projector.project(capture('sibling_result_second', {
    event_kind: 'tool',
    phase: 'end',
    name: 'lookup result',
    native_identity: 'execution-second',
    correlation: { parent_record_id: 'sibling_call_second' },
    semantic: {
      type: 'tool.result',
      status: 'succeeded',
      output: { branch: 'second' },
    },
  }))[0];

  expect(firstCall.kind).toBe('tool.call');
  expect(secondCall.kind).toBe('tool.call');
  expect(firstCall.data.call_id).toBe(secondCall.data.call_id);
  expect(firstResult.links).toEqual([{ type: 'result_of', record: firstCall.id }]);
  expect(secondResult.links).toEqual([{ type: 'result_of', record: secondCall.id }]);
  await expectContractRecords([firstCall, secondCall, firstResult, secondResult]);
});

it('caps active correlations under sustained unterminated and reused identities', () => {
  const maxActive = 8;
  const projector = new SemanticProjector(0, 8, maxActive);
  let rejectedRoots = 0;
  for (let index = 0; index < 10_000; index += 1) {
    const [record] = projector.project(capture(`stress_root_${index}`, {
      trace_id: `stress_trace_${index}`,
      event_kind: 'lifecycle',
      phase: 'start',
      name: 'stress run',
      semantic: { type: 'agent.run', name: 'stress run' },
    }));
    if (record.kind === 'loss') {
      expect(record.data.reason).toBe('active_correlation_limit');
      rejectedRoots += 1;
    }
  }

  const internal = projector as unknown as {
    rootsByTrace: Map<string, unknown>;
    correlationHistory: Map<string, unknown>;
  };
  expect(rejectedRoots).toBe(10_000 - maxActive);
  expect(internal.rootsByTrace.size).toBe(maxActive);
  expect(internal.correlationHistory.size).toBeLessThanOrEqual(16);

  const reused = new SemanticProjector(0, 8, maxActive);
  reused.project(capture('stress_tool_root', {
    event_kind: 'lifecycle',
    phase: 'start',
    name: 'tool stress run',
    semantic: { type: 'agent.run', name: 'tool stress run' },
  }));
  reused.project(capture('stress_tool_call_0', {
    event_kind: 'tool',
    phase: 'start',
    name: 'lookup',
    native_identity: 'stress-tool',
    correlation: { parent_record_id: 'stress_tool_root' },
    semantic: { type: 'tool.execution', name: 'lookup', input: { value: 0 } },
  }));
  let duplicateLosses = 0;
  for (let index = 1; index < 10_000; index += 1) {
    const [record] = reused.project(capture(`stress_tool_call_${index}`, {
      event_kind: 'tool',
      phase: 'start',
      name: 'lookup',
      native_identity: 'stress-tool',
      correlation: { parent_record_id: 'stress_tool_root' },
      semantic: { type: 'tool.execution', name: 'lookup', input: { value: index } },
    }));
    if (record.data.reason === 'duplicate_active_tool_identity') duplicateLosses += 1;
  }
  const reusedInternal = reused as unknown as {
    calls: Map<string, unknown>;
    callsByRecord: Map<string, unknown>;
    correlationHistory: Map<string, unknown>;
  };
  expect(duplicateLosses).toBe(9_999);
  expect(reusedInternal.calls.size).toBe(1);
  expect(reusedInternal.callsByRecord.size).toBe(1);
  expect(reusedInternal.correlationHistory.size).toBeLessThanOrEqual(10);
});
