import { createHash } from 'node:crypto';

import type { Json, JsonObject, SemanticCaptureEventV1 } from '../v1/generated.js';

type SemanticRecordKind =
  | 'run.start'
  | 'run.outcome'
  | 'scope'
  | 'message'
  | 'model.request'
  | 'model.response'
  | 'tool.proposal'
  | 'tool.call'
  | 'tool.result'
  | 'state'
  | 'error'
  | 'verification'
  | 'loss';

type SemanticRecordLink = {
  type: 'result_of' | 'derived_from' | 'verifies' | 'affects' | 'continues_from' | 'branches_from';
  record: string;
};

export type SemanticTraceRecord = {
  id: string;
  seq: number;
  time: string;
  kind: SemanticRecordKind;
  origin: 'observed' | 'context' | 'inferred';
  source: string;
  parent?: string;
  data: JsonObject;
  links?: SemanticRecordLink[];
  blob_refs?: Array<{
    path: string;
    sha256: string;
    bytes: number;
    media_type: string;
    scan: 'clean';
    source_path?: string;
  }>;
};

const RUN_TYPES = new Set(['agent.run', 'workflow.run', 'agent.generation']);
const ORIGINS = new Set(['observed', 'context', 'inferred']);
const SCOPE_TYPES = new Set(['agent', 'turn', 'step']);
const SCOPE_STATUSES = new Set(['completed', 'failed', 'cancelled', 'interrupted', 'unknown']);
const MESSAGE_ROLES = new Set(['system', 'developer', 'user', 'assistant', 'tool']);
const MODEL_STATUSES = new Set(['completed', 'incomplete', 'failed', 'cancelled']);
const VERIFICATION_SUBJECTS = new Set(['action', 'goal', 'delivery', 'side_effect', 'policy', 'custom']);
const VERIFICATION_STATUSES = new Set(['passed', 'failed', 'unknown']);
const REDUNDANT_CONTROL_TYPES = new Set(['agent.trace', 'capture.control']);
const OMITTED_PARENT_TYPES = new Set(['capture.control', 'capture.redundant']);
const ORPHAN_TOOL_TYPES = new Set([
  'tool.proposal',
  'tool.call',
  'tool.execution',
  'tool.result',
  'tool.error',
]);
const ORPHAN_SEMANTIC_TYPES = new Set([
  'message',
  'model.request',
  'model.response',
  'verification',
]);
const ORPHAN_SCOPE_TYPES = new Set(['scope', 'agent.scope', 'workflow.step']);
const CONTEXT_RECORD_KINDS = new Set<SemanticRecordKind>([
  'message',
  'model.response',
  'tool.result',
]);
const DELIVERABLE_RECORD_KINDS = new Set<SemanticRecordKind>([
  'message',
  'model.response',
  'tool.result',
  'state',
]);
const RESULT_RECORD_KINDS = new Set<SemanticRecordKind>([
  'model.response',
  'tool.result',
]);
const DEFAULT_MAX_COMPLETED_RECORDS = 4096;
const DEFAULT_MAX_ACTIVE_CORRELATIONS = 4096;

type ModelRequest = {
  record: string;
  sourceRecord: string;
  parent?: string;
  nativeKey?: string;
};

type ModelRequestResolution = {
  request?: ModelRequest;
  rejection?: 'ambiguous_model_response' | 'unmatched_model_response';
};

type Continuation = {
  links?: SemanticRecordLink[];
  unresolved: boolean;
};

type ToolProposal = {
  record: string;
  sourceRecord: string;
  callId: string;
  identityKey?: string;
};

export class SemanticProjector {
  private sequence: number;
  private readonly maxCompletedRecords: number;
  private readonly maxActiveCorrelations: number;
  private readonly rootsByTrace = new Map<string, { sourceRecord: string; record: string }>();
  private readonly scopes = new Map<string, { id: string; type: 'agent' | 'turn' | 'step' }>();
  private readonly proposals = new Map<string, ToolProposal>();
  private readonly proposalsByIdentity = new Map<string, ToolProposal>();
  private readonly proposalsByRecord = new Map<string, string>();
  private readonly calls = new Map<string, {
    record: string;
    sourceRecord: string;
    callId: string;
    identityParts: string[];
    parent?: string;
  }>();
  private readonly callsByRecord = new Map<string, string>();
  private readonly modelRequestsByNative = new Map<string, ModelRequest>();
  private readonly modelRequestsByRecord = new Map<string, ModelRequest>();
  private readonly expandableModelRequests = new Set<string>();
  private readonly projectedIds = new Map<string, string>();
  private readonly projectedKinds = new Map<string, SemanticRecordKind>();
  private readonly projectedParents = new Map<string, string>();
  private readonly projectedTraces = new Map<string, string>();
  private readonly projectedRoots = new Map<string, string>();
  private readonly omittedIds = new Set<string>();
  private readonly contextRecords = new Map<string, string>();
  private readonly turns = new Map<string, { record: string; sourceRecord: string }>();
  private readonly evictedTurns = new Set<string>();
  private readonly correlationHistory = new Map<string, true>();

  constructor(
    initialSequence = 0,
    maxCompletedRecords = DEFAULT_MAX_COMPLETED_RECORDS,
    maxActiveCorrelations = DEFAULT_MAX_ACTIVE_CORRELATIONS,
  ) {
    if (!Number.isSafeInteger(maxCompletedRecords) || maxCompletedRecords < 0) {
      throw new RangeError('maxCompletedRecords must be a non-negative safe integer');
    }
    if (!Number.isSafeInteger(maxActiveCorrelations) || maxActiveCorrelations < 0) {
      throw new RangeError('maxActiveCorrelations must be a non-negative safe integer');
    }
    this.sequence = initialSequence;
    this.maxCompletedRecords = maxCompletedRecords;
    this.maxActiveCorrelations = maxActiveCorrelations;
  }

  project(input: SemanticCaptureEventV1): SemanticTraceRecord[] {
    const semantic = input.semantic;
    const semanticType = text(semantic.type);
    const parent = this.parent(input);
    const capturedParent = input.correlation.parent_record_id;

    if (transparentOTelParent(input, semanticType)) {
      if (parent) this.rememberAlias(input, parent);
      else this.rememberOmitted(input.record_id);
      return [];
    }

    if (
      capturedParent
      && !parent
      && !this.omittedIds.has(capturedParent)
      && input.event_kind !== 'loss'
      && orphanMaterial(input, semanticType)
    ) {
      return [this.gap(
        input,
        'unresolved_parent',
        'The declared parent was not available in the projected trace.',
      )];
    }

    if (input.event_kind === 'tool') {
      const tool = this.projectTool(input, semanticType, parent);
      if (tool) return [tool];
    }

    if (semanticType === 'message') {
      const role = messageRoleOf(semantic.role);
      if (role && isJson(semantic.content)) {
        return [this.record(input, 'message', {
          role,
          content: semantic.content,
          ...(boundedText(semantic.name) ? { name: boundedText(semantic.name)! } : {}),
          ...(validId(semantic.call_id) ? { call_id: validId(semantic.call_id)! } : {}),
        }, parent)];
      }
    }

    if (input.event_kind === 'model' && semanticType === 'model.request') {
      const nativeKey = input.native_identity ? modelNativeKey(input) : undefined;
      if (
        this.modelRequestsByRecord.has(input.record_id)
        || (nativeKey && this.modelRequestsByNative.has(nativeKey))
      ) {
        return [this.correlationRejection(
          input,
          parent,
          'duplicate_active_model_identity',
          'A model request reused an identity that was already active.',
        )];
      }
      if (this.activeCorrelationCount() >= this.maxActiveCorrelations) {
        return [this.activeLimitLoss(input, parent)];
      }
      const context = semantic.context_refs === undefined
        ? undefined
        : resolveReferences(semantic.context_refs, this.contextRecords, false);
      const rawContextBase = semantic.context_base_ref;
      const currentRoot = capturedParent
        ? this.projectedRoots.get(capturedParent)
        : this.rootsByTrace.get(input.trace_id)?.record;
      const contextBase = typeof rawContextBase === 'string'
        && semantic.context_refs !== undefined
        && this.projectedKinds.get(rawContextBase) === 'model.request'
        && this.projectedTraces.get(rawContextBase) === input.trace_id
        && currentRoot !== undefined
        && this.projectedRoots.get(rawContextBase) !== undefined
        && this.projectedRoots.get(rawContextBase) === currentRoot
        && this.expandableModelRequests.has(rawContextBase)
        ? this.projectedIds.get(rawContextBase)
        : undefined;
      const unresolvedContextBase = rawContextBase !== undefined && !contextBase;
      const record = this.record(input, 'model.request', {
        ...(contextBase ? { context_base_ref: contextBase } : {}),
        ...(!unresolvedContextBase && context ? { context_refs: context.records } : {}),
        ...(boundedText(semantic.model) ? { model: boundedText(semantic.model)! } : {}),
        ...(shortStringArray(semantic.tools).length
          ? { tools: shortStringArray(semantic.tools) }
          : {}),
        ...(jsonObjectArray(semantic.tool_definitions)
          ? { tool_definitions: jsonObjectArray(semantic.tool_definitions)! }
          : {}),
        ...(isJsonObject(semantic.settings) ? { settings: semantic.settings } : {}),
      }, parent);
      const request = {
        record: record.id,
        sourceRecord: input.record_id,
        ...(parent ? { parent } : {}),
        ...(nativeKey ? { nativeKey } : {}),
      };
      if (input.native_identity) {
        this.modelRequestsByNative.set(nativeKey!, request);
      }
      this.modelRequestsByRecord.set(input.record_id, request);
      if (
        context
        && context.unresolved === 0
        && !unresolvedContextBase
      ) {
        this.expandableModelRequests.add(input.record_id);
      }
      this.pruneCorrelationHistory();
      if (unresolvedContextBase) {
        return [
          record,
          this.supplementalLoss(
            input,
            record,
            'unresolved_context_base_ref',
            1,
            'The context base did not resolve to an earlier expandable model request under the same run root, so the context suffix was omitted.',
          ),
        ];
      }
      if (!context || context.unresolved === 0) return [record];
      return [
        record,
        this.supplementalLoss(
          input,
          record,
          'unresolved_context_ref',
          context.unresolved,
          'One or more context references were invalid, unavailable, or not context-bearing records.',
        ),
      ];
    }

    if (input.event_kind === 'model' && semanticType === 'model.response') {
      const status = modelStatusOf(semantic.status, input.phase);
      const requestResolution = this.modelRequestFor(input);
      const request = requestResolution.request;
      const usage = usageOf(semantic.usage);
      const reasoning = reasoningOf(semantic.reasoning);
      const declaredRequestParent = input.correlation.parent_record_id
        ? this.projectedKinds.get(input.correlation.parent_record_id) === 'model.request'
        : false;
      if (requestResolution.rejection) {
        return [this.correlationRejection(
          input,
          declaredRequestParent && input.correlation.parent_record_id
            ? this.projectedParents.get(input.correlation.parent_record_id)
            : parent,
          requestResolution.rejection,
          requestResolution.rejection === 'ambiguous_model_response'
            ? 'Model response identities resolved to different active requests.'
            : 'Model response referenced a request that was no longer active.',
        )];
      }
      const responseParent = request
        ? request.parent
        : declaredRequestParent && input.correlation.parent_record_id
          ? this.projectedParents.get(input.correlation.parent_record_id)
          : parent;
      const record = this.record(input, 'model.response', {
        status,
        ...(boundedText(semantic.model) ? { model: boundedText(semantic.model)! } : {}),
        ...(isJson(semantic.content) ? { content: semantic.content } : {}),
        ...(reasoning.length ? { reasoning } : {}),
        ...(boundedText(semantic.finish_reason)
          ? { finish_reason: boundedText(semantic.finish_reason)! }
          : {}),
        ...(usage ? { usage } : {}),
      }, responseParent, request
        ? [{ type: 'result_of', record: request.record }]
        : undefined);
      if (request) this.consumeModelRequest(request);
      return [record];
    }

    if (
      input.event_kind === 'state'
      && (semanticType.startsWith('state.') || semanticType === 'agent.handoff')
    ) {
      const stateType = stateTypeOf(semantic.state_type) ?? stateTypeOf(semantic.type);
      if (stateType) {
        const result = this.exactReference(semantic.result_ref, RESULT_RECORD_KINDS);
        const record = this.record(input, 'state', {
          type: stateType,
          ...(stateVersionOf(semantic.version) !== undefined
            ? { version: stateVersionOf(semantic.version)! }
            : {}),
          ...(isJson(semantic.value) ? { value: semantic.value } : {}),
        }, parent, result ? [{ type: 'derived_from', record: result }] : undefined);
        return semantic.result_ref === undefined || result
          ? [record]
          : [record, this.supplementalLoss(
            input,
            record,
            'unresolved_result_ref',
            1,
            'The result reference did not resolve to an earlier model.response or tool.result record.',
          )];
      }
    }

    if (semanticType === 'verification') {
      const subject = verificationSubjectOf(semantic.subject);
      const status = verificationStatusOf(semantic.status);
      const verified = resolveReferences(semantic.records, this.projectedIds);
      if (subject && status && verified.records.length) {
        const record = this.record(input, 'verification', {
          subject,
          status,
          ...(boundedDetail(semantic.summary) ? { summary: boundedDetail(semantic.summary)! } : {}),
        }, parent, verified.records.map((verifiedRecord) => ({
          type: 'verifies',
          record: verifiedRecord,
        })));
        return verified.unresolved === 0
          ? [record]
          : [record, this.supplementalLoss(
            input,
            record,
            'unresolved_verification_ref',
            verified.unresolved,
            'One or more verification references were invalid or unavailable.',
          )];
      }
      if (subject && status && semantic.records !== undefined && verified.unresolved > 0) {
        return [this.record(input, 'loss', {
          reason: 'unresolved_verification_ref',
          stage: 'source',
          count: verified.unresolved,
          recoverable: false,
          detail: 'Verification references did not resolve to earlier projected records.',
        }, parent)];
      }
    }

    if (semanticType === 'capture.gap') {
      const record = this.record(input, 'loss', {
        reason: lossReasonOf(text(semantic.reason) ?? 'unsupported_semantic_projection'),
        stage: 'source',
        count: positiveInteger(
          typeof semantic.count === 'number' ? semantic.count : undefined,
        ) ?? 1,
        recoverable: false,
        ...(boundedDetail(semantic.detail)
          ? { detail: boundedDetail(semantic.detail)! } : {}),
      }, parent);
      return capturedParent && !parent && !this.omittedIds.has(capturedParent)
        ? [record, this.supplementalLoss(
          input,
          record,
          'unresolved_parent',
          1,
          'The declared parent was not available in the projected trace.',
        )]
        : [record];
    }

    if (input.event_kind === 'loss' && input.loss) {
      const affected = input.loss.affected_record_id
        ? this.projectedIds.get(input.loss.affected_record_id)
        : undefined;
      const record = this.record(input, 'loss', {
        reason: lossReasonOf(input.loss.reason),
        stage: lossStageOf(input.loss.stage),
        count: positiveInteger(input.loss.count) ?? 1,
        recoverable: input.loss.recoverable,
        ...(boundedPath(input.loss.affected_path) ? { path: boundedPath(input.loss.affected_path)! } : {}),
        ...(nonnegativeInteger(input.loss.bytes) !== undefined
          ? { bytes: nonnegativeInteger(input.loss.bytes)! }
          : {}),
        ...(boundedDetail(input.loss.detail) ? { detail: boundedDetail(input.loss.detail)! } : {}),
      }, parent, affected ? [{ type: 'affects', record: affected }] : undefined);
      const records = [record];
      if (input.loss.affected_record_id && !affected) {
        records.push(this.supplementalLoss(
          input,
          record,
          'unresolved_affected_ref',
          1,
          'The affected record reference was invalid or unavailable.',
        ));
      }
      if (capturedParent && !parent && !this.omittedIds.has(capturedParent)) {
        records.push(this.supplementalLoss(
          input,
          record,
          'unresolved_parent',
          1,
          'The declared parent was not available in the projected trace.',
        ));
      }
      return records;
    }

    if (input.event_kind === 'lifecycle' && input.phase === 'start') {
      if (!capturedParent && RUN_TYPES.has(semanticType)) {
        if (this.rootsByTrace.has(input.trace_id)) {
          return [this.gap(
            input,
            'ambiguous_root_start',
            'A second unparented root started while the trace already had an active root.',
          )];
        }
        if (this.activeCorrelationCount() >= this.maxActiveCorrelations) {
          return [this.activeLimitLoss(input, null)];
        }
        const continuation = this.continuation(input);
        const record = this.record(input, 'run.start', {
          name: boundedText(semantic.name) ?? boundedText(input.name) ?? 'agent run',
          ...(isJson(semantic.input) ? { input: semantic.input } : {}),
          ...(input.conversation_id ? { conversation_id: input.conversation_id } : {}),
          ...(input.turn_id ? { turn_id: input.turn_id } : {}),
          ...(input.turn_index !== undefined ? { turn_index: input.turn_index } : {}),
          ...(input.previous_turn_id ? { previous_turn_id: input.previous_turn_id } : {}),
        }, null, continuation.links);
        this.rootsByTrace.set(input.trace_id, {
          sourceRecord: input.record_id,
          record: record.id,
        });
        this.pruneCorrelationHistory();
        this.rememberTurn(input, record.id);
        return continuation.unresolved
          ? [record, this.unresolvedContinuationLoss(input, record)]
          : [record];
      }

      const declaredScope = scopeTypeOf(semantic.scope_type);
      const isScope = Boolean(parent) && (
        declaredScope !== undefined
        || Boolean(input.turn_id)
        || RUN_TYPES.has(semanticType)
        || semanticType === 'scope'
        || semanticType === 'agent.scope'
        || semanticType === 'workflow.step'
      );
      if (!isScope) {
        return material(input)
          ? [this.unsupported(input, parent)]
          : [];
      }
      if (this.activeCorrelationCount() >= this.maxActiveCorrelations) {
        return [this.activeLimitLoss(input, parent)];
      }
      const scopeType = declaredScope
        ?? (input.turn_id ? 'turn' : semanticType === 'agent.scope' ? 'agent' : 'step');
      const scopeId = validId(semantic.scope_id) ?? `scope_${digest(input.record_id)}`;
      this.scopes.set(input.record_id, { id: scopeId, type: scopeType });
      const continuation = this.continuation(input);
      const record = this.record(input, 'scope', {
        scope_id: scopeId,
        type: scopeType,
        phase: 'start',
        name: boundedText(semantic.name) ?? boundedText(input.name) ?? 'scope',
      }, parent, continuation.links);
      this.rememberTurn(input, record.id);
      return continuation.unresolved
        ? [record, this.unresolvedContinuationLoss(input, record)]
        : [record];
    }

    if (input.event_kind === 'lifecycle' && terminal(input.phase)) {
      const capturedParent = input.correlation.parent_record_id;
      const root = this.rootsByTrace.get(input.trace_id);
      if (capturedParent && root?.sourceRecord === capturedParent) {
        const error = errorDataOf(semantic.error);
        const status = outcomeStatus(semantic.status, input.phase);
        const contradictoryError = Boolean(error) && input.phase === 'end' && status !== 'failed';
        const summary = boundedDetail(semantic.summary);
        const output = this.exactReference(semantic.output_ref, DELIVERABLE_RECORD_KINDS);
        const record = this.record(input, 'run.outcome', {
          status: contradictoryError ? 'unknown' : status,
          ...(summary ? { summary } : {}),
          ...(isJson(semantic.output) ? { output: semantic.output } : {}),
          ...(error ? { error } : {}),
        }, parent, output ? [{ type: 'derived_from', record: output }] : undefined);
        this.rootsByTrace.delete(input.trace_id);
        this.pruneCorrelationHistory();
        if (contradictoryError) {
          return [record, this.supplementalLoss(
            input,
            record,
            'contradictory_terminal_error',
            1,
            'A normal terminal status was accompanied by error evidence.',
          )];
        }
        return semantic.output_ref === undefined || output
          ? [record]
          : [record, this.supplementalLoss(
            input,
            record,
            'unresolved_output_ref',
            1,
            'The output reference did not resolve to earlier deliverable evidence.',
          )];
      }

      const scope = capturedParent ? this.scopes.get(capturedParent) : undefined;
      if (scope) {
        const terminalScope = this.record(input, 'scope', {
          scope_id: scope.id,
          type: scope.type,
          phase: 'end',
          status: scopeStatus(semantic.status, input.phase),
        }, parent);
        this.scopes.delete(capturedParent!);
        this.pruneCorrelationHistory();
        const error = errorDataOf(semantic.error);
        return error
          ? [terminalScope, this.supplementalError(input, terminalScope.id, error)]
          : [terminalScope];
      }
    }

    if (input.event_kind === 'error' || input.phase === 'error') {
      const error = errorDataOf(semantic.error);
      if (error) return [this.record(input, 'error', error, parent)];
    }

    if (redundantControl(input, semanticType)) {
      if (parent) this.rememberAlias(input, parent);
      return [];
    }
    if (material(input)) {
      return [this.unsupported(input, parent)];
    }
    return [];
  }

  private projectTool(
    input: SemanticCaptureEventV1,
    semanticType: string,
    parent: string | undefined,
  ): SemanticTraceRecord | undefined {
    if (semanticType === 'tool.proposal') {
      const name = boundedText(input.semantic.name);
      if (!name || !isJson(input.semantic.input)) return undefined;
      const callId = callIdOf(input);
      const identityKey = toolProposalIdentityKey(input);
      if (
        this.proposals.has(callId)
        || (identityKey !== undefined && this.proposalsByIdentity.has(identityKey))
        || this.proposalsByRecord.has(input.record_id)
        || this.calls.has(toolExecutionKey(input))
      ) {
        return this.correlationRejection(
          input,
          parent,
          'duplicate_active_tool_identity',
          'A tool proposal reused an identity that was already active.',
        );
      }
      if (this.activeCorrelationCount() >= this.maxActiveCorrelations) {
        return this.activeLimitLoss(input, parent);
      }
      const record = this.record(input, 'tool.proposal', {
        call_id: callId,
        ...(displayToolIdentityOf(input)
          ? { native_call_id: displayToolIdentityOf(input)! }
          : {}),
        name,
        input: input.semantic.input,
      }, parent);
      const proposal = {
        record: record.id,
        sourceRecord: input.record_id,
        callId,
        ...(identityKey ? { identityKey } : {}),
      };
      this.proposals.set(callId, proposal);
      if (identityKey) this.proposalsByIdentity.set(identityKey, proposal);
      this.proposalsByRecord.set(input.record_id, callId);
      this.pruneCorrelationHistory();
      return record;
    }

    if (
      semanticType === 'tool.execution'
      && (input.phase === 'start' || input.phase === 'event')
    ) {
      const name = boundedText(input.semantic.name);
      if (!name || !isJson(input.semantic.input)) return undefined;
      const localCallId = callIdOf(input);
      const proposal = this.proposals.get(localCallId)
        ?? this.proposalsByIdentity.get(toolProposalIdentityKey(input) ?? '');
      const callId = proposal?.callId ?? localCallId;
      const executionKey = toolExecutionKey(input);
      if (this.calls.has(executionKey) || this.callsByRecord.has(input.record_id)) {
        return this.correlationRejection(
          input,
          parent,
          'duplicate_active_tool_identity',
          'A tool call reused an identity that was already active.',
        );
      }
      if (!proposal && this.activeCorrelationCount() >= this.maxActiveCorrelations) {
        return this.activeLimitLoss(input, parent);
      }
      const record = this.record(input, 'tool.call', {
        call_id: callId,
        ...(displayToolIdentityOf(input)
          ? { native_call_id: displayToolIdentityOf(input)! }
          : {}),
        name,
        input: input.semantic.input,
      }, parent, proposal ? [{ type: 'derived_from', record: proposal.record }] : undefined);
      if (proposal) {
        this.proposals.delete(proposal.callId);
        if (proposal.identityKey) this.proposalsByIdentity.delete(proposal.identityKey);
        this.proposalsByRecord.delete(proposal.sourceRecord);
      }
      this.calls.set(executionKey, {
        record: record.id,
        sourceRecord: input.record_id,
        callId,
        identityParts: toolIdentityParts(input),
        ...(parent ? { parent } : {}),
      });
      this.callsByRecord.set(input.record_id, executionKey);
      this.pruneCorrelationHistory();
      return record;
    }

    if (semanticType === 'tool.result' || semanticType === 'tool.error') {
      const parentCall = input.correlation.parent_record_id
        ? this.callsByRecord.get(input.correlation.parent_record_id)
        : undefined;
      const semanticCall = toolExecutionIdentityOf(input)
        ? canonicalCallId(input, toolExecutionIdentityOf(input)!)
        : undefined;
      const declaredCallParent = input.correlation.parent_record_id
        ? this.projectedKinds.get(input.correlation.parent_record_id) === 'tool.call'
        : false;
      const parentCallState = parentCall ? this.calls.get(parentCall) : undefined;
      const parentCompatible = parentCallState
        ? toolIdentityParts(input).every((identity) =>
          parentCallState.identityParts.includes(identity))
        : false;
      const callId = parentCall && parentCompatible ? parentCall : semanticCall ?? parentCall;
      const call = callId ? this.calls.get(callId) : undefined;
      if (
        !callId
        || !call
        || (declaredCallParent && !parentCall)
        || (parentCall && !parentCompatible)
      ) {
        if (parentCall && !this.calls.has(parentCall) && input.correlation.parent_record_id) {
          this.callsByRecord.delete(input.correlation.parent_record_id);
        }
        const declaredContainment = declaredCallParent && input.correlation.parent_record_id
          ? this.projectedParents.get(input.correlation.parent_record_id)
          : undefined;
        return this.record(input, 'loss', {
          reason: 'unmatched_tool_result',
          stage: 'source',
          count: 1,
          recoverable: false,
          detail: 'Tool result had no exact matching tool call.',
        }, declaredContainment ?? call?.parent ?? parent);
      }
      const status = toolStatus(input.semantic.status, input.phase, semanticType);
      const error = errorDataOf(input.semantic.error);
      if (error && status !== 'failed') {
        this.calls.delete(callId);
        this.callsByRecord.delete(call.sourceRecord);
        this.pruneCorrelationHistory();
        return this.record(input, 'loss', {
          reason: 'contradictory_terminal_error',
          stage: 'source',
          count: 1,
          recoverable: false,
          detail: 'A non-failed tool result was accompanied by error evidence.',
        }, call.parent, [{ type: 'affects', record: call.record }]);
      }
      const record = this.record(input, 'tool.result', {
        call_id: call.callId,
        ...(displayToolIdentityOf(input)
          ? { native_call_id: displayToolIdentityOf(input)! }
          : {}),
        status,
        ...(isJson(input.semantic.output) ? { output: input.semantic.output } : {}),
        ...(error ? { error } : {}),
      }, call.parent, [{ type: 'result_of', record: call.record }]);
      this.calls.delete(callId);
      this.callsByRecord.delete(call.sourceRecord);
      this.pruneCorrelationHistory();
      return record;
    }
    return undefined;
  }

  private record(
    input: SemanticCaptureEventV1,
    kind: SemanticRecordKind,
    data: JsonObject,
    parent: string | null | undefined = this.parent(input),
    links?: SemanticRecordLink[],
    remember = true,
  ): SemanticTraceRecord {
    const blobRefs = cleanBlobRefs(input);
    const record: SemanticTraceRecord = {
      id: input.record_id,
      seq: this.sequence + 1,
      time: input.observed_at,
      kind,
      origin: originOf(input.semantic.origin),
      source: sourceId(input.source.source_id),
      ...(parent ? { parent } : {}),
      data,
      ...(links?.length ? { links } : {}),
      ...(blobRefs.length ? { blob_refs: blobRefs } : {}),
    };
    this.sequence = record.seq;
    if (remember) {
      this.projectedIds.set(input.record_id, record.id);
      this.projectedKinds.set(input.record_id, kind);
      this.projectedTraces.set(input.record_id, input.trace_id);
      const root = kind === 'run.start'
        ? record.id
        : input.correlation.parent_record_id
          ? this.projectedRoots.get(input.correlation.parent_record_id)
          : this.rootsByTrace.get(input.trace_id)?.record;
      if (root) this.projectedRoots.set(input.record_id, root);
      if (parent) this.projectedParents.set(input.record_id, parent);
      if (CONTEXT_RECORD_KINDS.has(kind)) {
        this.contextRecords.set(input.record_id, record.id);
      }
      this.correlationHistory.set(input.record_id, true);
      this.pruneCorrelationHistory(
        startsActiveCorrelation(kind, data) ? input.record_id : undefined,
      );
    }
    return record;
  }

  private parent(input: SemanticCaptureEventV1): string | undefined {
    const parent = input.correlation.parent_record_id;
    return parent
      ? this.projectedIds.get(parent)
      : this.rootsByTrace.get(input.trace_id)?.record;
  }

  private modelRequestFor(input: SemanticCaptureEventV1): ModelRequestResolution {
    const native = input.native_identity
      ? this.modelRequestsByNative.get(modelNativeKey(input))
      : undefined;
    const parent = input.correlation.parent_record_id
      ? this.modelRequestsByRecord.get(input.correlation.parent_record_id)
      : undefined;
    const declaredRequestParent = input.correlation.parent_record_id
      ? this.projectedKinds.get(input.correlation.parent_record_id) === 'model.request'
      : false;
    if (native && parent && native.record !== parent.record) {
      return { rejection: 'ambiguous_model_response' };
    }
    if (declaredRequestParent && !parent) {
      return { rejection: 'unmatched_model_response' };
    }
    return { request: native ?? parent };
  }

  private exactReference(
    value: Json | undefined,
    allowedKinds: ReadonlySet<SemanticRecordKind>,
  ): string | undefined {
    if (typeof value !== 'string') return undefined;
    const projected = this.projectedIds.get(value);
    const kind = this.projectedKinds.get(value);
    return projected && kind && allowedKinds.has(kind) ? projected : undefined;
  }

  private consumeModelRequest(request: ModelRequest): void {
    this.modelRequestsByRecord.delete(request.sourceRecord);
    if (request.nativeKey) this.modelRequestsByNative.delete(request.nativeKey);
    this.pruneCorrelationHistory();
  }

  private continuation(input: SemanticCaptureEventV1): Continuation {
    const previous = input.previous_turn_id
      ? this.turns.get(input.previous_turn_id)
      : undefined;
    return {
      ...(previous
        ? { links: [{ type: 'continues_from' as const, record: previous.record }] }
        : {}),
      unresolved: Boolean(
        input.previous_turn_id
        && !previous
        && this.evictedTurns.has(input.previous_turn_id),
      ),
    };
  }

  private rememberTurn(input: SemanticCaptureEventV1, record: string): void {
    if (!input.turn_id) return;
    this.evictedTurns.delete(input.turn_id);
    this.turns.set(input.turn_id, { record, sourceRecord: input.record_id });
    this.pruneTurnHistory();
  }

  private rememberAlias(input: SemanticCaptureEventV1, record: string): void {
    const sourceRecord = input.record_id;
    const targetSourceRecord = input.correlation.parent_record_id;
    const root = targetSourceRecord
      ? this.projectedRoots.get(targetSourceRecord)
      : this.rootsByTrace.get(input.trace_id)?.record;
    this.projectedIds.set(sourceRecord, record);
    if (root) this.projectedRoots.set(sourceRecord, root);
    this.correlationHistory.set(sourceRecord, true);
    this.pruneCorrelationHistory();
  }

  private rememberOmitted(sourceRecord: string): void {
    this.omittedIds.add(sourceRecord);
    this.correlationHistory.set(sourceRecord, true);
    this.pruneCorrelationHistory();
  }

  private activeSourceRecords(): Set<string> {
    const active = new Set<string>();
    for (const root of this.rootsByTrace.values()) active.add(root.sourceRecord);
    for (const sourceRecord of this.scopes.keys()) active.add(sourceRecord);
    for (const proposal of this.proposals.values()) active.add(proposal.sourceRecord);
    for (const call of this.calls.values()) active.add(call.sourceRecord);
    for (const sourceRecord of this.modelRequestsByRecord.keys()) active.add(sourceRecord);
    return active;
  }

  private activeCorrelationCount(): number {
    return this.rootsByTrace.size
      + this.scopes.size
      + this.proposals.size
      + this.calls.size
      + this.modelRequestsByRecord.size;
  }

  private activeLimitLoss(
    input: SemanticCaptureEventV1,
    parent: string | null | undefined,
  ): SemanticTraceRecord {
    return this.correlationRejection(
      input,
      parent,
      'active_correlation_limit',
      'The active correlation limit was reached; the new correlation was rejected.',
    );
  }

  private correlationRejection(
    input: SemanticCaptureEventV1,
    parent: string | null | undefined,
    reason: string,
    detail: string,
  ): SemanticTraceRecord {
    const rejectionInput = this.projectedIds.has(input.record_id)
      ? {
          ...input,
          record_id: `rec_${digest([
            input.record_id,
            reason,
            String(this.sequence + 1),
          ].join('\0'))}`,
        }
      : input;
    return this.record(rejectionInput, 'loss', {
      reason,
      stage: 'source',
      count: 1,
      recoverable: false,
      detail,
    }, parent);
  }

  private pruneCorrelationHistory(extraPinned?: string): void {
    const active = this.activeSourceRecords();
    if (extraPinned) active.add(extraPinned);
    let completed = 0;
    for (const sourceRecord of this.correlationHistory.keys()) {
      if (!active.has(sourceRecord)) completed += 1;
    }
    for (const sourceRecord of this.correlationHistory.keys()) {
      if (completed <= this.maxCompletedRecords) break;
      if (active.has(sourceRecord)) continue;
      this.correlationHistory.delete(sourceRecord);
      this.projectedIds.delete(sourceRecord);
      this.projectedKinds.delete(sourceRecord);
      this.projectedParents.delete(sourceRecord);
      this.projectedTraces.delete(sourceRecord);
      this.projectedRoots.delete(sourceRecord);
      this.expandableModelRequests.delete(sourceRecord);
      this.contextRecords.delete(sourceRecord);
      this.omittedIds.delete(sourceRecord);
      completed -= 1;
    }
    this.pruneTurnHistory(active);
  }

  private pruneTurnHistory(active = this.activeSourceRecords()): void {
    let completed = 0;
    for (const turn of this.turns.values()) {
      if (!active.has(turn.sourceRecord)) completed += 1;
    }
    for (const [turnId, turn] of this.turns) {
      if (completed <= this.maxCompletedRecords) break;
      if (active.has(turn.sourceRecord)) continue;
      this.turns.delete(turnId);
      this.evictedTurns.add(turnId);
      while (this.evictedTurns.size > this.maxCompletedRecords) {
        const oldest = this.evictedTurns.values().next().value;
        if (oldest === undefined) break;
        this.evictedTurns.delete(oldest);
      }
      completed -= 1;
    }
  }

  private unresolvedContinuationLoss(
    input: SemanticCaptureEventV1,
    record: SemanticTraceRecord,
  ): SemanticTraceRecord {
    return this.supplementalLoss(
      input,
      record,
      'unresolved_previous_turn',
      1,
      'The previous turn reference was no longer available in bounded correlation history.',
    );
  }

  private unsupported(
    input: SemanticCaptureEventV1,
    parent: string | undefined,
  ): SemanticTraceRecord {
    return this.record(input, 'loss', {
      reason: 'unsupported_semantic_projection',
      stage: 'source',
      count: positiveInteger(
        typeof input.semantic.count === 'number' ? input.semantic.count : undefined,
      ) ?? 1,
      recoverable: false,
      detail: `Unsupported ${input.event_kind}/${input.phase} semantic record: ${boundedDetail(input.name) ?? 'unnamed'}.`,
    }, parent);
  }

  private gap(
    input: SemanticCaptureEventV1,
    reason: string,
    detail: string,
  ): SemanticTraceRecord {
    return this.record(input, 'loss', {
      reason,
      stage: 'source',
      count: 1,
      recoverable: false,
      detail,
    }, null, undefined, false);
  }

  private supplementalError(
    input: SemanticCaptureEventV1,
    parent: string,
    error: JsonObject,
  ): SemanticTraceRecord {
    const supplemental = {
      ...input,
      record_id: `error_${digest(input.record_id)}`,
      blob_refs: [],
    };
    return this.record(supplemental, 'error', error, parent);
  }

  private supplementalLoss(
    input: SemanticCaptureEventV1,
    affected: SemanticTraceRecord,
    reason: string,
    count: number,
    detail: string,
  ): SemanticTraceRecord {
    const supplemental = {
      ...input,
      record_id: `loss_${digest(`${input.record_id}\0${reason}`)}`,
      blob_refs: [],
    };
    return this.record(supplemental, 'loss', {
      reason,
      stage: 'source',
      count,
      recoverable: false,
      detail,
    }, affected.parent, [{ type: 'affects', record: affected.id }]);
  }
}

function transparentOTelParent(input: SemanticCaptureEventV1, semanticType: string): boolean {
  return OMITTED_PARENT_TYPES.has(semanticType)
    && input.source.name === 'generic:otel'
    && input.semantic.route === 'otel';
}

function startsActiveCorrelation(kind: SemanticRecordKind, data: JsonObject): boolean {
  return kind === 'run.start'
    || kind === 'model.request'
    || kind === 'tool.proposal'
    || kind === 'tool.call'
    || (kind === 'scope' && data.phase === 'start');
}

function sourceId(value: string): string {
  return `src_${digest(value)}`;
}

function digest(value: string): string {
  return createHash('sha256').update(value).digest('hex').slice(0, 24);
}

function text(value: Json | undefined): string {
  return typeof value === 'string' ? value : '';
}

function exactText(value: Json | undefined): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function boundedText(value: Json | undefined): string | undefined {
  if (typeof value !== 'string' || value.length === 0) return undefined;
  return [...value].slice(0, 256).join('');
}

function boundedDetail(value: Json | undefined): string | undefined {
  if (typeof value !== 'string' || value.length === 0) return undefined;
  return [...value].slice(0, 4096).join('');
}

function boundedPath(value: string | undefined): string | undefined {
  if (!value) return undefined;
  return [...value].slice(0, 2048).join('');
}

function validId(value: Json | undefined): string | undefined {
  return typeof value === 'string' && /^[a-z][a-z0-9._:-]{2,127}$/.test(value)
    ? value
    : undefined;
}

function isJson(value: Json | undefined): value is Json {
  return value !== undefined;
}

function isJsonObject(value: Json | undefined): value is JsonObject {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function messageRoleOf(
  value: Json | undefined,
): 'system' | 'developer' | 'user' | 'assistant' | 'tool' | undefined {
  return typeof value === 'string' && MESSAGE_ROLES.has(value)
    ? value as 'system' | 'developer' | 'user' | 'assistant' | 'tool'
    : undefined;
}

function verificationSubjectOf(value: Json | undefined):
  'action' | 'goal' | 'delivery' | 'side_effect' | 'policy' | 'custom' | undefined {
  return typeof value === 'string' && VERIFICATION_SUBJECTS.has(value)
    ? value as 'action' | 'goal' | 'delivery' | 'side_effect' | 'policy' | 'custom'
    : undefined;
}

function verificationStatusOf(value: Json | undefined): 'passed' | 'failed' | 'unknown' | undefined {
  return typeof value === 'string' && VERIFICATION_STATUSES.has(value)
    ? value as 'passed' | 'failed' | 'unknown'
    : undefined;
}

function resolveReferences(
  value: Json | undefined,
  ids: Map<string, string>,
  unique = true,
): { records: string[]; unresolved: number } {
  if (value === undefined) return { records: [], unresolved: 0 };
  if (!Array.isArray(value)) return { records: [], unresolved: 1 };
  const records: string[] = [];
  let unresolved = 0;
  for (const reference of value) {
    if (typeof reference !== 'string') {
      unresolved += 1;
      continue;
    }
    const projected = ids.get(reference);
    if (projected && (!unique || !records.includes(projected))) {
      records.push(projected);
    } else if (!projected) {
      unresolved += 1;
    }
  }
  return { records, unresolved };
}

function shortStringArray(value: Json | undefined): string[] {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.flatMap((item) => {
    const bounded = boundedText(item);
    return bounded ? [bounded] : [];
  }))];
}

function jsonObjectArray(value: Json | undefined): JsonObject[] | undefined {
  if (!Array.isArray(value) || !value.every(isJsonObject)) return undefined;
  return value;
}

function modelNativeKey(input: SemanticCaptureEventV1): string {
  return [
    input.trace_id,
    input.source.source_id,
    input.native_identity ?? '',
  ].join('\0');
}

function modelStatusOf(
  value: Json | undefined,
  phase: SemanticCaptureEventV1['phase'],
): 'completed' | 'incomplete' | 'failed' | 'cancelled' {
  if (phase === 'error') return 'failed';
  if (phase === 'cancelled') return 'cancelled';
  if (typeof value === 'string' && MODEL_STATUSES.has(value)) {
    return value as 'completed' | 'incomplete' | 'failed' | 'cancelled';
  }
  return 'completed';
}

function usageOf(value: Json | undefined): JsonObject | undefined {
  if (!isJsonObject(value)) return undefined;
  const inputTokens = nonnegativeInteger(value.input_tokens);
  const outputTokens = nonnegativeInteger(value.output_tokens);
  if (inputTokens === undefined && outputTokens === undefined) return undefined;
  return {
    ...(inputTokens !== undefined ? { input_tokens: inputTokens } : {}),
    ...(outputTokens !== undefined ? { output_tokens: outputTokens } : {}),
  };
}

function reasoningOf(value: Json | undefined): JsonObject[] {
  if (!Array.isArray(value)) return [];
  const reasoning: JsonObject[] = [];
  for (const item of value) {
    if (!isJsonObject(item)) continue;
    const type = item.type === 'text' || item.type === 'summary' ? item.type : undefined;
    const text = typeof item.text === 'string' && item.text.length > 0 ? item.text : undefined;
    if (!type || !text) continue;
    reasoning.push({ type, text });
  }
  return reasoning;
}

function stateTypeOf(value: Json | undefined): string | undefined {
  return typeof value === 'string' && /^[a-z][a-z0-9._-]{2,127}$/.test(value)
    ? value
    : undefined;
}

function stateVersionOf(value: Json | undefined): number | string | undefined {
  const integer = nonnegativeInteger(value);
  return integer ?? boundedText(value);
}

function errorDataOf(value: Json | undefined): JsonObject | undefined {
  if (!isJsonObject(value)) return undefined;
  const type = stateTypeOf(value.type);
  const message = boundedDetail(value.message);
  if (!type || message === undefined || typeof value.recoverable !== 'boolean') return undefined;
  const code = boundedText(value.code);
  return {
    type,
    message,
    recoverable: value.recoverable,
    ...(code ? { code } : {}),
    ...(isJson(value.details) ? { details: value.details } : {}),
  };
}

function lossReasonOf(value: string): string {
  return /^[a-z][a-z0-9_]{2,63}$/.test(value)
    ? value
    : 'unsupported_semantic_projection';
}

function lossStageOf(
  value: SemanticCaptureEventV1['loss'] extends infer Loss
    ? Loss extends { stage: infer Stage } ? Stage : never
    : never,
): 'source' | 'serialize' | 'scrub' | 'queue' | 'buffer' | 'persist' | 'recover' {
  if (value === 'snapshot' || value === 'serialize') return 'serialize';
  if (value === 'scrub' || value === 'scan') return 'scrub';
  if (value === 'queue') return 'queue';
  if (value === 'persist') return 'persist';
  if (value === 'recover') return 'recover';
  return 'source';
}

function positiveInteger(value: number | undefined): number | undefined {
  return Number.isSafeInteger(value) && value !== undefined && value > 0 ? value : undefined;
}

function nonnegativeInteger(value: Json | undefined): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
    ? value
    : undefined;
}

function redundantControl(input: SemanticCaptureEventV1, semanticType: string): boolean {
  if (semanticType === 'capture.redundant') return true;
  if (
    input.event_kind === 'correlation'
    && input.phase === 'gap'
    && (
      input.name === 'semantic_layer.context.gap'
      || input.name === 'semantic_layer.capture_input.gap'
    )
  ) return true;
  if (input.event_kind !== 'correlation' || input.native !== null) return false;
  if (!REDUNDANT_CONTROL_TYPES.has(semanticType)) return false;
  return Object.keys(input.semantic).every((key) => (
    key === 'type' || key === 'framework' || key === 'provider' || key === 'origin'
  ));
}

function material(input: SemanticCaptureEventV1): boolean {
  if (input.native !== null) return true;
  return Object.keys(input.semantic).some((key) => (
    key !== 'type' && key !== 'framework' && key !== 'provider' && key !== 'origin'
  ));
}

function orphanMaterial(input: SemanticCaptureEventV1, semanticType: string): boolean {
  if (material(input)) return true;
  if (input.event_kind === 'tool' && ORPHAN_TOOL_TYPES.has(semanticType)) return true;
  if (ORPHAN_SEMANTIC_TYPES.has(semanticType)) return true;
  if (input.event_kind === 'state' || input.event_kind === 'error') return true;
  return input.event_kind === 'lifecycle'
    && terminalOrStart(input.phase)
    && (
      RUN_TYPES.has(semanticType)
      || ORPHAN_SCOPE_TYPES.has(semanticType)
    );
}

function terminalOrStart(phase: SemanticCaptureEventV1['phase']): boolean {
  return phase === 'start' || terminal(phase);
}

function cleanBlobRefs(
  input: SemanticCaptureEventV1,
): NonNullable<SemanticTraceRecord['blob_refs']> {
  return input.blob_refs.flatMap((blob) => {
    const mediaType = boundedText(blob.mime_type) ?? 'application/octet-stream';
    if (
      blob.scan !== 'clean'
      || !/^[0-9a-f]{64}$/.test(blob.digest)
      || nonnegativeInteger(blob.byte_length) === undefined
    ) {
      return [];
    }
    return [{
      path: `blobs/${blob.digest}.blob`,
      sha256: blob.digest,
      bytes: blob.byte_length,
      media_type: mediaType,
      scan: 'clean' as const,
    }];
  });
}

function originOf(value: Json | undefined): 'observed' | 'context' | 'inferred' {
  return typeof value === 'string' && ORIGINS.has(value)
    ? value as 'observed' | 'context' | 'inferred'
    : 'observed';
}

function scopeTypeOf(value: Json | undefined): 'agent' | 'turn' | 'step' | undefined {
  return typeof value === 'string' && SCOPE_TYPES.has(value)
    ? value as 'agent' | 'turn' | 'step'
    : undefined;
}

function terminal(phase: SemanticCaptureEventV1['phase']): boolean {
  return phase === 'end' || phase === 'error' || phase === 'cancelled';
}

function outcomeStatus(
  value: Json | undefined,
  phase: SemanticCaptureEventV1['phase'],
): 'completed' | 'failed' | 'cancelled' | 'unknown' {
  if (phase === 'error') return 'failed';
  if (phase === 'cancelled') return 'cancelled';
  if (value === 'succeeded' || value === 'completed') return 'completed';
  if (value === 'failed' || value === 'cancelled' || value === 'unknown') return value;
  if (phase === 'end') return 'completed';
  return 'unknown';
}

function scopeStatus(
  value: Json | undefined,
  phase: SemanticCaptureEventV1['phase'],
): 'completed' | 'failed' | 'cancelled' | 'interrupted' | 'unknown' {
  if (phase === 'error') return 'failed';
  if (phase === 'cancelled') return 'cancelled';
  if (value === 'succeeded') return 'completed';
  if (typeof value === 'string' && SCOPE_STATUSES.has(value)) {
    return value as 'completed' | 'failed' | 'cancelled' | 'interrupted' | 'unknown';
  }
  return phase === 'end' ? 'completed' : 'unknown';
}

function callIdOf(input: SemanticCaptureEventV1): string {
  return canonicalCallId(input, canonicalToolIdentityOf(input) ?? input.record_id);
}

function semanticToolIdentityOf(input: SemanticCaptureEventV1): string | undefined {
  const normalized = exactText(input.semantic.native_call_id);
  if (normalized) return normalized;
  return exactText(input.semantic.call_id);
}

function canonicalToolIdentityOf(input: SemanticCaptureEventV1): string | undefined {
  // Adapters use one cross-language precedence rule for displayed tool identity.
  const semanticIdentity = semanticToolIdentityOf(input);
  if (semanticIdentity) return semanticIdentity;
  return exactText(input.native_identity);
}

function toolExecutionIdentityOf(input: SemanticCaptureEventV1): string | undefined {
  const semanticIdentity = semanticToolIdentityOf(input);
  const nativeIdentity = exactText(input.native_identity);
  if (semanticIdentity && nativeIdentity && semanticIdentity !== nativeIdentity) {
    return `${semanticIdentity}\0${nativeIdentity}`;
  }
  return semanticIdentity ?? nativeIdentity;
}

function toolIdentityParts(input: SemanticCaptureEventV1): string[] {
  return [...new Set([
    semanticToolIdentityOf(input),
    exactText(input.native_identity),
  ].filter((identity): identity is string => identity !== undefined))];
}

function toolExecutionKey(input: SemanticCaptureEventV1): string {
  return canonicalCallId(input, toolExecutionIdentityOf(input) ?? input.record_id);
}

function toolProposalIdentityKey(input: SemanticCaptureEventV1): string | undefined {
  const identity = canonicalToolIdentityOf(input);
  return identity ? `${input.trace_id}\0${identity}` : undefined;
}

function displayToolIdentityOf(input: SemanticCaptureEventV1): string | undefined {
  return boundedText(canonicalToolIdentityOf(input));
}

function canonicalCallId(input: SemanticCaptureEventV1, identity: string): string {
  return `call_${digest([
    input.source.source_id,
    input.source.identity_domain,
    input.trace_id,
    identity,
  ].join('\0'))}`;
}

function toolStatus(
  value: Json | undefined,
  phase: SemanticCaptureEventV1['phase'],
  semanticType: string,
): 'succeeded' | 'failed' | 'cancelled' {
  if (semanticType === 'tool.error' || phase === 'error') return 'failed';
  if (phase === 'cancelled') return 'cancelled';
  if (value === 'succeeded' || value === 'failed' || value === 'cancelled') return value;
  return 'succeeded';
}
