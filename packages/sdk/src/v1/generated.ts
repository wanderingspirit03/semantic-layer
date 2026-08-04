/* eslint-disable */
/** Generated from semantic_capture_event_v1. Schema digest: 4026f827aa6f5009f66deae75f8659e214b3a54fa4d38d128a215a381ea8db91. */

export type SemanticCaptureEventV1 = {
  [k: string]: unknown;
} & {
  schema: 'semantic_capture_event_v1';
  run_id: Id;
  record_id: Id;
  seq: number;
  observed_at: string;
  monotonic_ns: number;
  trace_id: Id;
  conversation_id?: Id;
  turn_id?: Id;
  turn_index?: number;
  previous_turn_id?: Id;
  source: {
    source_id: SourceId;
    name: Bounded;
    seam: Bounded;
    identity_domain: Bounded;
    official: boolean;
    version?: Bounded;
    [k: string]: unknown;
  };
  coverage?: CoverageIdentity;
  native_identity?: Bounded;
  event_kind:
    'lifecycle' | 'model' | 'tool' | 'state' | 'log' | 'error' | 'stream' | 'correlation' | 'unknown' | 'loss';
  phase: 'start' | 'event' | 'end' | 'error' | 'cancelled' | 'gap';
  name: Bounded;
  native: Json;
  semantic: JsonObject;
  correlation: {
    parent_record_id?: Id;
    parent_native_id?: Bounded;
    traceparent?: Bounded;
    /**
     * @maxItems 512
     */
    links?: Id[];
    [k: string]: unknown;
  };
  loss?: {
    reason: LossReason;
    stage: 'source' | 'snapshot' | 'serialize' | 'scrub' | 'scan' | 'queue' | 'persist' | 'recover';
    affected_record_id?: Id;
    affected_path?: string;
    bytes?: number;
    count?: number;
    recoverable: boolean;
    detail?: string;
    [k: string]: unknown;
  };
  /**
   * @maxItems 512
   */
  loss_refs: Id[];
  /**
   * @maxItems 512
   */
  blob_refs: {
    digest: Sha256;
    algorithm: 'sha256';
    mime_type?: string;
    byte_length: number;
    scan: 'clean' | 'blocked';
    inline_omitted: boolean;
    [k: string]: unknown;
  }[];
  provenance: {
    language: 'typescript' | 'python';
    sdk_name: Bounded;
    sdk_version: Bounded;
    capture_policy: 'rich_local_credential_scrubbed';
    [k: string]: unknown;
  };
  [k: string]: unknown;
};
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "id".
 */
export type Id = string;
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "source_id".
 */
export type SourceId = string;
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "bounded".
 */
export type Bounded = string;
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "sha256".
 */
export type Sha256 = string;
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "json".
 */
export type Json = null | boolean | number | string | Json[] | JsonObject;
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "loss_reason".
 */
export type LossReason =
  | 'credential_redaction'
  | 'configured_redaction'
  | 'scrubber_failure_payload_omitted'
  | 'serialization_failure'
  | 'unsafe_getter_avoided'
  | 'unsafe_helper_avoided'
  | 'size_overflow_blobbed'
  | 'size_overflow_discarded'
  | 'blob_scan_blocked'
  | 'queue_backpressure_drop'
  | 'persistence_failure'
  | 'unsupported_native_value'
  | 'source_rejection'
  | 'filter_limit_exclusion'
  | 'missing_parent_context'
  | 'parser_error_malformed_bytes'
  | 'crash_recovery'
  | 'uncertain_tail'
  | 'shutdown_timeout'
  | 'turn_order_ambiguous';
/**
 * @minItems 2
 * @maxItems 256
 *
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "participant_source_id_list".
 */
export type ParticipantSourceIdList = [SourceId, SourceId, ...SourceId[]];
/**
 * @maxItems 255
 *
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "secondary_source_id_list".
 */
export type SecondarySourceIdList = SourceId[];
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "ownership_decision".
 */
export type OwnershipDecision =
  | {
      type: 'coverage.ownership.v1';
      status: 'owned';
      primary_source_id: SourceId;
      participant_source_ids: ParticipantSourceIdList;
      /**
       * @minItems 1
       * @maxItems 255
       */
      secondary_source_ids: [SourceId, ...SourceId[]];
      final: true;
    }
  | {
      type: 'coverage.ownership.v1';
      status: 'ambiguous';
      participant_source_ids: ParticipantSourceIdList;
      /**
       * @maxItems 0
       */
      secondary_source_ids: [];
      final: true;
    }
  | {
      type: 'coverage.ownership.v1';
      status: 'evidence_only';
      participant_source_ids: ParticipantSourceIdList;
      /**
       * @maxItems 0
       */
      secondary_source_ids: [];
      final: true;
    };

/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "coverage_identity".
 */
export interface CoverageIdentity {
  operation: Bounded;
  domain: Bounded;
  identity_token: Sha256;
}
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "json_object".
 */
export interface JsonObject {
  [k: string]: Json;
}
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "ownership_ambiguity".
 */
export interface OwnershipAmbiguity {
  type: 'coverage.ownership.ambiguity.v1';
  decision_record_id: Id;
}
/**
 * This interface was referenced by `undefined`'s JSON-Schema
 * via the `definition` "ownership_ambiguity_correlation".
 */
export interface OwnershipAmbiguityCorrelation {
  /**
   * @minItems 1
   * @maxItems 1
   */
  links: [Id];
}
