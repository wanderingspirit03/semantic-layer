import { createHash, createHmac } from 'node:crypto';
import type {
  CaptureSource, CoverageKey, SourceMetadata, SourceOwnership, SourceOwnershipRule,
} from './types.js';

export type TrustedSourceClass = 'deep' | 'provider' | 'otel';
type SourceClass = TrustedSourceClass | 'custom';
type CoverageRole = 'owner' | 'evidence';
const MAX_BOUNDED_CHARS = 512;
const MAX_MANIFEST_SOURCE_CHARS = 256;
const MAX_COVERAGE_CLAIMS = 64;
const MAX_OWNERSHIP_RULES = 256;
const MAX_INSTALLED_SOURCES = 256;
const MAX_OWNERSHIP_GROUPS = 4096;

const trustedSources = new WeakMap<object, TrustedSourceClass>();

/** Internal adapter-registration seam. It is deliberately not exported by the package root. */
export function trustOfficialSource<T extends CaptureSource>(source: T, sourceClass: TrustedSourceClass): T {
  trustedSources.set(source, sourceClass);
  return source;
}

export type InstalledSource = Readonly<{
  metadata: SourceMetadata;
  sourceId: string;
  sourceClass: SourceClass;
}>;

export type SourceRegistration = Readonly<{
  source: InstalledSource;
  reused: boolean;
}>;

export type CoverageIdentity = Readonly<CoverageKey & { identityToken: string }>;

type ResolvedRule = Readonly<{
  action: 'promote';
  sourceId: string;
  operation: string;
  domain: string;
}>;

export type OwnershipDecision = Readonly<{
  operation: string;
  domain: string;
  identityToken: string;
  status: 'owned' | 'ambiguous' | 'evidence_only';
  primarySourceId?: string;
  participantSourceIds: readonly string[];
  secondarySourceIds: readonly string[];
}>;

export type OwnershipManifest = Readonly<{
  policy: Readonly<{
    namespace: string;
    rules: readonly Readonly<{
      action: 'promote';
      source_id: string;
      operation: string;
      domain: string;
    }>[];
  }>;
  policy_sha256: string;
  token: Readonly<{ algorithm: 'hmac-sha256'; scope: 'run_local'; key_persisted: false }>;
  finalization: Readonly<
    { state: 'collecting' | 'frozen' } | { state: 'finalized'; finalized_at: string }
  >;
  counters: Readonly<{
    groups: number;
    decisions: number;
    ambiguities: number;
    evidence_only: number;
    group_overflows: number;
    citation_overflows: 0;
  }>;
}>;

export type CoverageReservation = Readonly<{
  coverage?: CoverageIdentity;
  overflow?: 'group_limit';
  settle(accepted: boolean): void;
}>;

export class SourceOwnershipRegistry {
  readonly compatibilityDigest: string;
  private readonly namespace: string;
  private readonly rules: readonly ResolvedRule[];
  private readonly configuredPolicy: OwnershipManifest['policy'];
  private readonly sources = new Map<SourceMetadata, InstalledSource>();
  private readonly sourcesById = new Map<string, InstalledSource>();
  private readonly sourceIds = new Set<string>();
  private readonly activeSourceIds = new Set<string>();
  private readonly sourceNames = new Set(['manual', 'semantic-layer-runtime']);
  private readonly groups = new Map<string, {
    key: CoverageKey;
    identityToken: string;
    sources: Map<string, { source: InstalledSource; role: CoverageRole; count: number }>;
  }>();
  private groupOverflows = 0;
  private state: 'collecting' | 'frozen' | 'finalized' = 'collecting';
  private finalizedAt?: string;
  private frozenDecisions?: readonly OwnershipDecision[];

  constructor(serviceName: string, policy: SourceOwnership | undefined, private readonly runKey: Uint8Array) {
    this.namespace = policy?.namespace ?? `app/${createHash('sha256').update(serviceName).digest('hex').slice(0, 16)}`;
    bounded('sourceOwnership.namespace', this.namespace);
    if ((policy?.rules?.length ?? 0) > MAX_OWNERSHIP_RULES) {
      throw new TypeError(`sourceOwnership.rules must contain at most ${MAX_OWNERSHIP_RULES} entries`);
    }
    this.rules = Object.freeze([...(policy?.rules ?? [])]
      .map(validateRule)
      .map((rule) => Object.freeze({
        action: rule.action,
        sourceId: rule.source.startsWith('./')
          ? `${this.namespace}/${rule.source.slice(2)}`
          : rule.source,
        operation: rule.operation,
        domain: rule.domain,
      })));
    validatePolicy(this.namespace, this.rules);
    this.configuredPolicy = Object.freeze({
      namespace: this.namespace,
      rules: Object.freeze([...this.rules].sort(compareRule).map((rule) => Object.freeze({
        action: rule.action, source_id: rule.sourceId,
        operation: rule.operation, domain: rule.domain,
      }))),
    });
    this.compatibilityDigest = createHash('sha256')
      .update(JSON.stringify(this.configuredPolicy)).digest('hex');
  }

  register(source: CaptureSource): SourceRegistration {
    const sourceClass = trustedSources.get(source) ?? 'custom';
    validateSourceMetadata(source.metadata);
    const sourceId = sourceClass === 'custom'
      ? `${this.namespace}/${source.metadata.name}`
      : `official/${source.metadata.name.replace(/^official:/, '')}`;
    bounded('source metadata source_id', sourceId);
    validateSourceId(sourceId);
    const metadata: SourceMetadata = Object.freeze({
      ...source.metadata,
      identityDomain: sourceClass === 'custom'
        ? bounded('source metadata identityDomain', `${this.namespace}/${source.metadata.identityDomain}`)
        : source.metadata.identityDomain,
      official: sourceClass !== 'custom',
      coverage: Object.freeze(source.metadata.coverage.map((claim) => Object.freeze({ ...claim }))),
      ...(source.metadata.qualification ? {
        qualification: Object.freeze({ ...source.metadata.qualification }),
      } : {}),
    });
    const existing = this.sourcesById.get(sourceId);
    if (this.sourceNames.has(source.metadata.name)) {
      if (sourceClass !== 'custom' && existing
        && existing.sourceClass === sourceClass
        && sameSourceMetadata(existing.metadata, metadata)) {
        return Object.freeze({ source: existing, reused: true });
      }
      throw new TypeError(`duplicate or reserved source name: ${source.metadata.name}`);
    }
    if (existing || this.sourceIds.has(sourceId)) throw new TypeError(`duplicate source identity: ${sourceId}`);
    if (this.sourceIds.size >= MAX_INSTALLED_SOURCES) {
      throw new TypeError(`at most ${MAX_INSTALLED_SOURCES} capture sources may be installed`);
    }
    const installed = Object.freeze({ metadata, sourceId, sourceClass });
    this.sourceNames.add(metadata.name);
    this.sourceIds.add(sourceId);
    this.sources.set(metadata, installed);
    this.sourcesById.set(sourceId, installed);
    return Object.freeze({ source: installed, reused: false });
  }

  sourceId(metadata: SourceMetadata): string | undefined {
    return this.sources.get(metadata)?.sourceId;
  }

  source(metadata: SourceMetadata): InstalledSource | undefined {
    return this.sources.get(metadata);
  }

  activate(source: InstalledSource): void {
    this.activeSourceIds.add(source.sourceId);
  }

  coverage(source: InstalledSource, selected: CoverageKey | undefined): CoverageKey | undefined {
    const declared = selected ?? (source.metadata.coverage.length === 1
      ? source.metadata.coverage[0]
      : undefined);
    if (!declared || !source.metadata.coverage.some((claim) => sameCoverage(claim, declared))) return undefined;
    if (source.sourceClass !== 'custom' || this.promoted(source, declared)) {
      return Object.freeze({ operation: declared.operation, domain: declared.domain });
    }
    return Object.freeze({ operation: declared.operation, domain: `${this.namespace}/${declared.domain}` });
  }

  coverageIdentity(
    source: InstalledSource,
    nativeIdentity: string | undefined,
    selected: CoverageKey | undefined,
  ): CoverageIdentity | undefined {
    if (!nativeIdentity?.trim()) return undefined;
    const coverage = this.coverage(source, selected);
    if (!coverage) return undefined;
    return Object.freeze({
      ...coverage,
      identityToken: createHmac('sha256', this.runKey).update(nativeIdentity, 'utf8').digest('hex'),
    });
  }

  reserve(
    source: InstalledSource,
    coverage: CoverageIdentity | undefined,
  ): CoverageReservation {
    if (this.state !== 'collecting' || !coverage) return Object.freeze({ settle() {} });
    const groupId = JSON.stringify([coverage.operation, coverage.domain, coverage.identityToken]);
    let group = this.groups.get(groupId);
    if (!group) {
      if (this.groups.size >= MAX_OWNERSHIP_GROUPS) {
        this.groupOverflows += 1;
        return Object.freeze({ overflow: 'group_limit' as const, settle() {} });
      }
      group = {
        key: coverage, identityToken: coverage.identityToken, sources: new Map(),
      };
    }
    const existing = group.sources.get(source.sourceId);
    group.sources.set(source.sourceId, {
      source,
      role: this.coverageRole(source, coverage),
      count: (existing?.count ?? 0) + 1,
    });
    this.groups.set(groupId, group);
    let settled = false;
    return Object.freeze({
      coverage,
      settle: (accepted: boolean) => {
        if (settled) return;
        settled = true;
        if (accepted) return;
        const retained = this.groups.get(groupId);
        const participant = retained?.sources.get(source.sourceId);
        if (!retained || !participant) return;
        if (participant.count > 1) retained.sources.set(source.sourceId, {
          source: participant.source, count: participant.count - 1,
          role: participant.role,
        });
        else retained.sources.delete(source.sourceId);
        if (retained.sources.size === 0) this.groups.delete(groupId);
      },
    });
  }

  freeze(): readonly OwnershipDecision[] {
    if (this.frozenDecisions) return this.frozenDecisions;
    this.assertPolicyResolved();
    this.state = 'frozen';
    const decisions: OwnershipDecision[] = [];
    for (const group of this.groups.values()) {
      const bySource = group.sources;
      if (bySource.size < 2) continue;
      const sources = [...bySource.values()].map((item) => item.source)
        .sort((left, right) => compareUtf8(left.sourceId, right.sourceId));
      const eligible = sources.filter((source) => this.activeSourceIds.has(source.sourceId));
      if (eligible.length < 2) continue;
      const owners = eligible.filter((source) => bySource.get(source.sourceId)?.role === 'owner');
      const primary = owners.length === 1 ? owners[0] : undefined;
      const status = owners.length > 1 ? 'ambiguous' : primary ? 'owned' : 'evidence_only';
      decisions.push(Object.freeze({
        operation: group.key.operation,
        domain: group.key.domain,
        identityToken: group.identityToken,
        status,
        ...(primary ? { primarySourceId: primary.sourceId } : {}),
        participantSourceIds: Object.freeze(eligible.map((source) => source.sourceId)),
        secondarySourceIds: Object.freeze(
          primary ? eligible.filter((source) => source !== primary).map((source) => source.sourceId) : [],
        ),
      }));
    }
    this.frozenDecisions = Object.freeze(decisions.sort((left, right) => compareDecision(left, right)));
    return this.frozenDecisions;
  }

  resetEvidence(): void {
    if (this.state !== 'collecting') throw new Error('coverage ownership evidence is already frozen');
    this.groups.clear();
    this.groupOverflows = 0;
  }

  resetAfterPersistenceRecovery(): void {
    this.groups.clear();
    this.groupOverflows = 0;
    this.frozenDecisions = undefined;
    this.state = 'collecting';
    this.finalizedAt = undefined;
  }

  finalize(): void {
    if (this.state === 'collecting') throw new Error('coverage ownership must freeze before finalization');
    this.state = 'finalized';
    this.finalizedAt ??= new Date().toISOString();
  }

  assertPolicyResolved(): void {
    const unresolved = [...new Set(this.rules
      .filter((rule) => !this.sourceIds.has(rule.sourceId))
      .map((rule) => rule.sourceId))]
      .sort(compareUtf8);
    if (unresolved.length > 0) {
      throw new TypeError(`source ownership policy references uninstalled source: ${unresolved.join(', ')}`);
    }
  }

  manifest(): OwnershipManifest {
    const decisions = this.frozenDecisions ?? [];
    const policy = this.resolvedPolicy();
    return Object.freeze({
      policy,
      policy_sha256: createHash('sha256').update(JSON.stringify(policy)).digest('hex'),
      token: Object.freeze({ algorithm: 'hmac-sha256', scope: 'run_local', key_persisted: false }),
      finalization: this.state === 'finalized'
        ? Object.freeze({ state: 'finalized', finalized_at: this.finalizedAt! })
        : Object.freeze({ state: this.state }),
      counters: Object.freeze({
        groups: this.groups.size,
        decisions: decisions.length,
        ambiguities: decisions.filter((decision) => decision.status === 'ambiguous').length,
        evidence_only: decisions.filter((decision) => decision.status === 'evidence_only').length,
        group_overflows: this.groupOverflows,
        citation_overflows: 0,
      }),
    });
  }

  private promoted(source: InstalledSource, coverage: CoverageKey): boolean {
    return this.rules.some((rule) => rule.action === 'promote'
      && rule.sourceId === source.sourceId
      && sameCoverage(rule, coverage));
  }

  private coverageRole(source: InstalledSource, coverage: CoverageKey): CoverageRole {
    const matchingClaims = source.metadata.coverage.filter((claim) => {
      const surface = this.coverage(source, claim);
      return surface !== undefined && sameCoverage(surface, coverage);
    });
    return matchingClaims.length > 0
      && matchingClaims.every((claim) => claim.role === 'evidence')
      ? 'evidence'
      : 'owner';
  }

  private resolvedPolicy(): OwnershipManifest['policy'] {
    return Object.freeze({
      namespace: this.namespace,
      rules: Object.freeze(this.configuredPolicy.rules.filter((rule) => this.sourceIds.has(rule.source_id))),
    });
  }

}

function validateRule(rule: SourceOwnershipRule): SourceOwnershipRule {
  if (!rule || rule.action !== 'promote'
    || typeof rule.source !== 'string' || !rule.source.trim()
    || typeof rule.operation !== 'string' || !rule.operation.trim()
    || typeof rule.domain !== 'string' || !rule.domain.trim()) {
    throw new TypeError(
      'source ownership rules require promote action, source, operation, and domain; prefer is unsupported',
    );
  }
  if (!rule.source.startsWith('./') && !rule.source.startsWith('official/')) {
    throw new TypeError('source ownership rule source must be ./<custom> or official/<source>');
  }
  bounded('sourceOwnership.rules[].source', rule.source);
  bounded('sourceOwnership.rules[].operation', rule.operation);
  bounded('sourceOwnership.rules[].domain', rule.domain);
  return Object.freeze({ ...rule });
}

function validatePolicy(namespace: string, rules: readonly ResolvedRule[]): void {
  if (!namespace.trim() || namespace.includes('..')
    || !/^(?!builtin(?:\/|$)|official(?:\/|$))[^\s/]+(?:\/[^\s/]+)*$/u.test(namespace)) {
    throw new TypeError('source ownership namespace is invalid');
  }
  const uniqueRules = new Set<string>();
  for (const rule of rules) {
    validateSourceId(rule.sourceId);
    const serialized = JSON.stringify(rule);
    if (uniqueRules.has(serialized)) throw new TypeError('sourceOwnership.rules must not contain duplicates');
    uniqueRules.add(serialized);
  }
}

function sameCoverage(left: CoverageKey, right: CoverageKey): boolean {
  return left.operation === right.operation && left.domain === right.domain;
}

function sameSourceMetadata(left: SourceMetadata, right: SourceMetadata): boolean {
  return left.name === right.name
    && left.seam === right.seam
    && left.identityDomain === right.identityDomain
    && left.version === right.version
    && left.official === right.official
    && left.qualification?.status === right.qualification?.status
    && left.qualification?.profile === right.qualification?.profile
    && left.coverage.length === right.coverage.length
    && left.coverage.every((claim, index) => {
      const candidate = right.coverage[index];
      return candidate !== undefined
        && claim.operation === candidate.operation
        && claim.domain === candidate.domain
        && claim.role === candidate.role;
    });
}

function compareRule(left: ResolvedRule, right: ResolvedRule): number {
  return compareUtf8(JSON.stringify(left), JSON.stringify(right));
}

function compareDecision(left: OwnershipDecision, right: OwnershipDecision): number {
  return compareUtf8(
    JSON.stringify([left.operation, left.domain, left.identityToken]),
    JSON.stringify([right.operation, right.domain, right.identityToken]),
  );
}

function compareUtf8(left: string, right: string): number {
  return Buffer.compare(Buffer.from(left, 'utf8'), Buffer.from(right, 'utf8'));
}

function validateSourceMetadata(metadata: SourceMetadata): void {
  manifestBounded('source metadata.name', metadata.name);
  if (!/^[^\s/]+$/u.test(metadata.name.replace(/^official:/, ''))) {
    throw new TypeError('source metadata.name cannot form a valid source identity');
  }
  manifestBounded('source metadata.seam', metadata.seam);
  bounded('source metadata.identityDomain', metadata.identityDomain);
  if (metadata.version !== undefined) {
    manifestBounded('source metadata.version', metadata.version);
  }
  if (metadata.qualification !== undefined) {
    if (!['exact_qualified', 'capability_checked_unqualified', 'unknown']
      .includes(metadata.qualification.status)) {
      throw new TypeError('source metadata.qualification.status is invalid');
    }
    if (metadata.qualification.profile !== undefined) {
      manifestBounded(
        'source metadata.qualification.profile',
        metadata.qualification.profile,
      );
    }
    if (metadata.qualification.status === 'exact_qualified' && metadata.version === undefined) {
      throw new TypeError('source metadata exact_qualified status requires an observed version');
    }
  }
  if (metadata.coverage.length > MAX_COVERAGE_CLAIMS) {
    throw new TypeError(`source metadata.coverage must contain at most ${MAX_COVERAGE_CLAIMS} claims`);
  }
  const claims = new Set<string>();
  for (const claim of metadata.coverage) {
    if (!claim || Object.keys(claim).some((key) => !['operation', 'domain', 'role'].includes(key))) {
      throw new TypeError('source metadata.coverage[] contains unknown fields');
    }
    bounded('source metadata.coverage[].operation', claim.operation);
    bounded('source metadata.coverage[].domain', claim.domain);
    if (claim.role !== undefined && !['owner', 'evidence'].includes(claim.role)) {
      throw new TypeError('source metadata.coverage[].role must be owner or evidence');
    }
    const serialized = JSON.stringify(claim);
    if (claims.has(serialized)) throw new TypeError('source metadata.coverage[] must not contain duplicates');
    claims.add(serialized);
  }
}

function validateSourceId(value: string): void {
  if (!/^(?:builtin\/(?:manual|semantic-layer-runtime)|official\/[^\s/]+|(?!builtin\/|official\/)[^\s/]+(?:\/[^\s/]+)+)$/u.test(value)) {
    throw new TypeError('source identity is invalid');
  }
}

function bounded(field: string, value: string): string {
  if (typeof value !== 'string' || !value.trim()
    || codePointLength(value) < 1 || codePointLength(value) > MAX_BOUNDED_CHARS) {
    throw new TypeError(`${field} must contain between 1 and ${MAX_BOUNDED_CHARS} characters`);
  }
  return value;
}

function manifestBounded(field: string, value: string): string {
  if (typeof value !== 'string' || !value.trim()
    || codePointLength(value) > MAX_MANIFEST_SOURCE_CHARS) {
    throw new TypeError(
      `${field} must contain between 1 and ${MAX_MANIFEST_SOURCE_CHARS} characters`,
    );
  }
  return value;
}

function codePointLength(value: string): number {
  return [...value].length;
}
