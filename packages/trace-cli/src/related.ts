import { sep, resolve } from 'node:path';

import { validateArtifact } from 'semantic-layer-capture';
import {
  assertManifestScope,
  findLocalBundles,
  readLocalBundle,
} from './bundles.js';

type ProtectedCorrelation = {
  taskId: string;
  system: string;
  runId: string;
  parentRunId?: string;
  rootRunId?: string;
  attempt?: number;
};

type InternalNode = RelatedNode & { correlation: ProtectedCorrelation };

export type RelatedNode = {
  scope: string;
  root: string;
  system: string;
  attempt: number | null;
  retry: boolean;
};

export type RelatedEdge = {
  type: 'parent' | 'root';
  from: Pick<RelatedNode, 'scope' | 'root'>;
  to: Pick<RelatedNode, 'scope' | 'root'>;
};

export type RelatedWarning = {
  code:
    | 'ambiguous_parent'
    | 'ambiguous_root'
    | 'ambiguous_seed_roots'
    | 'duplicate_execution_attempt'
    | 'invalid_bundle_skipped'
    | 'missing_identity'
    | 'unresolved_parent'
    | 'unresolved_root';
  scope: string;
  root?: string;
};

export type RelatedReport = {
  seed: { scope: string; root: string | null };
  nodes: RelatedNode[];
  edges: RelatedEdge[];
  warnings: RelatedWarning[];
};

export async function findRelatedBundles(options: {
  output: string;
  tenant: string;
  seedScope: string;
  seedRoot?: string;
}): Promise<RelatedReport> {
  const paths = await findLocalBundles(options.output, options.tenant);
  const bundles: Array<{ scope: string; records: Record<string, unknown>[] }> = [];
  const warnings: RelatedWarning[] = [];
  for (const path of paths) {
    const scope = localScope(path, options.output);
    const validation = await validateArtifact(path);
    if (!validation.valid) {
      warnings.push({ code: 'invalid_bundle_skipped', scope });
      continue;
    }
    const { manifest, records } = await readLocalBundle(path);
    const parts = scope.split('/');
    try {
      assertManifestScope(manifest, { installation: parts[1]!, bundle: parts[2]! });
    } catch {
      warnings.push({ code: 'invalid_bundle_skipped', scope });
      continue;
    }
    bundles.push({ scope, records });
  }

  const seedBundle = bundles.find((bundle) => bundle.scope === options.seedScope);
  if (!seedBundle) throw new Error(`seed bundle is not a validated local bundle: ${options.seedScope}`);
  const seedStarts = seedBundle.records.filter(isRunStart);
  const selected = options.seedRoot
    ? seedStarts.filter((record) => record.id === options.seedRoot)
    : seedStarts;
  if (options.seedRoot && selected.length === 0) {
    throw new Error(`run.start record was not found in seed bundle: ${options.seedScope}`);
  }
  if (!options.seedRoot && selected.length > 1) {
    return {
      seed: { scope: options.seedScope, root: null },
      nodes: [],
      edges: [],
      warnings: [...warnings, { code: 'ambiguous_seed_roots', scope: options.seedScope }],
    };
  }
  const seed = selected[0];
  const seedCorrelation = seed ? protectedCorrelation(seed) : undefined;
  if (!seed || !seedCorrelation) {
    return {
      seed: { scope: options.seedScope, root: seed?.id ?? null },
      nodes: [],
      edges: [],
      warnings: [...warnings, {
        code: 'missing_identity',
        scope: options.seedScope,
        ...(seed ? { root: seed.id } : {}),
      }],
    };
  }

  const internal: InternalNode[] = [];
  for (const bundle of bundles) {
    for (const record of bundle.records.filter(isRunStart)) {
      const correlation = protectedCorrelation(record);
      if (!correlation) {
        warnings.push({ code: 'missing_identity', scope: bundle.scope, root: record.id });
        continue;
      }
      if (correlation.taskId !== seedCorrelation.taskId) continue;
      internal.push({
        scope: bundle.scope,
        root: record.id,
        system: correlation.system,
        attempt: correlation.attempt ?? null,
        retry: false,
        correlation,
      });
    }
  }
  internal.sort(compareNodes);

  const byExecution = new Map<string, InternalNode[]>();
  const byAttempt = new Map<string, InternalNode[]>();
  for (const node of internal) {
    const runId = node.correlation.runId;
    const executionKey = protectedKey(node.correlation.system, runId);
    byExecution.set(executionKey, [...(byExecution.get(executionKey) ?? []), node]);
    const attemptKey = `${executionKey}\u0000${node.correlation.attempt ?? 'none'}`;
    byAttempt.set(attemptKey, [...(byAttempt.get(attemptKey) ?? []), node]);
  }
  for (const nodes of byExecution.values()) {
    if (new Set(nodes.map((node) => node.correlation.attempt ?? 'none')).size > 1) {
      for (const node of nodes) node.retry = true;
    }
  }
  for (const duplicates of byAttempt.values()) {
    if (duplicates.length > 1) {
      for (const node of duplicates) warnings.push({
        code: 'duplicate_execution_attempt',
        scope: node.scope,
        root: node.root,
      });
    }
  }

  const edges: RelatedEdge[] = [];
  for (const node of internal) {
    addEdges('parent', node, node.correlation.parentRunId, byExecution, edges, warnings);
    if (node.correlation.rootRunId !== node.correlation.runId) {
      addEdges('root', node, node.correlation.rootRunId, byExecution, edges, warnings);
    }
  }
  edges.sort(compareEdges);
  warnings.sort(compareWarnings);
  return {
    seed: { scope: options.seedScope, root: seed.id },
    nodes: internal.map(({ correlation: _protected, ...safe }) => safe),
    edges,
    warnings,
  };
}

function addEdges(
  type: 'parent' | 'root',
  source: InternalNode,
  protectedTarget: string | undefined,
  byRun: Map<string, InternalNode[]>,
  edges: RelatedEdge[],
  warnings: RelatedWarning[],
): void {
  if (!protectedTarget) return;
  const targets = byRun.get(protectedKey(source.correlation.system, protectedTarget)) ?? [];
  if (targets.length === 0) {
    warnings.push({
      code: type === 'parent' ? 'unresolved_parent' : 'unresolved_root',
      scope: source.scope,
      root: source.root,
    });
    return;
  }
  if (targets.length > 1) {
    warnings.push({
      code: type === 'parent' ? 'ambiguous_parent' : 'ambiguous_root',
      scope: source.scope,
      root: source.root,
    });
    return;
  }
  for (const target of targets) edges.push({
    type,
    from: nodeReference(source),
    to: nodeReference(target),
  });
}

function protectedCorrelation(record: RunStart): ProtectedCorrelation | undefined {
  const data = asRecord(record.data);
  const correlation = asRecord(data?.correlation);
  const execution = asRecord(correlation?.execution);
  if (typeof correlation?.task_id !== 'string'
    || typeof execution?.system !== 'string'
    || typeof execution.run_id !== 'string') return undefined;
  const attempt = execution.attempt;
  if (attempt !== undefined && (!Number.isSafeInteger(attempt) || Number(attempt) < 0)) {
    return undefined;
  }
  return {
    taskId: correlation.task_id,
    system: execution.system,
    runId: execution.run_id,
    ...(typeof execution.parent_run_id === 'string'
      ? { parentRunId: execution.parent_run_id }
      : {}),
    ...(typeof execution.root_run_id === 'string'
      ? { rootRunId: execution.root_run_id }
      : {}),
    ...(typeof attempt === 'number' ? { attempt } : {}),
  };
}

type RunStart = Record<string, unknown> & { id: string; kind: 'run.start' };

function isRunStart(record: Record<string, unknown>): record is RunStart {
  return record.kind === 'run.start' && typeof record.id === 'string';
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function localScope(path: string, root: string): string {
  return path.slice(resolve(root).length + 1).split(sep).join('/');
}

function protectedKey(system: string, runId: string): string {
  return `${system}\u0000${runId}`;
}

function nodeReference(node: RelatedNode): Pick<RelatedNode, 'scope' | 'root'> {
  return { scope: node.scope, root: node.root };
}

function compareNodes(left: RelatedNode, right: RelatedNode): number {
  return `${left.scope}\u0000${left.root}`.localeCompare(`${right.scope}\u0000${right.root}`);
}

function compareEdges(left: RelatedEdge, right: RelatedEdge): number {
  return `${left.from.scope}\u0000${left.from.root}\u0000${left.type}\u0000${left.to.scope}\u0000${left.to.root}`
    .localeCompare(`${right.from.scope}\u0000${right.from.root}\u0000${right.type}\u0000${right.to.scope}\u0000${right.to.root}`);
}

function compareWarnings(left: RelatedWarning, right: RelatedWarning): number {
  return `${left.scope}\u0000${left.root ?? ''}\u0000${left.code}`
    .localeCompare(`${right.scope}\u0000${right.root ?? ''}\u0000${right.code}`);
}
