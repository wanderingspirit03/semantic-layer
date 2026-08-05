import type { ValidationReport } from 'semantic-layer-capture';

type JsonRecord = Record<string, any>;

export type TraceSummary = {
  tenant: string;
  installation: string;
  bundle: string;
  startedAt?: string;
  sealedAt?: string;
  outcomes: string[];
  sources: string[];
  models: string[];
  records: number;
  modelRequests: number;
  modelResponses: number;
  toolCalls: number;
  toolResults: number;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  tokenUsageObserved: boolean;
  errors: number;
  recoveries: number;
  lossRecords: number;
  lostEvents: number;
  lossReasons: Array<{ reason: string; records: number; items: number; recoverable: boolean }>;
  valid: boolean;
  validationIssues: string[];
};

export function summarize(
  scope: { tenant: string; installation: string; bundle: string },
  manifest: JsonRecord,
  records: JsonRecord[],
  validation: ValidationReport,
): TraceSummary {
  const models = new Set<string>();
  let inputTokens = 0;
  let outputTokens = 0;
  let totalTokens = 0;
  let tokenUsageObserved = false;
  for (const record of records) {
    if (typeof record.data?.model === 'string') models.add(record.data.model);
    if (record.kind === 'model.response' && record.data?.usage && typeof record.data.usage === 'object') {
      tokenUsageObserved = true;
      inputTokens += numeric(record.data.usage.input_tokens ?? record.data.usage.prompt_tokens);
      outputTokens += numeric(record.data.usage.output_tokens ?? record.data.usage.completion_tokens);
      totalTokens += numeric(record.data.usage.total_tokens);
    }
  }
  if (totalTokens === 0) totalTokens = inputTokens + outputTokens;
  const outcomes = records.flatMap((record) => (
    record.kind === 'run.outcome' && typeof record.data?.status === 'string'
      ? [record.data.status]
      : []
  ));
  const losses = records.filter((record) => record.kind === 'loss');
  const lossReasons = new Map<string, { records: number; items: number; recoverable: boolean }>();
  for (const loss of losses) {
    if (typeof loss.data?.reason !== 'string') continue;
    const current = lossReasons.get(loss.data.reason) ?? { records: 0, items: 0, recoverable: true };
    current.records += 1;
    current.items += numeric(loss.data.count);
    current.recoverable = current.recoverable && loss.data.recoverable === true;
    lossReasons.set(loss.data.reason, current);
  }
  return {
    ...scope,
    ...(typeof manifest.started_at === 'string' ? { startedAt: manifest.started_at } : {}),
    ...(typeof manifest.sealed_at === 'string' ? { sealedAt: manifest.sealed_at } : {}),
    outcomes,
    sources: Array.isArray(manifest.sources)
      ? manifest.sources.flatMap((source: JsonRecord) => typeof source?.name === 'string' ? [source.name] : [])
      : [],
    models: [...models].sort(),
    records: records.length,
    modelRequests: count(records, 'model.request'),
    modelResponses: count(records, 'model.response'),
    toolCalls: count(records, 'tool.call'),
    toolResults: count(records, 'tool.result'),
    inputTokens,
    outputTokens,
    totalTokens,
    tokenUsageObserved,
    errors: records.filter((record) => record.kind === 'error' || ['error', 'failed'].includes(record.data?.status)).length,
    recoveries: records.filter((record) => (
      record.kind === 'state'
      && typeof record.data?.type === 'string'
      && (record.data.type === 'recovery.retry' || record.data.type.endsWith('.retry'))
    )).length,
    lossRecords: losses.length,
    lostEvents: losses.reduce((total, record) => total + numeric(record.data?.count), 0),
    lossReasons: [...lossReasons].sort(([left], [right]) => left.localeCompare(right)).map(([reason, counts]) => ({ reason, ...counts })),
    valid: validation.valid,
    validationIssues: validation.issues,
  };
}

export function contentRecords(records: JsonRecord[]): JsonRecord[] {
  const privateKinds = new Set([
    'run.start',
    'message',
    'model.request',
    'model.response',
    'tool.proposal',
    'tool.call',
    'tool.result',
    'state',
    'verification',
    'error',
    'run.outcome',
  ]);
  return records.filter((record) => privateKinds.has(record.kind));
}

export function formatSummary(summary: TraceSummary): string {
  const lines = [
    `${summary.tenant}/${summary.installation}/${summary.bundle}`,
    `  time: ${summary.startedAt ?? 'unknown'}`,
    `  outcomes: ${summary.outcomes.join(', ') || 'unknown'}`,
    `  source: ${summary.sources.join(', ') || 'unknown'}`,
    `  model: ${summary.models.join(', ') || 'unknown'}`,
    `  records: ${summary.records}; model ${summary.modelRequests}/${summary.modelResponses}; tools ${summary.toolCalls}/${summary.toolResults}`,
    `  tokens: ${summary.tokenUsageObserved ? `input ${summary.inputTokens}; output ${summary.outputTokens}; total ${summary.totalTokens}` : 'not captured'}`,
    `  errors: ${summary.errors}; recoveries: ${summary.recoveries}; loss records: ${summary.lossRecords}; lost events: ${summary.lostEvents}`,
    `  loss reasons: ${summary.lossReasons.map((loss) => `${loss.reason}=${loss.items}`).join(', ') || 'none'}`,
    `  validation: ${summary.valid ? 'valid' : `invalid (${summary.validationIssues.join(', ')})`}`,
  ];
  return lines.join('\n');
}

function count(records: JsonRecord[], kind: string): number {
  return records.filter((record) => record.kind === kind).length;
}

function numeric(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}
