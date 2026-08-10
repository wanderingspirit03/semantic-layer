import type { Context } from '@trigger.dev/sdk/v3';
import type { TaviTriggerIdentity } from './tavi-trigger-attempt.js';

export type TriggerContextCorrelationFields = Readonly<{
  run: Pick<Context['run'], 'id' | 'parentTaskRunId' | 'rootTaskRunId'>;
  attempt: Pick<Context['attempt'], 'number'>;
}>;

/** Map the public Trigger 4.4.4 task context without importing its OTel types. */
export function triggerIdentityFromContext(
  ctx: TriggerContextCorrelationFields,
  researchId: string,
  traceparent?: string,
): TaviTriggerIdentity {
  return {
    runId: ctx.run.id,
    ...(ctx.run.parentTaskRunId
      ? { parentRunId: ctx.run.parentTaskRunId }
      : {}),
    rootRunId: ctx.run.rootTaskRunId ?? ctx.run.id,
    attemptNumber: ctx.attempt.number,
    researchId,
    ...(traceparent ? { traceparent } : {}),
  };
}
