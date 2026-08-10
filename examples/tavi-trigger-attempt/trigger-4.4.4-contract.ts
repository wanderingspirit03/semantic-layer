import { task } from '@trigger.dev/sdk/v3';
import { runTaviTriggerAttempt } from './tavi-trigger-attempt.js';
import { createTaviTriggerCancellationRegistry } from './trigger-cancellation.js';
import { triggerIdentityFromContext } from './trigger-context.js';

const cancellations = createTaviTriggerCancellationRegistry();

/** Compile-only proof of the task-local Trigger 4.4.4 cancellation seam. */
export const taviTriggerContractTask = task<
  'semantic-layer-trigger-contract',
  { researchId: string },
  { signalForwarded: boolean }
>({
  id: 'semantic-layer-trigger-contract',
  onCancel: async ({ ctx }) => {
    await cancellations.cancel(ctx.run.id);
  },
  run: async (payload: { researchId: string }, { ctx }) => {
    const cancellation = cancellations.register(ctx.run.id);
    return await runTaviTriggerAttempt({
      tenant: null,
      trigger: triggerIdentityFromContext(ctx, payload.researchId),
      cancellation,
      task: async ({ signal }) => ({ signalForwarded: signal === cancellation.signal }),
    });
  },
});
