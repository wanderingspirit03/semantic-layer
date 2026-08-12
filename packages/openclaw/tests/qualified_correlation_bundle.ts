import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCapture, validateArtifact } from 'semantic-layer-capture';
import { expect } from 'vitest';
import { createPluginDefinition, type PluginApi } from '../src/plugin.js';

export async function verifyQualifiedCorrelationBundle(
  hostVersion: string,
  register: (plugin: ReturnType<typeof createPluginDefinition>, api: PluginApi) => void,
): Promise<void> {
  const output = await mkdtemp(join(tmpdir(), 'semantic-layer-qualified-correlation-'));
  const handlers = new Map<string, (...args: unknown[]) => unknown>();
  let gatewayHandler: ((input: {
    params: Record<string, unknown>;
    respond(ok: boolean, payload?: unknown): void;
  }) => unknown) | undefined;
  let artifactPath = '';
  try {
    const plugin = createPluginDefinition({
      createRunCapture: createCapture,
      createUploader: () => ({
        async enqueueArtifact(path: string) {
          artifactPath = path;
          return { bundleId: 'bundle', bundleDigest: 'digest', state: 'pending' as const };
        },
        async flush() { return { timedOut: false, uploadedBundles: 0 }; },
        status() { return { lifecycle: 'running', pressure: 'ok' }; },
        async shutdown() {},
      }),
    }, { terminalGraceMs: 0 });
    const api: PluginApi = {
      pluginConfig: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'qualified-ingest-secret',
        identityKey: 'qualified-identity-secret-which-is-long-enough',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: `qualified-${hostVersion}`,
        outputDirectory: output,
      },
      on(name, handler) {
        handlers.set(name, handler as (...args: unknown[]) => unknown);
      },
      registerGatewayMethod(_name, handler) {
        gatewayHandler = handler as unknown as typeof gatewayHandler;
      },
      logger: { debug() {}, info() {}, warn() {}, error() {} },
      runtime: { version: hostVersion },
    };
    register(plugin, api);
    let bindResult: unknown;
    gatewayHandler?.({
      params: { runId: 'qualified-run', taskId: 'qualified-task' },
      respond(_ok, payload) { bindResult = payload; },
    });
    expect(bindResult).toEqual({ accepted: true });
    const context = { runId: 'qualified-run', sessionId: 'qualified-session' };
    handlers.get('before_model_resolve')?.({ prompt: 'hello' }, context);
    await handlers.get('agent_end')?.({ runId: context.runId, success: true }, context);

    const trace = await readFile(join(artifactPath, 'trace.jsonl'), 'utf8');
    expect(trace).toMatch(/"task_id":"task_[a-f0-9]{64}"/u);
    expect(trace).not.toContain('qualified-task');
    await expect(validateArtifact(artifactPath, {
      secretValues: ['qualified-task', 'qualified-identity-secret-which-is-long-enough'],
    })).resolves.toMatchObject({ valid: true, issues: [] });
  } finally {
    await rm(output, { recursive: true, force: true });
  }
}
