import { describe, expect, it } from 'vitest';
import { createPluginDefinition, REQUIRED_HOOKS } from '../src/plugin.js';

describe('OpenClaw plugin registration', () => {
  it('registers every supported observation hook synchronously without agent tools', () => {
    const hooks: string[] = [];
    let registeredTools = 0;
    const plugin = createPluginDefinition({
      createRunCapture: () => {
        throw new Error('not used');
      },
      createUploader: () => {
        throw new Error('not used');
      },
    });

    const result = plugin.register({
      pluginConfig: {},
      on(name: string) {
        hooks.push(name);
      },
      registerTool() {
        registeredTools += 1;
      },
      logger: { debug() {}, info() {}, warn() {}, error() {} },
    });

    expect(result).toBeUndefined();
    expect(hooks).toEqual(REQUIRED_HOOKS);
    expect(registeredTools).toBe(0);
  });

  it('stays inert and emits one actionable diagnostic while unconfigured', () => {
    const errors: string[] = [];
    const handlers = new Map<string, (...args: unknown[]) => unknown>();
    const plugin = createPluginDefinition({
      createRunCapture: () => {
        throw new Error('capture must stay inert');
      },
      createUploader: () => {
        throw new Error('uploader must stay inert');
      },
    });

    plugin.register({
      pluginConfig: {},
      on(name: string, handler: (...args: unknown[]) => unknown) {
        handlers.set(name, handler);
      },
      logger: {
        debug() {},
        info() {},
        warn() {},
        error(message: string) {
          errors.push(message);
        },
      },
    });

    handlers.get('before_model_resolve')?.(
      { runId: 'run-1', sessionId: 'session-1' },
      {},
    );
    handlers.get('before_model_resolve')?.(
      { runId: 'run-2', sessionId: 'session-2' },
      {},
    );
    expect(errors).toEqual([
      'Semantic Layer capture is disabled: configuration is incomplete. Run npx -y semantic-layer-openclaw-setup@0.1.0-pilot.6 doctor.',
    ]);
  });

  it('blocks activation below the minimum qualified host without affecting the host event', () => {
    let captures = 0;
    const errors: string[] = [];
    const handlers = new Map<string, (...args: unknown[]) => unknown>();
    const plugin = createPluginDefinition({
      createRunCapture: () => {
        captures += 1;
        throw new Error('capture must stay inert');
      },
      createUploader: () => {
        throw new Error('uploader must stay inert');
      },
    });

    plugin.register({
      pluginConfig: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-secret-value',
        identityKey: 'identity-secret-value-which-is-long-enough',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'fixture',
      },
      on(name: string, handler: (...args: unknown[]) => unknown) {
        handlers.set(name, handler);
      },
      logger: {
        debug() {},
        info() {},
        warn() {},
        error(message: string) {
          errors.push(message);
        },
      },
      runtime: { version: '2026.5.4' },
    });

    expect(() =>
      handlers.get('before_model_resolve')?.(
        { runId: 'run-1' },
        {
          runId: 'run-1',
          sessionId: 'session-1',
        },
      ),
    ).not.toThrow();
    expect(captures).toBe(0);
    expect(errors).toEqual([
      expect.stringContaining('requires OpenClaw 2026.5.5 or newer'),
    ]);
  });

  it('reports a newer runtime as in-range but unqualified before doctor', () => {
    const warnings: string[] = [];
    const plugin = createPluginDefinition({
      createRunCapture: () => {
        throw new Error('not used');
      },
      createUploader: () => {
        throw new Error('not used');
      },
    });

    plugin.register({
      pluginConfig: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-secret-value',
        identityKey: 'identity-secret-value-which-is-long-enough',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'fixture',
      },
      on() {},
      logger: {
        debug() {},
        info() {},
        warn(message: string) {
          warnings.push(message);
        },
        error() {},
      },
      runtime: { version: '2026.8.0' },
    });

    expect(warnings).toEqual([
      expect.stringContaining('in-range but unqualified'),
    ]);
    expect(warnings[0]).not.toContain('capability-checked');
  });

  it('activates the shared uploader on gateway start so pending bundles resume without a new run', () => {
    let uploaders = 0;
    const handlers = new Map<string, (...args: unknown[]) => unknown>();
    const plugin = createPluginDefinition({
      createRunCapture: () => {
        throw new Error('not used');
      },
      createUploader: () => {
        uploaders += 1;
        return {
          enqueueArtifact: async () => ({
            bundleId: 'x',
            bundleDigest: 'x',
            state: 'pending' as const,
          }),
          flush: async () => ({ timedOut: false, uploadedBundles: 0 }),
          status: () => ({ lifecycle: 'running' }),
          shutdown: async () => {},
        };
      },
    });

    plugin.register({
      pluginConfig: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-secret-value',
        identityKey: 'identity-secret-value-which-is-long-enough',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'fixture',
      },
      on(name: string, handler: (...args: unknown[]) => unknown) {
        handlers.set(name, handler);
      },
      logger: { debug() {}, info() {}, warn() {}, error() {} },
    });

    handlers.get('gateway_start')?.();
    handlers.get('gateway_start')?.();
    expect(uploaders).toBe(1);
  });
});
