import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  definePluginEntry as defineCurrentPluginEntry,
  type OpenClawPluginApi as CurrentPluginApi,
} from 'openclaw-qualified-current/plugin-sdk/plugin-entry';
import { validateJsonSchemaValue as validateWithCurrentHost } from 'openclaw-qualified-current/plugin-sdk/config-schema';
import { createPluginDefinition, REQUIRED_HOOKS } from '../src/plugin.js';
import { verifyQualifiedCorrelationBundle } from './qualified_correlation_bundle.js';

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
type CurrentHookName = Parameters<CurrentPluginApi['on']>[0];
const CURRENT_REQUIRED_HOOKS =
  REQUIRED_HOOKS satisfies readonly CurrentHookName[];

describe('exact OpenClaw 2026.7.1-2 host contract', () => {
  it('seals protected correlation through the installed host entry', async () => {
    await verifyQualifiedCorrelationBundle('2026.7.1-2', (plugin, api) => {
      const entry = defineCurrentPluginEntry({
        id: plugin.id,
        name: plugin.name,
        description: plugin.description,
        register: plugin.register,
      });
      entry.register(api as CurrentPluginApi);
    });
  });

  it('loads the plugin through the installed current entry shape with every required typed hook', () => {
    const hooks: string[] = [];
    const gatewayMethods: string[] = [];
    const plugin = createPluginDefinition({
      createRunCapture: () => {
        throw new Error('capture is not used during registration');
      },
      createUploader: () => {
        throw new Error('uploader is not used during registration');
      },
    });
    const registerOnCurrentHost: (api: CurrentPluginApi) => void =
      plugin.register;
    const currentEntry = defineCurrentPluginEntry({
      id: plugin.id,
      name: plugin.name,
      description: plugin.description,
      register: registerOnCurrentHost,
    });

    currentEntry.register({
      pluginConfig: {},
      on(name: CurrentHookName) {
        hooks.push(name);
      },
      registerGatewayMethod(name) {
        gatewayMethods.push(name);
      },
      logger: { debug() {}, info() {}, warn() {}, error() {} },
      runtime: { version: '2026.7.1-2' },
    } as CurrentPluginApi);

    expect(hooks).toEqual(CURRENT_REQUIRED_HOOKS);
    expect(gatewayMethods).toEqual(['semantic-layer.correlation.bind']);
  });

  it('accepts the published plugin configuration with the installed current schema validator', async () => {
    const manifest = JSON.parse(
      await readFile(join(packageRoot, 'openclaw.plugin.json'), 'utf8'),
    );
    const result = validateWithCurrentHost({
      schema: manifest.configSchema,
      cacheKey: 'semantic-layer-openclaw:qualified-current',
      value: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-key',
        identityKey: 'identity-key-with-at-least-thirty-two-characters',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'qualified-current',
      },
    });

    expect(result.ok).toBe(true);
  });
});
