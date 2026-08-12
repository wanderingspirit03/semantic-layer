import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  definePluginEntry as defineMinimumPluginEntry,
  type OpenClawPluginApi as MinimumPluginApi,
} from 'openclaw/plugin-sdk/plugin-entry';
import { validateJsonSchemaValue as validateWithMinimumHost } from 'openclaw/plugin-sdk/config-schema';
import { createPluginDefinition, REQUIRED_HOOKS } from '../src/plugin.js';
import { verifyQualifiedCorrelationBundle } from './qualified_correlation_bundle.js';

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
type MinimumHookName = Parameters<MinimumPluginApi['on']>[0];
const MINIMUM_REQUIRED_HOOKS =
  REQUIRED_HOOKS satisfies readonly MinimumHookName[];

describe('exact OpenClaw 2026.5.5 host contract', () => {
  it('seals protected correlation through the installed host entry', async () => {
    await verifyQualifiedCorrelationBundle('2026.5.5', (plugin, api) => {
      const entry = defineMinimumPluginEntry({
        id: plugin.id,
        name: plugin.name,
        description: plugin.description,
        register: plugin.register,
      });
      entry.register(api as MinimumPluginApi);
    });
  });

  it('seals native inbound correlation through the installed host entry', async () => {
    await verifyQualifiedCorrelationBundle('2026.5.5', (plugin, api) => {
      const entry = defineMinimumPluginEntry({
        id: plugin.id,
        name: plugin.name,
        description: plugin.description,
        register: plugin.register,
      });
      entry.register(api as MinimumPluginApi);
    }, 'native');
  });

  it('loads the plugin through the installed minimum entry shape with every required typed hook', () => {
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
    const registerOnMinimumHost: (api: MinimumPluginApi) => void =
      plugin.register;
    const minimumEntry = defineMinimumPluginEntry({
      id: plugin.id,
      name: plugin.name,
      description: plugin.description,
      register: registerOnMinimumHost,
    });

    minimumEntry.register({
      pluginConfig: {},
      on(name: MinimumHookName) {
        hooks.push(name);
      },
      registerGatewayMethod(name) {
        gatewayMethods.push(name);
      },
      logger: { debug() {}, info() {}, warn() {}, error() {} },
      runtime: { version: '2026.5.5' },
    } as MinimumPluginApi);

    expect(hooks).toEqual(MINIMUM_REQUIRED_HOOKS);
    expect(gatewayMethods).toEqual(['semantic-layer.correlation.bind']);
  });

  it('accepts the published plugin configuration with the installed minimum schema validator', async () => {
    const manifest = JSON.parse(
      await readFile(join(packageRoot, 'openclaw.plugin.json'), 'utf8'),
    );
    const result = validateWithMinimumHost({
      schema: manifest.configSchema,
      cacheKey: 'semantic-layer-openclaw:qualified-minimum',
      value: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-key',
        identityKey: 'identity-key-with-at-least-thirty-two-characters',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'qualified-minimum',
      },
    });

    expect(result.ok).toBe(true);
  });
});
