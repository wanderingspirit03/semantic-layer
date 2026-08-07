import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { validateJsonSchemaValue } from 'openclaw/plugin-sdk/config-schema';

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), '..');

describe('published plugin contract', () => {
  it('pins both qualified hosts and both runtime dependencies', async () => {
    const packageJson = JSON.parse(
      await readFile(join(packageRoot, 'package.json'), 'utf8'),
    );

    expect(packageJson.name).toBe('semantic-layer-openclaw');
    expect(packageJson.version).toBe('0.1.0-pilot.4');
    expect(packageJson.bin).toBeUndefined();
    expect(packageJson.dependencies).toEqual({
      'semantic-layer-capture': '0.2.0-beta.1',
      'semantic-layer-cloud': '0.1.0-pilot.3',
    });
    expect(packageJson.peerDependencies.openclaw).toBe(
      '>=2026.5.5 || 2026.7.1-1 || 2026.7.1-2',
    );
    expect(packageJson.devDependencies.openclaw).toBe('2026.5.5');
    expect(packageJson.devDependencies['openclaw-qualified-current']).toBe(
      'npm:openclaw@2026.7.1-2',
    );
    expect(packageJson.engines.node).toBe(
      '>=22.14.0 <23 || >=24.0.0 <25 || >=25.9.0',
    );
    expect(packageJson.openclaw).toMatchObject({
      extensions: ['./dist/index.js'],
      runtimeExtensions: ['./dist/index.js'],
      compat: {
        pluginApi: '>=2026.5.5',
        minGatewayVersion: '2026.5.5',
      },
      build: {
        openclawVersion: '2026.5.5',
        pluginSdkVersion: '2026.5.5',
        qualifiedHostVersions: ['2026.5.5', '2026.7.1-2'],
      },
    });
  });

  it('declares strict config, resolved secret inputs, and no agent-facing tools', async () => {
    const manifest = JSON.parse(
      await readFile(join(packageRoot, 'openclaw.plugin.json'), 'utf8'),
    );

    expect(manifest.id).toBe('semantic-layer-openclaw');
    expect(manifest.configSchema).toMatchObject({
      type: 'object',
      additionalProperties: false,
      oneOf: [
        { maxProperties: 0 },
        {
          required: [
            'endpoint',
            'ingestKey',
            'identityKey',
            'installationId',
            'serviceName',
          ],
        },
      ],
    });
    expect(manifest.configContracts.secretInputs.paths).toEqual([
      { path: 'ingestKey', expected: 'string' },
      { path: 'identityKey', expected: 'string' },
    ]);
    expect(manifest.secretInputs).toBeUndefined();
    expect(manifest.contracts?.tools).toBeUndefined();
    expect(manifest.configSchema.properties.richCapture).toBeUndefined();
    expect(manifest.uiHints.richCapture).toBeUndefined();
  });

  it('accepts only the native empty install state or a complete configuration', async () => {
    const manifest = JSON.parse(
      await readFile(join(packageRoot, 'openclaw.plugin.json'), 'utf8'),
    );
    const validate = (value: unknown) =>
      validateJsonSchemaValue({
        schema: manifest.configSchema,
        cacheKey: `semantic-layer-openclaw-test:${JSON.stringify(value)}`,
        value,
      });

    expect(validate({}).ok).toBe(true);
    expect(validate({ endpoint: 'https://ingest.example.test' }).ok).toBe(
      false,
    );
    expect(
      validate({
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-key',
        identityKey: 'identity-key-with-at-least-thirty-two-characters',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'customer-openclaw',
      }).ok,
    ).toBe(true);
    expect(
      validate({
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-key',
        identityKey: 'identity-key-with-at-least-thirty-two-characters',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'customer-openclaw',
        richCapture: true,
      }).ok,
    ).toBe(false);
    expect(
      validate({
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-key',
        identityKey: 'identity-key-with-at-least-thirty-two-characters',
        installationId: 'host-derived-name',
        serviceName: 'customer-openclaw',
      }).ok,
    ).toBe(false);
  });
});
