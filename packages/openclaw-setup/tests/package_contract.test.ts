import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), '..');

describe('published setup package contract', () => {
  it('is a standalone CLI artifact and never presents itself as an OpenClaw plugin', async () => {
    const packageJson = JSON.parse(
      await readFile(join(packageRoot, 'package.json'), 'utf8'),
    );

    expect(packageJson.name).toBe('semantic-layer-openclaw-setup');
    expect(packageJson.version).toBe('0.1.0-pilot.5');
    expect(packageJson.bin).toEqual({
      'semantic-layer-openclaw-setup': 'dist/bin.js',
    });
    expect(packageJson.openclaw).toBeUndefined();
    expect(packageJson.dependencies).toEqual({
      json5: '^2.2.3',
      'semantic-layer-cloud': '0.1.0-pilot.2',
    });
    expect(packageJson.engines.node).toBe(
      '>=22.14.0 <23 || >=24.0.0 <25 || >=25.9.0',
    );
  });
});
