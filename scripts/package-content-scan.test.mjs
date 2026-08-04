import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, mkdir, rm, unlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { scanPackageContents } from './package-content-scan.mjs';

test('package scan accepts source but rejects credential content and trace artifacts', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'semantic-layer-package-scan-'));
  try {
    await writeFile(
      join(directory, 'index.js'),
      'export const examples = ["postgres://localhost/test", "?api_key=example"];\n',
    );
    await scanPackageContents(directory, 'fixture');

    await writeFile(join(directory, 'leak.txt'), 'Authorization: Bearer package-fixture-token\n');
    await assert.rejects(
      scanPackageContents(directory, 'fixture'),
      /credential-like content in leak\.txt/,
    );
    await unlink(join(directory, 'leak.txt'));

    await mkdir(join(directory, '.semantic-layer'));
    await writeFile(join(directory, '.semantic-layer', 'trace.jsonl'), '{}\n');
    await assert.rejects(
      scanPackageContents(directory, 'fixture'),
      /unsafe packaged files: \.semantic-layer\/trace\.jsonl/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
