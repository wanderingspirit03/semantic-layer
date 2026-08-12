import { resolve } from 'node:path';
import { defineConfig } from 'vitest/config';

const here = resolve(import.meta.dirname);

export default defineConfig({
  resolve: {
    alias: {
      'semantic-layer-capture': resolve(here, '../../packages/sdk/src/index.ts'),
      'semantic-layer-cloud': resolve(here, '../../packages/cloud/src/index.ts'),
    },
  },
  test: {
    include: [resolve(here, '*.test.ts')],
  },
});
