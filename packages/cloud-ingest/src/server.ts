import { createIngestServer } from './http.js';
import { parseKeyRegistry } from './registry.js';
import { GcsObjectStore } from './storage.js';

function required(name: string): string { const value = process.env[name]; if (!value) throw new Error(`${name} is required`); return value; }
const service = createIngestServer({
  store: new GcsObjectStore(required('SEMANTIC_LAYER_BUCKET')),
  meterStore: new GcsObjectStore(required('SEMANTIC_LAYER_METER_BUCKET')),
  keyRegistry: parseKeyRegistry(required('SEMANTIC_LAYER_KEY_REGISTRY_JSON')),
});
const port = Number(process.env.PORT ?? 8080);
service.server.listen(port, '0.0.0.0', () => console.log(JSON.stringify({ status: 'listening', port })));
