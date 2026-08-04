import { createHash } from 'node:crypto';
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { compile } from 'json-schema-to-typescript';

const root = new URL('../../../contracts/capture/v1/', import.meta.url);
const traceV1Root = new URL('../../../contracts/trace/v1/', import.meta.url);
const traceV2Root = new URL('../../../contracts/trace/v2/', import.meta.url);
const eventUrl = new URL('semantic-capture-event.schema.json', root);
const traceManifestV1Url = new URL('semantic-trace-manifest.schema.json', traceV1Root);
const traceManifestV2Url = new URL('semantic-trace-manifest.schema.json', traceV2Root);
const traceRecordUrl = new URL('semantic-trace-record.schema.json', traceV1Root);
const outputUrl = new URL('../src/v1/generated.ts', import.meta.url);
const schemaDir = new URL('../schemas/', import.meta.url);
const packagedEventUrl = new URL('semantic-capture-event.schema.json', schemaDir);
const packagedTraceManifestUrl = new URL('semantic-trace-manifest.schema.json', schemaDir);
const packagedTraceManifestV2Url = new URL('semantic-trace-manifest-v2.schema.json', schemaDir);
const packagedTraceRecordUrl = new URL('semantic-trace-record.schema.json', schemaDir);
const eventText = await readFile(eventUrl, 'utf8');
const [traceManifestText, traceManifestV2Text, traceRecordText] = await Promise.all([
  readFile(traceManifestV1Url, 'utf8'),
  readFile(traceManifestV2Url, 'utf8'),
  readFile(traceRecordUrl, 'utf8'),
]);
const eventSchema = JSON.parse(eventText);
await validateExamples(eventSchema);
const digest = createHash('sha256').update(eventText).digest('hex');
const options = {
  bannerComment: `/* eslint-disable */\n/** Generated from semantic_capture_event_v1. Schema digest: ${digest}. */`,
  declareExternallyReferenced: true,
  enableConstEnums: false,
  format: true,
  style: { singleQuote: true, semi: true, trailingComma: 'all', printWidth: 120 },
  unreachableDefinitions: true,
};
// json-schema-to-typescript collapses conditional allOf branches into the base
// object. The authoritative packaged schema retains those runtime conditions;
// generated static types use the unconditional shape they refine.
const eventTypeSchema = structuredClone(eventSchema);
delete eventTypeSchema.properties.source.allOf;
const eventTypes = await compile(eventTypeSchema, 'SemanticCaptureEventV1', options);
const expected = `${eventTypes.trim()}\n`;
if (process.argv.includes('--write')) {
  await mkdir(schemaDir, { recursive: true });
  await Promise.all([
    writeFile(outputUrl, expected),
    writeFile(packagedEventUrl, eventText),
    writeFile(packagedTraceManifestUrl, traceManifestText),
    writeFile(packagedTraceManifestV2Url, traceManifestV2Text),
    writeFile(packagedTraceRecordUrl, traceRecordText),
  ]);
} else {
  const [
    generated,
    packagedEvent,
    packagedTraceManifest,
    packagedTraceManifestV2,
    packagedTraceRecord,
  ] = await Promise.all([
    readFile(outputUrl, 'utf8'),
    readFile(packagedEventUrl, 'utf8'),
    readFile(packagedTraceManifestUrl, 'utf8'),
    readFile(packagedTraceManifestV2Url, 'utf8'),
    readFile(packagedTraceRecordUrl, 'utf8'),
  ]);
  if (
    generated !== expected
    || packagedEvent !== eventText
    || packagedTraceManifest !== traceManifestText
    || packagedTraceManifestV2 !== traceManifestV2Text
    || packagedTraceRecord !== traceRecordText
  ) {
    throw new Error('generated TypeScript capture event model or packaged semantic schemas are stale');
  }
}

async function validateExamples(eventSchema) {
  const examplesUrl = new URL('examples/', root);
  const names = (await readdir(examplesUrl)).filter((name) => name.endsWith('.json')).sort();
  const required = [
    'isolated-ambiguity-marker-event.json',
    'isolated-ambiguous-event.json',
    'isolated-blob-event.json',
    'isolated-error-event.json',
    'isolated-loss-event.json',
    'isolated-multi-turn-event.json',
    'isolated-owned-event.json',
    'isolated-otel-event.json',
    'isolated-stream-event.json',
    'isolated-success-event.json',
    'isolated-tool-event.json',
    'isolated-unknown-event.json',
  ];
  const missing = required.filter((name) => !names.includes(name));
  if (missing.length) throw new Error(`capture v1 schema examples missing: ${missing.join(', ')}`);

  const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
  addFormats(ajv);
  const validateEvent = ajv.compile(eventSchema);
  for (const name of names) {
    const value = JSON.parse(await readFile(new URL(name, examplesUrl), 'utf8'));
    if (!validateEvent(value)) {
      throw new Error(`invalid capture v1 schema example ${name}: ${ajv.errorsText(validateEvent.errors)}`);
    }
  }
}
