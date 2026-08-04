import { readFile } from "node:fs/promises";

import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

const schemaUrl = new URL(
  "semantic-trace-manifest.schema.json",
  import.meta.url,
);
const v1SchemaUrl = new URL(
  "../v1/semantic-trace-manifest.schema.json",
  import.meta.url,
);
const exampleUrl = new URL("examples/managed/manifest.json", import.meta.url);
const [schema, v1Schema, managedManifest] = await Promise.all(
  [schemaUrl, v1SchemaUrl, exampleUrl].map(async (url) =>
    JSON.parse(await readFile(url, "utf8")),
  ),
);
const ajv = new Ajv2020({
  allErrors: true,
  strict: true,
  strictRequired: false,
});
addFormats(ajv);
const validateManifest = ajv.compile(schema);
const validateManifestV1 = ajv.compile(v1Schema);

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function errors() {
  return ajv.errorsText(validateManifest.errors, { separator: "\n" });
}

assert(
  validateManifest(managedManifest),
  `managed manifest v2 should validate\n${errors()}`,
);

const customManifest = structuredClone(managedManifest);
delete customManifest.installation_id;
assert(
  validateManifest(customManifest),
  `installation identity should be optional for unmanaged capture\n${errors()}`,
);

for (const status of [
  "exact_qualified",
  "capability_checked_unqualified",
  "unknown",
]) {
  const candidate = structuredClone(customManifest);
  candidate.sources[0].qualification = { status };
  assert(
    validateManifest(candidate),
    `${status} should be a valid qualification\n${errors()}`,
  );
}

for (const mutate of [
  (candidate) => {
    delete candidate.capture_policy;
  },
  (candidate) => {
    delete candidate.sources[0].qualification;
  },
  (candidate) => {
    candidate.sources[0].qualification.status = "claimed_qualified";
  },
  (candidate) => {
    candidate.record_schema = "semantic_trace_record_v2";
  },
  (candidate) => {
    candidate.installation_id = "host_customer-vm-1";
  },
  (candidate) => {
    delete candidate.sources[0].version;
  },
]) {
  const candidate = structuredClone(managedManifest);
  mutate(candidate);
  assert(
    !validateManifest(candidate),
    "invalid manifest v2 variant should be rejected",
  );
}

const legacyManifest = structuredClone(customManifest);
legacyManifest.schema = "semantic_trace_manifest_v1";
delete legacyManifest.capture_policy;
for (const source of legacyManifest.sources) delete source.qualification;
assert(
  validateManifestV1(legacyManifest),
  "legacy manifest should remain valid under v1",
);
assert(
  !validateManifest(legacyManifest),
  "manifest v1 must not validate as manifest v2",
);
assert(
  !validateManifestV1(managedManifest),
  "manifest v2 must not validate as manifest v1",
);

console.log("validated semantic trace manifest v2 contract");
