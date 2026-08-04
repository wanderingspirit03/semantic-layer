# Semantic Trace Manifest v2

Manifest v2 adds installation and source-qualification metadata without
changing trace records. Its exact schema discriminator is
`semantic_trace_manifest_v2`; `record_schema` remains
`semantic_trace_record_v1`.

`capture_policy` is always `rich-credential-scrubbed`. Each source carries its
own qualification status because a bundle can combine sources with different
qualification evidence:

- `exact_qualified` means the exact observed version passed its qualification
  fixture;
- `capability_checked_unqualified` means bounded checks passed but is not a
  support claim; and
- `unknown` makes missing or unattached qualification explicit.

`installation_id` is optional in the portable schema. A producer or transport
policy may require an opaque stable random value. It must not encode hostname,
user, project, email, or hardware identity. Current SDK writers select manifest
v2 when an installation identity is configured.

Readers select the schema from the exact `schema` value. They accept only the
documented v1 and v2 discriminators and do not infer a version from fields.
