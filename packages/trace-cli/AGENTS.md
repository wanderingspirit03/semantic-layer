# Trace CLI agent guide

This package is a public, read only operator tool for completed Semantic Layer
bundles in Google Cloud Storage.

- Keep every cloud operation read only. Do not add upload, overwrite, or delete
  commands.
- Treat `complete.json` as the visibility boundary. Check its exact scope and
  digest before using a bundle.
- Download into a private temporary sibling, validate it, then publish it with
  one atomic rename.
- Keep prompts, reasoning, model output, tool input, and tool output out of
  noninteractive and JSON output unless the caller explicitly asks for
  content. Interactive `show` includes content by default and must warn first.
- Never place config files, state files, or summaries inside downloaded bundles.
- Keep `--help`, `README.md`, tests, and behavior aligned.
- Run `pnpm --filter semantic-layer-traces test`, `typecheck`, and `build`, then
  run `pnpm test:packages` for packaging changes.

To use the CLI without prior context, run `semantic-layer-traces --help` and
follow the numbered workflow it prints.
