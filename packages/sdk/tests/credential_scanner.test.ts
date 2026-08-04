import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp, readFile, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { CredentialScanner } from "../src/v1/secret-scanner.js";
import { createCapture, validateArtifact } from "../src/index.js";

type Corpus = {
  detector_spec: string;
  scrub_cases: Array<{
    id: string;
    secret_values: string[];
    input: Record<string, unknown>;
    expected: Record<string, unknown>;
    redactions: number;
  }>;
  scan_cases: Array<{
    id: string;
    secret_values: string[];
    text: string;
    clean: boolean;
  }>;
};

const fixtureHex = "0123456789abcdef";
const fixtureValues = new Map([
  ["{{OPENROUTER_KEY_64}}", ["sk", "or", "v1", fixtureHex.repeat(4)].join("-")],
  ["{{OPENROUTER_KEY_32}}", ["sk", "or", "v1", fixtureHex.repeat(2)].join("-")],
]);

function expandFixture(value: unknown): unknown {
  if (typeof value === "string") return fixtureValues.get(value) ?? value;
  if (Array.isArray(value)) return value.map(expandFixture);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [key, expandFixture(child)]),
    );
  }
  return value;
}

const corpus = expandFixture(
  JSON.parse(
    readFileSync(
      new URL(
        "../../../contracts/capture/v1/credential-safety-cases.json",
        import.meta.url,
      ),
      "utf8",
    ),
  ),
) as Corpus;

describe("credential safety conformance", () => {
  for (const fixture of corpus.scrub_cases) {
    it(`scrubs ${fixture.id}`, () => {
      const scanner = new CredentialScanner(fixture.secret_values);
      const result = scanner.scrub(fixture.input as never);

      expect(result).toEqual({
        value: fixture.expected,
        redactions: fixture.redactions,
      });
      expect(scanner.scan(Buffer.from(JSON.stringify(result.value)))).toBe(
        true,
      );
    });
  }

  for (const fixture of corpus.scan_cases) {
    it(`inspects ${fixture.id}`, () => {
      const scanner = new CredentialScanner(fixture.secret_values);
      expect(scanner.scan(Buffer.from(fixture.text))).toBe(fixture.clean);
    });
  }

  it("uses the language-neutral detector specification", () => {
    expect(new CredentialScanner().detectorDigest).toBe(
      createHash("sha256").update(corpus.detector_spec).digest("hex"),
    );
  });

  it("raw-scans invalid UTF-8 blobs for exact known secret bytes", () => {
    const scanner = new CredentialScanner(["fixture-secret-value"]);
    expect(
      scanner.scan(
        Buffer.concat([
          Buffer.from([0xff, 0xfe]),
          Buffer.from("fixture-secret-value"),
          Buffer.from([0xff]),
        ]),
      ),
    ).toBe(false);
    expect(scanner.scan(Buffer.from([0xff, 0xfe, 0xfd]))).toBe(true);
  });

  it("applies one policy to reasoning, tools, and blobs while keeping a safe trace", async () => {
    const output = await mkdtemp(join(tmpdir(), "semantic-credential-policy-"));
    const secret = "fixture-secret-ÿÿ";
    const percentEncoded = "fixture-secret-%C3%BF%C3%BF";
    const base64Encoded = "Zml4dHVyZS1zZWNyZXQtw7/Dvw==";
    const capture = createCapture({
      output,
      serviceName: "credential-policy-paths",
      secretValues: [secret],
    });

    await capture.observe(
      "credential-policy-run",
      { input: { reasoning: [{ type: "text", text: `consider ${secret}` }] } },
      async (scope) => {
        await scope.tool(
          "fixture-tool",
          { authorization: { scheme: "custom", value: secret } },
          async () => ({ summary: `result ${percentEncoded}` }),
        );
        const receipt = scope.emit("state.binary-evidence", {
          content: new Uint8Array(Buffer.from(base64Encoded)),
        });
        await receipt.settled;
        return "safe";
      },
    );

    const artifact = (await capture.shutdown()).artifactPath;
    const trace = await readFile(join(artifact, "trace.jsonl"), "utf8");
    expect(trace).not.toContain(secret);
    expect(trace).not.toContain(percentEncoded);
    expect(trace).not.toContain(base64Encoded);
    expect(trace).toContain(`consider [REDACTED_CREDENTIAL]`);
    expect(trace).toContain("credential_redaction");
    expect(trace).toContain("blob_scan_blocked");
    expect(
      existsSync(join(artifact, "blobs"))
        ? await readdir(join(artifact, "blobs"))
        : [],
    ).toEqual([]);
    await expect(
      validateArtifact(artifact, { secretValues: [secret] }),
    ).resolves.toMatchObject({ valid: true, secretMatches: 0 });
  });
});
