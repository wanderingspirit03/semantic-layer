import { createHash } from "node:crypto";
import type { JsonValue } from "./types.js";

const DETECTOR_SPEC = "semantic-layer-credential-detectors-v4";
const DEFAULT_REDACTION_SENTINEL = "[REDACTED_CREDENTIAL]";

const SENSITIVE_KEYS = new Set([
  "authorization",
  "proxyauthorization",
  "cookie",
  "setcookie",
  "apikey",
  "xapikey",
  "xgoogapikey",
  "accesstoken",
  "refreshtoken",
  "authtoken",
  "clientsecret",
  "password",
  "passwd",
  "privatekey",
  "connectionstring",
  "secretaccesskey",
]);
const SIGNED_QUERY_INLINE =
  /([?&](?:token|key|api_key|apikey|access_token|refresh_token|client_secret|signature|sig|x-amz-signature|x-amz-security-token|x-amz-credential|x-goog-signature|x-goog-credential|credential)=)([^&#\s"'\\]+)/gi;
const CONNECTION_CREDENTIAL =
  /\b((?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp):\/\/)([^@/?#\s"'\\]+)@([^/?#\s"'\\]+)/gi;
const BEARER_CREDENTIAL =
  /((?:authorization|proxy-authorization)\s*:\s*)(Bearer\s+)([A-Za-z0-9._~+/-]{8,}=*)/gim;
const PROVIDER_FORMATS = [
  /AIza[0-9A-Za-z_-]{20,}/g,
  /sk-or-v1-[0-9A-Fa-f]{64}(?![0-9A-Za-z_-])/g,
  /sk-proj-[0-9A-Za-z_-]{32,}/g,
  /sk-admin-[0-9A-Za-z_-]{32,}/g,
  /sk-ant-api\d{2}-[0-9A-Za-z_-]{32,}/g,
  /-----BEGIN ((?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY)-----[\s\S]*?-----END \1-----/g,
];
const STRICT_JSON_PARSE: (text: string) => unknown = JSON.parse.bind(JSON);

export class CredentialScanner {
  readonly detectorDigest = createHash("sha256")
    .update(DETECTOR_SPEC)
    .digest("hex");
  private readonly exact: string[];
  private readonly exactBytes: Buffer[];
  private readonly redactionSentinel: string;

  constructor(secretValues: readonly string[] = []) {
    if (secretValues.some((value) => value && Buffer.byteLength(value) < 8)) {
      throw new TypeError("secretValues entries must contain at least 8 bytes");
    }
    this.exact = [...new Set(secretValues.flatMap(encodings))]
      .filter(Boolean)
      .sort((left, right) => right.length - left.length);
    this.exactBytes = this.exact.map((value) => Buffer.from(value));
    this.redactionSentinel = collisionFreeRedactionSentinel(this.exact);
  }

  scrub(value: JsonValue): { value: JsonValue; redactions: number } {
    let redactions = 0;
    const visit = (input: JsonValue, key?: string): JsonValue => {
      if (key && sensitiveKey(key) && input !== null) {
        redactions += 1;
        return this.redactionSentinel;
      }
      if (typeof input === "string") {
        const scrubbed = this.scrubString(input);
        redactions += scrubbed.redactions;
        return scrubbed.value;
      }
      if (Array.isArray(input)) return input.map((item) => visit(item));
      if (input && typeof input === "object") {
        return Object.fromEntries(
          Object.entries(input).map(([name, child]) => [
            name,
            visit(child, name),
          ]),
        );
      }
      return input;
    };
    return { value: visit(value), redactions };
  }

  /** Return whether opaque bytes or JSON/JSONL records contain no known credential. */
  scan(bytes: Uint8Array): boolean {
    const raw = Buffer.from(bytes);
    if (this.exactBytes.some((secret) => raw.indexOf(secret) >= 0))
      return false;

    const text = raw.toString("utf8");
    const records = parseJsonOrJsonLines(text);
    if (records) return records.every((record) => this.structuredClean(record));
    return this.opaqueClean(text);
  }

  private scrubString(value: string): { value: string; redactions: number } {
    let result = value;
    let redactions = 0;
    for (const exact of this.exact) {
      if (!result.includes(exact)) continue;
      const count = result.split(exact).length - 1;
      redactions += count;
      result = result.split(exact).join(this.redactionSentinel);
    }
    for (const pattern of PROVIDER_FORMATS) {
      pattern.lastIndex = 0;
      result = result.replace(pattern, () => {
        redactions += 1;
        return this.redactionSentinel;
      });
    }
    BEARER_CREDENTIAL.lastIndex = 0;
    result = result.replace(
      BEARER_CREDENTIAL,
      (_match, prefix: string, scheme: string, credential: string) => {
        if (credential === this.redactionSentinel)
          return `${prefix}${scheme}${credential}`;
        redactions += 1;
        return `${prefix}${scheme}${this.redactionSentinel}`;
      },
    );
    CONNECTION_CREDENTIAL.lastIndex = 0;
    result = result.replace(
      CONNECTION_CREDENTIAL,
      (_match, scheme: string, userInfo: string, host: string) => {
        const separator = userInfo.indexOf(":");
        const credential =
          separator >= 0 ? userInfo.slice(separator + 1) : userInfo;
        if (decoded(credential) === this.redactionSentinel) {
          return `${scheme}${userInfo}@${host}`;
        }
        redactions += 1;
        const safeUserInfo =
          separator >= 0
            ? `${userInfo.slice(0, separator)}:${this.redactionSentinel}`
            : this.redactionSentinel;
        return `${scheme}${safeUserInfo}@${host}`;
      },
    );
    SIGNED_QUERY_INLINE.lastIndex = 0;
    result = result.replace(
      SIGNED_QUERY_INLINE,
      (_match, prefix: string, credential: string) => {
        if (decoded(credential) === this.redactionSentinel)
          return `${prefix}${credential}`;
        redactions += 1;
        return `${prefix}${this.redactionSentinel}`;
      },
    );
    return { value: result, redactions };
  }

  private structuredClean(value: unknown, key?: string): boolean {
    if (key && sensitiveKey(key) && value !== null)
      return value === this.redactionSentinel;
    if (typeof value === "string") return this.opaqueClean(value);
    if (Array.isArray(value))
      return value.every((item) => this.structuredClean(item));
    if (value && typeof value === "object") {
      return Object.entries(value).every(([name, child]) =>
        this.structuredClean(child, name),
      );
    }
    return true;
  }

  private opaqueClean(text: string): boolean {
    if (this.exact.some((secret) => text.includes(secret))) return false;
    if (
      PROVIDER_FORMATS.some((pattern) => {
        pattern.lastIndex = 0;
        return pattern.test(text);
      })
    )
      return false;

    BEARER_CREDENTIAL.lastIndex = 0;
    for (const match of text.matchAll(BEARER_CREDENTIAL)) {
      if (match[3] !== this.redactionSentinel) return false;
    }
    CONNECTION_CREDENTIAL.lastIndex = 0;
    for (const match of text.matchAll(CONNECTION_CREDENTIAL)) {
      const userInfo = match[2];
      const separator = userInfo.indexOf(":");
      const credential =
        separator >= 0 ? userInfo.slice(separator + 1) : userInfo;
      if (decoded(credential) !== this.redactionSentinel) return false;
    }
    SIGNED_QUERY_INLINE.lastIndex = 0;
    for (const match of text.matchAll(SIGNED_QUERY_INLINE)) {
      if (decoded(match[2]) !== this.redactionSentinel) return false;
    }
    return true;
  }
}

function sensitiveKey(value: string): boolean {
  return SENSITIVE_KEYS.has(
    value.toLowerCase().replaceAll("-", "").replaceAll("_", ""),
  );
}

function decoded(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseJsonOrJsonLines(text: string): unknown[] | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return [STRICT_JSON_PARSE(trimmed)];
  } catch {
    const lines = text.split(/\r?\n/).filter((line) => line.trim());
    if (lines.length < 2) return null;
    try {
      return lines.map((line) => STRICT_JSON_PARSE(line));
    } catch {
      return null;
    }
  }
}

function collisionFreeRedactionSentinel(
  exactSecrets: readonly string[],
): string {
  if (
    !exactSecrets.some((secret) => DEFAULT_REDACTION_SENTINEL.includes(secret))
  ) {
    return DEFAULT_REDACTION_SENTINEL;
  }
  for (let counter = 0; counter <= Number.MAX_SAFE_INTEGER; counter += 1) {
    const candidate = `[SL:${counter.toString(16).padStart(16, "0")}]`;
    if (!exactSecrets.some((secret) => candidate.includes(secret)))
      return candidate;
  }
  throw new TypeError(
    "unable to construct a collision-free redaction sentinel",
  );
}

function encodings(value: string): string[] {
  const bytes = Buffer.from(value);
  const jsonEncoded = JSON.stringify(value).slice(1, -1);
  const percentEncoded = encodeRfc3986(value);
  const base64 = bytes.toString("base64");
  const base64url = bytes.toString("base64url");
  return [
    value,
    jsonEncoded,
    jsonEncoded.replaceAll("/", "\\/"),
    jsonEncoded.replace(/[\u0080-\uFFFF]/g, jsonUnicodeEscape),
    percentEncoded,
    percentEncoded.replace(/%[0-9A-F]{2}/g, (triplet) => triplet.toLowerCase()),
    base64,
    base64.replace(/=+$/, ""),
    base64url,
    `${base64url}${"=".repeat((4 - (base64url.length % 4)) % 4)}`,
  ];
}

function jsonUnicodeEscape(character: string): string {
  return `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`;
}

function encodeRfc3986(value: string): string {
  return encodeURIComponent(value).replace(
    /[!'()*]/g,
    (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}
