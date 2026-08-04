import { readFile, readdir } from 'node:fs/promises';
import { join, relative } from 'node:path';

const credentialPatterns = [
  /AIza[0-9A-Za-z_-]{20,}/,
  /sk-(?:or-v1-)?[0-9A-Za-z_-]{12,}/,
  /Bearer\s+[A-Za-z0-9._~+/-]{8,}/i,
  /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,
  /(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp):\/\/[^/@\s"']+:[^@\s"']+@[^\s"']+/i,
  /[?&](?:token|key|api_key|apikey|access_token|signature|sig|x-amz-signature|x-goog-signature|credential)=[A-Za-z0-9%._~+/-]{16,}/i,
];
const unsafePackagePath = /(^|\/)(?:\.env(?:\.[^/]*)?|id_(?:rsa|ed25519)|trace\.jsonl|[^/]+\.(?:pem|key|p12|pfx))$/i;
const unsafePackageDirectory = /(^|\/)(?:\.semantic-layer|blobs)(?:\/|$)/i;

export async function scanPackageContents(directory, label) {
  const files = await regularFiles(directory);
  const unsafe = files
    .map((file) => relative(directory, file))
    .filter((name) => unsafePackagePath.test(name) || unsafePackageDirectory.test(name));
  if (unsafe.length > 0) {
    throw new Error(`${label} contains unsafe packaged files: ${unsafe.join(', ')}`);
  }
  for (const file of files) {
    const text = (await readFile(file)).toString('utf8');
    if (credentialPatterns.some((pattern) => pattern.test(text))) {
      throw new Error(`${label} contains credential-like content in ${relative(directory, file)}`);
    }
  }
}

async function regularFiles(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await regularFiles(path));
    else if (entry.isFile()) files.push(path);
    else throw new Error(`package contains unsupported file type: ${path}`);
  }
  return files;
}
