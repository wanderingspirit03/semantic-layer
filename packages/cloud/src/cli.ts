#!/usr/bin/env node

import { readdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import {
  createCloudUploader,
  type CloudUploader,
  type CloudUploaderOptions,
} from './index.js';

type Parsed = {
  command: 'upload' | 'watch' | 'status' | 'doctor';
  positional?: string;
  endpoint?: string;
  spoolDirectory?: string;
  deadlineMs: number;
};

void main().catch((error: unknown) => {
  const code =
    typeof error === 'object' && error !== null && 'code' in error
      ? String(error.code)
      : 'CLI_ERROR';
  const message = error instanceof Error ? error.message : 'unexpected error';
  process.stderr.write(`${code}: ${message}\n`);
  process.exitCode = 1;
});

async function main(): Promise<void> {
  const parsed = parseArguments(process.argv.slice(2));
  const options: CloudUploaderOptions = {
    endpoint:
      parsed.endpoint ??
      process.env.SEMANTIC_LAYER_ENDPOINT ??
      'http://127.0.0.1',
    ...(parsed.spoolDirectory ? { spoolDirectory: parsed.spoolDirectory } : {}),
  };
  if (parsed.command === 'doctor') {
    await doctor(options);
    return;
  }
  const uploader = createCloudUploader(options);
  try {
    if (parsed.command === 'upload') {
      const receipt = await uploader.enqueueArtifact(
        parsed.positional as string,
      );
      const result = await uploader.flush({ deadlineMs: parsed.deadlineMs });
      printJson({ receipt, status: result });
      if (
        result.pendingBundles > 0 ||
        result.awaitingSpoolAdmissionBundles > 0 ||
        result.blockedBundles > 0 ||
        result.quarantineBundles > 0
      )
        process.exitCode = 1;
    } else if (parsed.command === 'watch') {
      await watch(uploader, parsed.positional as string);
    } else {
      await uploader.flush({ deadlineMs: 0 });
      printJson(uploader.status());
    }
  } finally {
    await uploader.shutdown();
  }
}

function parseArguments(arguments_: string[]): Parsed {
  const command = arguments_.shift();
  if (!command || !['upload', 'watch', 'status', 'doctor'].includes(command))
    usage();
  let endpoint: string | undefined;
  let spoolDirectory: string | undefined;
  let deadlineMs = 5 * 60 * 1_000;
  let positional: string | undefined;
  while (arguments_.length > 0) {
    const argument = arguments_.shift() as string;
    if (argument === '--key' || argument.startsWith('--key=')) {
      throw cliError(
        'CLI_KEY_FORBIDDEN',
        '--key is not supported; use SEMANTIC_LAYER_INGEST_KEY',
      );
    }
    if (argument === '--endpoint')
      endpoint = requireValue(argument, arguments_.shift());
    else if (argument.startsWith('--endpoint='))
      endpoint = requireValue('--endpoint', argument.slice(11));
    else if (argument === '--spool-directory')
      spoolDirectory = requireValue(argument, arguments_.shift());
    else if (argument.startsWith('--spool-directory='))
      spoolDirectory = requireValue('--spool-directory', argument.slice(18));
    else if (argument === '--deadline-ms')
      deadlineMs = parseDeadline(requireValue(argument, arguments_.shift()));
    else if (argument.startsWith('--deadline-ms='))
      deadlineMs = parseDeadline(argument.slice(14));
    else if (argument.startsWith('-'))
      throw cliError('CLI_ARGUMENT_INVALID', `unknown option ${argument}`);
    else if (positional)
      throw cliError('CLI_ARGUMENT_INVALID', 'too many positional arguments');
    else positional = argument;
  }
  if ((command === 'upload' || command === 'watch') && !positional) usage();
  if ((command === 'status' || command === 'doctor') && positional) usage();
  return {
    command: command as Parsed['command'],
    positional,
    endpoint,
    spoolDirectory,
    deadlineMs,
  };
}

async function watch(
  uploader: CloudUploader,
  outputDirectory: string,
): Promise<void> {
  const root = resolve(outputDirectory);
  const observed = new Set<string>();
  let stopping = false;
  const stop = (): void => {
    stopping = true;
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  try {
    while (!stopping) {
      const entries = await readdir(root, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory() || observed.has(entry.name)) continue;
        try {
          await uploader.enqueueArtifact(resolve(root, entry.name));
          observed.add(entry.name);
        } catch (error) {
          const code =
            typeof error === 'object' && error !== null && 'code' in error
              ? String(error.code)
              : 'ENQUEUE_FAILED';
          process.stderr.write(`${entry.name}: ${code}\n`);
        }
      }
      await uploader.flush({ deadlineMs: 1_000 });
      await wait(1_000);
    }
  } finally {
    process.removeListener('SIGINT', stop);
    process.removeListener('SIGTERM', stop);
  }
}

async function doctor(options: CloudUploaderOptions): Promise<void> {
  const checks: Record<string, { ok: boolean; detail?: string }> = {
    ingestKey: {
      ok: Boolean(process.env.SEMANTIC_LAYER_INGEST_KEY),
      detail: 'set SEMANTIC_LAYER_INGEST_KEY',
    },
    endpoint: { ok: false },
    spool: { ok: false },
  };
  const uploader = createCloudUploader(options);
  try {
    await uploader.flush({ deadlineMs: 0 });
    checks.spool = { ok: true };
    try {
      const response = await fetch(
        `${String(options.endpoint).replace(/\/+$/, '')}/health`,
      );
      checks.endpoint = { ok: response.ok, detail: `HTTP ${response.status}` };
    } catch {
      checks.endpoint = { ok: false, detail: 'unreachable' };
    }
  } finally {
    await uploader.shutdown();
  }
  printJson({ ok: Object.values(checks).every((check) => check.ok), checks });
  if (!Object.values(checks).every((check) => check.ok)) process.exitCode = 1;
}

function requireValue(option: string, value: string | undefined): string {
  if (!value)
    throw cliError('CLI_ARGUMENT_INVALID', `${option} requires a value`);
  return value;
}

function parseDeadline(value: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw cliError(
      'CLI_ARGUMENT_INVALID',
      '--deadline-ms must be a non-negative integer',
    );
  }
  return number;
}

function usage(): never {
  throw cliError(
    'CLI_USAGE',
    'usage: semantic-layer-cloud <upload PATH|watch DIRECTORY|status|doctor> [--endpoint URL] [--spool-directory PATH]',
  );
}

function cliError(code: string, message: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

function printJson(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolveWait) => setTimeout(resolveWait, milliseconds));
}
