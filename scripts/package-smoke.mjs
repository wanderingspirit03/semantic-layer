import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { access, lstat, mkdtemp, mkdir, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { scanPackageContents } from './package-content-scan.mjs';

const root = resolve(import.meta.dirname, '..');
const work = await mkdtemp(join(tmpdir(), 'semantic-layer-package-smoke-'));
const expectedLicense = await readFile(join(root, 'LICENSE'), 'utf8');

function runResult(command, args, cwd, options = {}) {
  return spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout: 300_000,
    ...options,
  });
}

function run(command, args, cwd, options) {
  const result = runResult(command, args, cwd, options);
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(' ')} failed\n${result.stdout}${result.stderr}`,
    );
  }
  return result.stdout;
}

async function packPublicPackage({ name, directory, version, bin }, packDir, unpackRoot) {
  const packageRoot = join(root, directory);
  const sourceManifest = JSON.parse(
    await readFile(join(packageRoot, 'package.json'), 'utf8'),
  );
  const sourceManifestText = JSON.stringify(sourceManifest);
  assert.equal(
    typeof sourceManifest.scripts?.prepack,
    'string',
    `${name} must build itself before publish`,
  );
  assert.doesNotMatch(
    sourceManifestText,
    /"workspace:/u,
    `${name} source manifest contains a dependency npm publish cannot resolve`,
  );
  const before = new Set(await readdir(packDir));
  run('pnpm', ['pack', '--pack-destination', packDir], packageRoot);
  const produced = (await readdir(packDir))
    .filter((file) => file.endsWith('.tgz') && !before.has(file));
  assert.equal(produced.length, 1, `${name} pack must produce exactly one tarball`);

  const tarball = join(packDir, produced[0]);
  const unpackDir = join(unpackRoot, name);
  await mkdir(unpackDir);
  run('tar', ['-xzf', tarball, '-C', unpackDir], root);
  await scanPackageContents(unpackDir, `${name} tarball`);

  const packedRoot = join(unpackDir, 'package');
  const manifestText = await readFile(join(packedRoot, 'package.json'), 'utf8');
  const manifest = JSON.parse(manifestText);
  assert.equal(manifest.name, name);
  assert.equal(manifest.version, version);
  assert.equal(manifest.private, undefined, `${name} must be public`);
  assert.equal(manifest.license, 'MIT', `${name} must declare the MIT license`);
  assert.equal(
    await readFile(join(packedRoot, 'LICENSE'), 'utf8'),
    expectedLicense,
    `${name} tarball must contain the repository MIT notice`,
  );
  assert.equal(manifest.scripts?.preinstall, undefined, `${name} must not use preinstall`);
  assert.equal(manifest.scripts?.install, undefined, `${name} must not use install scripts`);
  assert.equal(manifest.scripts?.postinstall, undefined, `${name} must not use postinstall`);
  assert.doesNotMatch(manifestText, /"workspace:/u, `${name} contains an unresolved workspace dependency`);

  if (name === 'semantic-layer-capture') {
    assert.equal(
      manifest.exports?.['./conformance'],
      undefined,
      'repository conformance fixtures must not be published as a package interface',
    );
  }
  if (name === 'semantic-layer-openclaw') {
    assert.equal(manifest.bin, undefined, 'runtime plugin must not publish a setup executable');
    await assert.rejects(
      access(join(packedRoot, 'dist', 'bin.js')),
      undefined,
      'runtime plugin tarball must not contain setup orchestration',
    );
  }
  if (bin) {
    const expectedBin = bin === 'semantic-layer-cloud'
      ? 'dist/cli.js'
      : 'dist/bin.js';
    assert.equal(manifest.bin?.[bin], expectedBin);
    const binPath = join(packedRoot, manifest.bin[bin]);
    const mode = (await lstat(binPath)).mode & 0o777;
    assert.notEqual(mode & 0o111, 0, `${name} bin must be executable`);
    assert.match(await readFile(binPath, 'utf8'), /^#!\/usr\/bin\/env node/u, `${name} bin needs a node shebang`);
  }
  return { manifest, tarball };
}

try {
  const packDir = join(work, 'typescript-packages');
  const unpackDir = join(work, 'typescript-unpacked');
  const installDir = join(work, 'typescript-install');
  await mkdir(packDir);
  await mkdir(unpackDir);
  await mkdir(installDir);
  const ingestManifest = JSON.parse(
    await readFile(join(root, 'packages/cloud-ingest/package.json'), 'utf8'),
  );
  assert.equal(ingestManifest.private, true, 'cloud ingest service must never be npm-published');
  const publicPackageSpecs = [
    {
      name: 'semantic-layer-capture',
      directory: 'packages/sdk',
      version: '0.2.0-beta.1',
    },
    {
      name: 'semantic-layer-cloud',
      directory: 'packages/cloud',
      version: '0.1.0-pilot.3',
      bin: 'semantic-layer-cloud',
    },
    {
      name: 'semantic-layer-openclaw',
      directory: 'packages/openclaw',
      version: '0.1.0-pilot.4',
    },
    {
      name: 'semantic-layer-openclaw-setup',
      directory: 'packages/openclaw-setup',
      version: '0.1.0-pilot.8',
      bin: 'semantic-layer-openclaw-setup',
    },
  ];
  const publicPackages = [];
  for (const spec of publicPackageSpecs) {
    publicPackages.push(await packPublicPackage(spec, packDir, unpackDir));
  }
  run('npm', [
    'install',
    ...publicPackages.map(({ tarball }) => tarball),
    '--prefix',
    installDir,
    '--ignore-scripts',
    '--omit=optional',
    '--omit=peer',
    '--legacy-peer-deps',
    '--no-package-lock',
    '--silent',
  ], root);
  run('node', [
    '--input-type=module',
    '-e',
    [
      'import { initialize, validateArtifact } from "semantic-layer-capture";',
      'import { createCloudUploader } from "semantic-layer-cloud";',
      'await import("semantic-layer-openclaw");',
      'import { readdir } from "node:fs/promises";',
      'if (typeof createCloudUploader !== "function") throw new Error("cloud uploader export is missing");',
      'const capture = initialize({ output: "./traces", serviceName: "tarball-smoke" });',
      'const value = await capture.observe("smoke", {}, (scope) => scope.tool(',
      '  "identity", { value: "ok" }, async (input) => input.value,',
      '));',
      'const closed = await capture.shutdown();',
      'if (value !== "ok") throw new Error("capture changed the application result");',
      'const files = (await readdir(closed.artifactPath)).sort();',
      'if (files.join(",") !== "manifest.json,trace.jsonl") throw new Error(`unexpected bundle: ${files}`);',
      'const report = await validateArtifact(closed.artifactPath);',
      'if (!report.valid) throw new Error(report.issues.join(","));',
    ].join('\n'),
  ], installDir);

  const cliState = join(work, 'cli-state');
  const spoolDirectory = join(cliState, 'cloud-spool');
  await mkdir(cliState);
  const cliEnvironment = {
    ...process.env,
    OPENCLAW_CONFIG_PATH: join(cliState, 'openclaw.json'),
    XDG_CONFIG_HOME: join(cliState, 'xdg-config'),
    XDG_DATA_HOME: join(cliState, 'xdg-data'),
    XDG_STATE_HOME: join(cliState, 'xdg-state'),
    SEMANTIC_LAYER_INGEST_KEY: 'package-smoke-ingest-secret',
  };
  const cloudBin = join(installDir, 'node_modules/.bin/semantic-layer-cloud');
  const statusOutput = run(cloudBin, [
    'status',
    '--spool-directory',
    spoolDirectory,
    '--endpoint',
    'http://127.0.0.1:1',
  ], installDir, { env: cliEnvironment });
  assert.equal(JSON.parse(statusOutput).pendingBundles, 0);
  assert.doesNotMatch(statusOutput, /package-smoke-ingest-secret/u);

  const doctor = runResult(cloudBin, [
    'doctor',
    '--spool-directory',
    spoolDirectory,
    '--endpoint',
    'http://127.0.0.1:1',
  ], installDir, { env: cliEnvironment });
  assert.notEqual(doctor.status, 0, 'offline doctor must report the unavailable endpoint');
  assert.equal(JSON.parse(doctor.stdout).ok, false);
  assert.doesNotMatch(`${doctor.stdout}${doctor.stderr}`, /package-smoke-ingest-secret/u);

  const beforeOpenClawHelp = (await readdir(cliState, { recursive: true })).sort();
  const openClawBin = join(installDir, 'node_modules/.bin/semantic-layer-openclaw-setup');
  const openClawHelp = run(openClawBin, ['--help'], installDir, { env: cliEnvironment });
  assert.match(openClawHelp, /^semantic-layer-openclaw-setup$/mu);
  assert.deepEqual(
    (await readdir(cliState, { recursive: true })).sort(),
    beforeOpenClawHelp,
    'OpenClaw help must not modify client configuration',
  );

  const pythonDist = join(work, 'python-package');
  await mkdir(pythonDist);
  run('uv', ['build', '--wheel', '--sdist', '--out-dir', pythonDist, join(root, 'packages/python')], root);
  const wheels = (await readdir(pythonDist)).filter((name) => name.endsWith('.whl'));
  if (wheels.length !== 1) {
    throw new Error(`Python package smoke expected one wheel, found ${wheels.length}`);
  }
  const [wheel] = wheels;
  const wheelDir = join(work, 'python-wheel');
  await mkdir(wheelDir);
  run('unzip', ['-q', join(pythonDist, wheel), '-d', wheelDir], root);
  await scanPackageContents(wheelDir, 'Python wheel');
  const distInfo = (await readdir(wheelDir)).find((name) => name.endsWith('.dist-info'));
  if (!distInfo) throw new Error('Python wheel does not contain a .dist-info directory');
  const metadata = await readFile(join(wheelDir, distInfo, 'METADATA'), 'utf8');
  assert.match(metadata, /^License: MIT License$/m, 'Python wheel must declare the MIT license');
  assert.match(metadata, /^License-File: LICENSE$/m, 'Python wheel must identify its license file');
  assert.equal(
    await readFile(join(wheelDir, distInfo, 'licenses', 'LICENSE'), 'utf8'),
    expectedLicense,
    'Python wheel must contain the repository MIT notice',
  );

  const sdists = (await readdir(pythonDist)).filter((name) => name.endsWith('.tar.gz'));
  if (sdists.length !== 1) {
    throw new Error(`Python package smoke expected one source distribution, found ${sdists.length}`);
  }
  const [sdist] = sdists;
  const sdistDir = join(work, 'python-sdist');
  await mkdir(sdistDir);
  run('tar', ['-xzf', join(pythonDist, sdist), '-C', sdistDir], root);
  await scanPackageContents(sdistDir, 'Python source distribution');
  const sdistRoots = await readdir(sdistDir);
  if (sdistRoots.length !== 1) throw new Error('Python source distribution must have one root');
  const sdistRoot = join(sdistDir, sdistRoots[0]);
  for (const excluded of ['uv.lock', 'tests', 'scripts']) {
    await assert.rejects(
      access(join(sdistRoot, excluded)),
      undefined,
      `Python source distribution must exclude ${excluded}`,
    );
  }
  const pythonOutput = join(work, 'python-output');
  run('uv', [
    'run',
    '--isolated',
    '--with',
    join(pythonDist, wheel),
    'python',
    '-c',
    [
      'import sys',
      'from pathlib import Path',
      'from semantic_layer import initialize',
      'from semantic_layer.validation import validate_artifact',
      'capture = initialize(output=sys.argv[1], service_name="wheel-smoke")',
      'with capture.observe("smoke") as scope:',
      '    value = scope.tool("identity", {"value": "ok"}, lambda item: item["value"])',
      'closed = capture.shutdown()',
      'artifact = Path(closed.artifact_path)',
      'assert value == "ok"',
      'assert sorted(path.name for path in artifact.iterdir()) == ["manifest.json", "trace.jsonl"]',
      'assert validate_artifact(artifact).valid',
    ].join('\n'),
    pythonOutput,
  ], root);

  const pythonSdistOutput = join(work, 'python-sdist-output');
  run('uv', [
    'run',
    '--isolated',
    '--with',
    join(pythonDist, sdist),
    'python',
    '-c',
    [
      'import sys',
      'from pathlib import Path',
      'from semantic_layer import initialize',
      'from semantic_layer.validation import validate_artifact',
      'capture = initialize(output=sys.argv[1], service_name="sdist-smoke")',
      'with capture.observe("smoke") as scope:',
      '    value = scope.tool("identity", {"value": "ok"}, lambda item: item["value"])',
      'closed = capture.shutdown()',
      'artifact = Path(closed.artifact_path)',
      'assert value == "ok"',
      'assert sorted(path.name for path in artifact.iterdir()) == ["manifest.json", "trace.jsonl"]',
      'assert validate_artifact(artifact).valid',
    ].join('\n'),
    pythonSdistOutput,
  ], root);

  process.stdout.write(
    'Public TypeScript tarballs and Python wheel/source package scans and clean installs passed\n',
  );
} finally {
  await rm(work, { recursive: true, force: true });
}
