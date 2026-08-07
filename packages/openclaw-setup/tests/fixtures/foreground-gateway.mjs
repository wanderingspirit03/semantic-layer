import { readFileSync, watch } from 'node:fs';

const configPath = process.env.ACTIVE_CONFIG_PATH;
const setupPidPath = process.env.SETUP_PID_PATH;
if (!configPath || !setupPidPath) process.exit(2);

let settled = false;
const observer = watch(configPath, () => {
  if (settled) return;
  try {
    const config = JSON.parse(readFileSync(configPath, 'utf8'));
    const plugin = config.plugins?.entries?.['semantic-layer-openclaw'];
    const install = config.plugins?.installs?.['semantic-layer-openclaw'];
    if (!plugin || !install) return;
    const setupPid = Number(readFileSync(setupPidPath, 'utf8'));
    if (!Number.isInteger(setupPid)) process.exit(3);
    settled = true;
    process.kill(setupPid, 'SIGTERM');
    observer.close();
    process.exit(0);
  } catch {
    // The config rename and PID file write can briefly race. The next event retries.
  }
});

process.stdout.write('READY\n');
