export const MINIMUM_HOST_VERSION = '2026.5.5' as const;
export const CURRENT_QUALIFIED_HOST_VERSION = '2026.7.1-2' as const;
export const QUALIFIED_HOST_VERSIONS = [
  MINIMUM_HOST_VERSION,
  CURRENT_QUALIFIED_HOST_VERSION,
] as const;
const CURRENT_NODE_FLOOR_HOST_VERSION = '2026.7.1';
const STABLE_HOST_VERSION = /^\d{4}\.\d+\.\d+(?:-\d+)?$/u;

type NodeRequirements = {
  node22Floor: string;
  node24Floor: string;
};

export type CompatibilityResult = {
  ok: boolean;
  qualification:
    'exact_qualified' | 'capability_checked_unqualified' | 'unknown';
  errors: string[];
  warnings: string[];
};

export function checkCompatibility(
  hostVersion: string,
  nodeVersion: string,
): CompatibilityResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  const stableHost = STABLE_HOST_VERSION.test(hostVersion);
  if (!stableHost) {
    errors.push(
      `OpenClaw ${hostVersion} is not a supported stable release; use ${MINIMUM_HOST_VERSION} or a newer stable release.`,
    );
  } else if (compareVersions(hostVersion, MINIMUM_HOST_VERSION) < 0) {
    errors.push(
      `OpenClaw ${hostVersion} is unsupported; install ${MINIMUM_HOST_VERSION} or newer.`,
    );
  } else if (
    !QUALIFIED_HOST_VERSIONS.includes(
      hostVersion as (typeof QUALIFIED_HOST_VERSIONS)[number],
    )
  ) {
    warnings.push(
      `OpenClaw ${hostVersion} is in-range but unqualified; the exact qualified builds are ${QUALIFIED_HOST_VERSIONS.join(', ')}. Complete doctor successfully before describing this host as capability-checked.`,
    );
  }

  if (stableHost) {
    const requirements = nodeRequirements(hostVersion);
    if (!nodeSafe(nodeVersion, requirements)) {
      errors.push(
        `Node ${nodeVersion} is unsupported by OpenClaw ${hostVersion}; use Node >=${requirements.node22Floor} <23, >=${requirements.node24Floor} <25, or >=25.9.0.`,
      );
    }
  }
  const qualification = QUALIFIED_HOST_VERSIONS.includes(
    hostVersion as (typeof QUALIFIED_HOST_VERSIONS)[number],
  )
    ? 'exact_qualified'
    : 'unknown';
  return { ok: errors.length === 0, qualification, errors, warnings };
}

function nodeRequirements(hostVersion: string): NodeRequirements {
  if (compareVersions(hostVersion, CURRENT_NODE_FLOOR_HOST_VERSION) >= 0) {
    return { node22Floor: '22.22.3', node24Floor: '24.15.0' };
  }
  const node22Floor = compareVersions(hostVersion, '2026.5.18') >= 0
    ? '22.19.0'
    : compareVersions(hostVersion, '2026.5.12') >= 0
      ? '22.16.0'
      : '22.14.0';
  return { node22Floor, node24Floor: '24.0.0' };
}

function nodeSafe(version: string, requirements: NodeRequirements): boolean {
  const [major = 0] = numericParts(version);
  if (major === 22) {
    return compareVersions(version, requirements.node22Floor) >= 0;
  }
  if (major === 24) {
    return compareVersions(version, requirements.node24Floor) >= 0;
  }
  if (major >= 25) return compareVersions(version, '25.9.0') >= 0;
  return false;
}

export function compareVersions(left: string, right: string): number {
  const leftParts = numericParts(left);
  const rightParts = numericParts(right);
  for (
    let index = 0;
    index < Math.max(leftParts.length, rightParts.length);
    index += 1
  ) {
    const difference = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (difference !== 0) return difference < 0 ? -1 : 1;
  }
  return 0;
}

function numericParts(version: string): number[] {
  return version
    .replace(/^v/, '')
    .split(/[.-]/u)
    .map((part) => Number.parseInt(part, 10) || 0);
}
