import { describe, it, expect } from 'vitest';
import { existsSync, readFileSync } from 'fs';
import { dirname, join, resolve } from 'path';

/**
 * The plugin is run from the operator's tsc build (`dist/`), which holds no replication scripts:
 * a module reading them when it loads (utils/replication.ts) crashes every plugin command. Its
 * imports must never reach one (a regression of v0.189.0's first commit, through utils/object-store).
 */
const src = join(__dirname, '..', 'src');

function imports(file: string): string[] {
  const text = readFileSync(file, 'utf8');
  // imports that stay in the JavaScript (not `import type`), and require() calls
  const specifiers = [
    ...[...text.matchAll(/^import\s+(?!type\b)[^'"]*from\s+'(\.[^']+)'/gm)].map((m) => m[1]),
    ...[...text.matchAll(/require\('(\.[^']+)'\)/g)].map((m) => m[1]),
  ];
  return specifiers
    .map((s) => resolve(dirname(file), s))
    .map((p) => (existsSync(`${p}.ts`) ? `${p}.ts` : join(p, 'index.ts')))
    .filter((p) => existsSync(p));
}

/** Every module the file loads, directly or not (paths relative to src/) */
function reachable(entry: string): string[] {
  const seen = new Set<string>();
  const queue = [join(src, entry)];
  while (queue.length) {
    const file = queue.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    queue.push(...imports(file));
  }
  return [...seen].map((f) => f.slice(src.length + 1));
}

describe('the kubectl plugin', () => {
  it('loads no module that reads the replication scripts', () => {
    const reached = reachable('plugin/kubectl-firebird.ts');
    expect(reached).toContain('plugin/cli.ts');
    expect(reached).not.toContain('utils/replication.ts');
  });
});

describe('the operator modules', () => {
  // utils/backup builds its Jobs' mounts from utils/replication's constants when it loads: were
  // utils/replication to load utils/backup, the one loaded first would see the other's constants
  // undefined (v0.190.0's first commit: a backup Job with an empty mountPath, refused by the API)
  it('keep utils/replication from loading utils/backup', () => {
    expect(reachable('utils/backup.ts')).toContain('utils/replication.ts');
    expect(reachable('utils/replication.ts')).not.toContain('utils/backup.ts');
  });
});
