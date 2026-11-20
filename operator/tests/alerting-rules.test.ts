import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync } from 'fs';
import { join } from 'path';
import { loadYaml } from '@kubernetes/client-node';

/**
 * The alerting rules (config/monitoring/firebird.rules.yaml): on metrics the operator exports, each
 * with a severity and its texts, and the same in the chart and config/deploy (hack/release/chart.sh
 * sync writes those). promtool checks and tests them in the CI (firebird.rules.test.yaml).
 */
const root = join(__dirname, '..', '..');
const rulesFile = readFileSync(join(root, 'config', 'monitoring', 'firebird.rules.yaml'), 'utf8');
interface Rule {
  alert: string;
  expr: string;
  for?: string;
  labels: Record<string, string>;
  annotations: Record<string, string>;
}
const groups = loadYaml<{ groups: Array<{ name: string; rules: Rule[] }> }>(rulesFile).groups;
const rules = groups.flatMap((g) => g.rules);

/** Every metric name the operator's source exports */
function exported(): Set<string> {
  const names = new Set<string>();
  const walk = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.name.endsWith('.ts')) {
        for (const m of readFileSync(path, 'utf8').matchAll(/'((?:firebird|cloudnative_firebird)_[a-z_]+)'/g)) names.add(m[1]);
      }
    }
  };
  walk(join(__dirname, '..', 'src'));
  return names;
}

describe('the alerting rules', () => {
  it('use only metrics the operator exports', () => {
    const known = exported();
    const used = new Set(rules.flatMap((r) => [...r.expr.matchAll(/\b((?:firebird|cloudnative_firebird)_[a-z_]+)\b/g)].map((m) => m[1])));
    expect(used.size).toBeGreaterThan(10);
    expect([...used].filter((name) => !known.has(name))).toEqual([]);
  });

  it('give every alert a unique name, a severity and its texts', () => {
    expect(new Set(rules.map((r) => r.alert)).size).toBe(rules.length);
    for (const r of rules) {
      expect(r.alert, r.alert).toMatch(/^Firebird[A-Za-z]+$/);
      expect(['critical', 'warning', 'info'], r.alert).toContain(r.labels.severity);
      expect(r.annotations.summary, r.alert).toBeTruthy();
      expect(r.annotations.description, r.alert).toBeTruthy();
    }
    // the backups and the cluster states the operator added for them
    expect(rules.map((r) => r.alert)).toEqual(expect.arrayContaining(['FirebirdBackupFailed', 'FirebirdBackupTooOld', 'FirebirdInstancesNotReady', 'FirebirdReplicaLagging']));
  });

  it('are the same in the chart and in config/deploy (hack/release/chart.sh sync)', () => {
    const fromGroups = rulesFile.slice(rulesFile.indexOf('groups:'));
    expect(readFileSync(join(root, 'charts', 'cloudnative-firebird', 'files', 'firebird.rules.yaml'), 'utf8')).toBe(fromGroups);
    const deploy = loadYaml<{ kind: string; spec: { groups: unknown } }>(readFileSync(join(root, 'config', 'deploy', 'prometheusrule.yaml'), 'utf8'));
    expect(deploy.kind).toBe('PrometheusRule');
    expect(deploy.spec.groups).toEqual(groups);
  });

  it('are tested by promtool for the alerts that matter most', () => {
    const tests = loadYaml<{ rule_files: string[]; tests: Array<{ alert_rule_test: Array<{ alertname: string }> }> }>(
      readFileSync(join(root, 'config', 'monitoring', 'firebird.rules.test.yaml'), 'utf8'),
    );
    expect(tests.rule_files).toEqual(['firebird.rules.yaml']);
    const tested = new Set(tests.tests.flatMap((t) => t.alert_rule_test.map((c) => c.alertname)));
    for (const name of tested) expect(rules.map((r) => r.alert), name).toContain(name);
    expect(tested.size).toBeGreaterThanOrEqual(8);
  });
});
