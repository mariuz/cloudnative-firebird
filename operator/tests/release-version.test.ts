import { describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadAllYaml } from '@kubernetes/client-node';
import { PLUGIN_VERSION } from '../src/plugin/cli';

const root = join(__dirname, '..', '..');

describe('the operator version', () => {
  it('is the image config/deploy/deployment.yaml deploys and the chart version (hack/release/version.sh)', () => {
    expect(PLUGIN_VERSION).toMatch(/^\d+\.\d+\.\d+$/);
    const deployment = readFileSync(join(root, 'config/deploy/deployment.yaml'), 'utf8');
    expect(deployment).toContain(`image: ghcr.io/mariuz/cloudnative-firebird:${PLUGIN_VERSION}\n`);
    const chart = readFileSync(join(root, 'charts/cloudnative-firebird/Chart.yaml'), 'utf8');
    expect(chart).toContain(`\nversion: ${PLUGIN_VERSION}\n`);
    expect(chart).toContain(`\nappVersion: "${PLUGIN_VERSION}"\n`);
    expect(execFileSync(join(root, 'hack/release/version.sh'), { encoding: 'utf8' }).trim()).toBe(PLUGIN_VERSION);
  });

  it("has its ROADMAP.md entry, the release notes (hack/release/notes.sh)", () => {
    const notes = execFileSync(join(root, 'hack/release/notes.sh'), [PLUGIN_VERSION], { encoding: 'utf8' });
    const lines = notes.split('\n');
    expect(lines[0]).toMatch(/^### \S/);
    expect(lines[2]).toMatch(new RegExp(`^_v${PLUGIN_VERSION.replace(/\./g, '\\.')}([^0-9.].*)?_$`));
    expect(lines[4]).toMatch(/^- \S/);
    // the default is the operator's version
    expect(execFileSync(join(root, 'hack/release/notes.sh'), { encoding: 'utf8' })).toBe(notes);
    // every entry of a version, in the roadmap's order (v0.4.0 has several); a version with none fails
    const old = execFileSync(join(root, 'hack/release/notes.sh'), ['0.4.0'], { encoding: 'utf8' });
    expect(old.split('\n').filter((l) => l.startsWith('### ')).length).toBeGreaterThan(1);
    expect(old).toContain('### Grafana Dashboard ConfigMaps');
    // the version is a whole token of the marker: v0.40.0 is not v0.4.0
    expect(execFileSync(join(root, 'hack/release/notes.sh'), ['0.40.0'], { encoding: 'utf8' })).not.toContain('Dedicated Backup CRDs');
    expect(() => execFileSync(join(root, 'hack/release/notes.sh'), ['9.9.9'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })).toThrow(/no entry marked v9\.9\.9/);
  });

  it('ships the same CRDs in the Helm chart (hack/release/chart.sh sync)', () => {
    const crds = readdirSync(join(root, 'config/crds')).filter((f) => f.endsWith('.yaml')).sort();
    expect(readdirSync(join(root, 'charts/cloudnative-firebird/crds')).filter((f) => f.endsWith('.yaml')).sort()).toEqual(crds);
    for (const f of crds) {
      expect(readFileSync(join(root, 'charts/cloudnative-firebird/crds', f), 'utf8'), f).toBe(readFileSync(join(root, 'config/crds', f), 'utf8'));
    }
  });
});

describe('the release install manifest (hack/release/bundle.sh)', () => {
  it('holds every CRD, then the namespace, ServiceAccount, RBAC, webhook and Deployment, in order', () => {
    const dir = mkdtempSync(join(tmpdir(), 'bundle-'));
    const file = execFileSync(join(root, 'hack/release/bundle.sh'), [join(dir, 'out.yaml')], { encoding: 'utf8' }).trim();
    expect(file).toBe(join(dir, 'out.yaml'));
    const text = readFileSync(file, 'utf8');
    expect(text.startsWith(`# cloudnative-firebird ${PLUGIN_VERSION}:`)).toBe(true);
    expect(execFileSync(join(root, 'hack/release/bundle.sh'), { encoding: 'utf8', cwd: dir }).trim()).toBe(`cloudnative-firebird-${PLUGIN_VERSION}.yaml`);

    const docs = loadAllYaml(text) as Array<{ kind: string; metadata: { name: string; namespace?: string } }>;
    const crds = readdirSync(join(root, 'config/crds')).filter((f) => f.endsWith('.yaml')).sort();
    const kinds = docs.map((d) => d.kind);
    expect(kinds.slice(0, crds.length)).toEqual(crds.map(() => 'CustomResourceDefinition'));
    expect(kinds.slice(crds.length)).toEqual([
      'Namespace',
      'ServiceAccount',
      'ClusterRole',
      'ClusterRoleBinding',
      'Role',
      'RoleBinding',
      'ClusterRole',
      'Service',
      'ValidatingWebhookConfiguration',
      'Deployment',
      'PodDisruptionBudget',
    ]);
    // the read-only role for users, aggregated into the built-in view, edit and admin roles
    const viewer = docs.find((d) => d.kind === 'ClusterRole' && d.metadata.name === 'cloudnative-firebird-viewer') as unknown as {
      metadata: { labels: Record<string, string> };
      rules: Array<{ apiGroups: string[]; resources: string[]; verbs: string[] }>;
    };
    expect(viewer).toBeDefined();
    for (const role of ['view', 'edit', 'admin']) expect(viewer.metadata.labels[`rbac.authorization.k8s.io/aggregate-to-${role}`]).toBe('true');
    expect(viewer.rules).toEqual([{ apiGroups: ['firebird.cloudnative-firebird.io'], resources: ['*'], verbs: ['get', 'list', 'watch'] }]);
    // the PodMonitor needs the Prometheus operator and stays out
    expect(kinds).not.toContain('PodMonitor');
    for (const d of docs) {
      if (d.kind !== 'CustomResourceDefinition' && d.kind !== 'Namespace' && !d.kind.startsWith('Cluster') && d.kind !== 'ValidatingWebhookConfiguration') {
        expect(d.metadata.namespace, `${d.kind} ${d.metadata.name}`).toBe('cloudnative-firebird-system');
      }
    }
    const deployment = docs.find((d) => d.kind === 'Deployment') as unknown as { spec: { template: { spec: { containers: Array<{ image: string }> } } } };
    expect(deployment.spec.template.spec.containers[0].image).toBe(`ghcr.io/mariuz/cloudnative-firebird:${PLUGIN_VERSION}`);
  });
});
