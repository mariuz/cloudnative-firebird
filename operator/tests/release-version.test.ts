import { describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadAllYaml } from '@kubernetes/client-node';
import { PLUGIN_VERSION } from '../src/plugin/cli';

const root = join(__dirname, '..', '..');

describe('the operator version', () => {
  it('is the image config/deploy/deployment.yaml deploys (hack/release/version.sh)', () => {
    expect(PLUGIN_VERSION).toMatch(/^\d+\.\d+\.\d+$/);
    const deployment = readFileSync(join(root, 'config/deploy/deployment.yaml'), 'utf8');
    expect(deployment).toContain(`image: ghcr.io/mariuz/cloudnative-firebird:${PLUGIN_VERSION}\n`);
    expect(execFileSync(join(root, 'hack/release/version.sh'), { encoding: 'utf8' }).trim()).toBe(PLUGIN_VERSION);
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
      'Service',
      'ValidatingWebhookConfiguration',
      'Deployment',
      'PodDisruptionBudget',
    ]);
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
