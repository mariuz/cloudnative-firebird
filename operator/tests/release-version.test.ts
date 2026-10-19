import { describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
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
