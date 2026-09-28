import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';
import { loadYaml as load } from '@kubernetes/client-node';
import {
  validateBackupSpec,
  validateClusterSpec,
  validateRestoreSpec,
  validateScheduledBackupSpec,
} from '../src/utils/validation';
import { validateUserSpec } from '../src/utils/users';

/**
 * The CRDs reject invalid specs at admission (hack/crd-validation/test.sh runs these manifests
 * against an API server in CI); the operator keeps validating every reconcile. Both must agree.
 */
const dir = join(__dirname, '..', '..', 'hack', 'crd-validation', 'cases');
const validators: Record<string, (obj: never) => void> = {
  FirebirdCluster: validateClusterSpec,
  FirebirdBackup: validateBackupSpec,
  FirebirdScheduledBackup: validateScheduledBackupSpec,
  FirebirdRestore: validateRestoreSpec,
  FirebirdUser: validateUserSpec,
};

describe('CRD admission rules and operator validation agree', () => {
  const cases = readdirSync(dir).filter((f) => f.endsWith('.yaml'));

  it('has cases for every resource kind', () => {
    const kinds = new Set(cases.map((f) => (load(readFileSync(join(dir, f), 'utf8')) as { kind: string }).kind));
    expect([...kinds].sort()).toEqual(Object.keys(validators).sort());
  });

  for (const file of cases) {
    const text = readFileSync(join(dir, file), 'utf8');
    const expectation = /^# expect: (.*)$/m.exec(text)![1];
    const obj = load(text) as { kind: string };
    it(`${file}: ${expectation === 'valid' ? 'accepted' : 'rejected'}`, () => {
      const validate = () => validators[obj.kind](obj as never);
      if (expectation === 'valid') expect(validate).not.toThrow();
      else expect(validate).toThrow();
    });
  }
});
