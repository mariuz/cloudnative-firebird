import { describe, it, expect } from 'vitest';
import { V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdCluster } from '../src/types';
import { tempVolumeErrors } from '../src/utils/temp-volume';
import { buildStatefulSet, firebirdConfSettings, quantitiesByValue, statefulSetNeedsUpdate } from '../src/utils/resources';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster } from './helpers/factories';

const firebird = (sts: V1StatefulSet) => sts.spec!.template.spec!.containers.find((c) => c.name === 'firebird')!;
const volume = (sts: V1StatefulSet) => sts.spec!.template.spec!.volumes!.find((v) => v.name === 'temp-data');
const claim: FirebirdCluster['spec']['ephemeralVolumeSource'] = {
  volumeClaimTemplate: { spec: { accessModes: ['ReadWriteOnce'], storageClassName: 'fast', resources: { requests: { storage: '10Gi' } } } },
};

describe('the temporary volume', () => {
  it('is not there by default: Firebird keeps its temporary files in /tmp', () => {
    const sts = buildStatefulSet(makeCluster());
    expect(volume(sts)).toBeUndefined();
    expect(firebird(sts).volumeMounts!.some((m) => m.name === 'temp-data')).toBe(false);
    expect(firebirdConfSettings(makeCluster())).not.toHaveProperty('TempDirectories');
  });

  it('is an emptyDir with the size limit, where TempDirectories points', () => {
    const cluster = makeCluster({ ephemeralVolumesSizeLimit: { temporaryData: '2Gi' } });
    const sts = buildStatefulSet(cluster);
    expect(volume(sts)).toEqual({ name: 'temp-data', emptyDir: { sizeLimit: '2Gi' } });
    expect(firebird(sts).volumeMounts).toContainEqual({ name: 'temp-data', mountPath: '/firebird-temp' });
    expect(firebird(sts).env).toContainEqual({ name: 'FIREBIRD_CONF_TempDirectories', value: '/firebird-temp' });
    // only the Firebird container
    for (const c of sts.spec!.template.spec!.containers.filter((c) => c.name !== 'firebird')) {
      expect((c.volumeMounts ?? []).some((m) => m.name === 'temp-data')).toBe(false);
    }
  });

  it('is a generic ephemeral volume with ephemeralVolumeSource', () => {
    expect(volume(buildStatefulSet(makeCluster({ ephemeralVolumeSource: claim })))).toEqual({ name: 'temp-data', ephemeral: claim });
  });

  it('leaves a TempDirectories of config.settings as it is', () => {
    const cluster = makeCluster({ ephemeralVolumesSizeLimit: { temporaryData: '1Gi' }, config: { settings: { TempDirectories: '/firebird-temp/sort;/tmp' } } });
    expect(firebirdConfSettings(cluster).TempDirectories).toBe('/firebird-temp/sort;/tmp');
    expect(firebird(buildStatefulSet(cluster)).env!.filter((e) => e.name === 'FIREBIRD_CONF_TempDirectories')).toHaveLength(1);
  });
});

describe('StatefulSet updates for the temporary volume', () => {
  // as the API server stores it: quantities in canonical form, claim fields defaulted
  const asStored = (sts: V1StatefulSet): V1StatefulSet => {
    const copy = JSON.parse(JSON.stringify(sts).replace(/"2048Mi"/g, '"2Gi"').replace(/"10240Mi"/g, '"10Gi"')) as V1StatefulSet;
    const v = copy.spec!.template.spec!.volumes!.find((x) => x.name === 'temp-data');
    if (v?.ephemeral?.volumeClaimTemplate?.spec) v.ephemeral.volumeClaimTemplate.spec.volumeMode = 'Filesystem';
    return copy;
  };

  it('are not needed for an unchanged cluster, whatever the server rewrote', () => {
    for (const extra of [
      { ephemeralVolumesSizeLimit: { temporaryData: '2048Mi' } },
      { ephemeralVolumeSource: { volumeClaimTemplate: { spec: { resources: { requests: { storage: '10240Mi' } } } } } },
    ] as Array<Partial<FirebirdCluster['spec']>>) {
      expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(makeCluster(extra))), buildStatefulSet(makeCluster(extra)))).toBe(false);
    }
  });

  it.each<[string, Partial<FirebirdCluster['spec']>, Partial<FirebirdCluster['spec']>]>([
    ['added', {}, { ephemeralVolumesSizeLimit: { temporaryData: '1Gi' } }],
    ['removed', { ephemeralVolumesSizeLimit: { temporaryData: '1Gi' } }, {}],
    ['resized', { ephemeralVolumesSizeLimit: { temporaryData: '1Gi' } }, { ephemeralVolumesSizeLimit: { temporaryData: '4Gi' } }],
    ['turned into a claim', { ephemeralVolumesSizeLimit: { temporaryData: '1Gi' } }, { ephemeralVolumeSource: claim }],
  ])('are needed when the volume is %s', (_what, before, after) => {
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(makeCluster(before))), buildStatefulSet(makeCluster(after)))).toBe(true);
  });

  it('compares quantities by value', () => {
    expect(quantitiesByValue({ sizeLimit: '2048Mi', a: { storage: '1Gi', other: '1Gi' } })).toEqual({ sizeLimit: 2147483648, a: { storage: 1073741824, other: '1Gi' } });
  });
});

describe('temporary volume validation', () => {
  it('accepts a size limit or a claim template', () => {
    expect(tempVolumeErrors(makeCluster({ ephemeralVolumesSizeLimit: { temporaryData: '500Mi' } }).spec)).toEqual([]);
    expect(() => validateClusterSpec(makeCluster({ ephemeralVolumeSource: claim }))).not.toThrow();
  });

  it.each<[Partial<FirebirdCluster['spec']>, string]>([
    [{ ephemeralVolumesSizeLimit: { temporaryData: 'lots' } }, 'temporaryData must be a positive quantity'],
    [{ ephemeralVolumesSizeLimit: { temporaryData: '0' } }, 'temporaryData must be a positive quantity'],
    [{ ephemeralVolumeSource: {} }, 'ephemeralVolumeSource needs a volumeClaimTemplate with a spec'],
    [{ ephemeralVolumeSource: claim, ephemeralVolumesSizeLimit: { temporaryData: '1Gi' } }, 'mutually exclusive'],
  ])('rejects %j', (extra, message) => {
    expect(() => validateClusterSpec(makeCluster(extra))).toThrow(message);
  });
});
