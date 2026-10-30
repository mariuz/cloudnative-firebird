import { describe, it, expect, beforeAll } from 'vitest';
import { makeCluster } from './helpers/factories';
import { buildStatefulSet } from '../src/utils/resources';
import { setOperatorImage } from '../src/utils/operator-image';
import { setServerVersion } from '../src/utils/segment-tls-default';
import { leftByDeletedCluster, leftoverDefaults, type FirstDefaults } from '../src/utils/leftover-defaults';
import type { FirebirdCluster } from '../src/types';

const failover = { instances: 3, replication: { enabled: true, failover: { enabled: true } } } as const;
const fresh: FirstDefaults = { segmentTls: true, primaryLease: true, podAntiAffinity: true };
/** The StatefulSet a deleted cluster ran, as the API server returns it, without its owner */
const ran = (d: FirstDefaults, spec: Partial<FirebirdCluster['spec']> = failover) => {
  const c = makeCluster({ ...spec, segmentTLS: { enabled: d.segmentTls } });
  c.metadata.annotations = {
    'firebird.cloudnative-firebird.io/primary-lease': d.primaryLease ? 'enabled' : 'pinned',
    'firebird.cloudnative-firebird.io/pod-anti-affinity': d.podAntiAffinity ? 'enabled' : 'pinned',
  };
  const sts = JSON.parse(JSON.stringify(buildStatefulSet(c)));
  delete sts.metadata.ownerReferences;
  return sts;
};
/** The recreated cluster, nothing decided yet */
const recreated = (spec: Partial<FirebirdCluster['spec']> = failover) => {
  const c = makeCluster(spec);
  delete c.spec.segmentTLS;
  delete c.metadata.annotations;
  return c;
};

describe('the first defaults of a cluster recreated over what a deleted one left', () => {
  beforeAll(() => {
    setOperatorImage('registry.example/cloudnative-firebird:1.2.3');
    setServerVersion({ major: 1, minor: 31 });
  });

  it('tells a leftover StatefulSet from the one an earlier version made for this cluster', () => {
    const own = JSON.parse(JSON.stringify(buildStatefulSet(makeCluster())));
    expect(leftByDeletedCluster(own, makeCluster())).toBe(false);
    expect(leftByDeletedCluster(ran(fresh), makeCluster())).toBe(true);
    own.metadata.ownerReferences[0].uid = 'deleted-uid';
    expect(leftByDeletedCluster(own, makeCluster())).toBe(true);
  });

  it('follows the pod template for each setting it shows', () => {
    for (const d of [fresh, { segmentTls: false, primaryLease: false, podAntiAffinity: false }, { segmentTls: true, primaryLease: false, podAntiAffinity: true }]) {
      expect(leftoverDefaults(ran(d), recreated(), fresh), JSON.stringify(d)).toEqual(d);
    }
  });

  it("takes a new cluster's default for a setting the template cannot show", () => {
    // without automatic failover no Lease holder runs, on or off: the default stays
    const single = { instances: 1 } as const;
    const left = ran({ segmentTls: true, primaryLease: false, podAntiAffinity: true }, single);
    expect(leftoverDefaults(left, recreated(single), fresh)).toEqual({ segmentTls: true, primaryLease: true, podAntiAffinity: true });
    expect(leftoverDefaults(left, recreated(single), { ...fresh, primaryLease: false }).primaryLease).toBe(false);
  });

  it("takes the anti-affinity only when it is the operator's, with this cluster's settings", () => {
    const zoned = { ...failover, podAntiAffinity: { topologyKey: 'topology.kubernetes.io/zone' } };
    expect(leftoverDefaults(ran(fresh, zoned), recreated(), fresh).podAntiAffinity).toBe(false);
    expect(leftoverDefaults(ran(fresh, zoned), recreated(zoned), fresh).podAntiAffinity).toBe(true);
  });
});
