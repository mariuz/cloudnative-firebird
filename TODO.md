# TODO

Open work, roughly in priority order. Firebird-level problems found while building the operator
are described, with reproduction steps, in [ISSUES.md](ISSUES.md).

## Replication

- [ ] **Report the publication hang upstream** ([ISSUES.md](ISSUES.md) issue 1) with
  `hack/repro/publication-under-load.sh`, capture thread stacks of the hung server, and track the
  fix version. Replication stays experimental until then.
- [ ] **Confirm or rule out the commit/TIP window** ([ISSUES.md](ISSUES.md) issue 2) with more
  `hack/repro/replica-seed-race.sh` runs; until then keep `allowLiveSeedFromPrimary` off by
  default.
- [ ] **Refresh the offline bootstrap seed.** It is only written when the primary database is
  created. Once the primary has pruned the segments that follow it (`segmentRetentionHours`),
  a cluster without any ready replica cannot seed a new one unless live seeding is allowed.
  Options: refresh the seed whenever the primary restarts (costs a full copy), or keep segments
  until a replica has consumed them.
- [ ] **Tie segment retention to replica progress** instead of a fixed age, so a slow or stopped
  replica cannot fall behind the archive (and so the archive does not grow unbounded when replicas
  keep up).
- [ ] **Enable replication on an existing cluster.** Publication is enabled only when the
  database is created; an existing single-instance database needs `ALTER DATABASE ENABLE
  PUBLICATION` / `INCLUDE ALL TO PUBLICATION` on the primary before replicas can be added.
- [ ] **Replication lag in status**: compare the primary's replication sequence with each
  replica's control-file position and publish it (feeds `readOnlyRouting` via the
  `replication-lag-seconds` annotation).
- [ ] **Planned switchover and failover**: promote a replica (`gfix -replica none`, enable
  publication), move the Lease and the `primary` ConfigMap entry, and demote or reseed the old
  primary. Depends on the seeding and lag work above.
- [ ] **Synchronous mode** (`sync_replica`): currently rejected by validation. Needs replica
  credentials in a Secret-backed replication.conf.
- [ ] **Encrypt segment shipping**: the segment server authenticates with the SYSDBA password
  but traffic is plain TCP inside the cluster (restricted by the NetworkPolicy when enabled).
- [ ] **Replica control file dependency**: seeding writes Firebird's replica control file
  (`ControlFile::DataV1` in `src/remote/server/ReplServer.cpp`). Re-verify the layout for every
  supported Firebird major version, or replace it with a supported mechanism if one is added.

## Backups and restore

- [ ] **Point-in-time recovery**: restore an `nbackup` base and replay the archived journal
  segments from S3 up to a target time. Segments are archived, nothing replays them yet.
- [ ] **Physical backups to S3**: `nbackup` runs in the primary's server and writes to its data
  directory. Shipping the file needs a transfer path, e.g. a `BACKUP <file>` command on the
  segment server, or taking the physical copy on a replica (which also keeps the load off the
  primary).
- [ ] **Retention**: `retentionPolicy` is accepted but not enforced, neither for server-side
  files nor for S3 objects.
- [ ] **Backups from a replica**: `gbak -b` works against a read-only replica (verified), which
  would keep backup load off the primary; pick a ready replica when one exists.
- [ ] **Backup verification**: optionally restore each backup into a scratch database and
  validate it.
- [ ] **Clone across NetworkPolicies**: a clone pod carries the target cluster's labels, so a
  source cluster with `networkPolicy.enabled` rejects it unless its `ingressFrom` allows it.
- [ ] **Sweep and diagnostics Jobs target the `<name>` Service**, which balances across all
  instances unless read-only routing labels the primary; point them at the primary instance
  like the backup Jobs.

## Users

- [ ] **Role management**: `FirebirdUser` grants roles that must already exist. Declaring roles
  (and their privileges) would complete the picture, e.g. a `FirebirdRole` resource or a
  `Database`-like resource as in CloudNativePG.
- [ ] **Dropping users on unready instances**: with `reclaimPolicy: delete`, instances that are not
  ready when the resource is deleted keep the user in their security database.
- [ ] **Password without SQL text**: the services API (`action_modify_user`) would keep the
  password out of `MON$STATEMENTS`, but takes it as a command-line argument; pick the lesser risk.
- [ ] **Existing users on upgrade**: clusters created before v0.12.0 start with a fresh security
  database seeded from the image (only SYSDBA); users created by applications before the upgrade
  were already lost on every pod restart and must be re-created (ideally as `FirebirdUser`).

## Other roadmap items

- [ ] TLS: Firebird has no native TLS listener; decide between WireCrypt only, a TLS proxy
  sidecar, or dropping the `tls` mount.
- [ ] **Failover**: the leader Lease is static. Implement switchover first, then failover with the
  Lease as promotion mutex (CloudNativePG 1.30); a fenced primary must never be failed over.
- [ ] Items from the CloudNativePG 1.28 – 1.30 review ([docs/cloudnative-pg-review.md](docs/cloudnative-pg-review.md)):
  Kubernetes events, replica re-creation (`unrecoverable`), `serviceAccountName` for workload
  identity (S3 without static keys), per-backup reconciliation pause, pod/container security
  contexts.
- [ ] Rolling updates with the primary last.
- [ ] Declarative database users and roles.
- [ ] Admission validation (CRD CEL rules or a webhook) so invalid specs are rejected at apply
  time rather than surfacing as a Degraded status.
