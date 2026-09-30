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
- [ ] **Refresh the offline bootstrap seed online**: the seed is refreshed when the primary
  starts after a clean stop, and the segments after it are kept up to `maxSegmentRetentionHours`
  while there is no replica. A primary running longer than that without a restart, and without a
  ready replica, still cannot seed a new one unless live seeding is allowed.
- [ ] **Prune applied segments early**: segments every replica has applied are still kept for
  `segmentRetentionHours` (the offline bootstrap seed and the journal archive upload may need
  them). Deleting them once applied and uploaded would bound the archive by replica progress.
- [ ] **Failover safety**: the Lease is moved by the operator, not held and renewed by the
  instances (CloudNativePG 1.30's promotion mutex). A primary that is alive but unready (e.g.
  overloaded) is restarted and re-seeded after a failover; one that is partitioned from the other
  pods but still ready to the kubelet is not failed over at all. Instance-side self-fencing
  (shut the database down when the Lease is lost) would close both gaps.
- [ ] **Synchronous replication** would make failover lossless; see below.
- [ ] **Switchover downtime**: writes stop from the primary shutdown until the target pod is ready
  again (two pod restarts). Promoting online (replica mode none and publication on a running
  replica) would need the replication sequence set without restarting.
- [ ] **Synchronous mode** (`sync_replica`): currently rejected by validation. Needs replica
  credentials in a Secret-backed replication.conf.
- [ ] **Encrypt segment shipping**: the segment server authenticates with the SYSDBA password
  but traffic is plain TCP inside the cluster (restricted by the NetworkPolicy when enabled).
- [ ] **Firebird internal formats**: seeding writes the replica control file
  (`ControlFile::DataV1` in `src/remote/server/ReplServer.cpp`) and switchover writes the
  `HDR_repl_seq` header clump (`src/jrd/ods.h`, ODS 13). Re-verify both for every supported
  Firebird major version, or replace them with supported mechanisms if Firebird adds any.

## Backups and restore

- [ ] **Point-in-time recovery**: restore an `nbackup` base and replay the archived journal
  segments from S3 up to a target time. Segments are archived, nothing replays them yet.
- [ ] **Physical backups to S3 without replication**: the file is copied through the segment
  server, which only runs with replication. Taking the copy on a replica would also keep the load
  off the primary, but nbackup on a replica (replica mode, read-only) is unverified.
- [ ] **Retention of nbackup backups, and of server-side backups without replication**:
  `retentionPolicy` prunes logical backups in S3, and server-side logical backups on clusters with
  replication (through the segment server). Without replication there is no deletion path on the
  primary's volume; nbackup retention, on S3 too, must keep every level 0 that a kept level 1 or 2
  depends on (chains span schedules).

## Users

- [ ] **Quoted role names**: `FirebirdRole` object names can be delimited identifiers
  (`quoted: true`), the role name itself is still a regular identifier.
- [ ] **Dropping users from instances unready for long**: the deletion waits 15 minutes for an
  instance that holds the user; one down longer (or scaled away with its volume kept) keeps it.
  An instance could drop pending users itself on start, from a list the operator keeps.
- [ ] **Password without SQL text**: the services API (`action_modify_user`) would keep the
  password out of `MON$STATEMENTS`, but takes it as a command-line argument; pick the lesser risk.
- [ ] **Existing users on upgrade**: clusters created before v0.12.0 start with a fresh security
  database seeded from the image (only SYSDBA); users created by applications before the upgrade
  were already lost on every pod restart and must be re-created (ideally as `FirebirdUser`).

## Other roadmap items

- [ ] TLS: Firebird has no native TLS listener; decide between WireCrypt only, a TLS proxy
  sidecar, or dropping the `tls` mount.
- [ ] **In-place configuration reloads**: every template change restarts the instances, even
  settings Firebird could apply without a restart. CloudNativePG reloads PostgreSQL in place when
  possible.
- [ ] **Non-root instances**: the official image runs the server as root (its entrypoint edits
  `/opt/firebird/*.conf` and the server needs its firebird-owned lock directory), so instance pods
  meet the `baseline` Pod Security Standard, not `restricted`. Running as the `firebird` user
  needs the configuration moved to a writable location (or an image that supports it).
- [ ] **Validation that needs other objects** stays in the operator: e.g. a restore into the
  cluster database, a clone of a cluster in another namespace, a missing Secret. A
  ValidatingAdmissionPolicy with parameter resources, or a webhook, could reject these at apply
  time too.
