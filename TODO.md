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

- [ ] **Working backup Jobs**: run `gbak`/`nbackup` through the service manager against the
  primary (not `localhost` in the Job pod), and ship an image with an S3 client for uploads.
  Taking physical (`nbackup`) backups from a replica keeps the load off the primary.
- [ ] **Restore and bootstrap recovery/clone** init containers still assume tools (`aws`, `nc`)
  that the Firebird image does not ship.
- [ ] **Journal archive CronJob**: same S3 tooling gap.

## Other roadmap items

- [ ] TLS: Firebird has no native TLS listener; decide between WireCrypt only, a TLS proxy
  sidecar, or dropping the `tls` mount.
- [ ] Instance fencing.
- [ ] Rolling updates with the primary last.
- [ ] Declarative database users and roles.
- [ ] Admission validation (CRD CEL rules or a webhook) so invalid specs are rejected at apply
  time rather than surfacing as a Degraded status.
