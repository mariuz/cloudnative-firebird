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
  `HDR_repl_seq` header clump (`src/jrd/ods.h`). Verified for Firebird 4.0.7 (ODS 13.0), 5.0.4
  (ODS 13.1) and the 6.0 snapshot (ODS 14.0, new header page layout, handled). Re-verify for
  the Firebird 6 release and every later major version (an unknown ODS is refused, not
  guessed), or replace them with supported mechanisms if Firebird adds any.

## Backups and restore

- [ ] **Recovery points within a segment**: point-in-time recovery applies whole journal
  segments (the journal has no timestamps); a target time lands on the end of the last segment
  archived before it. A shorter `archiveTimeoutSeconds` narrows the gap.
- [ ] **Point-in-time recovery from a replica's backup**: the replica's applied position lives in
  its replica control file, which the backup does not carry; such restores are refused.

## Users

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
