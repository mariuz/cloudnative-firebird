# TODO

Open work, roughly in priority order. Firebird-level problems found while building the operator
are described, with reproduction steps, in [ISSUES.md](ISSUES.md).

## Replication

- [ ] **Report the publication hang upstream** ([ISSUES.md](ISSUES.md) issue 1) with
  `hack/repro/publication-under-load.sh`, capture thread stacks of the hung server, and track the
  fix version. Replication stays experimental until then.
- [ ] **Report the commit/TIP window upstream** ([ISSUES.md](ISSUES.md) issue 2, confirmed):
  replicas created with Firebird's documented procedure (`nbackup -L`, `-SEQ -F`) lose the
  transactions committed at the moment of the lock (`hack/repro/replica-seed-race.sh`).
- [ ] **Failover safety**: the Lease is moved by the operator, not held and renewed by the
  instances (CloudNativePG 1.30's promotion mutex). The isolation check (v0.52.0) fences a primary
  cut off from both the API server and every replica; a primary that is partitioned from the other
  pods but still ready to the kubelet is not failed over at all, and one that still reaches the API
  server or a replica is never fenced.
- [ ] **Switchover downtime**: writes stop from the primary shutdown until the target pod is ready
  again (two pod restarts). Promoting online (replica mode none and publication on a running
  replica) would need the replication sequence set without restarting.
- [ ] **Synchronous replication follow-ups** (v0.53.0 attaches one standby): quorum of several
  standbys (CloudNativePG's `number` / `method: any`); attaching and detaching without the short
  write pause (Firebird reads `sync_replica` only when the database is opened); a restart of the
  only replica without blocking writes in `required` mode (v0.55.0 hands the standby over to
  another replica before a rolling update restarts it).
- [ ] **Encrypt segment shipping**: client connections are encrypted (WireCrypt, v0.54.0), but the
  segment server (journal segments, seed copies, backup files) authenticates with the SYSDBA
  password over plain TCP inside the cluster (restricted by the NetworkPolicy when enabled). The
  image's Perl has no TLS module; options are carrying the bytes over a Firebird connection, or a
  sidecar image with TLS.
- [ ] **Firebird internal formats**: seeding writes the replica control file
  (`ControlFile::DataV1` in `src/remote/server/ReplServer.cpp`) and switchover writes the
  `HDR_repl_seq` header clump (`src/jrd/ods.h`). Verified for Firebird 4.0.7 (ODS 13.0), 5.0.4
  (ODS 13.1) and the 6.0 snapshot (ODS 14.0, new header page layout, handled). Re-verify for
  the Firebird 6 release and every later major version (an unknown ODS is refused, not
  guessed), or replace them with supported mechanisms if Firebird adds any.

## Backups and restore

- [ ] **Point-in-time recovery across a failover**: a replica promoted at segment P continues its
  journal after the archive's highest segment U (v0.56.0), so the archive holds the lost primary's
  segments P+1..U, which are not in the new primary's history. A restore from an earlier backup
  with a target after the failover would replay them; record the lineage switch (P, U) in the
  archive and skip, or refuse, that range in `pitr-plan.pl`.
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
