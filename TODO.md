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
  cut off from both the API server and every replica. A primary that still looks ready but that
  neither the operator nor any replica reaches is failed over since v0.63.0, and its pod is
  deleted. Until then, though, a primary that still reaches the API server is never fenced, and
  it keeps accepting writes from clients on its side of the partition.
- [ ] **Switchover downtime**: writes stop from the primary shutdown until the target pod is ready
  again (two pod restarts). Promoting online (replica mode none and publication on a running
  replica) would need the replication sequence set without restarting.
- [ ] **Synchronous replication follow-ups** (one standby since v0.53.0, several since v0.61.0):
  attaching and detaching without the short write pause (Firebird reads `sync_replica` only when
  the database is opened); a restart of the only replica without blocking writes in `required`
  mode; an "any N of M" quorum (Firebird waits for every `sync_replica`).
- [ ] **Encrypt segment shipping**: client connections are encrypted (WireCrypt, v0.54.0), but the
  segment server (journal segments, seed copies, backup files) transfers over plain TCP inside the
  cluster (restricted by the NetworkPolicy when enabled). Since v0.64.0 requests are signed
  instead of carrying the SYSDBA password, but replies and data are neither encrypted nor
  signed. The image ships perl-base only (no TLS, no Digest modules); options are carrying the
  bytes over a Firebird connection, or a sidecar image with TLS.
- [ ] **Stop accepting the plain password** on the segment server once no supported upgrade path
  starts from a version before v0.64.0 (clients of those versions send it; current clients send it
  only to servers that answer the signed probe like those versions).
- [ ] **Firebird internal formats**: seeding writes the replica control file
  (`ControlFile::DataV1` in `src/remote/server/ReplServer.cpp`) and switchover writes the
  `HDR_repl_seq` header clump (`src/jrd/ods.h`). Verified for Firebird 4.0.7 (ODS 13.0), 5.0.4
  (ODS 13.1) and the 6.0 snapshot (ODS 14.0, new header page layout, handled). Re-verify for
  the Firebird 6 release and every later major version (an unknown ODS is refused, not
  guessed), or replace them with supported mechanisms if Firebird adds any.

## Backups and restore

Nothing open.

## Users

- [ ] **Existing users on upgrade**: clusters created before v0.12.0 start with a fresh security
  database seeded from the image (only SYSDBA); users created by applications before the upgrade
  were already lost on every pod restart and must be re-created (ideally as `FirebirdUser`).

## Other roadmap items

- [ ] **In-place configuration reloads**: every template change restarts the instances, even
  settings Firebird could apply without a restart. CloudNativePG reloads PostgreSQL in place when
  possible.
- [ ] **Validation that needs other objects** stays in the operator: e.g. a restore into the
  cluster database, a clone of a cluster in another namespace, a missing Secret. A
  ValidatingAdmissionPolicy with parameter resources, or a webhook, could reject these at apply
  time too.
