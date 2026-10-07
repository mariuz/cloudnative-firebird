# TODO

Open work, roughly in priority order. Firebird-level problems found while building the operator
are described, with reproduction steps, in [ISSUES.md](ISSUES.md).

## Replication

- [ ] **File the publication deadlock upstream** ([ISSUES.md](ISSUES.md) issue 1): the report,
  with the diagnosis from symbolized thread stacks, is ready in
  [docs/upstream](docs/upstream/01-replication-manager-header-deadlock.md); file it and track the
  fix version. Replication stays experimental until then.
- [ ] **File the commit/TIP window upstream** ([ISSUES.md](ISSUES.md) issue 2, confirmed): the
  report is ready in [docs/upstream](docs/upstream/02-commit-journaled-before-tip.md).
- [ ] **Failover safety**: the Lease is moved by the operator, not held and renewed by the
  instances (CloudNativePG 1.30's promotion mutex). The isolation check fences a primary cut off
  from both the API server and every replica (v0.52.0), and since v0.74.0 also one that neither
  the operator nor any replica has reached for `contactTimeoutSeconds`, before a cut-off primary
  is failed over (v0.63.0). Still open: a primary whose cluster DNS fails as well cannot tell it
  has replicas, and is then only fenced when it loses the API server too.
- [ ] **Synchronous replication follow-ups** (one standby since v0.53.0, several since v0.61.0):
  attaching and detaching without the short write pause (Firebird reads `sync_replica` only when
  the database is opened); an "any N of M" quorum (Firebird waits for every `sync_replica`).
  A restart of the only replica in `required` mode blocks writes by definition; since v0.73.0
  `detachForUpdates` lets rolling updates detach it first (asynchronous commits meanwhile), while
  other restarts (crash, drain) still block writes until it is back.
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

Nothing open.
