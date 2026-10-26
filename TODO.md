# TODO

Open work, roughly in priority order. Firebird-level problems found while building the operator
are described, with reproduction steps, in [ISSUES.md](ISSUES.md).

## Replication

- [ ] **File the publication deadlock upstream** ([ISSUES.md](ISSUES.md) issue 1): the report,
  with the diagnosis from symbolized thread stacks, is ready in
  [docs/upstream](docs/upstream/01-replication-manager-header-deadlock.md); file it and track the
  fix version. Replication stays experimental until then. Still unfixed in `master` at `4fed06f`
  (2026-10-18; swept 2026-10-23), so the report applies as written. The weekly "Firebird issues"
  workflow (`hack/repro/run-all.sh`) re-runs the reproduction against Firebird 4, 5 and the 6
  snapshot: a run where it stops reproducing on a new build is the signal to look for the fix.
- [ ] **File the commit/TIP window upstream** ([ISSUES.md](ISSUES.md) issue 2, confirmed): the
  report is ready in [docs/upstream](docs/upstream/02-commit-journaled-before-tip.md). Still in
  `master` at `4fed06f` (swept 2026-10-23); the weekly "Firebird issues" workflow re-runs
  `tip-window.sh` and `replica-seed-race.sh` too.
- [x] **Failover safety**: since v0.86.0 the primary can hold and renew the Lease itself
  (`failover.primaryLease`, CloudNativePG 1.30's promotion mutex): it fences itself when the
  Lease names another instance or cannot be renewed, and the operator promotes only once the
  Lease expired; since v0.87.0 new clusters get it by default (`PRIMARY_LEASE_DEFAULT`), clusters
  from before are pinned to the operator-moved Lease, and since v0.88.0 `PRIMARY_LEASE_MIGRATE`
  moves the pinned ones over one at a time. The isolation check fences a primary
  cut off from both the API server and every replica (v0.52.0), and since v0.74.0 also one that
  neither the operator nor any replica has reached for `contactTimeoutSeconds`, before a cut-off
  primary is failed over (v0.63.0), since v0.78.0 also when cluster DNS fails with the partition
  (the peers known from the last answer and the addresses the operator publishes, v0.80.0).
- [ ] **Synchronous replication follow-ups** (one standby since v0.53.0, several since v0.61.0):
  an "any N of M" quorum (Firebird waits for every `sync_replica`). Attaching and detaching
  without the short write pause is not possible with Firebird 5 or 6: `sync_replica` is read
  only when the database is opened, after every attachment closed, and nothing reloads it
  (verified v0.85.0, [ISSUES.md](ISSUES.md) issue 9); the Job now reports the pause's length.
  A restart of the only replica in `required` mode blocks writes by definition; since v0.73.0
  `detachForUpdates` lets rolling updates detach it first (asynchronous commits meanwhile), while
  other restarts (crash, drain) still block writes until it is back.
- [ ] **Firebird internal formats**: seeding writes the replica control file
  (`ControlFile::DataV1` in `src/remote/server/ReplServer.cpp`) and switchover writes the
  `HDR_repl_seq` header clump (`src/jrd/ods.h`). Verified for Firebird 4.0.7 (ODS 13.0), 5.0.4
  (ODS 13.1) and the 6.0 snapshot (ODS 14.0, new header page layout, handled). Since v0.81.0
  `hack/firebird-formats/verify.sh` checks both against an image's own engine and replica
  server, weekly for the 6 snapshot ("Firebird formats" workflow; last passed on 2026-10-19 for
  Firebird 4, 5 and the 6 snapshot, and `ControlFile::DataV1` is still the only control file
  version in `master` at `4fed06f`): run it (`workflow_dispatch`)
  for the Firebird 6 release and every later major version (an unknown ODS is refused, not
  guessed), or replace them with supported mechanisms if Firebird adds any.

## Backups and restore

Nothing open.

## Users

Nothing open.

## Other roadmap items

From the sweep of CloudNativePG's main branch after 1.30.1 (2026-10-26,
[docs/cloudnative-pg-review.md](docs/cloudnative-pg-review.md)), most important first:

- [ ] **Adopt or replace an orphan primary Lease** (CloudNativePG #11609): a cluster recreated
  over the resources a deleted one left behind (orphan propagation, a restore by a backup tool)
  keeps `<cluster>-lease` without its owner, naming the old primary; with the primary Lease on,
  the new primary's `lease-holder` fences itself when that name differs. Adopt the Lease (set the
  ownerReference and the current primary), or delete and recreate it when no instance runs yet.
- [ ] **Fence before the Lease expires** (#11357): the sidecar fences once the Lease went its whole
  duration without a renewal, the moment a replica becomes promotable; fence at a renew deadline
  (two thirds of the duration, as CloudNativePG's 10 of 15 s) instead.
- [ ] **Bound the segment server's local commands** (#11718): its `isql` and `fbsvcmgr` calls to
  the local server have no timeout, so a hung server blocks it; run them under `timeout`, as the
  metrics exporter does.
- [ ] **Conditional backup and restore status writes** (#11617): the status is replaced from the
  copy a reconcile started with; test the `resourceVersion` read, as the fencing reconcile does
  since v0.122.0, so a stale copy cannot write an older phase back.
- [ ] **Generated passwords for `FirebirdUser`** (#11337): generate the password into a Secret
  when none is given, with optional renewal.
- [ ] **A viewer ClusterRole** (#11684): `cloudnative-firebird-viewer`, read-only on the
  `firebird.cloudnative-firebird.io` resources and aggregated to `view`, with a README section.
