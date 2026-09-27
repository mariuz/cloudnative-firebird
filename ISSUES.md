# Known issues

Problems found while building journal replication against the official
[`firebirdsql/firebird`](https://github.com/FirebirdSQL/firebird-docker) image
(`firebirdsql/firebird:5`, server `LI-V5.0.4.1812`), with reproduction steps and how the
operator handles them. Open follow-up work is listed in [TODO.md](TODO.md).

The reproduction scripts in [`hack/repro/`](hack/repro) need only Docker. Each one starts its
own containers and removes them when it finishes.

| # | Issue | Kind | Reproduces |
|---|-------|------|------------|
| 1 | A publishing database hangs under concurrent connect / commit / disconnect | Firebird bug, open | Yes: 7 of 7 runs; 0 of 7 with pooled connections or without publication |
| 2 | Commit journaled before its TIP state: lock-based replica copies may miss a transaction | Firebird, suspected | Not reproduced |
| 3 | A physical copy inherits publication; a publishing replica fast-forwards past segments | Expected behaviour, handled | Yes, always |
| 4 | `nbackup -B 0` copies record the still-active segment | Expected behaviour, handled | Yes, always |
| 5 | `gstat -h` omits "Replication sequence" while it is 0 | Minor | Yes, always |
| 6 | The official image keeps the security database on the container filesystem | Image behaviour, handled | Yes, always |

---

## 1. A publishing database hangs under concurrent connect / commit / disconnect

**What happens.** A database with publication enabled (`ALTER DATABASE ENABLE PUBLICATION`,
journal and archive configured in `replication.conf`) stops answering after 10 to 40 seconds
when several clients each repeatedly connect, commit a small transaction and disconnect. New
connections and queries on existing ones hang as well. Nothing else has to touch the database.

**Reproduce.**

```sh
hack/repro/publication-under-load.sh                          # hangs
PUBLICATION=0 hack/repro/publication-under-load.sh            # control: completes
CONNECTIONS=persistent hack/repro/publication-under-load.sh   # pooled connections: completes
ARCHIVE_TIMEOUT=60 hack/repro/publication-under-load.sh       # archive timeout does not matter: hangs
```

The script starts a server with a replication journal, then runs 4 writers with 600 commits
each. Every commit is an insert plus an update, made through its own `isql` connection unless
`CONNECTIONS=persistent` is set.

**Observed.**

| Publication | Connections | `journal_archive_timeout` | Runs | Result |
|-------------|-------------|---------------------------|------|--------|
| on | one per commit | 3 s | 5 | all hung (after 10-40 s) |
| on | one per commit | 60 s | 2 | both hung (after 10 s) |
| off | one per commit | 3 s | 3 | all completed |
| on | persistent (one per writer) | 3 s | 2 | both completed |

A single writer that connects once per commit did not hang in any run, so the concurrency
matters. The replication log (`replication.log`) records nothing when the server hangs.
Thread stacks were not captured: `gdb` could not be installed in the test container.

**Operator impact.** Replication is marked experimental. Applications that hold pooled
connections were not affected in testing, including the operator's own end-to-end runs with
4 pooled writers while three replicas were seeded (3 of 3 runs completed, every replica
row-for-row identical to the primary). Applications that open a connection per transaction
against a replicated cluster can hang the primary. Report upstream: see TODO.md.

**Diagnosis history.** The hang was first attributed to the backup lock (`nbackup -L`,
`ALTER DATABASE BEGIN BACKUP`) and then to embedded access (`gstat -h` from another container),
because both reliably coincided with it. The tests behind that used per-commit connections.
Repeated with persistent connections, 15 lock cycles completed in each of 4 runs (2 with
`nbackup`, 2 with SQL through the server), and 2 runs of repeated `gstat -h` on the live file
completed as well. Neither the lock nor embedded access hangs the server on its own. The
replication sidecars still never open a live database file directly (see below), since that is
not a supported access pattern.

---

## 2. Commit journaled before its TIP state (suspected, not reproduced)

**What might happen.** In `TRA_commit` (`src/jrd/tra.cpp`), `REPL_trans_commit()` journals the
commit before `TRA_set_state()` marks the transaction committed in the TIP. If a backup lock
lands between the two, the commit is recorded in replication segment *S* while the TIP write
goes to the delta file. The copy would hold the transaction as uncommitted, and a replica built
from it with Firebird's documented procedure would skip segment *S* and miss that transaction.

**Status.** Not reproduced. `hack/repro/replica-seed-race.sh` follows the documented
procedure: `nbackup -L`, copy, `-N`, then `nbackup -SEQ -F` and `gfix -replica read_only` on
the copy.

| Configuration | Attempts | Missing transactions |
|---------------|----------|----------------------|
| 1 writer, lock from the server's container | 2 | none |
| 1 writer, lock from another container | 3 | none |
| 4 writers, lock from the server's container | 1 | none (a second attempt hit issue 1) |

The transaction losses seen early in development coincided with issue 3 (the replica copy still
had publication enabled), which on its own makes a replica skip segments.

```sh
hack/repro/replica-seed-race.sh
WRITERS=4 CONNECTIONS=persistent hack/repro/replica-seed-race.sh
LOCK_FROM=sidecar hack/repro/replica-seed-race.sh
```

**Operator handling.** New replicas are seeded without locking the primary:

1. a ready replica serves the seed copy: nothing commits on a replica, so it has no window;
2. otherwise the primary serves its *offline bootstrap seed*, a plain file copy taken when the
   database was created in the init container, before the server started;
3. a locked copy of the live primary is used only with
   `spec.replication.allowLiveSeedFromPrimary: true`. For those seeds, the new replica lists
   the copy's uncommitted transactions (`RDB$GET_TRANSACTION_CN <= 0`) that the primary's
   journal contains in its replica control file, so Firebird replays exactly those
   transactions.

---

## 3. A physical copy inherits publication

**What happens.** A replica made from a physical copy of the primary keeps
`ENABLE PUBLICATION`. With the shared `replication.conf`, it then journals the changes it
applies, and its own replication sequence advances. The replica server takes that as "the
database was replaced", fast-forwards, and silently skips every later segment.

**Handling.** Seeding runs `ALTER DATABASE DISABLE PUBLICATION` on the copy before
`gfix -replica read_only`. A copy of a replica already has publication disabled.

---

## 4. `nbackup -B 0` records the still-active segment

**What happens.** A level-0 backup's header records the segment that was active when the backup
started. Changes committed into that segment afterwards are not in the copy, but a replica
restored from it with `-SEQ` treats that segment as already applied.

**Handling.** The operator never seeds from `nbackup -B`.

---

## 5. `gstat -h` omits "Replication sequence" while it is 0

**What happens.** A database whose replication sequence is 0 (for example one created offline,
before anything was journaled) has no "Replication sequence" line in `gstat -h` output.

**Handling.** The init scripts treat a missing value as 0.

---

## 6. The official image keeps the security database on the container filesystem

**What happens.** `firebirdsql/firebird:5` stores users in `/opt/firebird/security5.fdb`, inside
the image, and only `/var/lib/firebird/data` is meant to be a volume. In Kubernetes every
container restart starts from the image again, so users created with `CREATE USER` disappear;
only SYSDBA survives because the entrypoint recreates its password on every start.

**Reproduce.** Create a user in a pod, delete the pod, and log in as that user after the
StatefulSet recreates it: "Your user name and password are not defined".

**Handling.** The `security-db-init` container copies the image's security database to
`/var/lib/firebird/data/system/security.fdb` on first start and writes a `databases.conf` whose
`security.db` alias (used by the entrypoint for SYSDBA) points there; the server uses it through
`SecurityDatabase`. Users are managed with `FirebirdUser` (README, "Users").

---

## Implementation notes

- **No embedded access to live databases.** The replication sidecars run in their own
  containers, so opening a live database file there would use an independent engine and lock
  table. They read header values through the local server (`MON$DATABASE`,
  `RDB$GET_CONTEXT('SYSTEM', 'REPLICATION_SEQUENCE')`), and take a replica's backup lock with
  `ALTER DATABASE BEGIN/END BACKUP` through its own server. `gstat` is only used on files no
  server has open.
- **Replication sequence in the header.** A replica's header has no replication sequence (the
  `HDR_repl_seq` clump), so a replica promoted as is starts its journal at segment 1 and the other
  replicas, which applied the old primary's segments up to *S*, skip everything it ships. Planned
  switchover therefore writes the clump (value *S*) into the target's header offline
  (`set-repl-seq.pl`, following `PAG_set_repl_sequence` in `src/jrd/pag.cpp`) before enabling
  publication; its journal then continues at *S + 1*. Verified with live writers: no rows lost,
  no re-seeding, no replication errors.
- **Replica control file.** Seeding writes Firebird's replica control file directly
  (`ControlFile::DataV1`, a 40-byte header plus `{tra_id, sequence}` entries, in
  `src/remote/server/ReplServer.cpp`). It is an internal format, not a public interface, so the
  layout needs re-verifying for each supported Firebird major version (see TODO.md).
