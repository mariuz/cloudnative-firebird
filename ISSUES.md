# Known issues

Problems found while building journal replication against the official
[`firebirdsql/firebird`](https://github.com/FirebirdSQL/firebird-docker) image
(`firebirdsql/firebird:5`, server `LI-V5.0.4.1812`), with reproduction steps and how the
operator handles them. Open follow-up work is listed in [TODO.md](TODO.md).

The reproduction scripts in [`hack/repro/`](hack/repro) need only Docker. Each one starts its
own containers and removes them when it finishes.

| # | Issue | Kind | Reproduces |
|---|-------|------|------------|
| 1 | A publishing database hangs under concurrent connect / commit / disconnect | Firebird bug, open | Yes: 7 of 7 runs on 5.0.4, 3 of 3 on the 6.0 snapshot, 2 of 3 on 4.0.7; 0 of 7 with pooled connections or without publication |
| 2 | Commit journaled before its TIP state: lock-based replica copies miss transactions | Firebird behaviour, handled | Yes: window transactions in 18 of 20 backups; 2 transactions lost in 3 of 3 documented-procedure replicas (8 writers) |
| 3 | A physical copy inherits publication; a publishing replica fast-forwards past segments | Expected behaviour, handled | Yes, always |
| 4 | `nbackup -B 0` copies record the still-active segment | Expected behaviour, handled | Yes, always |
| 5 | `gstat -h` omits "Replication sequence" while it is 0 | Minor | Yes, always |
| 6 | The official image keeps the security database on the container filesystem | Image behaviour, handled | Yes, always |
| 7 | Firebird 6 refuses header statistics for a database in full shutdown | Firebird 6 behaviour, handled | Yes, always (6.0 snapshot) |

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

Other Firebird versions (`IMAGE=... hack/repro/publication-under-load.sh`, publication on, one
connection per commit, 3 s archive timeout):

| Image | Server | Runs | Result |
|-------|--------|------|--------|
| `firebirdsql/firebird:6-snapshot` | 6.0.0.2191 | 3 | all hung (after 10 s) |
| `firebirdsql/firebird:4` | 4.0.7 | 3 | 1 hung (after 20 s), 1 stopped progressing while the server still answered, 1 completed |

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

## 2. Commit journaled before its TIP state: lock-based copies miss transactions (confirmed)

**What happens.** In `TRA_commit` (`src/jrd/tra.cpp`), `REPL_trans_commit()` journals the
commit before `TRA_set_state()` marks the transaction committed in the TIP. A backup lock
(`ALTER DATABASE BEGIN BACKUP`, `nbackup -L`, `nbackup -B`) that lands between the two switches
the journal (`REPL_journal_switch` in `src/jrd/nbak.cpp`) after the commit was journaled in
segment *S*, while the TIP write goes to the delta file. The copy holds the transaction as not
committed, although the journal has its commit in a segment the copy's header (sequence *S*)
says it includes. A replica built from the copy with Firebird's documented procedure
(`nbackup -SEQ -F`, `gfix -replica read_only`) skips segment *S* and never commits it.

**Reproduce.**

```sh
hack/repro/tip-window.sh                                        # counts window transactions per backup
WRITERS=8 CONNECTIONS=persistent ROWS=3000 hack/repro/replica-seed-race.sh   # documented procedure: rows missing
```

`tip-window.sh` takes level-0 backups (`action_nbak`, like the operator's physical backups) under
8 writers, restores each with its sequence (`nbackup -SEQ -R`) and lists the transactions whose
commit (a block ending with `opCommitTransaction`) is in a segment <= *S* but which the copy holds
as not committed (`RDB$GET_TRANSACTION_CN` -2). Persistent connections avoid issue 1.

**Observed** (Firebird 5.0.4):

| Test | Runs | Result |
|------|------|--------|
| `tip-window.sh`, 8 writers | 20 backups | 27 window transactions, in 18 of the 20 copies |
| `replica-seed-race.sh`, 8 writers, persistent connections | 3 | 2 transactions lost on the replica in every run |
| `replica-seed-race.sh`, 1 to 4 writers (earlier) | 6 | none lost |
| the operator's live seed (`allowLiveSeedFromPrimary`), 8 writers | 4 | replica identical to the primary; e.g. 4 window transactions replayed from segment 3 |

With per-row conflicts the loss is partly masked: the next transaction's update of the missing
row is applied as an insert ("record being updated does not exist, inserting instead"), so the
row counts match and only the lost transaction's other changes are missing.

**Operator handling.** New replicas are seeded without locking the primary:

1. a ready replica serves the seed copy: nothing commits on a replica, so it has no window;
2. otherwise the primary serves its *offline bootstrap seed*, a plain file copy taken when the
   database was created in the init container, before the server started;
3. a locked copy of the live primary is used only with
   `spec.replication.allowLiveSeedFromPrimary: true`. For those seeds, the new replica lists
   the copy's uncommitted transactions (`RDB$GET_TRANSACTION_CN <= 0`) that the primary's
   journal contains in its replica control file, so Firebird replays exactly those
   transactions: the window transactions are among them.

Point-in-time recovery plans the same way (`pitr-plan.pl`, which also covers transactions
started after the copy's next transaction and checks that each starts in the segments it has).
Plain physical restores are not affected: a window transaction was not yet committed for its
client when the lock was taken, so the restored database is consistent as of the lock.

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

## 7. Firebird 6 refuses header statistics for a database in full shutdown

**What happens.** On the Firebird 6.0 snapshot (`firebirdsql/firebird:6-snapshot`, server
`LI-T6.0.0.2191`), `fbsvcmgr host:service_mgr action_db_stats dbname <db> sts_hdr_pages` fails
with `database <db> shutdown` once the database is in full shutdown. Firebird 4 and 5 return the
header, with "full shutdown" in its attributes. `gstat -h host:<db>` no longer reads a database
on another host either: it fails with `No such file or directory`.

```sh
fbsvcmgr host:service_mgr action_properties dbname /db prp_shutdown_mode prp_sm_full prp_force_shutdown 0
fbsvcmgr host:service_mgr action_db_stats dbname /db sts_hdr_pages   # Firebird 6: "database /db shutdown"
```

**Handling.** Planned switchover reads the old primary's final replication sequence after the
full shutdown: when the service manager refuses, it asks the old primary's segment server
(`HEADER`), which reads the sequence from the header page on disk (nothing writes it in full
shutdown). The fencing Job takes that error as "in full shutdown". The readiness probe treats
any failure as not ready, so a fenced instance stays unready either way.

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
