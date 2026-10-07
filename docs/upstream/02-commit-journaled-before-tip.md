# A replica created from a locked copy can miss transactions committed at the moment of the lock

*Draft issue for [FirebirdSQL/firebird](https://github.com/FirebirdSQL/firebird/issues), ready to
file. Found while building journal replication for a Kubernetes operator
([ISSUES.md](../../ISSUES.md) issue 2).*

---

**Title:** Replication: commit is journaled before its TIP state, so a backup lock in between
makes a replica created from the copy skip the transaction

## Summary

`TRA_commit` journals a commit (`REPL_trans_commit`) before it marks the transaction committed
on the TIP (`TRA_set_state`). A backup lock (`nbackup -L`, `nbackup -B`,
`ALTER DATABASE BEGIN BACKUP`) that lands between the two switches the replication journal
after the commit was written to segment *S*, while the TIP write goes to the delta file. The
copy then holds the transaction as not committed, but its header says it includes every
segment up to *S*. A replica created from it with the documented procedure
(`nbackup -SEQ -F`, then `gfix -replica read_only`) starts after *S*, so it never applies that
transaction: the replica silently differs from the primary.

## Versions

5.0.4.1812 (official Docker image `firebirdsql/firebird:5`), Linux x64, SuperServer. The
ordering below is unchanged in `master` (`tra.cpp:546-547`) as of this writing.

## Reproduce

Only Docker is needed. With the scripts from
[`hack/repro/`](https://github.com/mariuz/cloudnative-firebird/tree/main/hack/repro):

```sh
# documented replica creation under load, then compares primary and replica row by row
WRITERS=8 CONNECTIONS=persistent ROWS=3000 ./replica-seed-race.sh

# counts the transactions in the window directly, for level-0 backups taken under load
./tip-window.sh
```

`replica-seed-race.sh` runs 8 writers (one persistent connection each; each transaction
inserts a row and updates the previous one) against a publishing database, and meanwhile
creates a replica as documented:

```sh
nbackup -L mydb.fdb && cp mydb.fdb copy.fdb && nbackup -N mydb.fdb
nbackup -SEQ -F copy.fdb
gfix -replica read_only copy.fdb   # then serve it to the replica with journal_source_directory
```

It then waits for the replica to apply the archived journal and compares both databases.

`tip-window.sh` takes level-0 backups (`fbsvcmgr action_nbak`) under the same load, restores
each with its sequence (`nbackup -SEQ -R`), and lists the transactions that the copy holds as
not committed (`RDB$GET_TRANSACTION_CN` -2) although their commit block
(`opCommitTransaction`) is in an archived segment <= *S*.

## Observed (5.0.4)

| Test | Result |
|------|--------|
| `replica-seed-race.sh`, 8 writers | 2 transactions missing on the replica in 3 of 3 runs |
| `replica-seed-race.sh`, 1 to 4 writers | none missing in 6 runs (the window is narrow) |
| `tip-window.sh`, 20 backups | 27 window transactions, in 18 of the 20 copies |
| `tip-window.sh`, 10 backups (repeated later) | 10 window transactions, in 8 of the 10 copies |

When later transactions update the missing rows, the replica's log shows
`record being updated does not exist, inserting instead`, which hides the loss in row counts:
only the lost transaction's other changes are missing.

## Analysis

`src/jrd/tra.cpp`, `TRA_commit` (5.0.4, lines 539-542):

```cpp
// Set the state on the inventory page to be committed

REPL_trans_commit(tdbb, transaction);
TRA_set_state(tdbb, transaction, transaction->tra_number, tra_committed);
```

`src/jrd/nbak.cpp`, `BackupManager::beginBackup` (line 356) switches the journal while
setting the stalled state:

```cpp
header->hdr_flags = (header->hdr_flags & ~Ods::hdr_backup_mask) | newState;
...
REPL_journal_switch(tdbb);
```

So with a lock between the two calls:

1. the commit record is in segment *S* (journaled before the switch);
2. the TIP page write happens after the lock, so it goes to the delta file, not the main file;
3. the copy of the main file has the transaction active/dead, and replication sequence *S*;
4. the replica, positioned after *S*, never sees the commit again.

A plain restore of such a copy is consistent (the client had not been told the commit
succeeded when the lock was taken); only replicas built on it lose the transaction.

## Possible fixes

- Hold the backup state shared (`BackupManager` state lock, as page writes do) across
  `REPL_trans_commit` and `TRA_set_state`, so a lock cannot land between them; or
- make the journal switch in `beginBackup` wait for commits that are journaled but not yet on
  the TIP; or
- record in the copy (or in the segment) which transactions were journaled as committed before
  the switch, so `nbackup -SEQ -F` / the replica can replay them.

Reversing the two calls would change what a failed journal write means for the commit, so it
is probably not an option.

## Workaround (what the operator does)

Seed replicas from another replica (nothing commits there), from a copy taken before the
server started, or, for a locked copy of a live primary, list the copy's uncommitted
transactions that have journal blocks in segments <= *S* in the new replica's control file so
that they are replayed.
