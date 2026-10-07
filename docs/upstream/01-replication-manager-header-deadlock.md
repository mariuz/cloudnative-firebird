# Deadlock creating the replication manager: header page latched shared twice while a writer waits

*Draft issue for [FirebirdSQL/firebird](https://github.com/FirebirdSQL/firebird/issues), ready to
file. Found while building journal replication for a Kubernetes operator
([ISSUES.md](../../ISSUES.md) issue 1).*

---

**Title:** Server hangs when the replication manager is created under concurrent load:
`Replication::Manager::Manager` latches the header page shared twice while a writer is queued

## Summary

A database with publication enabled (journal and archive configured in `replication.conf`)
stops answering after about 10 seconds when several clients each repeatedly connect, insert a
row, commit and disconnect. New connections hang too; the server has to be killed. Thread
stacks of the hung server (5.0.4 with its debug symbols) show a self-deadlock on the header
page latch inside the replication manager's constructor, which holds
`GlobalObjectHolder::m_mutex` at the same time, so every replicating statement then blocks
as well.

## Versions

| Server | Result |
|--------|--------|
| 5.0.4.1812 (`firebirdsql/firebird:5`), 8 writers | hung in 4 of 4 runs, within 10 s |
| 5.0.4.1812, 4 writers | hung in 7 of 7 runs on one host, 0 of 5 on another (4 vCPUs) |
| 6.0 snapshots (`firebirdsql/firebird:6-snapshot`), 4 writers | hung in 3 of 3 runs (6.0.0.2191) and 2 of 2 (6.0.0.2196) |
| 4.0.7.3271 (`firebirdsql/firebird:4`), 4 writers | 2 of 3 runs hung or stopped progressing |

Linux x64, SuperServer, official Docker images. The code below is unchanged in `master` and
`v5.0-release` as of this writing.

Controls (5.0.4): without publication, and with one persistent connection per writer
(a connection pool), every run completed.

## Reproduce

Only Docker is needed. With the scripts from
[`hack/repro/`](https://github.com/mariuz/cloudnative-firebird/tree/main/hack/repro):

```sh
./publication-under-load.sh                         # hangs within about 10 s
PUBLICATION=0 ./publication-under-load.sh           # control: completes
CONNECTIONS=persistent ./publication-under-load.sh  # control: completes
```

What the script does, in short:

```sh
# replication.conf
database = /var/lib/firebird/data/mydb.fdb
{
    journal_directory = /var/lib/firebird/data/replication/journal
    journal_archive_directory = /var/lib/firebird/data/replication/archive
    journal_archive_command = "test ! -f $(archivepathname) && cp $(pathname) $(archivepathname)"
    journal_archive_timeout = 3
}
```

```sql
ALTER DATABASE ENABLE PUBLICATION;
ALTER DATABASE INCLUDE ALL TO PUBLICATION;
CREATE TABLE app (id INT PRIMARY KEY, note VARCHAR(10));
```

Then 8 concurrent writers, each running 600 times, through a new connection every time:

```sh
echo "insert into app values ($i, 'w'); update app set note = 'u' where id = $i - 1; commit;" \
  | isql -q localhost:/var/lib/firebird/data/mydb.fdb
```

`KEEP=1 ./publication-under-load.sh` leaves the hung server running, and
`./capture-stacks.sh fb-repro-load` prints its thread stacks with the release's debug
symbols (gdb on the Docker host).

## Stacks

Four hung 5.0.4 servers all show the same picture (full output of one:
[`01-replication-manager-header-deadlock-stacks.txt`](01-replication-manager-header-deadlock-stacks.txt)).

The thread creating the replication manager, holding `GlobalObjectHolder::m_mutex` and a
shared latch on the header page, waits for a second shared latch on the header page:

```
Thread 10
  #8  Jrd::BufferDesc::addRef at jrd/cch.cpp:5187
  #9  get_buffer at jrd/cch.cpp:3815
  #10 CCH_fetch_lock at jrd/cch.cpp:872
  #11 CCH_fetch at jrd/cch.cpp:801
  #13 PAG_get_clump at jrd/pag.cpp:1054
  #14 Jrd::Database::getReplSequence at jrd/Database.cpp:394
  #15 Replication::Manager::Manager at jrd/replication/Manager.cpp:133
  #16 Jrd::Database::GlobalObjectHolder::getReplManager at jrd/Database.cpp:724
  #18 (getReplicator) at jrd/replication/Publisher.cpp:131
  #21 REPL_store at jrd/replication/Publisher.cpp:516
  #22 Jrd::StoreNode::store at dsql/StmtNodes.cpp:8220
```

A transaction start waits for an exclusive latch on the header page:

```
Thread 13
  #8  Jrd::BufferDesc::addRef at jrd/cch.cpp:5187
  #13 bump_transaction_id at jrd/tra.cpp:2099
  #14 transaction_start at jrd/tra.cpp:3552
  #15 TRA_start at jrd/tra.cpp:1789
```

The other statements wait for `m_mutex` (1 to 4 threads per hang):

```
Thread 7
  #2  Firebird::Mutex::enter at common/classes/locks.h:209
  #4  Jrd::Database::GlobalObjectHolder::getReplManager at jrd/Database.cpp:721
  #9  REPL_store at jrd/replication/Publisher.cpp:516
```

and new attachments wait for the header page (`PAG_header` from `JProvider::internalAttach`)
or for the database's existence mutex.

## Analysis

`Replication::Manager::Manager` (`src/jrd/replication/Manager.cpp`, 5.0.4) latches the header
page shared, then calls `getReplSequence()`, which latches it shared again:

```cpp
// Manager.cpp:125-133
WIN window(HEADER_PAGE_NUMBER);
CCH_FETCH(tdbb, &window, LCK_read, pag_header);
...
// Call below will fetch header page with LCK_read lock, it is allowed and OK.
m_sequence = dbb->getReplSequence(tdbb);      // -> PAG_get_clump: CCH_FETCH(..., LCK_read, ...)
```

The second shared latch is not always granted. The page latch is a `SyncObject` taken
without an owner (`BufferDesc::addRef`, `bdb_syncPage.lock(NULL, ...)`), and
`SyncObject::lock` grants a shared request only while no one waits (`while (waiters == 0) //
fair locking`, `src/common/classes/SyncObject.cpp`). If another thread asks for the header page
exclusively between the two fetches (`bump_transaction_id`, `tra.cpp:2099`,
`CCH_FETCH(..., LCK_write, ...)` for every new transaction), the writer waits for the first
shared latch and the second shared request waits behind the writer:

1. thread A: `getReplManager` takes `m_mutex`, the constructor latches the header shared;
2. thread B: `bump_transaction_id` requests the header exclusive and waits for A;
3. thread A: `PAG_get_clump` requests the header shared again and waits behind B.

Neither can proceed, and A still holds `m_mutex`: every replicating statement then blocks in
`getReplManager`, and every attachment and transaction start blocks on the header page.

Why only with a connection per transaction: the manager is created when a replicating
attachment first needs it after the database was opened. The hung servers were creating it
10 seconds into the load, so it is created again during the run, presumably each time the
database is reopened after its last attachment closed. Each creation is a chance to hit the
window while other attachments start transactions. With pooled connections it is created once,
before the load.

## Possible fixes

- Read the sequence from the page already latched in the constructor instead of fetching it
  again (e.g. a `getReplSequence` overload taking the header page, or `PAG_get_clump` on the
  window's buffer).
- Or create the manager without holding the header latch across a second fetch, if the race
  the comment describes (concurrent sequence change while the change log is created) can be
  covered otherwise.

Either way, `CCH_FETCH` of a page the thread already holds shared, with writers possible,
looks unsafe in general with the fair `SyncObject`; other recursive fetches of the header page
may deserve a look.

## Workaround

Pooled (persistent) connections: no hang in any run.
