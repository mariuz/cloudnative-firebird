# Declarative databases on replicated clusters

Status: design, 2026-11-20. Tracked in [TODO.md](../TODO.md) ("Declarative databases on replicated
clusters").

## The gap

A `FirebirdDatabase` works on clusters without replication only (`databaseClusterError` in
`operator/src/utils/databases.ts`, enforced by the admission webhook and the controller). A
replicated cluster has its one database, `spec.databaseName`. Replication covers that one file
from end to end:

| Layer | Where | One database because |
|---|---|---|
| `replication.conf` | `buildReplicationConf` (`utils/replication.ts`) | one `database = <path>` section; journal, archive and source directories per cluster (`replicationDirectories`) |
| Pod environment | `replicationEnv`, `buildReplicationContainers` | `DATABASE_PATH`, `JOURNAL_DIR`, `ARCHIVE_DIR`, `SOURCE_DIR`, `STATE_FILE`, … once per pod |
| Segment server | `fb-agent/src/server.rs`, `status.rs`, `seed.rs`, `promote.rs`, `role.rs` | one `Database`, one archive, one `Files`, one STATE probe; `control_path` takes "the only {GUID} file" of the source directory |
| Segment protocol | `server.rs` (LIST, GET, WAIT, STATE, HEADER, POSITION, SEED, TXNS, PLAN, PROMOTE, …) | no request names a database |
| Puller | `puller.rs` | one source directory, one state file |
| Replication init | `init-instance.sh`, `init.rs` | promotion/demotion, publication, journal floor, seeding of `$DATABASE_PATH` |
| Switchover, failover, sync standby | `jobs.rs`, `switchover.ts`, `synchronous.ts` | `DATABASE_PATH`; candidates ranked by one stream's position; one `sync.conf` |
| Isolation check, metrics | `isolation.rs`, `metrics.rs` | `DATABASE_PATH` |
| Lag status and metrics | `replication-lag.ts`, `ReplicaLagStatus`, `firebird_replication_lag_*` | keyed by replica only |
| Backups | `backup.ts`, `pitr-restore.sh`, `volume-snapshot.ts`, replica clusters | physical backups, the journal archive upload, PITR and snapshots cover the cluster database |

## Design

### Every database follows the cluster's primary

The cluster keeps one primary instance. Every replicated database is published on it and is a
replica on every other instance. A switchover or failover moves all of them together. There is
no per-database primary, which keeps the Services, the Lease, fencing and the rolling update as
they are.

### Layout on the volume

The cluster database keeps the directories it has today (`<base>/journal`, `archive`, `source`,
`.last-pulled`), so existing clusters change nothing on disk. A declarative database `<file>`
(the file name of `managedDatabasePath`, e.g. `app.fdb`) gets its own tree:

```
<base>/databases/<file>/journal
<base>/databases/<file>/archive
<base>/databases/<file>/source
<base>/databases/<file>/.last-pulled
<base>/databases/<file>/sync.conf
```

`replication.conf` gets one `database = <path>` section per database with those directories. The
sections are otherwise identical to today's one.

### One agent, several streams

The agent keeps one process per role (one segment server, one puller per instance). It learns the
databases from a list in the cluster ConfigMap (`replicated-databases`: one `<file>` per line, the
cluster database implied). It does not get one environment variable set per database.

- **Segment server**: a map from database file to its `Files` (database, archive, source, seeds,
  STATE probe, prune and recovery-point threads).
- **Protocol**: an optional selector in front of the request, inside the signed line:
  `DB <file> <request>`, e.g. `DB app.fdb LIST`. Without it a request is about the cluster
  database, as today, so servers and clients of different versions keep working during a rolling
  update. A server of an earlier version answers `ERR bad request` to a selector. A client then
  treats that database as not yet served there and asks again later; this is how the puller already
  handles WAIT.
  - The segment names already carry the database's file name (`<base>.journal-<seq>`), so a GET
    can be checked against the selected archive.
  - PING, VERSION, the backup files (FILE, STORE, …), ISOLATION, PRIMARYSEEN and FENCE stay
    instance-wide and take no selector.
- **Puller**: one loop per database, each with its own source directory and state file, sharing
  the connection settings and TLS.
- **Replication init**: the same steps per database (publication on the primary, replica mode and
  the journal floor elsewhere, seeding when a replica has no copy). The cluster database comes
  first; a declarative database that cannot be seeded yet does not hold up the instance, and its
  puller waits for a seed.

### Creating and dropping a database

On a replicated cluster the `FirebirdDatabase` Job creates the database on the primary only, then
enables its publication (`ALTER DATABASE ENABLE PUBLICATION`, `INCLUDE ALL TO PUBLICATION`).

The operator then adds the file to `replicated-databases` and to `replication.conf`. The pod
template carries the replication configuration's hash, so this rolls the instances, replicas
first and the primary last. When a replica restarts, its init container finds no copy of the new
database and seeds it from the primary's segment server (SEED), as a new replica is seeded today.
Its puller then follows.

Dropping a database reverses this. The operator removes the file from the configuration (another
roll), drops it on the primary, deletes the copies and the database's directory tree on every
instance, and removes it from the archive upload (below).

Adding or removing a replicated database therefore restarts the instances once, primary last.
That is the price of keeping `replication.conf` a file Firebird reads at attach time. The
alternative, a configuration reload without a restart, would need the Firebird server to re-read
`replication.conf` for an attached database, which it does not do.

### Promotion

- **Switchover** (Job and in-place promotion): the in-place promotion runs per database, the
  cluster database first. The target is promoted once every database has applied its last archived
  segment. If one database cannot be promoted in place, the target falls back to a re-seed of that
  database from the new primary, as the cluster database does today.
- **Failover**: candidates are ranked by the cluster database's position, then by the least
  position among the other databases. A candidate that has no copy of a declarative database yet
  (still seeding) ranks below one that has.
- **Synchronous replication** (`replication.mode: sync`): one `sync.conf` per database. The standby
  is the same instance for all of them.

### Status and metrics

- `status.replication.lag` gains a `databases` list per replica, with the same fields as today,
  for the declarative databases. The cluster database's fields stay where they are.
- `firebird_replication_lag_seconds` and `firebird_replication_lag_segments` gain a `database`
  label. The rules in `config/monitoring/firebird.rules.yaml` aggregate over it, so an alert fires
  for the worst database.
- Read-only routing (`readOnlyRouting`) takes a replica's worst lag over all its databases.

### Backups

- A `FirebirdBackup` already names a database (`spec.database`) for logical backups. Physical
  backups (nbackup) and volume snapshots gain the same field.
- The journal archive upload covers every replicated database, under `<prefix>/<file>/`. The cluster
  database keeps today's prefix.
- PITR restores one database at a time, as it does now. Its target is named by `spec.database`.
- A replica cluster (`replica-cluster.ts`) replicates the cluster database only until a later phase.
  Its configuration refuses declarative databases until then.

## Phases

The admission webhook keeps refusing a `FirebirdDatabase` on a replicated cluster until phase 3 is
released. Phase 2 is reachable only through the cluster annotation
`firebird.cloudnative-firebird.io/replicated-databases: experimental`, used by the CI.

1. **Agent: several streams.** *(1a, the segment server and the puller: v0.177.0; 1b, the
   replication init and seeding, `init-databases.sh`: v0.178.0)* `DB <file>` in the segment server and its clients. The per-database
   `Files` map, the puller loop per database and the replication init per database, all driven by
   `replicated-databases`. With an empty list the agent behaves as today; the conformance tests
   prove it, and new ones cover two databases. No operator change.
2. **Operator: create, replicate, drop.** `replication.conf` sections, the `replicated-databases`
   key, the `FirebirdDatabase` Job against the primary with publication, the roll, seeding of the
   replicas, dropping, and lag status and metrics per database. This phase is behind the annotation.
   The kind CI creates a second database on a three-instance cluster and checks rows replicated to
   both replicas.
3. **Promotion.** Switchover (Job and in-place), failover ranking and synchronous replication over
   every database. The admission webhook accepts `FirebirdDatabase` on replicated clusters from
   here. The kind CI repeats its switchover and failover checks with two databases.
4. **Backups.** The journal archive upload per database, PITR's and physical backups'
   `spec.database`, and volume snapshots of every database. Replica clusters follow.

## Open questions

- **The roll on add and remove.** Recommended: accept it (above), and say so in the
  `FirebirdDatabase` documentation and events. Adding a database to a replicated cluster then takes
  as long as a rolling update.
- **Databases that were not declared.** A file someone created by hand with `isql` on the primary
  is not replicated. The operator does not discover databases; it replicates what is declared.
- **How many databases.** Each adds one puller loop, one STATE attachment and one archive. Nothing
  is quadratic, but the cost grows with each database. The webhook can cap it (say 16) until
  measured.
