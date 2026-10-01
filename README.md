# cloudnative-firebird

A cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/) database, written in TypeScript. Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg).

## Overview

`cloudnative-firebird` automates the deployment and lifecycle management of Firebird database clusters on Kubernetes. It introduces the `FirebirdCluster` Custom Resource Definition (CRD) and a controller that reconciles cluster state.

### Features

- **Declarative cluster management** via `FirebirdCluster` CRD
- **StatefulSet-based** deployment for stable pod identity and storage
- **Persistent storage** via PersistentVolumeClaims, with online volume expansion when `spec.storage.size` grows
- **Declarative hibernation** (`spec.hibernated`) that scales to zero while keeping data
- **Journal-based asynchronous replication** (Firebird 4, 5 and the 6.0 snapshot, experimental) with replicas seeded without locking the primary
- **Backups and restores** as Jobs against the primary: `gbak`/`nbackup` through the service manager, logical and physical backups to S3, `FirebirdBackup` / `FirebirdScheduledBackup` / `FirebirdRestore` resources
- **Bootstrap** a new cluster from an S3 backup or by cloning another cluster
- **Declarative users** (`FirebirdUser`, after CloudNativePG's `DatabaseRole`) with Secret-backed passwords, role grants and a reclaim policy; users persist across pod restarts
- **Declarative roles** (`FirebirdRole`): a database role and exactly the privileges it holds on tables, views, procedures, functions, packages, sequences and exceptions
- **Automatic failover** (opt-in) to the most advanced replica; the old primary is re-seeded when it returns
- **Planned switchover** with the `targetPrimary` annotation: no data loss, the other replicas continue without re-seeding
- **Rolling updates with the primary last** (`primaryUpdateStrategy` / `primaryUpdateMethod`, as in CloudNativePG): replicas are restarted one at a time, then the primary is restarted or switched over
- **Replica re-seeding** with a pod annotation (CloudNativePG `unrecoverable`)
- **Instance fencing** via the `fencedInstances` annotation (CloudNativePG format): the database is shut down, the pod keeps running
- **Lag-aware read-only routing** to replicas via the `<name>-replica` Service (`spec.replication.readOnlyRouting`)
- **Secret-based** SYSDBA password management
- **Automatic service creation** (ClusterIP + headless for StatefulSet DNS)
- **Status reporting** with conditions and phase tracking, and **Kubernetes events** for switchovers, failovers, fencing, re-seeding, rolling updates, backups, restores and users
- **Owner references** for automatic garbage collection of child resources
- **Graceful shutdown** with SIGTERM/SIGINT handling

## Architecture

```
                    ┌─────────────────────┐
                    │  FirebirdCluster CR  │
                    │  (Your manifest)     │
                    └────────┬────────────┘
                             │ watches
                    ┌────────▼────────────┐
                    │  Operator (TS)       │
                    │  - Controller        │
                    │  - Reconcile loop    │
                    └────────┬────────────┘
                             │ creates/manages
          ┌──────────────────┼──────────────────┐
          │                  │                  │
┌─────────▼──────┐  ┌────────▼───────┐  ┌──────▼──────────┐
│  StatefulSet   │  │   Service      │  │  Headless Svc   │
│  (Firebird     │  │  (ClusterIP)   │  │  (Pod DNS)      │
│   instances)   │  │  port 3050     │  │  port 3050      │
└────────────────┘  └────────────────┘  └─────────────────┘
         │
┌────────▼───────┐
│ PersistentVol  │
│ ClaimTemplates │
│ (Firebird data)│
└────────────────┘
```

## Quick Start

### Prerequisites

- Kubernetes cluster (1.24+)
- `kubectl` configured to point at your cluster

### Firebird versions

The operator runs the official [`firebirdsql/firebird`](https://hub.docker.com/r/firebirdsql/firebird)
images (`spec.imageName`, default `firebirdsql/firebird:latest`, currently 5.0). Replication,
planned switchover, failover, fencing, re-seeding, backups and restores, and point-in-time
recovery (into a running cluster or as a bootstrap) were verified end to end with:

| Image | Server | ODS |
|-------|--------|-----|
| `firebirdsql/firebird:4` | 4.0.7 | 13.0 |
| `firebirdsql/firebird:5` (`latest`) | 5.0.4 | 13.1 |
| `firebirdsql/firebird:6-snapshot` | 6.0.0.2191 (snapshot, commit 4ca39c2) | 14.0 |

Firebird 6 is not released yet: a snapshot is a development build, and its on-disk structure can
still change before the release. Two Firebird 6 changes matter to the operator, both handled:
the header page layout of ODS 14 (switchover writes the replication sequence there), and header
statistics through the service manager, which Firebird 6 refuses for a database in full shutdown
([ISSUES.md](ISSUES.md), issue 7). Firebird 6 also names journal segments
`<database>_<guid>.journal-<n>`.

### Install the CRDs

```bash
kubectl apply -f config/crds/
```

### Deploy the Operator

```bash
kubectl apply -f config/deploy/namespace.yaml
kubectl apply -f config/deploy/serviceaccount.yaml
kubectl apply -f config/deploy/rbac.yaml
kubectl apply -f config/deploy/deployment.yaml
```

### Create a Firebird Cluster

```bash
# Create the superuser secret
kubectl apply -f config/samples/secret.yaml

# Create a minimal Firebird cluster
kubectl apply -f config/samples/firebirdcluster_minimal.yaml
```

Check the cluster status:

```bash
kubectl get firebirdclusters
kubectl describe firebirdcluster my-firebird-cluster
```

Connect to the database (port-forward for local testing):

```bash
kubectl port-forward svc/my-firebird-cluster 3050:3050
```

## FirebirdCluster CRD Reference

Invalid specs are rejected when they are applied: the CRDs carry OpenAPI constraints and CEL
validation rules (`x-kubernetes-validations`) for the same checks the operator runs on every
reconcile, e.g. mutually exclusive bootstrap sources, cron schedules, physical backups to S3 without replication,
restore paths outside the data directory, `sync` replication, shrinking `storage.size` and
immutable `clusterName` / `username` fields. No admission webhook is needed. The operator still
validates each reconcile (for objects created before an upgrade) and reports `Degraded`.
`hack/crd-validation/test.sh` checks the rules against an API server.

```yaml
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdCluster
metadata:
  name: my-cluster
  namespace: default
spec:
  # Number of Firebird instances (required, 1-10)
  instances: 1

  # Docker image (defaults to firebirdsql/firebird:latest)
  imageName: firebirdsql/firebird:latest

  # Reference to Secret containing SYSDBA password (key: "password")
  superuserSecret:
    name: my-firebird-secret

  # Storage configuration (required)
  # Growing size expands existing PVCs in place (needs allowVolumeExpansion
  # on the StorageClass); shrinking is rejected by the API server.
  storage:
    size: 1Gi
    storageClass: standard   # optional

  # Container resource requests/limits (optional)
  resources:
    requests:
      cpu: "100m"
      memory: "256Mi"
    limits:
      cpu: "500m"
      memory: "512Mi"

  # Scheduled backups (optional): see "Backups and Restores" below
  backup:
    enabled: true
    schedule: "0 2 * * *"

  # Prometheus monitoring (optional)
  monitoring:
    enablePodMonitor: true

  # Database file created on each instance's PVC (optional, defaults to mydb.fdb)
  databaseName: mydb.fdb

  # Additional container environment variables (optional)
  env:
    - name: TZ
      value: UTC
```

### Instance Runtime

Instances run the official [`firebirdsql/firebird`](https://github.com/FirebirdSQL/firebird-docker)
image. The PVC is mounted at the image data directory `/var/lib/firebird/data`, where the
entrypoint creates `databaseName` on first start and runs `bootstrap.initSql`. The superuser
Secret sets the SYSDBA password (`FIREBIRD_ROOT_PASSWORD`) and is exposed to client tools as
`ISC_USER`/`ISC_PASSWORD`, and `config.settings` are applied through `FIREBIRD_CONF_<key>`
environment variables, so changing them rolls the pods. Sweep and diagnostics Jobs connect to
the primary instance by its headless-Service name (like backups, and updated after a switchover or
failover) instead of mounting the instance PVC; diagnostics
use online validation (`fbsvcmgr action_validate`), which works while clients are connected.

### Security Contexts

The official image runs the Firebird server as root, so the instance pods cannot meet the
`restricted` Pod Security Standard; they meet `baseline` and drop what they can
(CloudNativePG 1.28 `podSecurityContext` / `securityContext`):

| | Instance pods (all containers) | Operator Jobs (backups, restores, archive, maintenance, fencing, switchover, users) |
|---|---|---|
| user | root (image default) | `firebird` (uid 84), `runAsNonRoot` |
| capabilities | all dropped except `CHOWN`, `DAC_OVERRIDE` (the server's firebird-owned lock directory), `FOWNER` (init scripts) | all dropped |
| privilege escalation | no | no |
| seccomp | `RuntimeDefault` | `RuntimeDefault` |
| Pod Security Standard | `baseline` | `restricted` |

`spec.podSecurityContext` and `spec.securityContext` are merged over the instance defaults, e.g.
to add `supplementalGroups` or use a different `fsGroup`:

```yaml
spec:
  podSecurityContext:
    fsGroup: 2000
  securityContext:
    readOnlyRootFilesystem: false
```

Changing them (and upgrading from a version without these defaults) rolls the instances.

### Status Fields

| Field | Description |
|-------|-------------|
| `phase` | Current cluster phase: `Creating`, `Running`, `Updating`, `Degraded`, `Deleting`, `Paused`, `Hibernated` |
| `instances` | Configured number of instances |
| `readyInstances` | Number of ready instances |
| `conditions` | Standard Kubernetes status conditions (`Ready`, `Progressing`, `Degraded`) |
| `replicationStatus` | Primary pod, number of active replicas, and with read-only routing the `readRoutablePods` and `laggingReplicas` |
| `volumes` | Per-PVC requested size, capacity and expansion state (`Ready`, `Resizing`, `ResizeFailed`, `ShrinkRejected`) |
| `rollingUpdate` | With replication: the StatefulSet revision being rolled out, the outdated instances and the current step |

### Hibernation

Set `spec.hibernated: true` to stop a cluster without losing data: the StatefulSet is
scaled to zero, backup/sweep/diagnostics/journal-archive CronJobs are suspended, and the
PodDisruptionBudget is removed so node drains are not blocked. PVCs, Services and
configuration are retained, and the cluster reports the `Hibernated` phase. Set it back
to `false` to resume with the same volumes.

```bash
kubectl patch firebirdcluster my-cluster --type merge -p '{"spec":{"hibernated":true}}'
```

### Replication (experimental)

> **Experimental.** On Firebird 5.0.4 (`firebirdsql/firebird:5`) a database that publishes to a
> replication journal can hang when several clients connect, commit and disconnect concurrently
> (one connection per transaction). Applications using connection pools were not affected in
> testing. See [ISSUES.md](ISSUES.md), issue 1, before enabling replication in production.

With `spec.replication.enabled: true` (asynchronous mode), instance 0 is the primary and the
other instances are read-only replicas:

- The primary's init container creates the database offline with publication enabled, runs
  `bootstrap.initSql`, and keeps an **offline bootstrap seed** (a file copy taken before the
  server starts). Full journal segments are archived on the primary's volume.
- Two sidecars ship segments: `segment-server` serves the local archive and seed copies, and
  `segment-puller` fetches new segments from the current primary into the replica's
  `journal_source_directory`. They use only the Firebird image's perl, authenticate with the
  SYSDBA password, and never open a live database file directly (all access goes through the
  local server).
- A new replica is seeded from a **ready replica** (locked through that replica's server), or
  from the primary's offline bootstrap seed while every later segment is still archived. The
  live primary is only locked when `replication.allowLiveSeedFromPrimary` is set, so seeding
  adds no load to the primary.
- The seed stays usable: while no replica holds segments back (a single instance), the primary
  keeps the segments after the seed up to `maxSegmentRetentionHours` (7 days by default), so an
  instance added later can be seeded from it. Once they are pruned, the primary's init container
  takes a fresh offline copy the next time the primary starts after a clean stop (a rolling
  update, a restart). After an unclean stop the server goes on writing the journal segment it
  had open, so the refresh waits for the next clean restart.

```yaml
spec:
  instances: 3
  replication:
    enabled: true
    archiveTimeoutSeconds: 10     # ship partially filled segments after 10s
    segmentRetentionHours: 24     # keep archived segments on the primary for 24h
    maxSegmentRetentionHours: 168 # ... and up to 7 days while a replica has not applied them
```

Archived segments are deleted from the primary after `segmentRetentionHours`, except those a
replica has not applied yet: the operator sends the primary's segment server the lowest segment
the replicas applied (`status.replicationStatus.segmentRetention`), and those segments are kept up
to `maxSegmentRetentionHours` (default 168, never below `segmentRetentionHours`). A replica that
is slow or stopped for a while (a node drain, a long maintenance) then catches up from the archive
instead of needing a re-seed. A replica that is not ready keeps its last known position; one that
is scaled away no longer holds segments back. The floor is stored on the primary's volume.

With `pruneAppliedSegments: true` the primary deletes segments as soon as every replica has
applied them (below that floor; the floor segment itself may be partly applied), instead of
keeping them for `segmentRetentionHours`, so the archive is bounded by the replicas' progress.
With `journalArchiveS3`, a segment is also kept until the journal archive Job has uploaded it (it
reports the uploaded segments to the primary after each successful upload). New replicas are then
seeded from a ready replica, since the primary's offline bootstrap seed needs every segment after
it. The retention settings still apply to segments no replica has applied.

**Enabling replication on an existing cluster.** Setting `replication.enabled` on a running
cluster keeps the primary's data. The operator restarts the primary first; its init container
enables publication on the existing database offline and writes the offline bootstrap seed, as
for a database created with replication. The other instances are restarted next. Without
replication each of them had a database of its own, which cannot become a replica: it is kept
aside as `pre-replication-<timestamp>.fdb` in the instance's data directory (never deleted) and
the instance is seeded from the primary. A database that already publishes but has no seed gets
one on its next clean start (see above).

### Replication Lag

With replication, every reconcile (at least every 30 s) the operator asks the primary's segment
server for its archived journal segments and each ready replica's segment server for the segment
it has applied. A replica's lag is the number of archived segments it has not applied and the age
of the oldest of them:

```yaml
status:
  replicationStatus:
    primaryPod: my-cluster-0
    lastArchivedSequence: 1422
    replicas:
      - name: my-cluster-1
        appliedSequence: 1422
        pendingSegments: 0
        lagSegments: 0
        lagSeconds: 0
      - name: my-cluster-2
        appliedSequence: 1417
        pendingSegments: 3
        lagSegments: 5
        lagSeconds: 41
```

The lag is also written to each replica's `firebird.cloudnative-firebird.io/replication-lag-seconds`
annotation, which lag-aware read-only routing (`readOnlyRouting.maxLagSeconds`) uses to take
lagging replicas out of the `-replica` Service. Transactions still in the primary's active segment
are not counted (`archiveTimeoutSeconds` bounds them). With `networkPolicy.enabled`, the generated
policy lets the operator's pods reach the segment port (namespace from `OPERATOR_NAMESPACE`).

### Operator Metrics

The operator serves Prometheus metrics on `/metrics` of its health port (8080, container port
`http`), updated on every reconcile. The replication lag it measures is exported directly, so
alerting on it needs no exporter sidecar:

| Metric | Labels | Meaning |
|--------|--------|---------|
| `firebird_cluster_instances` | `namespace`, `cluster` | `spec.instances` |
| `firebird_cluster_ready_instances` | `namespace`, `cluster` | Instances whose pod is ready |
| `firebird_cluster_fenced_instances` | `namespace`, `cluster` | Fenced instances |
| `firebird_cluster_ready` | `namespace`, `cluster` | 1 when the phase is `Running` |
| `firebird_replication_last_archived_sequence` | `namespace`, `cluster` | Last segment archived on the primary |
| `firebird_replication_lag_seconds` | `namespace`, `cluster`, `pod` | Age of the oldest archived segment the replica has not applied |
| `firebird_replication_lag_segments` | `namespace`, `cluster`, `pod` | Archived segments the replica has not applied |
| `firebird_replication_pending_segments` | `namespace`, `cluster`, `pod` | Segments received but not applied yet |
| `firebird_replication_applied_sequence` | `namespace`, `cluster`, `pod` | Segment applied by the replica |
| `firebird_operator_reconciles_total` | `namespace`, `cluster`, `result` | Reconciles by result (`success`, `error`) |

A replica that cannot be measured has no lag series (alert on `absent()` or on
`firebird_cluster_ready_instances`); the series of a deleted cluster are dropped.
`config/deploy/podmonitor.yaml` scrapes the operator with the Prometheus Operator, and the
generated Grafana dashboard has ready-instance and replication-lag panels. For example:

```yaml
- alert: FirebirdReplicaLagging
  expr: firebird_replication_lag_seconds > 60
  for: 5m
```

### Planned Switchover

Promote a replica with the `targetPrimary` annotation (CloudNativePG's `kubectl cnpg promote`):

```bash
kubectl annotate firebirdcluster my-cluster firebird.cloudnative-firebird.io/targetPrimary=my-cluster-1 --overwrite
kubectl get firebirdcluster my-cluster -o jsonpath='{.status.switchover}'
```

1. **Stopping**: a Job puts the primary's database into full shutdown (no more writes; clients are
   disconnected), reads its last replication sequence *S*, and waits until segment *S* is archived
   and every ready replica, the target included, has applied it (the segment servers report each
   replica's control file position with a `POSITION` query).
2. **Promoting**: the operator moves the leader Lease and the `primary` ConfigMap entry to the
   target and restarts the target and the old primary. Their init containers work offline: the
   target's header gets replication sequence *S* (so its journal continues at *S + 1* and the other
   replicas keep applying without re-seeding), replica mode none and publication, plus a fresh
   offline bootstrap seed; the old primary becomes a read-only replica positioned after *S*.
   Replicas that were not ready are re-seeded.
3. **Completed** once both restarted pods are ready.

Writes are unavailable from the start of step 1 until the target is ready again (the Job plus two
pod restarts). If the Job fails (for example a replica does not catch up within five minutes) the
old primary is brought back online and stays primary; change the annotation to retry. There is
no automatic failover yet; see TODO.md.

### Automatic Failover

Opt in per cluster (replication is experimental):

```yaml
spec:
  replication:
    enabled: true
    failover:
      enabled: true
      delaySeconds: 30     # how long the primary may be unavailable (not ready) first
```

When the primary pod has not been ready for `delaySeconds` (`status.primaryNotReadySince`), the
operator runs an election Job: every ready replica gets up to a minute to apply the segments it
already received, then reports its position (`POSITION`), and the most advanced one wins. The
election changes nothing, so if the primary recovers meanwhile it is simply discarded. The winner
is promoted exactly like a planned switchover (its journal continues after the last segment it
applied), the Lease and the `targetPrimary` annotation move to it, replicas behind it are
re-seeded, and the old primary is **re-seeded** when it comes back (restarted if its pod is still
there), because it may have committed transactions that never reached a replica.

- Replication is asynchronous: transactions the replicas had not received when the primary
  failed are lost. `archiveTimeoutSeconds` bounds how long a committed transaction can wait on
  the primary before being shipped.
- A fenced primary is never failed over, and there is no failover without a ready replica.
- `status.switchover` reports the failover (`kind: failover`, phases `Electing`, `Promoting`,
  `Completed` or `Failed`).

### Rolling Updates

Without replication the StatefulSet controller rolls the pods. With replication the StatefulSet
uses the `OnDelete` update strategy and the operator rolls them itself (CloudNativePG's
approach), so the primary is restarted only once:

1. Outdated replicas are restarted one at a time, highest ordinal first, each as soon as every
   instance is ready again (the operator watches the instance pods). A restarted replica continues from its replication state; nothing is
   re-seeded.
2. The primary is updated last, according to:

```yaml
spec:
  primaryUpdateStrategy: unsupervised   # or supervised
  primaryUpdateMethod: restart          # or switchover
```

- `unsupervised` + `restart` (default): the primary pod is restarted in place. Writes stop until
  it is ready again. Automatic failover leaves a primary restarted this way alone for up to five
  minutes (or `failover.delaySeconds` if longer), so a planned restart does not become a lossy
  failover.
- `unsupervised` + `switchover`: the operator sets the `targetPrimary` annotation to an updated
  replica; the planned switchover (no data loss) demotes the old primary, whose restart updates
  it. Falls back to a restart when no updated replica is ready or the switchover to it failed.
- `supervised`: the replicas are updated and the primary is left alone
  (`status.rollingUpdate.message` says so) until you switch over with the `targetPrimary`
  annotation or delete the primary pod.

Nothing is restarted while a switchover, failover or re-seed is in progress. Fenced instances are
not restarted; they are updated once unfenced. `status.phase` is `Updating` while instances run
an older revision, and `status.rollingUpdate` lists them.

### Re-seeding a Replica

To re-seed a broken or lagging replica (after CloudNativePG's `unrecoverable` annotation),
annotate its pod:

```bash
kubectl annotate pod my-cluster-2 firebird.cloudnative-firebird.io/reseed=true
```

The operator sees the annotation on the pod right away, lists the request in the cluster ConfigMap
and restarts the pod; its replication init container discards the database and the replication state (not the
security database, so users stay) and seeds it again from another ready replica or the primary's
offline seed. The primary is never re-seeded. `status.reseedingInstances` lists requests until the
new pod is ready. The PVC is kept, which is enough when the data is bad but the volume is fine.

When the volume itself is lost or unusable (local storage on a node that is gone, a broken disk),
ask for a new one:

```bash
kubectl annotate pod my-cluster-2 firebird.cloudnative-firebird.io/reseed=volume
```

The operator records the request in `status.recreatingVolumes`, deletes the claim
(`firebird-data-my-cluster-2`) and the pod, and deletes the pod again while the StatefulSet
recreates it against the old, terminating claim (or waits for a claim it only creates with a new
pod). Once a new claim exists, the pod starts on an empty volume and is seeded like a new replica;
its users (`FirebirdUser`) are applied again, since the volume changed. The request ends when the
pod is ready (`VolumeRecreated` event). Only replicas of a replication cluster qualify: the primary
(switch over first) and standalone instances would start with an empty database, so the annotation
is ignored there.

Known issues and open work are tracked in [ISSUES.md](ISSUES.md) and [TODO.md](TODO.md).

### Backups and Restores

Backups and restores run as Jobs that reach the current primary over the network (the leader
Lease holder, as `<pod>.<cluster>-headless`); they never mount an instance volume.

| | Where the backup goes | How |
|---|---|---|
| logical (`gbak`), no `s3` | primary's data directory | service manager `action_backup` |
| physical (`nbackup`, level 0-2), no `s3` | primary's data directory | service manager `action_nbak` |
| logical with `s3` | S3 object `<prefix>/<file>` | `gbak` streams to the Job pod, an `aws` CLI container uploads it |
| physical with `s3` | S3 object `<prefix>/<file>` | `action_nbak` into the data directory, copied to the Job pod through the primary's backup file server, removed from the volume, uploaded |

Physical backups are written by the primary's server into its data directory. With `s3`, the Job
copies the file through the primary's backup file server and removes it from the volume whether or
not the copy succeeded, so the volume needs room for one backup at a time. The backup file server
is the replication sidecar (`segment-server`), or on clusters without replication a small sidecar
(`backup-files`, the same script in a files-only mode); it only serves plain `*.nbk` and `*.fbk`
names in the data directory, to the cluster's own pods (NetworkPolicy), authenticated with the
SYSDBA password. A physical restore from S3 works the other way:
the Job downloads the files (`backupPath` and `incrementalBackupPaths` are object keys), copies
them next to the database (`restore-<name>-<n>.nbk`), restores them with `action_nrest` and
removes them again. The S3 client image defaults to `amazon/aws-cli` and can be changed with
`s3.clientImage`. Server-side backups share the primary's volume, so they protect against logical
errors, not against losing the volume.

**Backups from a replica.** `target: prefer-standby` (CloudNativePG's `target`) on
`spec.backup`, a `FirebirdBackup` or a `FirebirdScheduledBackup` takes backups to S3 from a
replica instead of the primary, so the primary carries no backup load: `gbak` reads the read-only
replica and streams a logical backup to the Job pod, and a physical backup runs `nbackup` in the
replica's server, its file copied through the replica's segment server. The replica is the ready,
unfenced, non-lagging one with the lowest ordinal (a scheduled backup keeps using it while it stays
healthy); without one the backup runs on the primary. Server-side files belong on the primary's
volume, so those backups always run on the primary. `status.instance` of a `FirebirdBackup` names
the instance it ran on. An `nbackup` chain lives in the backup history of the database it was
taken on, so the schedules of one chain should share a target: a level 1 or 2 on an instance
without a level 0 fails ("Cannot find record ... backup level 0"). A physical restore clears the
replica mode a backup taken on a replica carries, so the restored database is writable.

**Retention.** `retentionPolicy` (`<n>d`, `<n>w` or `<n>m` for 30 days, as in CloudNativePG) on
`spec.backup` or a `FirebirdScheduledBackup` is enforced for backups to S3: after each
upload the Job deletes the schedule's objects (`backup-<schedule>-<timestamp>.fbk` under its
prefix) older than the window, always keeping the newest one, so a stopped schedule never loses
its last backup. Other schedules and other objects are never touched. Server-side logical
backups (`backup-<schedule>-<timestamp>.fbk` in the primary's data directory) are pruned the same
way after each backup (and its verification), listed and deleted through the primary's backup file
server.

`nbackup` series (`nbackup-l<level>-<schedule>-<timestamp>.nbk`, in S3 or on the primary) are pruned along their chains: a level 1 or 2 backup builds on the latest earlier level 0
or 1 of the database, of any schedule, and the Job reads these chains from the primary's
`RDB$BACKUP_HISTORY`. An expired file is only deleted when no kept backup's chain needs it: a
weekly level 0 stays while a daily level 1 built on it is kept. Backups of a schedule stored in
another location, taken on demand or by hand, and files the history does not know, count as
kept; so keep the schedules of one chain in the same location (bucket and prefix, or the primary's
data directory), or their bases are only freed when those backups' schedule is gone.

**Verification.** `verify: true` on a `FirebirdBackup`, a `FirebirdScheduledBackup` or
`spec.backup` restores every logical backup into a scratch database and runs a full validation;
a backup that does not restore or validate fails its Job. A backup to S3 is checked in the Job's
work volume before it is uploaded (so a bad backup is never stored; the volume needs room for the
backup and the restored database). A server-side backup is restored next to itself on the
primary through its service manager (`.verify-<name>.fdb`, temporary disk space and load on the
primary), validated online and dropped. A verified `FirebirdBackup` reports `status.verified`.

```yaml
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdBackup          # one-off; FirebirdScheduledBackup takes a cron schedule
metadata:
  name: before-upgrade
spec:
  clusterName: my-cluster
  s3:                         # optional; omit to keep the backup on the primary
    bucket: firebird-backups
    prefix: my-cluster
    endpoint: https://s3.example.com
    secretRef:                # optional: without it, credentials come from the pod (see below)
      name: s3-credentials    # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
---
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdRestore
metadata:
  name: inspect-before-upgrade
spec:
  clusterName: my-cluster
  backupName: before-upgrade  # or backupPath (+ s3 and/or incrementalBackupPaths for nbackup chains)
  targetDatabase: before-upgrade.fdb
```

The status of each resource follows its Job (`Running`/`Restoring`, then `Completed` or
`Failed`); a backup reports its `location`. A restore always creates a **new database file**
next to the cluster database (default `restore-<name>.fdb`) and refuses to overwrite the cluster
database. A physical restore that fails (e.g. an increment that does not belong to the chain)
removes its partial database, which `nbackup` leaves locked, so the Job's retries start clean; a
target file that existed before is never touched. To replace a database, bootstrap a new cluster from the backup:

```yaml
spec:
  bootstrap:
    recovery:                 # restore a gbak backup from S3 (sourcePath is the object key)
      sourcePath: backup-before-upgrade.fbk
      s3: { bucket: firebird-backups, prefix: my-cluster, secretRef: { name: s3-credentials } }
    # or clone a running cluster by streaming gbak from its Service:
    # clone: { sourceCluster: my-cluster, namespace: prod, superuserSecret: { name: prod-su } }
```

When the source cluster has `networkPolicy.enabled`, its generated NetworkPolicy admits the
instance pods of every cluster that clones from it (matched by namespace and cluster label, port
3050 only), so a clone works without widening `ingressFrom`. The source is reconciled as soon as
the clone is created, before the clone's instances copy the database.

**S3 without static keys.** `s3.secretRef` is optional everywhere (backups, scheduled backups,
restores, bootstrap recovery, journal archiving). Without it the `aws` CLI uses the credentials of
its pod, typically workload identity (EKS IRSA or Pod Identity) through a service account set with
`spec.serviceAccountName` (CloudNativePG 1.29). The instance pods and every Job the operator runs
for the cluster use that service account:

```yaml
spec:
  serviceAccountName: firebird-backups   # existing ServiceAccount, e.g. annotated with an IAM role
  backup:
    enabled: true
    s3: { bucket: firebird-backups, region: eu-west-1 }
```

**Pausing a resource.** The annotation `firebird.cloudnative-firebird.io/reconciliationDisabled:
"true"` on a `FirebirdBackup`, `FirebirdScheduledBackup`, `FirebirdRestore`, `FirebirdUser` or `FirebirdRole`
(CloudNativePG 1.29 `cnpg.io/reconciliationDisabled`) makes the operator leave it, its status and
its Jobs / CronJob alone until the annotation is removed; a new backup or restore created with it
does not start. Deleting a paused `FirebirdUser` or `FirebirdRole` keeps the Firebird user or role
(as with `retain`).
Clusters are paused with `spec.suspended`.

With replication enabled, only the primary bootstraps; replicas are then seeded by replication.
With `replication.journalArchiveS3`, a CronJob copies archived journal segments from the
primary's segment server to `<prefix>/journals/`, each with an empty marker object
`<segment>.archived-<YYYYMMDDTHHMMSSZ>` recording when the primary archived it.

**Point-in-time recovery.** A physical restore with `pointInTime` replays the archived journal on
top of an `nbackup` chain (taken on the primary), up to a target:

```yaml
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdRestore
metadata:
  name: before-the-bad-deploy
spec:
  clusterName: my-cluster
  restoreType: physical
  backupPath: nbackup-l0-weekly-20261001T000000Z.nbk
  incrementalBackupPaths: [nbackup-l1-daily-20261004T000000Z.nbk]
  s3: { bucket: firebird-backups, prefix: my-cluster, secretRef: { name: s3-credentials } }
  pointInTime:
    targetTime: "2026-10-04T10:15:00Z"   # or targetSegment: 1234; neither: every archived segment
    # journalS3: {...}                   # default: the cluster's replication.journalArchiveS3
```

The restore Job restores the chain into a scratch database in the Job pod, keeping its
replication sequence (`nbackup -SEQ -R`), makes it a read-only replica and lets a private Firebird
server in the pod apply the archived segments to it, like a replica would. It then makes the
database a normal one and restores it into the target database on the primary, like any physical
restore. The `nbackup` lock switches the journal to a new segment, so the segments after the
backup's sequence hold exactly the later changes; transactions still open in the backup are
replayed from their first segment (the Job fetches earlier segments when one started before the
backup). The Job checks that no transaction complete in the backup has changes after it.

Segments are applied whole: `targetTime` recovers every segment archived at or before it, so the
recovery point is up to `replication.archiveTimeoutSeconds` (plus the archive delay) before the
target; transactions not committed by the end of the last segment are rolled back. A target before
the backup fails the restore. The Job pod needs room for the database twice (scratch database and
its level-0 copy) plus the chain and the segments. Segments uploaded before v0.45.0 have no
marker: their upload time stands in for the archive time, which only makes `targetTime` more
conservative. Server-side chains work too (`backupPath` names directly in the data directory).

**Bootstrapping a cluster to a point in time.** A new cluster can start from the recovered
database instead (CloudNativePG's `bootstrap.recovery` with a recovery target):

```yaml
spec:
  instances: 3
  replication: { enabled: true, journalArchiveS3: { bucket: firebird-backups, prefix: my-cluster-restored } }
  bootstrap:
    recovery:
      sourcePath: nbackup-l0-weekly-20261001T000000Z.nbk          # nbackup level 0 object key
      incrementalPaths: [nbackup-l1-daily-20261004T000000Z.nbk]
      s3: { bucket: firebird-backups, prefix: my-cluster, secretRef: { name: s3-credentials } }
      pointInTime:
        targetTime: "2026-10-04T10:15:00Z"
        journalS3: { bucket: firebird-backups, prefix: my-cluster, secretRef: { name: s3-credentials } }
```

Before the StatefulSet exists, the operator creates the first instance's volume (under the
StatefulSet's claim name, so the StatefulSet adopts it) and runs the Job `<cluster>-pitr-recovery`
on it (as the Firebird user, with `fsGroup` making the new volume writable). The Job replays the
journal as above and leaves the database where a restored one is expected; the cluster stays
`Creating` ("Recovering the database to a point in time") until it completes. The instances then
start as for any bootstrap: with replication the primary enables publication on it and the other
instances are seeded from it. A failed Job leaves the cluster `Degraded`; delete the Job to retry.
`journalS3` is required here and must not be the new cluster's own `journalArchiveS3` (it would
archive its segments over the ones it recovers from); more than one instance needs replication.

### Users

Firebird users live in the **security database**, which the official image keeps on the container
filesystem. The operator moves it to the instance volume (`system/security.fdb`, seeded from the
image by the `security-db-init` container), so users survive pod restarts. SYSDBA is still managed
through `spec.superuserSecret`.

Users are declared as their own resources, like CloudNativePG's `DatabaseRole`:

```yaml
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdUser
metadata:
  name: app-user            # Firebird user APP_USER (or set spec.username)
spec:
  clusterName: my-cluster
  passwordSecret:
    name: app-user-password # key "password" (or passwordSecret.key)
  roles: [reader]           # granted in the cluster database; others are revoked
  active: true              # false: the user cannot log in
  admin: false              # true: RDB$ADMIN in the security database
  reclaimPolicy: retain     # delete: drop the user when this resource is deleted
```

A Job applies the user to **every instance** (`CREATE OR ALTER USER`, each instance has its own
security database, which replication does not ship) and the role grants to the cluster database
(on the primary with replication, where they replicate; on every instance otherwise).
`status.instances` records the applied state per instance and volume, so scaled-up, re-seeded or
re-created instances get the user when they become ready, and a changed Secret is applied within
a resync interval. A failed Job (for example a role that does not exist) is retried after five
minutes; `kubectl logs job/fbuser-<name>` shows the error.

With `reclaimPolicy: delete`, deleting the resource drops the user from every ready instance and
revokes its grants. An instance that holds the user (per `status.instances`) but is not ready at
the time is waited for: the resource stays in phase `Dropping` (`status.droppedFrom` lists where
the user is gone) and the user is dropped there once the instance is ready again. After 15
minutes the finalizer is released and the drop is left pending (Warning event): it is recorded in
the ConfigMap `<cluster>-pending-user-drops`, the instance's `security-db-init` container drops
the user from its security database the next time it starts, before the server accepts any
connection, and once the instance is ready the operator drops it again through a Job
(`drop-users-<pod>`, in case it became ready without a restart) and clears the entry. A user that
a `FirebirdUser` declares again meanwhile is not dropped; an instance scaled away with its volume
deleted needs nothing.

Security notes: the password is read from the Secret inside the Job and only sent to the servers
as SQL text, where it can briefly show in `MON$STATEMENTS` or a trace session, to administrators
only (other users see just their own attachments). The services API is no better: a trace session
logs its user management requests with the password too (`-ADD <user> -PW <password>`, verified
with Firebird 5), and it would also put the password on the Job's command line. Users are not part
of `gbak` backups; keep the `FirebirdUser` objects (for example in Git) to re-create them on a
restored or cloned cluster.

### Roles

A `FirebirdRole` declares a role of the cluster database and exactly the privileges it holds;
users get it through `FirebirdUser.spec.roles`:

```yaml
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdRole
metadata:
  name: reporting            # role REPORTING (or set spec.roleName)
spec:
  clusterName: my-cluster
  privileges:
    - privileges: [SELECT]
      object: { kind: table, name: orders }         # table or view: SELECT, INSERT, UPDATE, DELETE, REFERENCES, ALL
    - privileges: [EXECUTE]
      object: { kind: procedure, name: monthly_report }  # procedure, function, package: EXECUTE
    - privileges: [USAGE]
      object: { kind: sequence, name: report_seq }  # sequence, exception: USAGE
    - privileges: [SELECT]
      object: { kind: table, name: 'Sales 2026', quoted: true }  # created as CREATE TABLE "Sales 2026"
  reclaimPolicy: delete      # drop the role when this resource is deleted (default: retain)
```

A Job creates the role if it does not exist, then revokes everything the role holds and grants
the listed privileges in one transaction: privileges removed from the spec (or granted by hand)
are revoked, and none is ever missing in between. Memberships (users granted the role) are kept.
With replication the Job runs on the primary and the privileges replicate (`status.appliedHash`);
without it, every instance's database gets them (`status.instances`, applied again on a new
volume). Object names are regular identifiers (stored in upper case); `quoted: true` uses the
name exactly as written, for objects created with a delimited identifier (case-sensitive, with
spaces, punctuation or a reserved word such as `"ORDER"`). A Job that fails, for
example on an object that does not exist, is retried after five minutes (`kubectl logs
job/fbrole-<name>`). With `reclaimPolicy: delete` the role is dropped with its privileges and
memberships.

Role names follow SQL too: `roleName: reporting` is the role `REPORTING`, while a name in double
quotes is used exactly as written, e.g. `roleName: '"Sales Team"'` (a quote inside is written
`""`). `FirebirdUser.spec.roles` refers to such a role the same way (`roles: ['"Sales Team"']`).

### Fencing

Fencing follows CloudNativePG: the `firebird.cloudnative-firebird.io/fencedInstances` annotation
holds a JSON list of instance names, `["*"]` fences every instance, and `[]` (or removing the
annotation) lifts the fence.

```bash
# fence one instance
kubectl annotate firebirdcluster my-cluster \
  'firebird.cloudnative-firebird.io/fencedInstances=["my-cluster-1"]' --overwrite
# lift all fences
kubectl annotate firebirdcluster my-cluster 'firebird.cloudnative-firebird.io/fencedInstances=[]' --overwrite
```

The Firebird server is the container's main process, so instead of stopping it the operator puts
the instance's database into **full shutdown** (`gfix -shut full -force 0`, through the service
manager) with a short Job: existing attachments are closed and no client, replica apply or backup
can attach. The pod and its volume stay, so the files can be inspected (`kubectl exec`). The
shutdown is stored in the database header and survives pod restarts. The readiness probe requires
the database to be online, so a fenced instance is not Ready and leaves the Services, read routing
and replica seeding. **A fenced primary is not failed over**: writes stop until the fence is
lifted. A fenced replica stops applying segments and catches up once unfenced.

`status.fencedInstances` lists the applied fences and the `Fenced` condition reports changes in
progress or failed Jobs (retried on the next reconcile). Upgrading the operator rolls existing
instances once, to switch them from the TCP readiness probe to the database-online probe.

### Events

The operator records Kubernetes events on its resources (CloudNativePG 1.29 / 1.30), so
`kubectl describe firebirdcluster my-cluster` and `kubectl get events` show what it did:

| Resource | Reasons |
|---|---|
| `FirebirdCluster` | `SwitchoverStarted`, `SwitchoverPromoting`, `SwitchoverCompleted`, `SwitchoverFailed` (warning); `PrimaryNotReady`, `FailoverStarted`, `FailingOver`, `FailoverFailed` (warnings), `FailoverCancelled`, `FailoverCompleted`; `InstanceFenced`, `InstanceUnfenced`, `FencingFailed` (warning); `ReseedStarted`, `ReseedCompleted`; `RollingUpdate`, `RollingUpdateCompleted`; `ReplicaLagging` (warning); `VolumeResizing`, `VolumeResizeFailed` (warning); `ReconcileFailed` (warning) |
| `FirebirdBackup` | `BackupStarted`, `BackupCompleted`, `BackupFailed` (warning) |
| `FirebirdRestore` | `RestoreStarted`, `RestoreCompleted`, `RestoreFailed` (warning) |
| `FirebirdUser` | `UserApplied`, `UserFailed` (warning), `UserDropped` |
| `FirebirdRole` | `RoleApplied`, `RoleFailed` (warning), `RoleDropped` |

Events are recorded on transitions; a repeated event (e.g. the same reconcile error) increments
the count of the previous one for ten minutes, like client-go's event recorder.

### Read-Only Traffic Routing

With `spec.replication.readOnlyRouting.enabled: true` the operator labels every pod with
`firebird.cloudnative-firebird.io/role` (`primary` / `replica`) and
`firebird.cloudnative-firebird.io/read-routable`. The primary Service selects only the
primary pod, and the `<name>-replica` Service selects only ready replicas whose replication
lag is within `maxLagSeconds` (default 30). Lag is read from the
`firebird.cloudnative-firebird.io/replication-lag-seconds` pod annotation, which the operator
publishes from its own measurement (see Replication Lag); replicas without a lag report are routed
on readiness alone. When no replica qualifies, reads fall back to the primary unless
`fallbackToPrimary: false`. Routing follows readiness changes as the operator sees them on
its instance pod watch, and lag changes on the reconcile every 30 seconds.

## Development

### Project Structure

```
cloudnative-firebird/
├── operator/               # TypeScript operator source
│   ├── src/
│   │   ├── types/          # CRD TypeScript type definitions
│   │   ├── controllers/    # FirebirdCluster and backup/restore controllers
│   │   ├── replication/    # Scripts shipped to instance pods (replication, journal archive)
│   │   ├── utils/          # Resource builders, logger
│   │   ├── operator.ts     # Watch/event loop
│   │   └── index.ts        # Entrypoint
│   ├── tests/              # Unit tests (Vitest)
│   ├── Dockerfile
│   ├── package.json
│   └── tsconfig.json
├── config/
│   ├── crds/               # CRD YAML manifests
│   ├── samples/            # Example FirebirdCluster resources
│   └── deploy/             # Operator deployment manifests (RBAC, Deployment, etc.)
└── README.md
```

### Build

```bash
cd operator
npm install
npm run build
```

### Test

```bash
cd operator
npm test
```

### Run Locally (against a cluster)

```bash
cd operator
npm run dev
```

The operator uses `~/.kube/config` when `KUBERNETES_SERVICE_HOST` is not set (local development mode).

## Roadmap

For planned features inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg)—including automated failover, `nbackup` physical backups, PITR, `gfix` sweeping, cert-manager TLS, and dedicated backup CRDs—see [ROADMAP.md](ROADMAP.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
