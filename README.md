# cloudnative-firebird

A cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/) database, written in TypeScript. Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg).

## Overview

`cloudnative-firebird` automates the deployment and lifecycle management of Firebird database clusters on Kubernetes. It introduces the `FirebirdCluster` Custom Resource Definition (CRD) and a controller that reconciles cluster state.

### Features

- **Declarative cluster management** via `FirebirdCluster` CRD
- **StatefulSet-based** deployment for stable pod identity and storage
- **Persistent storage** via PersistentVolumeClaims, with online volume expansion when `spec.storage.size` grows
- **Declarative hibernation** (`spec.hibernated`) that scales to zero while keeping data
- **Journal-based asynchronous replication** (Firebird 4, 5 and the 6.0 snapshot, experimental) with replicas seeded without locking the primary, and an optional **synchronous standby** (`mode: sync`)
- **Backups and restores** as Jobs against the primary: `gbak`/`nbackup` through the service manager, logical and physical backups to S3, `FirebirdBackup` / `FirebirdScheduledBackup` / `FirebirdRestore` resources
- **Bootstrap** a new cluster from an S3 backup or by cloning another cluster
- **Declarative users** (`FirebirdUser`, after CloudNativePG's `DatabaseRole`) with Secret-backed passwords, role grants and a reclaim policy; users persist across pod restarts
- **Declarative roles** (`FirebirdRole`): a database role and exactly the privileges it holds on tables, views, procedures, functions, packages, sequences and exceptions
- **Automatic failover** (opt-in) to the synchronous standby (no transaction lost) or the most advanced replica; the old primary is re-seeded when it returns
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
images (`spec.imageName`; without it the operator's `FIREBIRD_DEFAULT_IMAGE` environment variable,
or `firebirdsql/firebird:latest`, currently 5.0). Replication,
planned switchover, failover, fencing, re-seeding, backups and restores, and point-in-time
recovery (into a running cluster or as a bootstrap) were verified end to end with:

| Image | Server | ODS |
|-------|--------|-----|
| `firebirdsql/firebird:4` | 4.0.7 | 13.0 |
| `firebirdsql/firebird:5` (`latest`) | 5.0.4 | 13.1 |
| `firebirdsql/firebird:6-snapshot` | 6.0.0.2191 (snapshot, commit 4ca39c2) | 14.0 |

CI runs the kind integration tests on each of these images (the operator's
`FIREBIRD_DEFAULT_IMAGE`; failures on the Firebird 6 snapshot are reported without failing the
workflow). Firebird 6 is not released yet: a snapshot is a development build, and its on-disk structure can
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
kubectl apply -f config/deploy/webhook.yaml
kubectl apply -f config/deploy/deployment.yaml
```

The operator runs **two replicas** with leader election, like CloudNativePG's operator:
- **The leader:** only the replica holding the Lease `cloudnative-firebird-operator` in the
  operator's namespace watches and reconciles. The metric `cloudnative_firebird_operator_leader`
  is 1 on it.
- **The webhook:** every replica serves the admission webhook, from the shared certificate Secret.
- **Handover:** a leader that stops releases the Lease, so the other replica takes over at once.
  A leader that crashes, or loses the API server, is replaced once the Lease expires: 15 seconds,
  measured on each replica's own clock.
- **Losing the Lease:** a leader that cannot renew it within 10 seconds exits instead of
  reconciling alongside its successor.

A PodDisruptionBudget keeps one replica through node drains, and the replicas prefer different
nodes. `LEADER_ELECTION=false` runs a single replica without the Lease.

#### Upgrading the operator

Apply the manifests of the new version, CRDs and RBAC first. The operator then rolls each
cluster's instances (replicas first, the primary last) onto the new scripts. While they roll,
instances of the old and new version talk to each other, so each release keeps talking to the one
before. Releases also keep the earlier ones they still have to: since v0.83.0 that means v0.64.0
and later. An operator from before v0.64.0 has to be upgraded to v0.82.0 first, and its clusters
rolled. Its instances send the SYSDBA password in plain text, which current segment servers refuse,
so replicas would stop pulling from an upgraded primary until they had restarted themselves.

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
restore paths outside the data directory, `sync` replication with one instance, shrinking `storage.size` and
immutable `clusterName` / `username` fields. The operator still
validates each reconcile (for objects created before an upgrade) and reports `Degraded`.
`hack/crd-validation/test.sh` checks the rules against an API server.

The **admission webhook** (`config/deploy/webhook.yaml`, served by the operator) adds the checks
that need other objects. It **refuses**:

- a `FirebirdRestore` whose target is the cluster database;
- a new `FirebirdRestore` whose target file already exists on the primary (asked from its segment
  server, or the backup file server without replication; admitted when it does not answer within
  1.5 seconds, and the operator asks again before it creates the restore Job);
- a restore from a `FirebirdBackup` that does not exist or failed;
- a restore whose `restoreType` does not match its backup;
- a point-in-time restore whose source cannot be used (not a physical backup, no journal archive);
- the few spec checks the CRD rules cannot express, e.g. `WireCrypt` settings with
  `tls.enabled`, or a fencing annotation naming other clusters' instances.

It **warns** without refusing (objects may be applied in any order) about:

- a `FirebirdCluster` that does not exist yet or is hibernated;
- missing Secrets (superuser, S3 credentials, a user's password and its key);
- a clone source that does not exist;
- a backup a restore waits for.

Updates are only checked when the spec (or, on a cluster, the annotations) changes.

The operator creates the webhook's certificates itself, with no cert-manager needed: a CA and a
serving certificate in the Secret `cloudnative-firebird-webhook-cert`, renewed 30 days before
they expire, with the CA put into the webhook configuration's `caBundle`. The webhook's
`failurePolicy` is `Ignore`, so objects are still admitted while the operator is down and checked
when it reconciles. `WEBHOOK_ENABLED=false` on the operator Deployment turns it off.

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

The official image runs the Firebird server as root, so by default the instance pods meet the
`baseline` Pod Security Standard and drop what they can (CloudNativePG 1.28
`podSecurityContext` / `securityContext`). With `runAsFirebirdUser: true` every instance
container runs as the image's `firebird` user instead, and the pods meet `restricted`:

| | Instance pods (default) | Instance pods (`runAsFirebirdUser: true`) | Operator Jobs (backups, restores, archive, maintenance, fencing, switchover, users) |
|---|---|---|---|
| user | root (image default) | `firebird` (uid 84), `runAsNonRoot`, `fsGroup` 84 | `firebird` (uid 84), `runAsNonRoot` |
| capabilities | all dropped except `CHOWN`, `DAC_OVERRIDE` (the server's firebird-owned lock directory), `FOWNER` (init scripts) | all dropped | all dropped |
| privilege escalation | no | no | no |
| seccomp | `RuntimeDefault` | `RuntimeDefault` | `RuntimeDefault` |
| Pod Security Standard | `baseline` | `restricted` | `restricted` |

The image entrypoint applies `FIREBIRD_CONF_*` settings by editing `/opt/firebird/firebird.conf`,
which only root may write: with `runAsFirebirdUser` a first init container (`firebird-home`) copies
`/opt/firebird` (about 45 MB) into an `emptyDir` that the Firebird container mounts there, and the
init scripts no longer `chown` (everything they write is the firebird user's). Turning it on rolls
the instances; the volume becomes group-owned by `fsGroup` 84 (on storage that supports
`fsGroup`; local-path volumes are world-writable). Verified with operator-generated pods on
Firebird 5: seeding, journal shipping, switchover, failover, synchronous replication and
point-in-time recovery, with the server process running as uid 84.

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

### Encryption in Transit

Firebird has no TLS listener. Client connections are encrypted by Firebird's own wire protocol
(WireCrypt), with a session key from the Srp authentication. Firebird 4 and later already require
it on the server by default (`WireCrypt = Required`, `AuthServer = Srp256`), so a client that does
not encrypt is refused. With operator-generated pods on Firebird 4.0.7, 5.0.4 and the 6.0
snapshot, an unencrypted client gets "Incompatible wire encryption levels" and others negotiate
ChaCha64.

```yaml
spec:
  tls:
    enabled: true   # WireCrypt = Required and WireCryptPlugin = ChaCha64, ChaCha
```

`tls.enabled` makes it strict: only the ChaCha64 and ChaCha plugins are offered, not the RC4-based
Arc4, so a client that can only use Arc4 (e.g. Firebird 3) is refused. `config.settings` that would
weaken it (another `WireCrypt`, or `Arc4` in `WireCryptPlugin`) are rejected. `tls.secretName` and
`tls.issuerRef` are deprecated and ignored: earlier versions mounted a certificate (and created a
cert-manager Certificate) that Firebird never read; a `TLSCertificateIgnored` event says so.

Journal segment shipping, seed copies between instances and backup files copied through the
segment server travel as plain TCP inside the cluster unless `segmentTLS` is enabled (below); `networkPolicy.enabled`
restricts the segment port to the cluster's own pods and Jobs, and the operator. Requests to the segment server are signed with
the SYSDBA password (HMAC-SHA256 over the request, its time and a nonce) instead of carrying it,
so the password never crosses the network and a captured request cannot be replayed or altered.
Replies and transferred bytes are not signed; segment TLS (below) encrypts them. Since v0.83.0
segment servers accept signed requests only: the plain password that clients before v0.64.0 sent
is refused (`ERR unauthorized (unsigned)`), so upgrades must start from v0.64.0 or later (see
*Upgrading the operator*).

#### Segment TLS

Full guide with diagrams, the reasons for it, switching an existing cluster and troubleshooting:
[docs/segment-tls.md](docs/segment-tls.md).

```yaml
spec:
  segmentTLS:
    enabled: true   # Kubernetes 1.29 or later (native sidecar containers)
```

Since v0.77.0 this is the default for **new** clusters on Kubernetes 1.29 or later: the operator
writes `segmentTLS.enabled` into a new cluster's spec on its first reconcile (`true` there, `false`
on older Kubernetes) and records a `SegmentTLSDefaulted` event. Clusters created by an earlier
version are pinned to `false`, so an upgrade changes nothing for them; set `enabled: true` to move
one over. The operator's `SEGMENT_TLS_DEFAULT` (`auto`, `true`, `false`) changes the default.
Since v0.79.0 the operator can move pinned clusters itself, one idle cluster at a time:
`SEGMENT_TLS_MIGRATE=pinned` moves the clusters it pinned (annotation
`firebird.cloudnative-firebird.io/segment-tls-migration: pinned`), `all` every cluster with
`enabled: false`. Annotate a cluster with `skip` to keep it out (docs/segment-tls.md).
`SEGMENT_TLS_REQUIRED=true` (v0.84.0) refuses plain segment shipping: admission denies a cluster
created with `enabled: false` or switched to it, and a cluster still plain gets a `SegmentTLS: False`
condition, a `SegmentTLSRequired` event and `firebird_cluster_segment_tls 0` until it is moved.

With `segmentTLS.enabled`, every segment server connection (journal segments, seed copies, backup
files, the operator's lag and health checks) is carried over mutual TLS 1.3. The Firebird images
have no TLS tooling, so each instance pod runs a small proxy from the operator's own image as a
native sidecar (`segment-tls`, an init container with `restartPolicy: Always`): it accepts TLS on
the segment port (3051) and forwards to the segment server, which then listens on localhost only,
and it relays the pod's own segment clients to the other instances. Backup, restore and failover
Jobs get the client side of the proxy only. The operator connects over TLS itself.

The operator creates a CA per cluster and one certificate signed by it (client and server use) in
the `<cluster>-segment-tls` Secret, owned by the cluster. It renews the certificate 30 days before
it expires and the CA a year before; the proxies load renewed files without a restart. Pods mount
the certificate, its key and the CA certificate, never the CA's key. A peer is authenticated by its
certificate's chain to the cluster's CA, so only the cluster's own pods and Jobs (and the operator)
are accepted; signed requests (above) still apply on top.

Switching `segmentTLS` on or off is a template change: the rolling update restarts every instance,
the primary last. Replication goes on meanwhile: the operator lists the instances still serving in
plain text in the `<cluster>-segment-tls-peers` ConfigMap, the proxies connect to those in plain
text, and they accept plain connections until 5 minutes after the last instance switched. Then
only TLS is accepted. The operator reaches each instance in the mode its pod runs.
Managing the Secret needs `create` and `update` on Secrets in the operator's ClusterRole
(`config/deploy/rbac.yaml`). The sidecar uses the image of the running operator (`OPERATOR_IMAGE`
overrides it), so instance and Job pods must be able to pull it.

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
  `journal_source_directory`. They use only the Firebird image's perl, sign their requests with
  the SYSDBA password (the password itself is not sent), and never open a live database file directly (all access goes through the
  local server).
- A new replica is seeded from a **ready replica** (locked through that replica's server), or
  from the primary's offline bootstrap seed while every later segment is still archived, and
  only otherwise from a **locked copy of the live primary** (`replication.allowLiveSeedFromPrimary`,
  on by default; `false` never locks the primary, and a new replica then waits for a ready
  replica or a fresh offline seed). A locked copy of a primary under load holds some
  transactions as uncommitted although their commit is already journaled
  ([ISSUES.md](ISSUES.md), issue 2, confirmed): the new replica's control file lists every
  transaction open in the copy from its first archived segment, so they are replayed, and a
  transaction whose start is no longer archived fails the seed rather than being replayed
  partially.
- The seed stays usable: while no replica holds segments back (a single instance), the primary
  keeps the segments after the seed up to `maxSegmentRetentionHours` (7 days by default), so an
  instance added later can be seeded from it. Once they are pruned, the primary's init container
  takes a fresh offline copy the next time the primary starts after a clean stop (a rolling
  update, a restart). After an unclean stop the server goes on writing the journal segment it
  had open, so the refresh waits for the next clean restart. Until then a new replica is seeded
  from a live copy (unless `allowLiveSeedFromPrimary: false`).

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
| `firebird_cluster_segment_tls` | `namespace`, `cluster` | 1 when segment shipping is encrypted (`spec.segmentTLS.enabled`) |
| `firebird_cluster_primary_lease_age_seconds` | `namespace`, `cluster` | Seconds since the primary last renewed its Lease (`failover.primaryLease`; -1 without a renewal time) |
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
   replica's control file position with a `POSITION` query). It then promotes the target **in
   place**, without restarting it (its segment server's `PROMOTE`). This takes a full shutdown of
   about a second, during which:
   - the target's header gets replication sequence *S*, so its journal continues at *S + 1* and
     the other replicas keep applying without re-seeding;
   - it gets a fresh offline bootstrap seed;
   - it is brought back online with replica mode none and publication.
2. **Promoting**: the operator moves the leader Lease, the `primary` ConfigMap entry and the
   routing label to the target, and restarts the old primary. Its init container makes it a
   read-only replica positioned after *S*. Replicas that were not ready are re-seeded.
   `status.switchover.promotedInPlace` says the target kept running.
3. **Completed** once the target is ready and the old primary is ready as a new pod.

Writes are unavailable from the start of step 1 until the target is promoted, about the Job's
run time, without pod restarts. If the in-place promotion fails, the target stays a replica:
the operator then restarts it, and its init container promotes it offline in the same way,
which is slower (a pod restart). If the Job fails (for example a replica does not catch up
within five minutes) the old primary is brought back online and stays primary, unless the target
reports that it was already promoted; change the annotation to retry.

A target promoted in place keeps the role until its next restart even if the ConfigMap has not
reached its files yet (a `promoted` marker in its replication directory, removed by the init
container). A restart that finds the promote directive applies nothing twice.

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
election changes nothing, so if the primary recovers meanwhile it is simply discarded.

Once the operator commits to the failover:
- The Lease and the `targetPrimary` annotation move to the winner.
- A promote Job then promotes the winner **in place**, like a planned switchover: no restart, and
  its journal continues after the last segment it applied, or after the journal archive's last
  segment if that is higher. `status.switchover.promotedInPlace` records it.
- If that fails, the winner is restarted and promoted offline instead.
- Replicas behind the winner are re-seeded.
- The old primary is **re-seeded** when it comes back (restarted if its pod is still there),
  because it may have committed transactions that never reached a replica.

- Replication is asynchronous: transactions the replicas had not received when the primary
  failed are lost. `archiveTimeoutSeconds` bounds how long a committed transaction can wait on
  the primary before being shipped.
- A fenced primary is never failed over, and there is no failover without a ready replica.
- `status.switchover` reports the failover (`kind: failover`, phases `Electing`, `Promoting`,
  `Completed` or `Failed`).
- **Isolation check** (CloudNativePG's `isolationCheck`, on by default with failover): the
  primary's replication sidecar checks every 5 seconds whether it can reach the Kubernetes API
  server or the segment server of any other instance (through the headless Service). When it has
  reached neither for `timeoutSeconds`, it puts its database into full shutdown, like a fenced
  instance: clients cut off with it (e.g. on a partitioned node) cannot keep writing to a primary
  that a failover replaces on the other side. The failover delay is raised to at least
  `timeoutSeconds + 10`, so the old primary has fenced itself before a replica is promoted. If
  the operator reaches it again while it still holds the Lease (no failover happened), it brings
  the database back online (`PrimaryRejoined` event); otherwise it is re-seeded as above.

```yaml
    failover:
      enabled: true
      isolationCheck:
        enabled: true        # default
        timeoutSeconds: 20   # default, 5 to 3600
        fenceWhenUnreached: true   # default
        contactTimeoutSeconds: 60  # default, 45 to 3600
```

- **A primary nothing reaches** (`fenceWhenUnreached`, since v0.74.0): reaching the API server
  is not enough. The segment server records every authenticated request it answers (the
  operator's checks at least every reconcile, the replicas' pullers every 5 seconds). When
  neither the operator nor any replica has reached the primary for `contactTimeoutSeconds`, and
  the headless Service lists other instances, the primary fences itself the same way, although
  it still reaches the API server. That is a primary cut off on the other side of a partition
  (below): it stops taking writes before the operator promotes a replica, which waits
  `contactTimeoutSeconds - 15` (45 seconds by default) for a cut-off primary instead of
  `delaySeconds` when that is longer. The price: if the operator (every replica of it) and every
  instance stop reaching the primary for that long at the same time, it stops taking writes until
  the operator reaches it again and brings it back online. `fenceWhenUnreached: false` turns
  this off.

  The other instances are found through cluster DNS, which can be lost in the same partition
  (since v0.78.0). The primary looks them up on every check, with a 2-second limit, and keeps
  the addresses of the last answer on its volume. When DNS does not answer, it goes by those
  addresses together with the instances' current addresses, which the operator publishes in the
  cluster ConfigMap (`peer-addresses`, since v0.80.0), or by its list of ready replicas when it
  knows no address at all. It then fences itself only when none of the known addresses answers either. A DNS outage
  alone, in which it still reaches the other instances, does not fence it: the operator and the
  replicas cannot resolve it then either, and nothing fails it over.

- **Cut-off primary**: a primary whose pod stays ready (the kubelet still sees it) but that the
  rest of the cluster has lost is failed over too. Each replica's segment puller records when it
  last reached the primary's segment server (`PRIMARYSEEN`); the primary counts as unavailable
  when the operator cannot reach its segment server either and every ready replica it reaches
  has not reached the primary for 30 seconds or more (at least one must answer). The
  `PrimaryNotReady` event then says `ready but cut off`, and the failover proceeds as above
  after `delaySeconds`; it deletes the old primary's pod, which comes back as a replica and is
  re-seeded, so clients connected to it on its side of the partition stop writing to it.

Before v0.74.0, a primary that still reached the API server was not fenced, so clients that
still reached it could keep writing to it until the failover deleted its pod (writes then
discarded by the re-seed). It now fences itself first (above), unless `fenceWhenUnreached` is
off. The fence needs the cluster DNS (the headless Service) to know that replicas exist.

#### Primary Lease (promotion mutex)

CloudNativePG 1.30 makes the primary hold a Lease and promotes only once it is free. Opt in
(v0.86.0):

```yaml
    failover:
      enabled: true
      primaryLease:
        enabled: true
        durationSeconds: 15   # default; 5 to 120
```

Without it the cluster Lease (`<cluster>-lease`) only records which instance is the primary: the
operator moves it when it promotes, and nothing renews it. With it a `lease-holder` sidecar (from
the operator image, like the segment TLS proxy) in every instance pod renews the Lease every
third of `durationSeconds` while its pod is the primary and its database is online, through the
pod's ServiceAccount (a Role on that one Lease and a RoleBinding the operator manages, and a
token projected into the sidecar alone):

- the primary **fences** its database (the isolation check's full shutdown) when the Lease names
  another instance (the operator promoted a replica) or when it could not renew the Lease for
  `durationSeconds` (it cannot reach the API server: the operator may be promoting a replica on
  the other side). Clients that still reach it cannot write to a database that may no longer be
  the primary. The sidecar brings it back online itself once it can re-acquire the Lease (the
  Lease still names it, or nobody: with the version it read, so a failover that took the Lease
  over meanwhile wins), which also covers a fence by the isolation check; the operator does not
  rejoin an isolated primary itself with the primary Lease on. A primary replaced meanwhile is
  re-seeded;
- a primary whose database is down stops renewing, so the Lease expires on its own;
- the operator **promotes a replica only once the Lease has expired**: a primary that still
  renews it is alive, online and reaching the API server, and is never replaced. A failover that
  has elected its target waits (`status.switchover.message`, a `PrimaryLeaseHeld` event) until
  then, and takes the Lease over with the version it read: a renewal by the old primary that
  lands first wins, and the failover looks again. A planned switchover stops the primary first,
  as before.

`firebird_cluster_primary_lease_age_seconds` shows how long ago the primary renewed the Lease.
Enabling it restarts the instances (a rolling update, the primary last) to add the sidecar; the
kind CI enables it, cuts the primary off from the API server alone and checks that it fences
itself before the operator fails over.

### Synchronous Replication

```yaml
spec:
  instances: 3
  replication:
    enabled: true
    mode: sync
    synchronous:
      dataDurability: required      # default; or preferred
      standbyUnavailableSeconds: 30 # preferred only
      detachForUpdates: false       # required only: see Rolling updates below
      number: 1                     # synchronous standbys (default 1, at most instances - 1)
```

With `mode: sync` one replica, the **synchronous standby**, receives every change from the
primary directly (Firebird's `sync_replica`): a commit completes only once the standby applied it,
so the standby never misses a committed transaction. The other replicas stay asynchronous.
`status.synchronous` shows the last attach or detach and its state (`Attaching`, `Attached`,
`Detaching`, `Detached`, `Failed`) and `standbys`, every attached standby, with `SyncStandby*`
events.

**Several standbys** (`synchronous.number`, CloudNativePG's `number`): the primary lists every
attached standby as a `sync_replica`, and Firebird applies each commit on all of them before it
completes (verified: with one of two standbys down, commits fail and are applied on neither). So
`number: 2` means two standbys that each have every committed transaction; there is no "any one
of them" quorum. They are attached and detached one at a time; each one's unavailability counts
on its own (`preferred` detaches only the one that is down, `required` blocks writes until it is
back), and lowering `number` detaches the highest ordinal. A failover promotes the lowest-ordinal
ready standby. Rolling updates restart the other replicas first, then the standbys from the
highest ordinal down, each handed over as below.

- **Attaching** (a sync-standby Job, as soon as a replica is ready and has caught up, lowest
  ordinal first): the primary is put into full shutdown for a moment, the replica applies the
  primary's last journal segment, stops applying the journal (it would otherwise apply every
  change twice), and the primary's segment server writes the `sync_replica` entry to the file
  `replication.conf` includes; Firebird reads it when the database is opened again. Writes pause
  for these few seconds and clients are disconnected, as at the start of a switchover.
  `status.synchronous.message` and the `SyncStandbyAttached` event say how long (`writes paused
  3s`, v0.85.0).
- **Why writes pause**: Firebird reads `replication.conf` (with the included file) only when the
  database is opened, at its first attachment after the last one closed, and has nothing to
  reload it (verified on Firebird 5 and the 6 snapshot; [ISSUES.md](ISSUES.md) issue 9,
  `hack/repro/sync-replica.sh`). With clients attached only a shutdown creates that moment. It
  is also the point where the journal and the synchronous stream agree, so a standby switches
  between them without missing or repeating a change. The pause is the attach itself, not a
  wait: the standby is attached only once it has caught up.
- **Commits**: Firebird applies each transaction on the standby before the commit returns.
  `replication.conf` sets `report_errors = true` and `disable_on_error = false`, so when the
  standby cannot be reached the write fails on the primary (`Replication error`) and is not
  committed; once the standby is back, writes succeed again on their own.
- **dataDurability**: `required` keeps it that way while the standby is down (no write commits
  without it, CloudNativePG's `required`). `preferred` detaches a standby that has not been ready
  for `standbyUnavailableSeconds`; writes then continue asynchronously until a standby is attached
  again.
- **Detaching** (a sync-standby Job, again with a short write pause): before a planned switchover
  (the switchover waits for it), a re-seed or fencing of the standby, when it is removed by scaling
  down, when `mode` goes back to `async`, and in `preferred` mode as above. The standby's replica
  control file is moved to the primary's last segment, so it continues from the journal without
  being re-seeded; a standby that cannot be reached is re-seeded. A re-seed request for the
  standby (`reseed` annotation) waits until it is detached.
- **Failover**: an attached, ready standby is promoted without an election, and no committed
  transaction is lost. It is promoted in place like an elected replica; its journal continues
  after the last segment it saw archived on the old primary. The other replicas, which may lack the old primary's unshipped segments,
  are re-seeded from it, like the old primary when it returns. Without a ready standby, the
  election runs as for asynchronous replication.
- **Rolling updates** restart nothing while a sync-standby Job runs, and the standby last of the
  replicas, only once it is detached:
  with another updated replica ready and caught up, the standby is handed over to it first
  (detach, then attach the other one; two short write pauses instead of writes waiting through
  the restart); with `preferred` it is detached even without one (writes continue asynchronously
  until it is attached again after its restart). With `required` and no other replica, it is
  restarted attached and writes wait until it is ready again, unless `detachForUpdates: true`:
  then it is detached first as with `preferred`, and commits made during its restart are
  asynchronous (they reach it through the journal once it is back, and it is attached again).
  This gives up `required`'s guarantee for the length of the restart, for planned updates only;
  with two or more replicas the handover keeps it.
- Any other pod restart of the standby blocks writes with `required` until it is ready again. A
  standby whose volume is replaced outside the operator waits in its init container until it is
  detached (annotate it `reseed=true`).
- **Firebird 5 or later.** Firebird 4 commits while a `sync_replica` cannot be reached: it logs
  the error but does not return it to the client (`report_errors` has no effect there), and the
  replica never receives the transaction (verified on 4.0.7, [ISSUES.md](ISSUES.md) issue 8). A
  standby could then be promoted without transactions the primary committed, so on Firebird 4 the
  operator attaches no standby (it asks the primary's segment server for the engine version,
  `VERSION`), detaches any attached one, replicates asynchronously, and says so in
  `status.synchronous` (phase `Failed`) with a `SyncStandbyFailed` warning; automatic failover
  elects the most advanced replica instead.
- Firebird 5 and later read the standby's password from the server's environment
  (`password_env`). Nothing is written to the ConfigMap.

`hack/repro/sync-replica.sh` shows the Firebird behaviour this relies on: strict synchronous
commits, a replica applying the journal as well getting every change twice, and `sync_replica`
read from an included file when the database is opened again.

### Pod Management

The StatefulSet creates and recreates instance pods in parallel (`podManagementPolicy:
Parallel`). With the default `OrderedReady`, a deleted pod is not recreated while a lower ordinal
is not ready: after a failover to a higher ordinal, the promoted replica waited for the failed
primary, which waited to be re-seeded from it. The policy cannot be changed on an existing
StatefulSet, so the operator re-creates StatefulSets made by earlier versions once, orphaning
their pods: the new StatefulSet adopts them with the same template, and nothing restarts.

### Rolling Updates

The StatefulSet uses the `OnDelete` update strategy and the operator rolls the pods itself
(CloudNativePG's approach), so the primary is restarted only once:

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

**In place, without a restart:** when the only change between an instance's revision and the new
one is container resources (`spec.resources`, the exporter's resources), the operator resizes the
running pods (Kubernetes in-place pod resize, 1.33 and later) and marks them updated. It does
this for all instances at once, the primary included. No pod restarts, the primary stays where
it is, and a `RollingUpdate` event says `resized in place`. These cases still restart:
- a lower memory limit, since the running server may use more;
- a new memory limit where there was none;
- a change of QoS class;
- a resize the node refuses or doesn't apply within five minutes;
- any other change.

Firebird reads `firebird.conf` only when the server starts, so `config.settings` changes always
restart the instances.

Without replication the instances are independent. The operator restarts them one at a time,
highest ordinal first, each once every instance is ready again (as the StatefulSet controller's
`RollingUpdate` did), with no switchover and no supervision. Resource-only changes are applied in
place as above. Since v0.71.0 the StatefulSets of such clusters switch to `OnDelete` too; the
template does not change, so nothing restarts.

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
names in the data directory, to the cluster's own pods (NetworkPolicy), for requests signed
with the SYSDBA password. A physical restore from S3 works the other way:
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
For point-in-time recovery, the replica's segment server takes the `nbackup` with the segment
puller paused and every received segment applied, and keeps the replica's control file with it
(`<file>.nbk.ctl` next to the object): the primary's segment the copy reflects, and the
transactions it had in progress. The recovery continues the journal from there. The synchronous
standby receives changes outside the journal, so its backups carry no position and cannot be
used for point-in-time recovery.

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
target file that existed before is never touched: the operator refuses a restore into an existing
file before it creates the Job (status `Failed`, `already exists on <pod>`), and so does the
admission webhook when the restore is applied. To replace a database, bootstrap a new cluster from the backup:

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
The highest segment the Job listed for upload is kept in `status.journalArchiveSequence`. A
replica promoted by a failover continues its journal after it, even when it applied fewer
segments of the lost primary: otherwise its first segments would take the names of segments the
old primary had already uploaded (Firebird 4 and 5 segment names carry no GUID), and the Job
would skip them as uploaded. The promoted instance records the switch, and the Job uploads it
as an empty marker object `<database>.lineage-<P>-<U>`: segments P+1..U are the lost primary's,
not in the history of the segments after U. Point-in-time recovery to a target after the
failover skips them: it replays up to P, restarts the replay after U (transactions still open at
P are rolled back, as on the promoted replica) and continues with the new primary's segments. A
target before the failover replays the lost primary's segments as they were. A backup taken on
the lost primary after segment P cannot reach a target after the failover (refused).

**Point-in-time recovery.** A physical restore with `pointInTime` replays the archived journal on
top of an `nbackup` chain (taken on the primary, or on an asynchronous replica), up to a target:

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

`targetTime` recovers every segment archived at or before it, and the next segment up to the
target: the journal has no timestamps, so the primary's segment server samples the segments
being written every second and keeps "<time> <length>" recovery points per segment, which the
journal archive Job uploads as `<segment>.points`. The header length of a segment only grows by
whole writes (a commit's blocks), so the restore cuts the next segment at its last recorded
length at or before the target (header length set, file truncated) and replays it too: the
recovery point is within about a second of the target. Segments without points (archived before
v0.60.0, or by a primary of an older version) are applied whole, so the recovery point is then up
to `replication.archiveTimeoutSeconds` (plus the archive delay) before the target. Transactions
not committed by the recovery point are rolled back. `targetSegment` and the latest point apply
whole segments. A target before
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


#### Users created with SQL, and upgrades from before v0.12.0

A user created with plain SQL (`CREATE USER`, by an application or an administrator) exists only in
the security database of the instance it was created on: replication does not ship security
databases. Before v0.12.0 the security database was on the container filesystem, so such users were
lost on every pod restart, and a cluster upgraded from then starts with SYSDBA only.

`hack/users/unmanaged-users.sh <cluster> [-n <namespace>]` lists the users of a cluster that no
`FirebirdUser` manages, with the instances that have them. `--yaml` prints a `FirebirdUser` for each,
with its current active and admin flags and the roles it holds in the cluster database. They are
listed because a `FirebirdUser` revokes the roles it does not list. Firebird keeps password
verifiers, not passwords, so each resource refers to a Secret `<name>-password` you create with
the password the application uses. Applying the resource sets the user's password to the Secret's
value on every instance.

```sh
hack/users/unmanaged-users.sh my-cluster
# USER          ACTIVE   ADMIN  INSTANCES
# LEGACY_APP    yes      no     my-cluster-0
hack/users/unmanaged-users.sh my-cluster --yaml > users.yaml
kubectl create secret generic legacy-app-password --from-literal=password='...'
kubectl apply -f users.yaml
```

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
| `FirebirdCluster` | `SwitchoverStarted`, `SwitchoverPromoting`, `SwitchoverCompleted`, `SwitchoverFailed` (warning); `PrimaryNotReady`, `FailoverStarted`, `FailingOver`, `FailoverFailed` (warnings), `FailoverCancelled`, `FailoverCompleted`, `PrimaryRejoined`, `PrimaryLeaseHeld`; `SyncStandbyAttaching`, `SyncStandbyAttached`, `SyncStandbyDetaching`, `SyncStandbyDetached`, `SyncStandbyFailed` (warning); `TLSCertificateIgnored` (warning); `InstanceFenced`, `InstanceUnfenced`, `FencingFailed` (warning); `ReseedStarted`, `ReseedCompleted`; `RollingUpdate`, `RollingUpdateCompleted`; `ReplicaLagging` (warning); `VolumeResizing`, `VolumeResizeFailed` (warning); `ReconcileFailed` (warning) |
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

The operator writes two Firebird-internal formats itself: the replica control file and the
`HDR_repl_seq` header clump. `hack/firebird-formats/verify.sh [image ...]` (Docker) checks them
against each image's own engine and replica server: the sequence the engine reports, and that the
replica server skips and applies segments exactly at the position the control file records. The
"Firebird formats" workflow runs it for Firebird 4, 5 and the 6 snapshot when those scripts
change, every week, and on demand for any image (`workflow_dispatch`).

### Run Locally (against a cluster)

```bash
cd operator
npm run dev
```

The operator uses `~/.kube/config` when `KUBERNETES_SERVICE_HOST` is not set (local development mode).

## Roadmap

For planned features inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg)—including automated failover, `nbackup` physical backups, PITR, `gfix` sweeping, and dedicated backup CRDs—see [ROADMAP.md](ROADMAP.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
