# cloudnative-firebird Roadmap

Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg), `cloudnative-firebird` aims to deliver an enterprise-grade, cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/).

This document outlines the feature roadmap for upcoming releases, categorized by core operational domain.

> **Status note (v0.35.0):** journal replication (experimental) with replica re-seeding and lag metrics,
> planned switchover, automatic failover and rolling updates with the primary last, Kubernetes events, backups/restores, instance fencing and declarative users work against the official `firebirdsql/firebird` image (section 7). Some items below
> were marked done before they were implemented; they are annotated where that is the case
> (failover, synchronous replication, point-in-time recovery). The latest CloudNativePG changes
> (1.28 – 1.30.1) are reviewed in [docs/cloudnative-pg-review.md](docs/cloudnative-pg-review.md).
> Firebird-level problems found along the way are in [ISSUES.md](ISSUES.md), open work in
> [TODO.md](TODO.md).

---

## Roadmap Overview

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             cloudnative-firebird                                 │
└───────┬───────────────────┬───────────────────┬───────────────────┬──────────────┘
        │                   │                   │                   │
┌───────▼──────┐    ┌───────▼──────┐    ┌───────▼──────┐    ┌───────▼──────┐
│  HA &        │    │  Backup &    │    │  Database    │    │  Security &  │
│  Replication │    │  Disaster    │    │  Admin       │    │  Observ-     │
│  (Journals)  │    │  Recovery    │    │  (gfix)      │    │  ability     │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
```

---

## Detailed Feature Roadmap

### 1. High Availability & Replication (Inspired by CloudNative-PG HA)

- [x] **Basic Primary & Replica Service Split** *(v0.2.0)*
  - Dedicated `-replica` Service alongside primary ClusterIP service.
- [x] **Automated Failover** *(Lease since v0.4.0; failover v0.14.0)*
  - Opt-in `replication.failover`: when the primary has not been ready for `delaySeconds`, an election Job asks every ready replica for its applied position and the most advanced one is promoted (same offline promotion as a planned switchover); the Lease and the `targetPrimary` annotation move to it.
  - Replicas behind it and the old primary (when it returns) are re-seeded. Asynchronous replication: unshipped transactions are lost. A fenced primary is never failed over.
- [x] **Firebird 4.0+ Journal-Based Replication Management & PITR Archiving** *(v0.5.0, working since v0.9.0)*
  - Dynamic journal file sync, status tracking, and continuous archiving to S3.
  - Quorum management for synchronous replication (`mode: sync`): not implemented, rejected by validation.
- [x] **Smart Read-Only Traffic Routing** *(v0.6.0)*
  - Pod readiness and replication lag-aware endpoint management for read replicas.
  - Role-labelled pods: the rw Service targets the primary (leader Lease holder), the `-replica` Service targets eligible replicas.
  - Configurable `maxLagSeconds` threshold and fallback to the primary when no replica qualifies.

---

### 2. Backup, Disaster Recovery & PITR (Inspired by CloudNative-PG Barman Integration)

- [x] **Basic `gbak` Logical Backup CronJobs** *(v0.1.0)*
- [x] **Physical Incremental Backups (`nbackup`)** *(v0.3.0)*
  - Support for `nbackup` level 0 (full base backup) and level 1/2 incremental backups.
  - Delta file locking management during physical backup window.
- [x] **Cloud Object Storage Support (S3, GCS, Azure Blob)** *(v0.4.0)*
  - Direct upload of backup archives (`gbak` / `nbackup`) to object storage.
  - Secret-based cloud credentials management (`AWS_ACCESS_KEY_ID`, `GCS_KEY`, etc.).
- [x] **Point-In-Time Recovery (PITR) & Journal Archiving** *(archiving since v0.5.0, working since v0.10.0)*
  - Continuous archiving of Firebird 4.0+ replication journal files to object storage.
  - Replay of archived journal segments on top of `nbackup` base backups for exact timestamp recovery:
    not implemented yet (TODO.md).
- [x] **Dedicated Backup CRDs** *(v0.4.0)*
  - `FirebirdBackup`: On-demand backup custom resource.
  - `FirebirdScheduledBackup`: Cron-based scheduled backup custom resource with retention rules.
  - `FirebirdRestore`: Declarative restore custom resource for target database recovery.

---

### 3. Bootstrap & Cluster Cloning (Inspired by CloudNative-PG Bootstrap)

- [x] **Bootstrap from Cloud Backup** *(v0.5.0)*
  - Provision a new `FirebirdCluster` pre-populated from an existing S3/GCS backup.
- [x] **Cluster-to-Cluster Cloning** *(v0.5.0)*
  - Clone an active `FirebirdCluster` in the same or target namespace.
- [x] **Custom Initialization Scripts** *(v0.3.0)*
  - Execute DDL/DML SQL scripts (`.sql` ConfigMaps) during initial cluster creation.

---

### 4. Database Maintenance & Day-2 Operations (Inspired by CloudNative-PG Operations)

- [x] **PodDisruptionBudget (PDB) Reconciliation** *(v0.2.0)*
  - Ensures minimum available instances during node drains and maintenance.
- [x] **Suspended / Maintenance Mode** *(v0.2.0)*
  - Declarative `spec.suspended` flag to pause reconciliation during manual maintenance.
- [x] **Automated Database Sweeping (`gfix -sweep`)** *(v0.3.0)*
  - Cron-driven garbage collection sweeping to prevent Oldest Interesting Transaction (`OIT`) / Oldest Active Transaction (`OAT`) gaps.
- [x] **Online Database Diagnostics (`gfix -v -full`)** *(v0.4.0)*
  - Scheduled online database integrity checks and status reporting into CR status conditions.
- [x] **Declarative `firebird.conf` Management** *(v0.3.0)*
  - ConfigMap projections for custom `firebird.conf` settings (`DefaultCacheMem`, `FileSystemCacheThreshold`, `LockHashSlots`).
  - Automatic pod reload notification on configuration updates.
- [x] **Volume Expansion Reconciliation** *(v0.6.0)*
  - Storage PVC resizing support without cluster downtime.
  - In-place PVC expansion on `spec.storage.size` growth, shrink protection, and per-volume resize status.

---

### 5. Security & Isolation (Inspired by CloudNative-PG Security)

- [x] **TLS Encryption (`WireCrypt` / TLS)** *(v0.4.0)*
  - Encrypted client-to-database connections using Firebird 4.0+ TLS capabilities.
- [x] **cert-manager Integration** *(v0.4.0)*
  - Automated TLS certificate generation, injection, and zero-downtime rotation.
- [x] **Automated NetworkPolicies** *(v0.3.0)*
  - Auto-generated Kubernetes NetworkPolicy resources restricting port 3050 access to approved client labels/namespaces.
- [x] **SYSDBA & User Password Rotation** *(v0.5.0)*
  - Automated password rotation tracking for superuser and application credentials stored in Kubernetes Secrets.

---

### 6. Observability & Monitoring (Inspired by CloudNative-PG Monitoring)

- [x] **Prometheus PodMonitor CRD Support** *(v0.1.0)*
- [x] **Firebird Metrics Exporter Sidecar** *(v0.4.0)*
  - Embedded `firebird_exporter` sidecar container exposing metrics for:
    - Active attachments and connections
    - Page reads, page writes, and cache hit ratios
    - Transaction counters (`OIT`, `OAT`, `Next Transaction`)
    - Replication lag and journal queue depth
- [x] **Grafana Dashboard ConfigMaps** *(v0.4.0)*
  - Pre-built Grafana visualization dashboards shipped as deployment ConfigMaps.

---

### 7. Production Hardening & Cluster Lifecycle (Inspired by CloudNative-PG Operations)

- [x] **Complete Operator RBAC** *(v0.7.0)*
  - ClusterRole covers every resource the operator manages (CronJobs, Jobs, Leases, PDBs, NetworkPolicies, ConfigMaps, PodMonitors, Certificates, backup/restore CRDs).
- [x] **Correct Patch Semantics** *(v0.7.0)*
  - Whole-object updates are sent as JSON merge patches; Lease timestamps use `MicroTime` precision.
- [x] **Event Filtering & Resync-Based Retry** *(v0.7.0)*
  - Status-only watch events (unchanged `metadata.generation`) no longer trigger reconciles; failures retry on the periodic resync.
- [x] **Declarative Hibernation** *(v0.7.0)*
  - `spec.hibernated` scales the cluster to zero and suspends its CronJobs while retaining PVCs, Services and configuration.
- [x] **API-Server Integration Coverage** *(v0.7.0)*
  - kind-based CI exercises spec updates, PDB/Lease reconciliation, hibernation and resume against a real API server.
- [x] **Official Image Runtime Alignment** *(v0.8.0)*
  - PVC mounted at the image data directory, SYSDBA password via `FIREBIRD_ROOT_PASSWORD`, database created from `spec.databaseName`, `firebird.conf` via `FIREBIRD_CONF_*`.
  - Sweep and online-validation Jobs reach the primary over the network instead of mounting its PVC.
  - CI proves data survives a pod restart on a real kind cluster.
- [x] **Functional Journal Replication** *(v0.9.0)*
  - Shared `replication.conf`, perl segment-shipping sidecars, offline bootstrap seed on the primary.
  - Replicas seeded from ready replicas or the primary's offline seed; the live primary is not locked by default.
  - Experimental: a publishing Firebird 5.0.4 server hangs under concurrent per-transaction connections (ISSUES.md issue 1).
  - Follow-ups (lag reporting, seed refresh, retention by replica progress, sync mode) in TODO.md.
- [x] **Working Backups & Restores** *(v0.10.0)*
  - `gbak`/`nbackup` through the primary's service manager; logical backups streamed to the Job pod and uploaded by an `aws` CLI container.
  - `FirebirdBackup`, `FirebirdScheduledBackup` and `FirebirdRestore` are watched, and their status follows the Jobs; restores create a new database file.
  - Bootstrap from an S3 backup or by cloning a running cluster (`gbak` stream), primary-only with replication.
  - Journal archive CronJob fetches segments from the primary's segment server instead of mounting its volume.
- [x] **Planned Switchover** *(v0.13.0)*
  - `targetPrimary` annotation (CloudNativePG `kubectl cnpg promote`): stop writes on the primary, wait until every ready replica applied its last segment, move the Lease and promote the target offline with the old primary's replication sequence, demote the old primary to a replica.
  - No data loss; the other replicas continue without re-seeding; replicas that were not ready are re-seeded; a failed attempt leaves the old primary in place.
- [x] **Instance Fencing** *(v0.11.0)*
  - `firebird.cloudnative-firebird.io/fencedInstances` annotation in CloudNativePG's format (a JSON list of instances, `["*"]` for all).
  - The fenced database is put into full shutdown through the service manager; the pod keeps running, is not Ready and leaves the Services. No failover.
  - Readiness requires the database to be online; `status.fencedInstances` and a `Fenced` condition report the applied state.
- [x] **CloudNativePG 1.30 alignment** *(v0.11.0)*
  - Scale subresource label selector (`status.selector`) for HPA / VPA.
  - Immutable `clusterName` on backup, scheduled backup and restore resources (CEL).
- [x] **Rolling Updates with Primary Last** *(v0.15.0)*
  - With replication the StatefulSet uses `OnDelete` and the operator restarts outdated replicas one at a time, then the primary.
  - `primaryUpdateStrategy: unsupervised | supervised` and `primaryUpdateMethod: restart | switchover` (CloudNativePG); automatic failover waits for a primary restarted by the update.
- [x] **Declarative Database Users & Roles** *(v0.12.0)*
  - `FirebirdUser` resource per user (CloudNativePG 1.30 `DatabaseRole`): Secret-backed password, `active`, `admin`, role grants, `reclaimPolicy: retain | delete`.
  - Applied to every instance's security database and the role grants to the cluster database; tracked per instance and volume.
  - The security database moved from the container filesystem to the instance volume, so users survive pod restarts.
- [x] **Replica re-seeding** *(v0.12.0, CloudNativePG 1.28 `unrecoverable`)*
  - `firebird.cloudnative-firebird.io/reseed=true` on a replica pod: the replication init discards the database and replication state and seeds it again from a ready replica. The volume and its security database (users) are kept; the primary is never re-seeded.
- [x] **Quoted Object Names in Roles** *(v0.35.0)*
  - `FirebirdRole` privileges take `object.quoted: true` for objects created with delimited identifiers (`CREATE TABLE "Orders"`): the name is used as written (case-sensitive; spaces, `;`, `"` and reserved words work, verified against Firebird 5's isql). Validated by the operator and by CRD CEL rules (no control characters, no leading or trailing spaces); unquoted names keep their hashes, so existing roles are not applied again.
  - Fix: a reconcile no longer resets the phase of an existing cluster to `Creating` until it finishes (every resync made a running cluster read `Creating` briefly).
- [x] **Physical Backups to S3** *(v0.34.0)*
  - `type: physical` with `s3` on `spec.backup`, `FirebirdBackup` and `FirebirdScheduledBackup` (clusters with replication): the primary's server writes the `nbackup` file into its data directory, the Job copies it through the primary's segment server (new `FILE` / `STORE` / `REMOVE` commands for plain `*.nbk` names, served from a child process so transfers do not hold up replicas), removes it from the volume and uploads it.
  - `FirebirdRestore` with `restoreType: physical` and `s3` (or a completed physical `FirebirdBackup` in S3) downloads the chain, stores it next to the database, restores it with `action_nrest` and removes the copies, also when the restore fails.
- [x] **Offline Bootstrap Seed Kept Usable** *(v0.33.0)*
  - Without a replica holding segments back, the primary keeps the segments after its offline bootstrap seed up to `maxSegmentRetentionHours`, so an instance added later is seeded without locking the primary.
  - A missing or stale seed is refreshed by the primary's init container on a start after a clean stop (verified: the restarted server journals from the header sequence + 1; after an unclean stop it continues the open segment, so the refresh waits). A database that already publishes without a seed now gets one.
- [x] **Declarative Roles** *(v0.32.0)*
  - `FirebirdRole`: a role of the cluster database and exactly the privileges it holds (tables, views, procedures, functions, packages, sequences, exceptions). Revoke-all and grant in one transaction, so removed privileges go and none is missing in between; memberships are kept. Applied on the primary with replication, on every instance without; `reclaimPolicy: delete` drops the role. CRD CEL rules check privileges against the object kind.
- [x] **Re-creating a Replica's Volume** *(v0.31.0)*
  - `firebird.cloudnative-firebird.io/reseed=volume` on a replica pod replaces its PVC (a lost node with local storage, a broken disk): the operator deletes the claim and the pod, deletes the pod again until the StatefulSet has created a new claim, and the replica is seeded on the empty volume. Tracked in `status.recreatingVolumes`; the primary and standalone instances are refused.
- [x] **Backup Verification** *(v0.30.0)*
  - `verify: true` on backups, scheduled backups and `spec.backup` restores each logical backup into a scratch database (`gbak -c`) and runs a full validation; a backup that does not restore or validate fails. S3 backups are checked in the Job before upload; server-side backups through the primary's service manager, the scratch database dropped afterwards. `status.verified` on `FirebirdBackup`.
- [x] **Clones Through NetworkPolicies** *(v0.29.0)*
  - A source cluster with `networkPolicy.enabled` admits the instance pods of the clusters cloning from it (`bootstrap.clone`, any namespace; matched by namespace and cluster label, port 3050 only). The source is reconciled when a clone appears. Previously the clone was rejected unless `ingressFrom` was widened.
- [x] **Dropping Users from Unready Instances** *(v0.28.0)*
  - Deleting a `FirebirdUser` with `reclaimPolicy: delete` drops the user from the ready instances, then waits (phase `Dropping`, up to 15 minutes) for instances that hold it but are not ready, and drops it there once they are; `status.droppedFrom` tracks the progress. Previously those instances kept the user.
- [x] **Segment Retention Follows the Replicas** *(v0.27.0)*
  - The primary keeps archived journal segments a replica has not applied yet past `segmentRetentionHours`, up to `maxSegmentRetentionHours` (default 7 days): the operator sends the lowest applied segment to the primary's segment server (`RETAIN`), remembering replicas that are not ready. A slow or temporarily stopped replica catches up instead of needing a re-seed; `status.replicationStatus.segmentRetention` shows the floor.
- [x] **Instance Pod Watch** *(v0.26.0)*
  - The operator watches the instance pods of every cluster and reconciles a cluster as soon as one of its instances is created, deleted, becomes ready or unready, starts terminating, changes revision or is annotated for re-seeding, instead of waiting for the 30-second resync. Rolling updates, re-seeding, failover detection and read-only routing react right away.
  - The labels and annotations the operator writes on pods (role, read-routable, replication lag) are ignored, so its own patches do not trigger reconciles; bursts of pod events are coalesced into one reconcile per cluster.
- [x] **Maintenance Jobs on the Primary** *(v0.25.0)*
  - Sweep and online-validation Jobs connect to the primary instance (the Lease holder) rather than the `<name>` Service, which balances across all instances unless read-only routing is enabled; their CronJobs follow a switchover or failover.
- [x] **Operator Metrics** *(v0.24.0)*
  - `/metrics` on the operator's health port: instance counts, readiness, fencing, reconcile results and the measured replication lag per replica (`firebird_replication_lag_seconds`, `_lag_segments`, `_pending_segments`, `_applied_sequence`), so lag can be alerted on without an exporter.
  - Sample operator `PodMonitor`; the generated Grafana dashboard gains ready-instance and lag panels.
- [x] **Enabling Replication on an Existing Cluster** *(v0.23.0)*
  - The primary restarts first: publication is enabled on the existing database offline and the bootstrap seed written. Other instances keep their own database aside (`pre-replication-<timestamp>.fdb`) and are seeded as replicas.
- [x] **Backups from a Replica** *(v0.22.0)*
  - `target: prefer-standby` (CloudNativePG) takes logical backups to S3 from a ready, unfenced, non-lagging replica, keeping the load off the primary; falls back to the primary. `status.instance` reports where a backup ran.
- [x] **Backup Retention** *(v0.21.0)*
  - `retentionPolicy` (`<n>d`, `<n>w`, `<n>m`) on `spec.backup` and `FirebirdScheduledBackup` is enforced for logical backups to S3: expired objects of the schedule are deleted after each upload, the newest is always kept. Validated by the operator and the CRDs.
- [x] **Replication Lag** *(v0.20.0)*
  - The operator measures each replica's lag from the segment servers (archived segments on the primary vs. the replica's applied position) and reports it in `status.replicationStatus.replicas`.
  - Published as the `replication-lag-seconds` pod annotation, so lag-aware read-only routing now works without an external source.
  - Fixed: generated NetworkPolicies lost their `from` restrictions when created (the client's model names the field `_from`), so they allowed every pod.
- [x] **Pod and Container Security Contexts** *(v0.19.0, CloudNativePG 1.28)*
  - Instance pods: seccomp `RuntimeDefault`, no privilege escalation, all capabilities dropped except `CHOWN`, `DAC_OVERRIDE` and `FOWNER` (the official image runs the server as root): `baseline` Pod Security Standard.
  - Operator Jobs run as the non-root `firebird` user with no capabilities: `restricted` Pod Security Standard.
  - `spec.podSecurityContext` / `spec.securityContext` override the instance defaults.
- [x] **Service Accounts and Paused Resources** *(v0.18.0, CloudNativePG 1.29)*
  - `spec.serviceAccountName` for the instance pods and every Job of the cluster; `s3.secretRef` is optional, so S3 can be reached through workload identity (EKS IRSA / Pod Identity) instead of static keys.
  - `firebird.cloudnative-firebird.io/reconciliationDisabled` pauses a single backup, scheduled backup, restore or user.
- [x] **Admission Validation** *(v0.17.0)*
  - CRD CEL rules (`x-kubernetes-validations`) and OpenAPI constraints reject invalid specs at apply time, without a webhook: bootstrap sources, cron schedules, S3 references, physical backups to S3 without replication, restore paths, `sync` replication, storage shrink, reserved user and role names.
  - `hack/crd-validation/test.sh` runs valid and invalid manifests against the API server in CI; a unit test keeps the operator's own validation in agreement with the same manifests.
- [x] **Kubernetes Events** *(v0.16.0)*
  - Events on `FirebirdCluster`, `FirebirdBackup`, `FirebirdRestore` and `FirebirdUser` for switchovers, failovers (including `PrimaryNotReady`, CloudNativePG's `PrimaryStatusCheckFailed`), fencing, re-seeding, rolling updates, lagging replicas, volume expansion, reconcile failures, backups, restores and users (CloudNativePG 1.29 / 1.30).
  - Repeated events are aggregated into a count, like client-go's event recorder.

---

## Feature Comparison: CloudNative-PG vs cloudnative-firebird

| Feature Domain | CloudNative-PG (PostgreSQL) | cloudnative-firebird Status | Planned Release |
|---|---|---|---|
| **Primary/Replica Setup** | Native Streaming Replication | Basic Replica Service | **v0.2.0 (Done)** |
| **Read-Only Routing** | `-ro` / `-r` Services | Lag-aware `-replica` Service | **v0.6.0 (Done)** |
| **Failover / Promotion** | Automated Failover | Election of the most advanced replica (opt-in) | **v0.14.0 (Done)** |
| **Physical Backup** | Barman Cloud / `pg_basebackup` | `nbackup` (Level 0-2) | **v0.3.0 (Done)** |
| **Logical Backup** | `pg_dump` / CronJob | `gbak` CronJob & CRDs | **v0.1.0 (Done)** |
| **PITR (Point-In-Time)** | Continuous Archiving | Journal Archiving to S3 | **v0.5.0 (Done)** |
| **Bootstrap / Restore** | From Backup / Clone | Dedicated Restore & Bootstrap | **v0.5.0 (Done)** |
| **Node Maintenance** | PDB / Drain Handling | PDB Reconciled | **v0.2.0 (Done)** |
| **Volume Expansion** | PVC Resize | In-place PVC Expansion | **v0.6.0 (Done)** |
| **Hibernation** | Declarative Hibernation | `spec.hibernated` | **v0.7.0 (Done)** |
| **Switchover** | `kubectl cnpg promote` | `targetPrimary` annotation | **v0.13.0 (Done)** |
| **Rolling Updates** | Primary last, `primaryUpdateStrategy` / `primaryUpdateMethod` | Same settings, `OnDelete` StatefulSet | **v0.15.0 (Done)** |
| **Declarative Roles** | `DatabaseRole` / `managed.roles` | `FirebirdUser` | **v0.12.0 (Done)** |
| **Fencing** | Instance Fencing | `fencedInstances` annotation, database full shutdown | **v0.11.0 (Done)** |
| **Auto-Sweeping / Maintenance** | VACUUM Scheduling | `gfix -sweep` CronJob | **v0.3.0 (Done)** |
| **Security & TLS** | cert-manager | WireCrypt & cert-manager | **v0.4.0 (Done)** |
| **Metrics Exporter** | Built-in Exporter | Exporter Sidecar & PodMonitor | **v0.4.0 (Done)** |
