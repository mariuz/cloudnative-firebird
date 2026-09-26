# cloudnative-firebird Roadmap

Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg), `cloudnative-firebird` aims to deliver an enterprise-grade, cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/).

This document outlines the feature roadmap for upcoming releases, categorized by core operational domain.

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
- [x] **Automated Leader Election & Failover** *(v0.4.0)*
  - Kubernetes Lease-based leader monitoring for primary instance failover.
  - Automatic promotion of replica instances to primary role when primary pod fails.
- [x] **Firebird 4.0+ Journal-Based Replication Management & PITR Archiving** *(v0.5.0)*
  - Dynamic journal file sync, status tracking, and continuous archiving to S3.
  - Quorum management for synchronous replication (`mode: sync`).
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
- [x] **Point-In-Time Recovery (PITR) & Journal Archiving** *(v0.5.0)*
  - Continuous archiving of Firebird 4.0+ replication journal files to object storage.
  - Replay of archived journal segments on top of `nbackup` base backups for exact timestamp recovery.
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
- [ ] **Planned Switchover**
  - Declarative promotion of a chosen replica (`gfix -replica none`) with Lease handover and demotion of the old primary.
- [ ] **Instance Fencing**
  - Isolate individual instances (stop Firebird, keep the pod and PVC) for troubleshooting.
- [ ] **Rolling Updates with Primary Last**
  - Update replicas first, then switch over before restarting the primary to minimise write downtime.
- [ ] **Declarative Database Users & Roles**
  - Manage Firebird users, roles and grants from the `FirebirdCluster` spec with Secret-backed passwords.

---

## Feature Comparison: CloudNative-PG vs cloudnative-firebird

| Feature Domain | CloudNative-PG (PostgreSQL) | cloudnative-firebird Status | Planned Release |
|---|---|---|---|
| **Primary/Replica Setup** | Native Streaming Replication | Basic Replica Service | **v0.2.0 (Done)** |
| **Read-Only Routing** | `-ro` / `-r` Services | Lag-aware `-replica` Service | **v0.6.0 (Done)** |
| **Failover / Promotion** | Automated Failover | K8s Lease Leader Election | **v0.4.0 (Done)** |
| **Physical Backup** | Barman Cloud / `pg_basebackup` | `nbackup` (Level 0-2) | **v0.3.0 (Done)** |
| **Logical Backup** | `pg_dump` / CronJob | `gbak` CronJob & CRDs | **v0.1.0 (Done)** |
| **PITR (Point-In-Time)** | Continuous Archiving | Journal Archiving to S3 | **v0.5.0 (Done)** |
| **Bootstrap / Restore** | From Backup / Clone | Dedicated Restore & Bootstrap | **v0.5.0 (Done)** |
| **Node Maintenance** | PDB / Drain Handling | PDB Reconciled | **v0.2.0 (Done)** |
| **Volume Expansion** | PVC Resize | In-place PVC Expansion | **v0.6.0 (Done)** |
| **Hibernation** | Declarative Hibernation | `spec.hibernated` | **v0.7.0 (Done)** |
| **Switchover** | `kubectl cnpg promote` | Planned Switchover | Planned |
| **Fencing** | Instance Fencing | Instance Fencing | Planned |
| **Auto-Sweeping / Maintenance** | VACUUM Scheduling | `gfix -sweep` CronJob | **v0.3.0 (Done)** |
| **Security & TLS** | cert-manager | WireCrypt & cert-manager | **v0.4.0 (Done)** |
| **Metrics Exporter** | Built-in Exporter | Exporter Sidecar & PodMonitor | **v0.4.0 (Done)** |
