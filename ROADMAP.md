# cloudnative-firebird Roadmap

Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg), `cloudnative-firebird` aims to deliver an enterprise-grade, cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/).

This document outlines the feature roadmap for upcoming releases, categorized by core operational domain.

> **Status note (v0.54.0):** journal replication (experimental) with replica re-seeding and lag metrics,
> planned switchover, automatic failover and rolling updates with the primary last, Kubernetes events, backups/restores, instance fencing and declarative users work against the official `firebirdsql/firebird` image (section 7). Some items below
> were marked done before they were implemented; they are annotated where that is the case
> (failover, synchronous replication; both are implemented now). The latest CloudNativePG changes
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
  - Synchronous replication (`mode: sync`) with one synchronous standby since v0.53.0; quorum of several standbys not implemented.
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
- [x] **Point-In-Time Recovery (PITR) & Journal Archiving** *(archiving since v0.5.0, working since v0.10.0, replay since v0.45.0)*
  - Continuous archiving of Firebird 4.0+ replication journal files to object storage.
  - Replay of archived journal segments on top of `nbackup` base backups up to a target time or
    segment (`FirebirdRestore.spec.pointInTime`), at segment granularity.
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
  - Encrypted client-to-database connections. Corrected in v0.54.0: Firebird has no TLS listener; connections are encrypted by WireCrypt (required by default since Firebird 4), and `tls.enabled` restricts it to the ChaCha plugins.
- [x] ~~**cert-manager Integration**~~ *(v0.4.0, withdrawn in v0.54.0)*
  - The certificate was generated and mounted but never used by Firebird; `tls.secretName` / `tls.issuerRef` are now ignored (`TLSCertificateIgnored` event).
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
- [x] **Encryption in Transit Decided** *(v0.54.0)*
  - Firebird has no TLS listener; client connections are encrypted by WireCrypt, which Firebird 4 and later already require on the server by default (verified with operator-generated pods on 4.0.7, 5.0.4 and the 6.0 snapshot: an unencrypted client is refused, others negotiate ChaCha64).
  - `tls.enabled` now means strict wire encryption: `WireCrypt = Required` and `WireCryptPlugin = ChaCha64, ChaCha` (no RC4-based Arc4; an Arc4-only client is refused), and validation rejects `config.settings` that weaken it.
  - `tls.secretName` / `tls.issuerRef` are deprecated and ignored: the certificate was mounted but never read, and no cert-manager Certificate is created any more (`TLSCertificateIgnored` event). Segment shipping stays plain TCP inside the cluster (TODO.md).
- [x] **Synchronous Replication** *(v0.53.0)*
  - `replication.mode: sync` attaches one replica as the synchronous standby (Firebird `sync_replica`, strict: `report_errors = true`, `disable_on_error = false`): a commit completes only once the standby applied it, and fails while the standby cannot be reached. `synchronous.dataDurability: required | preferred` (CloudNativePG); `preferred` detaches a standby unavailable for `standbyUnavailableSeconds`.
  - A replica must not apply the same changes from the journal and synchronously (verified: it applies them twice), so a sync-standby Job attaches and detaches it with the primary briefly in full shutdown at the end of its last segment: the standby stops applying the journal, and the primary's segment server writes `sync_replica` to a file `replication.conf` includes, which Firebird reads when the database is opened again (verified on Firebird 4, 5 and 6). Detaching moves the standby's replica control file to the primary's last segment, so it continues from the journal without re-seeding (a running replica server picks the new position up).
  - The standby is detached before a planned switchover, a re-seed or fencing of it, and scaling it away. A failover promotes an attached, ready standby without an election: no committed transaction is lost, and the other replicas are re-seeded from it.
  - Safeguards: the standby's segment puller stops applying the journal only while the primary names it (`SYNCTO`), and an instance being seeded waits while the primary still replicates to it.
  - Fix (rolling updates): a reconcile no longer starts by rewriting the status of an existing cluster from its watch copy, which could predate the previous reconcile's write and drop the primary restart a rolling update had just recorded; automatic failover then counted the planned restart as an outage and failed over (kind CI, Firebird 4 and 6).
  - Fix (all failovers): instance StatefulSets use `podManagementPolicy: Parallel`. With `OrderedReady`, a failover to a higher ordinal deadlocked: the promoted replica was not recreated while the failed primary (lower ordinal) was not ready, and that one waited to be re-seeded from it (found by the new kind test, failing over from `sync-0` to `sync-1`). StatefulSets of earlier versions are re-created once with their pods orphaned and adopted; nothing restarts.
- [x] **Primary Isolation Check** *(v0.52.0, CloudNativePG `isolationCheck`)*
  - With automatic failover, the primary's segment server runs `isolation-check.pl`: when the primary has reached neither the Kubernetes API server nor another instance's segment server for `failover.isolationCheck.timeoutSeconds` (default 20), it puts its database into full shutdown, so clients cut off with it cannot write while a replica is promoted. The failover delay is at least the timeout plus 10 seconds.
  - A self-fenced primary that still holds the Lease when the operator reaches it again is brought back online through the segment server (`ISOLATION`, `REJOIN`; `PrimaryRejoined` event) instead of being failed over.
  - Verified on Firebird 4.0.7, 5.0.4 and the 6.0 snapshot in a container without network (fenced after the timeout, writes refused, back online on `REJOIN`, fenced again while still isolated), and on an internal network where only a peer answered on the headless name (not fenced until the peer stopped).
- [x] **Live Seeds by Default** *(v0.51.0)*
  - `replication.allowLiveSeedFromPrimary` defaults to true: when no replica is ready and the offline bootstrap seed is stale, a new replica is seeded from a locked copy of the live primary instead of waiting for the primary's next clean restart, so seeding no longer depends on restarts. Ready replicas and the offline seed still come first; `false` keeps the primary from ever being locked. Safe since the commit/TIP window is confirmed and handled (v0.49.0) and live seeds plan with `PLAN` (v0.50.0); verified with the setting unset (window transactions replayed, replica identical) and opted out (seed refused).
- [x] **Stricter Live Seed Planning** *(v0.50.0)*
  - Live seeds (`allowLiveSeedFromPrimary`) ask the primary's segment server with the new `PLAN` command, which scans the whole archive like `TXNS` and also returns the transactions numbered from the copy's next transaction on with blocks before the lock, and whether each first block begins its transaction. `replica-control.pl --next` records them all and refuses a transaction that began in a pruned segment instead of replaying it partially; against an older primary it falls back to `TXNS`.
  - Verified with operator-generated pods: 8 writers (replica identical, window transactions replayed), and a 200 000-row transaction journaled two segments before the lock and committed after it (replayed from segment 1, replica identical). A first attempt with `pitr-plan.pl` over the segments up to the lock only missed that transaction: a live seed cannot see the segments after the lock, so the plan is made where the whole archive is.
- [x] **Commit/TIP Window Confirmed** *(v0.49.0)*
  - ISSUES.md issue 2 confirmed on Firebird 5.0.4: `hack/repro/tip-window.sh` finds transactions committed in the journal before a backup lock but not in the copy in 18 of 20 level-0 backups under 8 writers, and replicas built with Firebird's documented procedure lose 2 transactions per run (3 of 3). The operator's live seed replays them (replica identical in 4 of 4 runs); `replica-seed-race.sh` gains persistent connections and `ROWS`.
  - A cluster deleted while it is reconciled no longer logs "Reconciliation failed": when a call fails with 404 and the cluster itself is gone (its StatefulSet garbage-collected under the reconcile), the reconcile stops quietly instead of recording a failure and a `ReconcileFailed` event.
- [x] **Default Image Setting and CI per Firebird Version** *(v0.48.0)*
  - The operator's `FIREBIRD_DEFAULT_IMAGE` environment variable sets the Firebird image of clusters without `spec.imageName` (CloudNativePG's operator-wide default image), e.g. a mirror in a private registry; the default stays `firebirdsql/firebird:latest`.
  - The kind integration tests run once per Firebird version (default image, `firebirdsql/firebird:4`, `firebirdsql/firebird:6-snapshot`) by setting it, and check the version the instances run. The snapshot run is reported without failing the workflow.
- [x] **Firebird 4 and 6 Verified** *(v0.47.0)*
  - Replication (seeding, switchover, failover, re-seeding, fencing), backups, restores and point-in-time recovery verified end to end with operator-generated pods on `firebirdsql/firebird:4` (4.0.7) and `firebirdsql/firebird:6-snapshot` (6.0.0.2191), besides Firebird 5.
  - Firebird 6 (ODS 14) moved the header page fields: `set-repl-seq.pl` (switchover, failover) now reads the ODS version and uses the ODS 13 or 14 layout, and refuses an unknown one.
  - Firebird 6 refuses header statistics for a database in full shutdown (ISSUES.md, issue 7): switchover reads the old primary's final sequence through its segment server (`HEADER`, from the header page on disk), and the fencing Job takes the refusal as "in full shutdown".
- [x] **Bootstrap to a Point in Time** *(v0.46.0)*
  - `bootstrap.recovery.pointInTime` (with `incrementalPaths` and a required `journalS3`): before the StatefulSet exists, the operator creates the first instance's volume under the StatefulSet's claim name and runs the Job `<cluster>-pitr-recovery` on it (CloudNativePG's recovery Job). The Job replays the journal archive like a point-in-time `FirebirdRestore` and leaves the database where the instances' init containers expect a restored one (`.bootstrap.fdb` with replication). The cluster stays `Creating` until it completes, then the StatefulSet adopts the volume. Kubernetes 1.24+ has no native sidecars, so the S3 client runs alongside the Firebird container in a Job rather than in init containers.
  - Validated by the operator and CRD CEL rules: an nbackup source in S3, a journal archive other than the new cluster's own, replication for more than one instance.
- [x] **Point-In-Time Recovery** *(v0.45.0)*
  - `FirebirdRestore.spec.pointInTime` (`targetTime`, `targetSegment`, or every archived segment; `journalS3` defaults to the cluster's journal archive): the restore Job restores the `nbackup` chain into a scratch database with its replication sequence (`nbackup -SEQ -R`), makes it a read-only replica and lets a private Firebird server in the Job pod apply the archived segments, then restores the result into the target database on the primary. An S3 client container in the pod lists the archive and downloads the segments the replay asks for.
  - The `nbackup` lock switches the journal (`BEGIN BACKUP`), so the segments after the backup's sequence hold the later changes; `pitr-plan.pl` writes the replica control file with every transaction open in the backup (not committed in it, or started after its next transaction) and its first segment, fetching earlier segments until each one starts in the directory, and refuses a transaction complete in the backup with changes after it. Verified with Firebird 5: concurrent writers, a long transaction open across both backups whose journal blocks were flushed segments before the base, and cuts between segments.
  - The journal archive Job records each segment's archive time as an empty marker object (`<segment>.archived-<time>`, from `ARCHIVED`). The Job scripts (`fetch-segments.pl`, `pitr-plan.pl`, `pitr-restore.sh`) are no longer hashed into the instance pod templates.
- [x] **Physical Backups From a Replica** *(v0.44.0)*
  - `target: prefer-standby` now applies to physical backups to S3: `nbackup` runs in the chosen replica's server (verified with Firebird 5 on a read-only replica, while it keeps applying the primary's segments) and the file is copied through its segment server. Chains live in the backup history of the instance they were taken on. A physical restore clears the replica mode such a backup carries (`prp_rm_none`), so the restored database is writable.
- [x] **Backup File Server Without Replication** *(v0.43.0)*
  - Instances of clusters without replication run a `backup-files` sidecar: the segment server in a files-only mode (`FILE` / `STORE` / `REMOVE` / `FILES` for plain `*.nbk` / `*.fbk` names). Physical backups to S3, physical restores from S3 and `retentionPolicy` for server-side backups now work without replication; the replication requirement is gone from the operator and the CRD rules. The cluster ConfigMap always ships its scripts (hashed into the pod template), and the NetworkPolicy admits the cluster's own pods on its port.
- [x] **Pruning Applied Journal Segments** *(v0.42.0)*
  - `replication.pruneAppliedSegments: true` deletes archived segments on the primary as soon as every replica has applied them (below the measured floor) and, with `journalArchiveS3`, the archive Job has uploaded them: the Job now reports its uploads to the primary's segment server (`UPLOADED`) after a successful sync. The archive is bounded by the replicas' progress instead of `segmentRetentionHours`.
  - Decision on user passwords: they stay in SQL. A Firebird 5 trace logs services API user management with the password as well, and the services API would add the Job's command line; both are visible to administrators only.
- [x] **Pending User Drops** *(v0.41.0)*
  - A `FirebirdUser` (`reclaimPolicy: delete`) deleted while an instance holding it stays unready for 15 minutes no longer leaves the user there: the drop is recorded in `<cluster>-pending-user-drops`, the instance's `security-db-init` container drops it (embedded, before the server starts) on its next start, and the operator drops it again through a Job once the instance is ready, then clears the entry. Users declared again are kept. Verified with Firebird 5.
- [x] **Delimited Role Names** *(v0.40.0)*
  - `FirebirdRole.spec.roleName` and `FirebirdUser.spec.roles` accept names in double quotes (`'"Sales Team"'`, `""` for a quote), used exactly as written: created, granted, revoked and dropped as delimited identifiers (verified with Firebird 5, including names with `"` and `'`). Regular names keep their SQL and hashes, so existing roles and users are not applied again. Validated by the operator and CRD CEL rules.
- [x] **Rolling Updates Without Spurious Failovers** *(v0.39.0)*
  - Fix: a reconcile that started from a stale copy of the cluster rewrote the status without the rolling update's record of the primary restart, so automatic failover no longer waited for the restarted primary and promoted a replica (seen once in the kind tests). The rolling update now reads its state from the stored cluster, like the switchover state machine.
- [x] **Retention of nbackup Chains** *(v0.38.0)*
  - `retentionPolicy` now prunes physical (`nbackup`) series too, in S3 and (with replication) on the primary. Chains span schedules (a level N backup builds on the latest earlier level N-1 of the database, verified with Firebird 5), so the Job reads them from the primary's `RDB$BACKUP_HISTORY` and deletes an expired file only when no kept backup's chain needs it. Backups of other locations, on-demand ones and files unknown to the history count as kept.
- [x] **Clean Retries of Failed Physical Restores** *(v0.37.0)*
  - A failed `action_nrest` leaves a partial target database locked for backup merging (it expects a `.delta` file, so it cannot even be dropped), and every retry of the restore Job failed with "File exists". The Job now fixes it up (`action_nfix`) and drops it, server-side and from S3; a target that existed before the restore is never touched (nrest refuses it before writing anything). Verified with Firebird 5.
- [x] **Retention of Server-Side Backups** *(v0.36.0)*
  - `retentionPolicy` on `spec.backup` and `FirebirdScheduledBackup` now also prunes server-side logical backups on clusters with replication: after each backup (and its verification) the Job lists the schedule's `backup-<schedule>-<timestamp>.fbk` files in the primary's data directory through the segment server (new `FILES` command; `FILE` / `STORE` / `REMOVE` accept `*.fbk` too) and deletes the expired ones, always keeping the newest. Other schedules, manual backups and nbackup files are never touched.
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
| **PITR (Point-In-Time)** | Continuous Archiving | Journal Archiving to S3, replay onto `nbackup` chains | **v0.45.0 (Done)** |
| **Bootstrap / Restore** | From Backup / Clone | Dedicated Restore & Bootstrap | **v0.5.0 (Done)** |
| **Node Maintenance** | PDB / Drain Handling | PDB Reconciled | **v0.2.0 (Done)** |
| **Volume Expansion** | PVC Resize | In-place PVC Expansion | **v0.6.0 (Done)** |
| **Hibernation** | Declarative Hibernation | `spec.hibernated` | **v0.7.0 (Done)** |
| **Switchover** | `kubectl cnpg promote` | `targetPrimary` annotation | **v0.13.0 (Done)** |
| **Rolling Updates** | Primary last, `primaryUpdateStrategy` / `primaryUpdateMethod` | Same settings, `OnDelete` StatefulSet | **v0.15.0 (Done)** |
| **Declarative Roles** | `DatabaseRole` / `managed.roles` | `FirebirdUser` | **v0.12.0 (Done)** |
| **Fencing** | Instance Fencing | `fencedInstances` annotation, database full shutdown | **v0.11.0 (Done)** |
| **Auto-Sweeping / Maintenance** | VACUUM Scheduling | `gfix -sweep` CronJob | **v0.3.0 (Done)** |
| **Security & TLS** | cert-manager | WireCrypt (required by default; strict with `tls.enabled`) | **v0.4.0, corrected v0.54.0** |
| **Metrics Exporter** | Built-in Exporter | Exporter Sidecar & PodMonitor | **v0.4.0 (Done)** |
