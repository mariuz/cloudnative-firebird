# cloudnative-firebird Roadmap

Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg), `cloudnative-firebird` aims to deliver an enterprise-grade, cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/).

This document outlines the feature roadmap for upcoming releases, categorized by core operational domain.

> **Status note (v0.86.0):** journal replication (experimental) with replica re-seeding and lag metrics,
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
  - Synchronous replication (`mode: sync`) with one synchronous standby since v0.53.0, several (`synchronous.number`) since v0.61.0; Firebird waits for every listed standby, so there is no "any N of M" quorum.
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
- [x] **Primary Lease as a Promotion Mutex** *(v0.86.0, opt-in; CloudNativePG 1.30's primary Lease)*
  - The cluster Lease was only moved by the operator; nothing held it. With `failover.primaryLease.enabled`, a `lease-holder` sidecar (operator image, native sidecar) in every instance pod renews the Lease every third of `durationSeconds` (default 15) while its pod is the primary and its database is online (segment server `STATE`), through the pod's ServiceAccount: a Role on that one Lease and a RoleBinding the operator manages, a token projected into the sidecar alone.
  - The primary fences its database (segment server `FENCE`, the isolation check's full shutdown) when the Lease names another instance or could not be renewed for the Lease's duration (API requests time out after 5 s), and brings it back itself (`REJOIN`) once it re-acquires the Lease, with the version it read. A database that is down stops the renewals. The operator no longer rejoins an isolated primary itself with the primary Lease on.
  - The operator promotes a failover's elected replica only once the Lease has expired (`PrimaryLeaseHeld` event while it waits), taking the Lease over with the version it read (a renewal that lands first wins); `firebird_cluster_primary_lease_age_seconds`. Unit tests cover the holder's decisions, the pod wiring, the RBAC objects and the gate; the kind CI cuts the primary off from the API server alone and checks that it fences itself before the failover replaces it.
- [x] **Synchronous Attach Without the Write Pause: Not Possible** *(v0.85.0, investigated)*
  - Firebird 5.0.3 and the 6.0 snapshot read `replication.conf`, and the file it includes, when a database is opened (its first attachment after the last one closed) and keep it with the database's global objects; nothing reloads it. A changed `sync_replica` is ignored while any attachment stays open and takes effect once every attachment is closed, without a shutdown (`hack/repro/sync-replica.sh`, [ISSUES.md](ISSUES.md) issue 9). With clients attached only a shutdown creates that moment, and it is also where the journal and the synchronous stream agree, so the pause stays.
  - The sync-standby Job now reports how long writes were stopped (`paused 3s` on the second line of its termination message); `status.synchronous.message` and the attach and detach events carry it.
- [x] **Refusing Plain Segment Shipping** *(v0.84.0, opt-in)*
  - Segment TLS is the default for new clusters (v0.77.0) and pinned clusters can be moved over (v0.79.0), but nothing stopped a cluster from being created plain, or switched back. `SEGMENT_TLS_REQUIRED=true` closes that: admission denies a cluster created with `segmentTLS.enabled: false` or updated to it (an already plain cluster may still be scaled or reconfigured), new clusters default to `true` whatever the Kubernetes version (with a startup warning when native sidecars are missing), and a cluster still plain gets the condition `SegmentTLS: False` (`PlainSegmentShipping`), a `SegmentTLSRequired` warning event and the new metric `firebird_cluster_segment_tls` at 0, until `SEGMENT_TLS_MIGRATE` or its owner moves it. The operator never restarts a cluster for it.
  - Unit tests cover the setting and the default, the admission rules for create and update, and the condition, event and metric of a plain cluster against a TLS one; the kind CI sets the variable on the operator, checks a plain cluster's condition and event, a denied creation and a denied switch-off, an allowed change of the plain cluster, and the metric, then unsets it.
- [x] **Signed Segment Requests Only** *(v0.83.0)*
  - Since v0.64.0 every client signs its segment server requests (HMAC-SHA256 with the SYSDBA password, a time and a nonce), but servers still accepted the plain `<password> <request>` form for clients of earlier versions, and clients first sent a signed `PING` to every server to tell an earlier one, which got the plain form.
  - Servers now refuse anything but a signed request (`ERR unauthorized (unsigned)`, even with the right password), and clients (`segment-auth.pl` for every Perl script and Job, the operator's `segmentRequest`) always sign, without the probe: one connection less per new server, and no code path that sends the password.
  - Upgrades start from v0.64.0 or later (README, *Upgrading the operator*): an operator from before then goes to v0.82.0 first. Unit tests check that the server refuses the plain form with the right password too, and that both clients send exactly one signed line that never contains the password.
- [x] **Adopting Users Created with SQL** *(v0.82.0)*
  - Users created with plain SQL exist only in the security database of the instance they were created on (replication does not ship it), and clusters upgraded from before v0.12.0 start with SYSDBA only (the security database was on the container filesystem). TODO.md asked for them to be re-created as `FirebirdUser`.
  - `hack/users/unmanaged-users.sh <cluster> [-n ns] [--yaml]` lists the users of every ready instance's security database that no `FirebirdUser` of the cluster manages (by its Firebird user name), with the instances that have them, and prints `FirebirdUser` resources to adopt them: active and admin flags as they are, the roles they hold in the cluster database (delimited names quoted; a `FirebirdUser` revokes the roles it does not list), and a Secret reference for the password, which Firebird cannot give back (verifiers only).
  - The kind CI creates a user with SQL on the primary and grants it a role, checks that the script lists exactly that user on that instance, that the generated resource keeps the role and passes server-side validation, and that nothing is listed once the user is dropped.
- [x] **Checking Firebird's Internal Formats per Image** *(v0.81.0)*
  - Seeding writes the replica control file (`ControlFile::DataV1`) and switchover the `HDR_repl_seq` header clump: formats Firebird does not document, verified by hand for 4.0.7, 5.0.4 and the 6.0 snapshot, and otherwise only covered indirectly by the long kind runs.
  - `hack/firebird-formats/verify.sh [image ...]` checks both in one container per image, with that image's own engine and replica server: the sequence `set-repl-seq.pl` writes (added and replaced) is the one the engine reports; and a replica whose control file (written by `replica-control.pl`) records one segment more than the copy has skips the primary's next segment and applies the one after, then the server moves the position on in the same format. The check was confirmed to fail with a wrong position (without a control file Firebird starts from the database's own sequence, which is why the file records one more).
  - The "Firebird formats" workflow runs it for Firebird 4, 5 and the 6 snapshot when those scripts change, weekly (a new snapshot may change a format), and on demand for any image, e.g. the Firebird 6 release.
- [x] **Current Peer Addresses for the Isolation Check** *(v0.80.0)*
  - Without DNS, the isolation check (v0.78.0) went by the peer addresses of its last DNS answer. A replica that restarted with a new pod IP while DNS was down then counted as unreachable, so a DNS outage combined with replica restarts could fence a primary that nothing would fail over.
  - The operator now publishes every instance's current pod IP in the cluster ConfigMap (`peer-addresses`; instances being deleted or without an address yet are left out). The pods mount it, so the kubelet keeps it current while the node reaches the API server, without DNS. When DNS does not answer, the check uses those addresses together with its cache (its own address excluded), and fences only when none of them answers.
  - Unit tests cover the published list (ready or not, no address, being deleted) and a stale cache with a current published address; the kind CI checks that the primary's mounted file lists every instance's address before it is cut off.
- [x] **Moving Existing Clusters to Segment TLS** *(v0.79.0, opt-in)*
  - Clusters created before v0.77.0 stay pinned to plain segment shipping until their owners switch. The operator now marks the clusters it pins (annotation `firebird.cloudnative-firebird.io/segment-tls-migration: pinned`) and, with `SEGMENT_TLS_MIGRATE=pinned`, switches them on itself; `all` also moves clusters pinned before the annotation existed and those whose owners chose `false`; `skip` keeps a cluster out.
  - Only an idle cluster is moved (running, every instance ready, no rolling update, switchover, failover or fencing, Kubernetes 1.29 or later), and one at a time across the operator: `enabled: true` and the annotation `in-progress` go in one patch, the usual lag-free switch follows, and the annotation becomes `done` once every instance runs the proxy. An owner who turns it off again during the migration gets `skip`. Events: `SegmentTLSMigrationStarted`, `SegmentTLSMigrated`, `SegmentTLSMigrationSkipped`.
  - Unit tests cover the setting, pinned against owner-chosen clusters, every reason to wait, one cluster at a time, finishing and stepping back, and the controller's patch; the kind CI creates a pinned cluster, sets `SEGMENT_TLS_MIGRATE=pinned` on the operator and checks that it ends with the proxy, the annotation `done` and both events.
- [x] **Fencing a Cut-Off Primary Without DNS** *(v0.78.0)*
  - The isolation check fences a primary that neither the operator nor any replica has reached for `contactTimeoutSeconds` (v0.74.0), but only when the headless Service lists other instances. It found them through cluster DNS, so a primary that lost DNS with the partition (the DNS servers on the other side) concluded it had no replicas and kept taking writes until the operator's failover deleted its pod, and those writes were lost at the re-seed.
  - The primary now looks its peers up on every check, in a child process with a 2-second limit (a DNS server that never answers made each lookup take tens of seconds over the search domains), and keeps the last answer on its volume. When DNS does not answer (not "no such name"), it uses those addresses, or the operator's ready replicas from the mounted cluster ConfigMap when it knew none, and fences itself only when none of the known addresses answers either (1-second probes). A DNS outage alone, in which it still reaches its peers, does not fence it: nothing fails it over then.
  - Unit tests cover the cache, a DNS server that fails or never answers, a peer that still answers, the operator's list as fallback and "no such name"; the kind CI cuts the primary off from its peers, the operator and DNS (kube-dns and CoreDNS) and checks that it fences itself from the peers it knew before the operator replaces it.
- [x] **Segment TLS by Default** *(v0.77.0)*
  - New clusters get `segmentTLS.enabled: true` when the API server is Kubernetes 1.29 or later (native sidecars), read at operator startup. The operator writes the value into the spec on a cluster's first reconcile, before any pod exists, and records a `SegmentTLSDefaulted` event, so the decision is visible and never changes afterwards.
  - Clusters that already have a StatefulSet (created by an earlier version) are pinned to `false`: upgrading the operator does not restart or change them. Values set by the user are kept. `SEGMENT_TLS_DEFAULT` (`auto`, `true`, `false`) changes the default; admission warns when segment TLS is asked for on Kubernetes without native sidecars.
  - Unit tests cover the version rule, the operator setting, new and existing clusters, and the admission warning; the kind CI checks that its first cluster is defaulted to TLS (spec, event, proxy, backup file server on localhost), and every later cluster runs with the default (the replicated one is pinned to plain so the switch step still starts from plain).
- [x] **Switching Segment TLS Without Replica Lag** *(v0.76.0)*
  - Turning `segmentTLS` on or off restarts the instances one by one, the primary last. Until the primary had restarted, replicas restarted in the new mode could not pull from it: they lagged through the whole rolling update, and Jobs reached only the instances in their own mode.
  - The operator now publishes the instances' modes in the `<cluster>-segment-tls-peers` ConfigMap, written before the StatefulSet on every reconcile: `plain-peers` (the instances without the proxy when switching on, all of them when switching off) and `accept-plain-until` (5 minutes after the last plain instance, for the kubelet's ConfigMap update delay). The proxies' client sides connect to the listed instances in plain text; their server sides tell TLS from plain text by the first byte (0x16, a TLS handshake record) and accept plain connections until then. Afterwards TLS only, as before; a failed TLS handshake never falls back to plain text.
  - Unit tests cover the proxy in both modes on one port, the client's routing and the ConfigMap in both directions; the kind CI holds the primary back (`primaryUpdateStrategy: supervised`) while the replicas already run the proxy, checks that they keep replicating from it and that it reaches them in plain text, then lets it restart and checks that plain requests are refused once the grace period is over.
- [x] **Encrypted Segment Shipping** *(v0.75.0, CloudNativePG's TLS between instances)*
  - Journal segments, seed copies and backup files went over plain TCP between the segment servers (requests were signed since v0.64.0, replies and data were not). The Firebird images have no TLS tooling (no openssl CLI, stunnel or Perl TLS module) and a pure-Perl cipher was too slow (under 1 MB/s).
  - `segmentTLS.enabled: true` runs a proxy from the operator image (`dist/segment-tls.js`) as a native sidecar in every instance pod (Kubernetes 1.29+): mutual TLS 1.3 on the segment port, forwarded to the segment server on localhost, and a client side the Perl clients reach with `SEGMENT_PROXY` (`CONNECT <host> <port>`, then the request as before). Jobs get the client side; the operator connects over TLS itself.
  - The operator manages a CA per cluster and one client/server certificate in `<cluster>-segment-tls` (renewed 30 days before expiry, the CA a year before; proxies reload it). Pods never mount the CA's key. During the rolling update that switches the mode, the operator reaches each instance in the mode its pod runs.
  - Unit tests run the proxy against the Perl segment server and client and check refused foreign CAs and plain connections; the kind CI enables it on the replicated cluster and checks replication, the lag measurement, a refused plain request, a backup Job and the restricted Pod Security Standard.
  - Design, diagrams and operations: [docs/segment-tls.md](docs/segment-tls.md).
- [x] **Fencing a Primary Nothing Reaches** *(v0.74.0, CloudNativePG's isolation check, extended)*
  - A primary that still reached the API server but that neither the operator nor any replica reached (one side of a network partition) was never fenced: clients on its side kept writing to it for the 1.5 to 3 minutes until the cut-off failover (v0.63.0) deleted its pod, and those writes were lost at the re-seed. Without a ready replica answering the operator, indefinitely.
  - The segment server now records every authenticated request it answers (`last-contact`): the operator's checks at least every reconcile, the replicas' segment pullers every 5 seconds. The isolation check fences the primary (database in full shutdown, as before) when nothing has reached it for `failover.isolationCheck.contactTimeoutSeconds` (default 60, 45 to 3600), counted from when it became the primary, and the headless Service lists other instances. On by default with the isolation check; `fenceWhenUnreached: false` turns it off. The operator brings it back online if it still holds the Lease when it reaches it again (`REJOIN`).
  - The operator waits `contactTimeoutSeconds - 15` (45 seconds) before failing over a cut-off primary, so it has fenced itself first: every replica must have lost it for 30 seconds, at most one pull after it was last reached, plus 10 seconds for the fence.
  - Unit tests run the isolation check against a recorded contact; the kind CI cuts a primary off from every pod and the operator with iptables (keeping the API server and DNS), and checks that it fences itself before the failover replaces it.
- [x] **Rolling Updates Without Waiting for the Only Synchronous Standby** *(v0.73.0, opt-in)*
  - With `dataDurability: required` and one replica, a rolling update restarted the synchronous standby attached, and every write failed until it was ready again: that is what `required` means (as with CloudNativePG), since no commit completes without a standby. With two or more replicas the standby is handed over first, so this only concerns clusters with a single replica.
  - `synchronous.detachForUpdates: true` (default false) lets the rolling update detach the only standby before its restart, as `preferred` does: two short write pauses (detach, then attach after the restart) instead of writes failing through the restart. Commits made meanwhile are asynchronous and reach the standby through the journal. Other restarts (a crash, a node drain) still block writes with `required`.
  - The kind CI rolls its synchronous cluster with `required` and `detachForUpdates`, commits on the primary while the standby is down, and checks the row on the standby once it is attached again.
- [x] **In-Place Resource Changes** *(v0.70.0, CloudNativePG applies what it can without a restart)*
  - Firebird has nothing to reload (`firebird.conf` is read when the server starts), but container resources can change without a restart, through Kubernetes in-place pod resize. The rolling update compares each outdated pod's StatefulSet revision with the new one (ControllerRevisions). When only container resources differ, it resizes the running pods (`pods/resize`) and labels them with the new revision once the kubelet has applied it. This covers all instances at once, the primary included.
  - Restarted as before:
    - a lower or newly set memory limit;
    - a change of QoS class;
    - a resize that is infeasible or not applied within five minutes;
    - any other template change.
  - The kind CI (Kubernetes 1.35) changes a replicated cluster's CPU limit and checks that every pod keeps its UID, runs the new revision with the new limit, and that the primary did not move.
  - Since v0.71.0 clusters without replication too: their StatefulSet uses `OnDelete` as well, and the operator restarts their independent instances one at a time, highest ordinal first (as the StatefulSet controller did), when the change cannot be applied in place. The kind CI resizes a two-instance cluster without replication in place after a QoS change restarted it.
- [x] **Operator High Availability** *(v0.69.0, CloudNativePG's leader election)*
  - The operator ran a single replica: when it was down, nothing reconciled (failover decisions included) and the admission webhook was not served. It now runs two replicas with leader election on the Lease `cloudnative-firebird-operator`, following client-go's design:
    - expiry is measured on each replica's own clock, so skew between nodes does not matter;
    - the leader renews every 2 seconds and exits when it cannot renew within 10;
    - a clean shutdown releases the Lease;
    - a crashed leader is replaced after 15 seconds.
  - Only the leader watches and reconciles. Every replica serves the admission webhook and is ready, so the webhook stays up while the leader changes. `cloudnative_firebird_operator_leader` shows the leader. A PodDisruptionBudget and preferred anti-affinity keep a replica through drains.
  - The kind CI checks that only the leader reconciles, then deletes the leader (an immediate handover) and force-kills the next one (a takeover after expiry), and checks that the new leader reconciles each time.
- [x] **Synchronous Standby Promoted in Place** *(v0.68.0)*
  - The failover to an attached synchronous standby was the last promotion that restarted its target. `PROMOTE` now accepts a standby. Its journal continues after the last segment it saw archived on the old primary (`sync-seen`), when that is higher than its control file position, as in the offline promotion. Its standby state is cleared.
  - Verified with operator-generated pods: a standby promoted after the primary died has every row committed on the primary, and it keeps running. The other replica re-seeds and follows, and it then becomes the new primary's standby. The kind CI checks `promotedInPlace` for the synchronous failover.
- [x] **Failover Without Restarting the Elected Replica** *(v0.67.0)*
  - Automatic failover restarted the elected replica so that its init container could promote it. Now, once the operator has committed to the failover (the Lease is moved; the election itself stays discardable), a promote Job sends the replica's segment server `PROMOTE`, with the journal archive's last segment, and reports the reply.
  - On success the replica keeps running (`status.switchover.promotedInPlace`). On any failure, and for the synchronous standby, it is restarted and promoted offline as before; a database already promoted is left as it is.
  - The offline promotion now also brings a database online first, in case an in-place promotion was cut short.
  - Fixed in the in-place promotion (v0.66.0): its bootstrap seed copy, taken in full shutdown, was still in full shutdown and replica mode, so replicas seeded from the new primary failed. It is now prepared like the offline promotion's seed once the database is online again (found by the failover e2e: the lagging replica and the old primary now re-seed from the promoted replica).
  - Verified with operator-generated pods: the elected replica takes writes without a restart, the other replica follows, the old primary is re-seeded, and a later restart with the directive changes nothing. The kind CI checks the in-place promotion.
- [x] **Switchover Without Restarting the Target** *(v0.66.0)*
  - A planned switchover restarted both the target and the old primary, and writes were down for both restarts. The switchover Job now promotes the target in place through its segment server's `PROMOTE`:
    - it pauses the puller and waits until every received segment is applied;
    - it puts the database into full shutdown, during which the server has the file closed;
    - it writes the replication sequence S (or the journal archive's last segment, with a lineage marker) into the header, replaces the replication state, and writes the offline bootstrap seed;
    - it brings the database back online with replica mode none and publication.
  - In Docker this took about a second on Firebird 4, 5 and the 6 snapshot. The journal continues at S + 1, and the other replicas follow without re-seeding.
  - Only the old primary restarts, to be demoted. The routing label moves the clients without a restart, and a `promoted` marker covers the time until the ConfigMap reaches the pod's files.
  - Safeguards:
    - on any failure the target stays a replica and is promoted offline, with a restart as before;
    - a Job that failed after promoting does not bring the old primary back;
    - a restart with the promote directive applies nothing twice.
  - Verified end to end with operator-generated pods; the kind CI checks that the target keeps its pod.
- [x] **Admission Webhook** *(v0.65.0, CloudNativePG's validating webhook)*
  - The operator serves a validating webhook (`config/deploy/webhook.yaml`) for checks the CRD rules cannot express because they need other objects. It refuses what the operator would fail for good: a restore over the cluster database, from a missing or failed backup, of the wrong type, or with an unusable point-in-time source. It also runs the operator's spec validation, including the checks the CRDs lack.
  - Missing clusters, Secrets and clone sources, and backups still running, are warnings (`kubectl` shows them), since one apply may create objects in any order. Updates are checked only when the spec changes, so the operator's own updates and deletions are never blocked.
  - Certificates without cert-manager: the operator creates a CA and a serving certificate (ECDSA P-256; DER written by the operator, since Node has no X.509 writer), keeps them in a Secret, renews them before expiry, and fills in the `caBundle`. `failurePolicy: Ignore`; every reconcile still validates.
  - The kind CI checks a refused restore, an admitted one, and a warning for a missing Secret against the API server.
  - Since v0.72.0 it also refuses a new restore into a database file that already exists on the primary. Only the instance sees its data directory, so the webhook asks the primary's segment server (`EXISTS <name>`, a plain file name in the data directory; also served by the backup file sidecar of clusters without replication). Without an answer within 1.5 seconds the restore is admitted. The operator asks again before it creates the restore Job and fails the restore with the reason, instead of a Job failing on nbackup's "File exists". The kind CI checks both paths on a replicated cluster and the webhook on a cluster without replication.
- [x] **Signed Segment Server Requests** *(v0.64.0, CloudNativePG 1.30 authenticated operator-to-instance calls)*
  - Every client of the segment server used to send the SYSDBA password in plain text with each request: the operator, the segment pullers, seeding, and the switchover, failover, sync-standby, backup and journal archive Jobs. Requests are now signed instead: `SIG1 <epoch> <nonce> <HMAC-SHA256(password, "<epoch> <nonce> <request>")> <request>`. The server accepts a request within five minutes of its own clock, once per nonce, and answers a bad signature, an old request or a replay with `ERR unauthorized (<reason>)`.
  - The Firebird image ships perl-base only, without Digest::SHA, so SHA-256 and HMAC are written in plain Perl (`segment-auth.pl`, included into each script when the ConfigMap is built). Unit tests check them against Node's crypto.
  - Upgrades: each client first sends a signed `PING`. A server of an earlier version answers it with `ERR unauthorized` and gets the legacy form; the client asks again a minute later. Servers still accept the legacy form from clients of earlier versions. Replies and transferred bytes are neither signed nor encrypted (TODO.md). (Since v0.83.0 neither: the probe is gone and the plain form is refused.)
- [x] **Failover of a Cut-Off Primary** *(v0.63.0)*
  - A primary whose pod stayed ready but that the rest of the cluster had lost was never failed over. Each replica's segment puller now records when it last reached the primary (`primary-seen`, segment server `PRIMARYSEEN`). The operator treats a ready primary as unavailable when it cannot reach its segment server and every ready replica it reaches has not reached the primary for 30 seconds or more. The automatic failover then runs as usual after `delaySeconds` and deletes the old primary's pod, which re-seeds as a replica.
  - The `PrimaryNotReady` event says why (`ready but cut off`, with each replica's last contact). The kind CI cuts a ready primary off from the other pods (iptables in a `kubectl debug` container, API server still reachable) and checks the failover, the moved Lease, the data, and the re-seeded old primary.
- [x] **Synchronous Replication Needs Firebird 5** *(v0.62.0)*
  - Running the recent features on Firebird 4 and the 6 snapshot showed that Firebird 4.0.7 commits while a `sync_replica` is unreachable: the error goes to `replication.log` only, and the replica never receives the transaction ([ISSUES.md](ISSUES.md) issue 8; `hack/repro/sync-replica.sh` shows it for one replica). A synchronous standby on Firebird 4 could be promoted without committed transactions.
  - The operator now asks the primary's segment server for its engine version (`VERSION`): on Firebird 4 it attaches no standby, detaches an attached one, replicates asynchronously and reports it (`status.synchronous` phase `Failed`, `SyncStandbyFailed` warning), and automatic failover elects instead of promoting the standby. No standby is attached while the version is not known yet.
  - The same runs passed on Firebird 4 and the 6 snapshot for recovery points, point-in-time recovery from a replica's backup and across a failover, non-root instances, and (on the 6 snapshot) two synchronous standbys. The kind CI checks the refusal on Firebird 4.
- [x] **Several Synchronous Standbys** *(v0.61.0, CloudNativePG `synchronous.number`)*
  - `synchronous.number` (default 1, at most instances - 1) standbys are attached one at a time; the primary lists each as a `sync_replica` (segment server `SYNC h1,h2`, `SYNCTO` reports the list), and Firebird applies every commit on all of them (verified: with one of two down, commits fail and are applied on neither).
  - `status.synchronous.standbys` lists the attached standbys, `unavailable` tracks each one's readiness (statuses of earlier versions are read as one standby). Each standby is detached on its own (fencing, re-seeding, `preferred` unavailability, scale-down, a lower `number`: the highest ordinal), re-seeds and volume re-creations wait for every standby, a switchover waits until all are detached, a failover promotes the lowest-ordinal ready one, and rolling updates restart the standbys last, highest ordinal first, each handed over first.
  - Verified with operator-generated pods (two standbys: attach under writes, every commit on both, one down blocks commits, detaching one keeps the other synchronous, convergence without duplicates); the kind CI scales its synchronous cluster to three instances with `number: 2`.
- [x] **Recovery Points Within a Segment** *(v0.60.0)*
  - Point-in-time recovery applied whole journal segments, so a target time landed on the end of the last segment archived before it (up to `archiveTimeoutSeconds` early). The primary's segment server now samples the segments being written every second (`RECOVERY_POINTS`, with a journal archive) and keeps "<time> <length>" points per segment; the archive Job uploads them as `<segment>.points`.
  - A segment's header length only grows by whole writes (a commit's blocks), and a segment cut at a recorded length (header length set, file truncated) is applied by the replica server up to there (both verified). The restore cuts the segment after the target at its last point at or before it: the recovery point is within about a second of the target.
  - Verified with operator-generated pods: one connection committing every 0.25 s, target in the middle of a segment; the recovery cut the next segment at its point and has 31 rows, within the 26–33 window one-second sampling allows, where whole segments stopped at the end of the previous segment.
- [x] **Non-Root Instances** *(v0.59.0)*
  - `runAsFirebirdUser: true` runs every instance container as the image's `firebird` user (uid 84, `fsGroup` 84) with all capabilities dropped: the instance pods meet the `restricted` Pod Security Standard (the default stays `baseline`, root).
  - The unmodified image entrypoint runs on a writable copy of `/opt/firebird` (an `emptyDir` filled by the `firebird-home` init container); the init scripts chown only as root.
  - Verified with operator-generated pods on Firebird 5 (seeding, switchover, failover, synchronous replication, point-in-time recovery across a failover, server process as uid 84); the kind CI runs the synchronous replication cluster this way and checks it against an enforced `restricted` namespace.
- [x] **Point-in-Time Recovery from a Replica's Backup** *(v0.58.0)*
  - A physical backup with `target: prefer-standby` is taken through the replica's segment server (`NBACKUP`): the segment puller is paused, every received segment applied, and the replica control file (position and transactions in progress) is kept with the backup as `<file>.nbk.ctl` in S3. Retention removes it with the backup.
  - Point-in-time recovery adopts it instead of planning from the database header (`pitr-plan.pl --describe`), downloading from the first segment its transactions in progress need. Backups without one (older ones, the synchronous standby's) are still refused.
  - Verified with operator-generated pods: level 0 and level 1 taken on the replica while the primary committed rows and kept a transaction open across both; recovery to the latest point matches the primary.
- [x] **Point-in-Time Recovery Across a Failover** *(v0.57.0)*
  - A replica promoted at segment P continues after the archive's segment U (v0.56.0); segments P+1..U are the lost primary's. The promoted instance records the switch, its segment server reports it (`LINEAGE`) and the journal archive Job uploads an empty marker `<database>.lineage-<P>-<U>`.
  - Recovery to a target after the failover replays up to P, stops the server (transactions open at P are rolled back), moves the replica control file after U (`pitr-plan.pl --reposition`) and continues with the new primary's segments; Firebird itself refuses a gap ("Required segment … is missing", verified). A backup taken on the lost primary after P is refused for such a target.
  - Verified with operator-generated pods: backup before the failover, 20 rows lost in it, 10 rows on the new primary; recovery to the latest point has exactly the new primary's rows and none of the lost ones.
- [x] **Promotion After the Journal Archive** *(v0.56.0)*
  - A failover could promote a replica that had applied fewer segments than the lost primary had already uploaded to `journalArchiveS3`. Its journal then reused those segment names (Firebird 4 and 5 names carry no GUID), and the archive Job skipped its segments as uploaded (reproduced on Firebird 4: 0 of 10 new segments uploaded).
  - The archive Job's fetch step reports the highest segment it listed (termination message), the operator keeps the maximum in `status.journalArchiveSequence` and passes it with the promote directive, and the promoted replica's journal continues after it (verified on Firebird 4 and 5 with operator-generated pods: elected at segment 20, promoted after the archive's 60, its segments 61–70 uploaded).
- [x] **Synchronous Standby in Rolling Updates** *(v0.55.0)*
  - The rolling update restarts the synchronous standby last of the replicas, and not while it is being attached or detached.
  - Before restarting it, the operator hands it over to another updated, caught-up replica (detach, then attach the other one), so writes pause twice for a few seconds instead of waiting through the restart. With `dataDurability: preferred` the standby is detached even without another replica; with `required` and no other replica it is restarted attached (writes wait, as before).
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
  - CRD CEL rules (`x-kubernetes-validations`) and OpenAPI constraints reject invalid specs at apply time (the admission webhook, v0.65.0, adds checks that need other objects): bootstrap sources, cron schedules, S3 references, physical backups to S3 without replication, restore paths, `sync` replication, storage shrink, reserved user and role names.
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
