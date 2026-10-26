# CloudNativePG review (1.28 – 1.30.1, and main to October 2026)

cloudnative-firebird follows [CloudNativePG](https://github.com/cloudnative-pg/cloudnative-pg)
where Firebird allows it. This review walks through the CloudNativePG changes from 1.28.0
(December 2025) to 1.30.1 (September 23, 2026), based on the release notes in
`docs/src/release_notes/` and the fencing documentation (`docs/src/fencing.md`), and records what
each change means for this operator.

Status: **done** (in this repository), **planned** (listed in [ROADMAP.md](../ROADMAP.md) or
[TODO.md](../TODO.md)), **aligned** (already equivalent), **n/a** (no Firebird counterpart).

## Adopted in this round

| CloudNativePG | Here | Status |
|---|---|---|
| Fencing through `cnpg.io/fencedInstances` (a JSON list of instance names, `["*"]` for all) | `firebird.cloudnative-firebird.io/fencedInstances`, same format. The Firebird server is the container's main process, so instead of stopping it the database is put into full shutdown (`gfix -shut full` through the service manager); the pod keeps running, is not Ready, and no failover happens | done |
| 1.30: the `Cluster` scale subresource exposes `status.selector` for HPA / VPA | `labelSelectorPath: .status.selector` on the `FirebirdCluster` scale subresource | done |
| `kubectl get cluster` columns: Age, Instances, Ready, Status, Primary | `kubectl get firebirdcluster`: Age, Instances, Ready, Phase, Primary; `-o wide` adds the image the instances run (`status.image`), the last successful backup and the phase reason. Backups show their type and error, scheduled backups their type (v0.127.0) | done |
| `ScheduledBackup` status: `lastCheckTime`, `lastScheduleTime`, `nextScheduleTime` | `FirebirdScheduledBackup` status: the same three (`nextScheduleTime` cleared while suspended), for CronJob and volume snapshot schedules; printer columns and `kubectl firebird status` (v0.126.0) | done |
| 1.30: the `cluster` reference of `ScheduledBackup`, `Database` and similar resources is immutable (CEL rule) | `clusterName` is immutable on `FirebirdBackup`, `FirebirdScheduledBackup`, `FirebirdRestore` and `FirebirdUser` | done |
| 1.28: `alpha.cnpg.io/unrecoverable` (delete a replica's pod and PVCs, recreate it) | `firebird.cloudnative-firebird.io/reseed=true` on a replica pod (v0.12.0): the data is discarded and re-seeded from a ready replica, keeping the PVC (a StatefulSet recreates the pod immediately, so deleting its claim can deadlock) and the security database | done |
| `primaryUpdateStrategy` (`unsupervised` / `supervised`) and `primaryUpdateMethod` (`restart` / `switchover`): rolling updates restart the replicas first and the primary last | Same fields (v0.15.0). The StatefulSet uses `OnDelete` (with or without replication since v0.71.0) and the operator restarts one instance at a time, or resizes it in place when only resources changed; automatic failover waits for a primary restarted by the update | done |
| 1.29: Kubernetes events during reconciliation; 1.30: `PrimaryStatusCheckFailed` warning event | Events on clusters, backups, restores and users (v0.16.0); `PrimaryNotReady` when the primary stops being ready (with automatic failover enabled), aggregated like client-go | done |
| 1.29: shared `serviceAccountName` for workload identity (IRSA, Workload Identity) | `spec.serviceAccountName` for the instance pods and all Jobs; `s3.secretRef` optional, so the `aws` CLI can use the pod's credentials (v0.18.0) | done |
| 1.29: `cnpg.io/reconciliationDisabled` on backups | `firebird.cloudnative-firebird.io/reconciliationDisabled` on backups, scheduled backups, restores and users (v0.18.0); clusters use `spec.suspended` | done |
| 1.28: pod `securityContext` and per-container `containerSecurityContext` | `spec.podSecurityContext` / `spec.securityContext` over hardened defaults (v0.19.0): the official image runs the server as root and needs `DAC_OVERRIDE` for its lock directory, so instance pods meet `baseline`; operator Jobs run as the non-root `firebird` user and meet `restricted` | done |
| 1.30: `DatabaseRole` CRD with `databaseRoleReclaimPolicy: retain \| delete` | `FirebirdUser` (v0.12.0): Secret-backed password, `active`, `admin`, role grants, `reclaimPolicy`. Firebird users live in per-instance security databases, so they are applied to every instance; the security database was moved onto the instance volume first | done |

## Planned

| CloudNativePG | What it means here |
|---|---|
| 1.30: primary `Lease` as a promotion mutex; the instance must hold it before acting as primary, and releases it on clean shutdown | **Aligned (v0.86.0, the default for new clusters since v0.87.0):** with `failover.primaryLease.enabled` (or the recorded default) the primary's `lease-holder` sidecar renews the `<cluster>-lease` while its database is online, fences the database when the Lease names another instance or cannot be renewed, and the operator promotes a replica only once the Lease expired. Off, the Lease is only moved by the operator as before (v0.13.0, v0.14.0). |
| 1.30 security: operator-to-instance calls authenticated with a client certificate | Requests to the segment server are signed with the SYSDBA password (HMAC-SHA256, time and nonce: the password is not sent, requests cannot be replayed) since v0.64.0; since v0.76.0 the transfers run over mutual TLS with the cluster's own CA (`spec.segmentTLS`, the default for new clusters since v0.77.0; docs/segment-tls.md). |

## Already aligned

| CloudNativePG | Here |
|---|---|
| 1.30: validation during reconciliation when admission webhooks are unavailable, surfaced in status | The CRDs reject invalid specs with CEL rules (v0.17.0), and a validating webhook (v0.65.0, `failurePolicy: Ignore`, certificates managed by the operator) adds the checks that need other objects. Every reconcile still validates the spec (and the fencing annotation) and reports `Degraded` with the message, so objects admitted while the webhook was unavailable are caught too. |
| 1.29: "terminal error" phase for doomed backups | Invalid backup specs and failed Jobs end in `Failed` and are not retried. |
| 1.28: standard `app.kubernetes.io/*` labels | Used on every generated resource. |
| 1.28: replicas detect network drops within 5 s (`tcp_user_timeout`) | The segment puller uses 10 s connect and 60 s read timeouts and polls every 5 s. |
| 1.30: stable instance serials | StatefulSet ordinals are stable by construction. |

## Spec sweep (v0.119.0)

CloudNativePG 1.30.1's `Cluster` spec and status (`api/v1/cluster_types.go`), field by field,
against this operator. Adopted in this sweep:

| CloudNativePG | Here |
|---|---|
| `spec.description` | `spec.description`, shown by `kubectl firebird status` |
| `spec.enablePDB` | `spec.enablePDB`: false removes the cluster's PodDisruptionBudget (a node maintenance window removes it for its duration) |
| `monitoring.podMonitorRelabelings`, `podMonitorMetricRelabelings` | The same, on every endpoint of the cluster's PodMonitor |
| `status.instanceNames`, `writeService`, `readService` | The same: the instance pods in order, the primary Service, and `<name>-replica` with replication (unless turned off) or the primary Service again |

Already covered or not applicable (the field, and where it lives here):

| CloudNativePG | Here |
|---|---|
| `imageName`, `imageCatalogRef`, `imagePullSecrets`, `schedulerName`, `priorityClassName`, `instances`, `storage`, `resources`, `affinity`, `nodeMaintenanceWindow`, `primaryUpdateStrategy`, `primaryUpdateMethod`, `ephemeralVolumesSizeLimit`, `inheritedMetadata`, `serviceAccountName`, `superuserSecret`, `probes`, `backup`, `bootstrap`, `replica`, `monitoring.customQueries*`, `monitoring.enablePodMonitor`, `managed.services`, `primaryLease` | Same names (`affinity` as `affinity`, `podAntiAffinity`, `topologySpreadConstraints`, `nodeSelector`, `tolerations`; `primaryLease` under `replication.failover`) |
| `postgresql` (parameters, `pg_hba`, `pg_ident`, `shared_preload_libraries`, `ldap`, `syncReplicaElectionConstraint`, `synchronous`) | `config.settings` (firebird.conf), `replication.synchronous`; no `pg_hba`: client access is restricted by the NetworkPolicy |
| `minSyncReplicas`, `maxSyncReplicas` | `replication.synchronous` (one or several standbys, no "any N of M" quorum: Firebird waits for every `sync_replica`) |
| `logLevel` | The operator's `LOG_LEVEL`; the instances log through Firebird's own `firebird.log` |
| `enableSuperuserAccess` | n/a: SYSDBA always exists in Firebird; its Secret is `superuserSecret` |
| `certificates` (server and client TLS Secrets) | n/a: Firebird has no TLS of its own; WireCrypt encrypts the wire protocol (`tls.enabled` makes it required), and segment shipping runs over segment TLS |
| `walStorage` | n/a as a separate volume: the journal lives on the instance volume (`replication.journalDirectory`), archived to S3 |
| `serviceAccountTemplate` | `serviceAccountName`: the operator does not create a ServiceAccount per cluster |
| `podSelectorRefs` | n/a (`pg_hba` entries from pod selectors); `networkPolicy.ingressFrom` |
| `externalClusters` | `bootstrap.clone` and `replica.journalS3` name their sources inline |
| `tablespaces` | n/a: Firebird has no tablespaces |
| `plugins` (CNPG-I) | n/a: no plugin interface; backups run as Jobs with a configurable S3 client image |
| `replicationSlots` | n/a: journal segments are files, retained by `segmentRetentionHours` and the replicas' progress |
| `managed.roles` | `FirebirdUser` and `FirebirdRole` resources |
| `monitoring.tls`, `disableDefaultQueries`, `metricsQueriesTTL` | The exporter sidecars serve plain HTTP inside the cluster; the default metrics come from the exporter image; queries are read on every scrape |
| Status: `currentPrimary`, `targetPrimary`, `phase`, `phaseReason`, `conditions`, `readyInstances`, `instances`, `selector`, `image`, `managedRolesStatus`, `topology`, `pvcCount`, `jobCount`, `danglingPVC`, `resizingPVC`, `firstRecoverabilityPoint`, `lastSuccessfulBackup`, `timelineID`, `systemID`, `onlineUpdateEnabled`, `switchReplicaClusterStatus` | `replicationStatus.primaryPod`, `switchover`, `phase`, `phaseReason`, `conditions`, `readyInstances`, `instances`, `selector`, `image`, the `FirebirdUser` / `FirebirdRole` statuses, `volumes`, `replicaCluster`, `majorUpgrade`, `imageCheck`; `status.lastSuccessfulBackup`, `lastFailedBackup`, `firstRecoverabilityPoint` and their `ByMethod` maps (v0.123.0), the backups' own statuses and `kubectl firebird backups`; no timeline (the journal sequence continues across promotions) |

## Main branch after 1.30.1 (swept 2026-10-26)

CloudNativePG has released nothing after 1.30.1 (its `v1.30.md` and `v1.29.md` end with 1.30.1
and 1.29.3, September 23, 2026). The changes merged to its `main` branch since then, the future
1.31, were read from the commit messages (to `10bfae1`, October 26, 2026). Open items are in
[TODO.md](../TODO.md) ("Other roadmap items").

| CloudNativePG (main, PR) | Here | Status |
|---|---|---|
| A replica that stops being the target primary stops renewing the primary Lease (#11677) | The `lease-holder` sidecar renews only while its pod is the primary (the role in the ConfigMap file, or the promoted marker) and its database is online: a replica never renews, whatever the target. | aligned |
| The primary isolation check moves to the Lease: once the last renewal is `renewDeadline` old (10 of 15 s), the primary probes its peers and shuts PostgreSQL down at once, before a replica may promote (#11357) | The sidecar fences the database once the Lease could not be renewed for its whole duration, the moment it expires; the operator promotes only after that, through the election Job, so the margin is that Job's time. Fencing at a renew deadline before the expiry, as CloudNativePG now does, is an open item. The peer-based isolation check (`contactTimeoutSeconds`, v0.74.0) stays separate. | planned |
| The liveness probe becomes a watchdog: it fails when the Lease loop stopped calling the API, or when PostgreSQL ignores an immediate shutdown for `immediateShutdownTimeout` (#11358) | Every Lease call of the sidecar is bounded by a 5 s timeout and the loop runs in a native sidecar that the kubelet restarts if it exits, so a stuck loop is not expected; the liveness probe stays a TCP check of the Firebird port. A shutdown Firebird ignores is not detected. | aligned in part |
| A cluster recreated over the resources it left behind finds an orphan primary Lease: it is adopted, or deleted and recreated when restoring over orphan PVCs, and the operator may delete leases (#11609) | The operator creates `<cluster>-lease` only when none exists and otherwise leaves it as it is, so a cluster recreated after a deletion with orphan propagation (or a restore by a backup tool) keeps a Lease without its owner, naming the old primary: with the primary Lease on, the new primary's sidecar would fence itself if that name differs. Adopting or replacing an orphan Lease is an open item. | planned |
| A connect timeout on the instance manager's local connections, which hung on a stopped postmaster (#11718) | The metrics exporter runs every query under `timeout` (`QUERY_TIMEOUT_SECONDS`, 10 s); the segment server's local `isql` and `fbsvcmgr` calls have none, so a hung server blocks it (the operator's own requests to it time out). Bounding them is an open item. | planned |
| The status patches that start a Backup are conditional on the `resourceVersion` read, so a stale reconcile cannot start it twice (#11617) | Backup Jobs have deterministic names (a second create gets a conflict, which is ignored), so a backup cannot run twice; but the status is replaced from the copy the reconcile started with, so a stale copy can write an older phase back for a moment, as the fencing status could before v0.122.0. Conditional status writes are an open item. | planned |
| A declarative role can have its password generated into a `basic-auth` Secret, with renewal (#11337) | `FirebirdUser` takes its password from an existing Secret (`passwordSecret`). Generated passwords are an open item. | planned |
| A `cnpg-viewer-role` ClusterRole and read-only DBA RBAC samples (#11684) | No viewer role is shipped; read access is whatever the cluster's own roles grant. A `cloudnative-firebird-viewer` ClusterRole (aggregated to `view`) is an open item. | planned |
| `kubectl cnpg status` copes with a missing `pods/exec` permission (#11687) | `kubectl firebird status` reads the API only; `isql`, `logs` and `report` need exec or logs and say so through the API's own errors. | aligned |
| The exec timeout applies to each retry (#11651) | The operator never execs into pods (Jobs and the segment servers do the work); the plugin runs `kubectl exec` once. | n/a |
| `pg_basebackup` `additionalArgs` and `tablespaceMapping`, image extension paths, timeline-aware barman-cloud WAL markers, the replica cluster's system ID while PostgreSQL is down, 1.31 announced as the last release with native Barman Cloud, a TLA+ model of the primary Lease | PostgreSQL- or Barman-specific; the Lease protocol here is tested by unit tests and the kind CI, not modelled. | n/a |

## Not applicable

- PgBouncer / `Pooler` features (image catalogs, TLS settings, `auth_user`, metrics): Firebird has
  no pooler in this operator.
- `pg_upgrade` major upgrades and extension images: Firebird upgrades go through
  `gbak` backup and restore; tracked with bootstrap/restore.
- `search_path` pinning (CVE-2026-55769), SCRAM password encoding (CVE-2026-55765), TLS client
  certificates for roles, `pg_hba` pod selectors: PostgreSQL-specific. Firebird client access is
  restricted by the generated NetworkPolicy.
- Quorum-based failover (1.28): synchronous replication attaches one standby (v0.53.0), which a
  failover promotes without loss; a quorum of several standbys is not implemented.
- Barman Cloud plugin migration: backups here run as Jobs with a configurable S3 client image
  (`s3.clientImage`), so the transport is already outside the operator image.
