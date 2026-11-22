# cloudnative-firebird and Kubebird

[Kubebird](https://github.com/henryx/kubebird) (Apache 2.0) is another Kubernetes operator for
Firebird. This page compares the two operators. It was written against Kubebird's `main` at commit
`222269d` (2026-11-21: release 0.4.0 plus unreleased changes) and cloudnative-firebird v0.184.0.
Both projects move quickly, so check their READMEs for the current state.

## In short

The two operators set out to do different things.

- **Kubebird** manages a **single Firebird server** (an `Instance`) that holds several databases.
  It covers that server's lifecycle as a whole: its databases, users, aliases and shadow files, and
  backups to a local volume. It also offers delete and recreate with automatic restore, and major
  upgrades through `gbak`.
- **cloudnative-firebird** manages a **cluster** of Firebird servers (`FirebirdCluster`). Following
  [CloudNativePG](https://github.com/cloudnative-pg/cloudnative-pg), it has one primary and
  replicas that follow it through Firebird's journal replication. It adds switchover, failover,
  rolling updates with the primary last, backups and point-in-time recovery to S3, and a separate
  CRD for each concern.

Choose Kubebird for one server, Firebird 3 support, a single small CR, and backups on a volume in
the cluster. Choose cloudnative-firebird for high availability, replicas, off-cluster backups and
PITR, or the CloudNativePG way of working.

## At a glance

| | Kubebird | cloudnative-firebird |
|---|---|---|
| Language, framework | Go, kubebuilder / controller-runtime | TypeScript operator; a Rust agent (`fb-agent`) in the instance pods |
| Custom resources | `Instance` (one CRD; databases, users and backups are fields of it) | `FirebirdCluster`, `FirebirdDatabase`, `FirebirdUser`, `FirebirdRole`, `FirebirdBackup`, `FirebirdScheduledBackup`, `FirebirdRestore`, `FirebirdImageCatalog`, `FirebirdClusterImageCatalog` |
| Topology | One pod (a single-replica StatefulSet) | 1 to n instances: a primary and replicas |
| Firebird versions tested in CI | 3.0.14, 4.0.3, 5.0.4 | 4, 5, 6 snapshot (replication needs 4 or later) |
| Watched namespaces | The ones listed in `WATCH_NAMESPACE` | All namespaces |
| Admission webhook | No (CRD schema and CEL rules) | Yes (validates clusters, backups, restores, databases, users) |
| Installation | `install.yaml` from a release, kustomize, `make deploy` | `install.yaml` bundle from a release, Helm chart |
| kubectl plugin | No | `kubectl firebird` (status, switchover, backups, `isql`, reports, …) |
| Metrics | controller-runtime metrics endpoint (kubebuilder scaffold) | Operator and per-instance metrics, `PrometheusRule` alerts, custom queries |

## Databases

| | Kubebird | cloudnative-firebird |
|---|---|---|
| Declared in | `spec.databases[]` of the `Instance` | One `FirebirdDatabase` per database, plus the cluster database `spec.databaseName` |
| Options | page size, charset, collation, owner, alias, shadow file | page size, character set, an `initSql` script, `reclaimPolicy` (`retain` / `delete`) |
| Aliases | `databases.conf` from a ConfigMap; one alias per database (clients connect by alias) | Not managed (clients use the path) |
| Shadow files | Yes, on a separate shadow PVC | No |
| Owner | `owner`: SYSDBA or a declared user; the database is created and restored as that user | Created by SYSDBA; access through `FirebirdUser` grants and `FirebirdRole` privileges |
| Removing one | `DROP DATABASE` when it leaves the list | `reclaimPolicy: delete` drops it; `retain` keeps the file |
| With several instances | — | Created on every instance; on replicated clusters, published on the primary and replicated to every replica (behind an experimental annotation, see [multi-database-replication.md](multi-database-replication.md)) |

## Users and credentials

Both operators generate the SYSDBA password into a Secret, or take an existing one, and keep the
security database on the data volume, so users survive a pod restart.

| | Kubebird | cloudnative-firebird |
|---|---|---|
| Regular users | `spec.authentication.<user>`, each with a generated or supplied Secret; created, re-passworded and deleted on the running server | `FirebirdUser` resources: Secret-backed or generated passwords, role grants, a reclaim policy, on every instance |
| Roles and privileges | — | `FirebirdRole`: a database role with exactly the privileges it lists on tables, views, procedures, functions, packages, sequences and exceptions |
| SYSDBA password | From the Secret; a change restarts the pod (a hash of the password on the pod template) | From the Secret (`FIREBIRD_ROOT_PASSWORD`), set when an instance starts |
| Secrets on deletion | Never deleted; reused when an `Instance` of the same name is recreated | Kept with no owner; adopted by a recreated cluster of the same name |

## Backups and restores

The two operators differ most here.

| | Kubebird | cloudnative-firebird |
|---|---|---|
| Destination | A dedicated backup PVC in the cluster (the only destination implemented) | S3 (or compatible), or server-side files on the instance volume; volume snapshots (CSI) |
| Logical (`gbak`) | Yes, through the Services API of the running server | Yes, through the service manager |
| Physical (`nbackup`) | Yes, with optional level 0 + level 1 pools | Yes, with levels |
| Schedules | `backup.retention`: fixed hourly / daily / weekly / monthly / yearly CronJobs, each with its own retention | `FirebirdScheduledBackup`: any cron schedule; `retentionPolicy` |
| Compression | gzip (`backup.compress`) | — |
| On-demand backups | `kubectl create job --from=cronjob/…` | `FirebirdBackup`, `kubectl firebird backup` |
| Backup on delete | `backupOnDelete` (default on): stops the pod and backs up every database and the security database before removing the volumes | Volumes are never deleted with the cluster, so no backup is needed |
| Restore | Automatic, when an `Instance` of the same name is recreated over the retained backup PVC | `FirebirdRestore` into a running cluster (a new database file), or a new cluster bootstrapped from a backup or cloned from another cluster |
| Point-in-time recovery | — | Yes, from the archived replication journal (`journalArchiveS3`), including for declarative databases (v0.183.0) |
| Security database | Included in the delete-time backup only; Firebird refuses an online backup of it | Volume snapshots include it, so a cluster bootstrapped from one keeps the source's users; `FirebirdUser` resources recreate the declared users on every instance |
| Off-site copy | — | Replica clusters follow the journal archive of a cluster elsewhere |

## Availability and day-2 operations

| | Kubebird | cloudnative-firebird |
|---|---|---|
| Replicas | — | Journal replication to n replicas, which are seeded without locking the primary |
| Synchronous standby | — | `replication.mode: sync` |
| Switchover / failover | — | Planned switchover with no data loss; opt-in automatic failover to the sync standby or the most advanced replica, with a primary Lease and fencing |
| Read-only traffic | — | `<name>-replica` Service, with lag-aware routing |
| Rolling updates | The StatefulSet's own rolling update (one pod) | Replicas one at a time, then the primary restarted or switched over |
| Hibernation, fencing, maintenance | — | `spec.hibernated`, the `fencedInstances` annotation, node maintenance windows |
| Encryption in transit | — | Wire encryption settings; TLS for replication segments |
| Status | Printer columns `VERSION`, `STATUS`, `DATABASES`, `MESSAGE`; `status.error`, `status.warning` (orphaned backups) | Phase and conditions, replication lag per replica and per database, Kubernetes events |

## Upgrades between Firebird versions

Both operators roll a minor or patch change out as an image change, and both refuse a move to an
older major version.

- **Kubebird** upgrades a major version (3 to 4, 4 to 5) through `gbak`. It backs up every
  database and the security database with the old image, deletes the volumes, then restores onto
  the new version. It needs the local backup volume. From Firebird 3 it migrates the users as the
  Firebird 4.0 migration guide describes.
- **cloudnative-firebird** upgrades a major on-disk structure (ODS) offline, as CloudNativePG does
  (Firebird 5 to 6). It converts the database and the security database on the primary, and the
  replicas are seeded again. Image catalogs let a cluster reference a major version, so editing
  the catalog rolls the change out. Firebird 4 to 5 has the same major ODS and is a rolling update.

## Deleting

- **Kubebird** deletes the primary and shadow volumes with the `Instance`. It keeps the backup
  volume, after a final backup when `backupOnDelete` is on. With no backup volume configured,
  deleting an `Instance` destroys its data.
- **cloudnative-firebird** never deletes a cluster's volumes, which have no owner. A cluster
  recreated under the same name adopts the StatefulSet, Services, ConfigMaps and Secrets that were
  left behind.

## Pod security

The official `firebirdsql/firebird` image runs as root. Both operators meet the `baseline` Pod
Security Standard by default: no privilege escalation, `RuntimeDefault` seccomp.
cloudnative-firebird also drops every capability except `CHOWN`, `DAC_OVERRIDE` and `FOWNER`. With
`runAsFirebirdUser: true`, its instance pods meet `restricted`. Its Jobs always meet `restricted`.

## Ideas worth borrowing

Kubebird has features that cloudnative-firebird does not, which may be worth adding:

- **Aliases**: a `databases.conf` entry per declarative database, so clients connect by name
  rather than by path.
- **Collation and owner** on `FirebirdDatabase`.
- **Shadow files** for a database without replication.
- **Compressed backups** (gzip).
- **Firebird 3**, for clusters without replication (journal replication needs Firebird 4).
- **A warning about orphaned backups**: Kubebird flags backups of databases that are no longer
  declared. cloudnative-firebird could do the same for the backups of a removed `FirebirdDatabase`.
