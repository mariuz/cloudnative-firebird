# cloudnative-firebird

A cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/) database, written in TypeScript. Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg).

## Overview

`cloudnative-firebird` automates the deployment and lifecycle management of Firebird database clusters on Kubernetes. It introduces the `FirebirdCluster` Custom Resource Definition (CRD) and a controller that reconciles cluster state.

### Features

- **Declarative cluster management** via `FirebirdCluster` CRD
- **StatefulSet-based** deployment for stable pod identity and storage
- **Persistent storage** via PersistentVolumeClaims, with online volume expansion when `spec.storage.size` grows
- **Declarative hibernation** (`spec.hibernated`) that scales to zero while keeping data
- **Journal-based asynchronous replication** (Firebird 4.0+, experimental) with replicas seeded without locking the primary
- **Lag-aware read-only routing** to replicas via the `<name>-replica` Service (`spec.replication.readOnlyRouting`)
- **Secret-based** SYSDBA password management
- **Automatic service creation** (ClusterIP + headless for StatefulSet DNS)
- **Status reporting** with conditions and phase tracking
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

### Install the CRD

```bash
kubectl apply -f config/crds/firebirdcluster.yaml
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
  # on the StorageClass); shrinking is rejected and reported in status.volumes.
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

  # Backup configuration (optional)
  backup:
    enabled: true
    schedule: "0 2 * * *"
    retentionPolicy: "7d"

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
the primary through the read-write Service instead of mounting the instance PVC; diagnostics
use online validation (`fbsvcmgr action_validate`), which works while clients are connected.

### Status Fields

| Field | Description |
|-------|-------------|
| `phase` | Current cluster phase: `Creating`, `Running`, `Updating`, `Degraded`, `Deleting`, `Paused`, `Hibernated` |
| `instances` | Configured number of instances |
| `readyInstances` | Number of ready instances |
| `conditions` | Standard Kubernetes status conditions (`Ready`, `Progressing`, `Degraded`) |
| `replicationStatus` | Primary pod, number of active replicas, and with read-only routing the `readRoutablePods` and `laggingReplicas` |
| `volumes` | Per-PVC requested size, capacity and expansion state (`Ready`, `Resizing`, `ResizeFailed`, `ShrinkRejected`) |

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

```yaml
spec:
  instances: 3
  replication:
    enabled: true
    archiveTimeoutSeconds: 10     # ship partially filled segments after 10s
    segmentRetentionHours: 24     # keep archived segments on the primary for 24h
```

Known issues and open work are tracked in [ISSUES.md](ISSUES.md) and [TODO.md](TODO.md).

### Read-Only Traffic Routing

With `spec.replication.readOnlyRouting.enabled: true` the operator labels every pod with
`firebird.cloudnative-firebird.io/role` (`primary` / `replica`) and
`firebird.cloudnative-firebird.io/read-routable`. The primary Service selects only the
primary pod, and the `<name>-replica` Service selects only ready replicas whose replication
lag is within `maxLagSeconds` (default 30). Lag is read from the
`firebird.cloudnative-firebird.io/replication-lag-seconds` pod annotation, published by the
replication agent or metrics exporter; replicas without a lag report are routed on readiness
alone. When no replica qualifies, reads fall back to the primary unless
`fallbackToPrimary: false`. Clusters are re-reconciled every 30 seconds so routing follows
readiness and lag changes.

## Development

### Project Structure

```
cloudnative-firebird/
├── operator/               # TypeScript operator source
│   ├── src/
│   │   ├── types/          # CRD TypeScript type definitions
│   │   ├── controllers/    # FirebirdCluster controller (reconcile logic)
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
