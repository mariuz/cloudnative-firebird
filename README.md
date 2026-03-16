# cloudnative-firebird

A cloud-native Kubernetes operator for [Firebird SQL](https://firebirdsql.org/) database, written in TypeScript. Inspired by [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg).

## Overview

`cloudnative-firebird` automates the deployment and lifecycle management of Firebird database clusters on Kubernetes. It introduces the `FirebirdCluster` Custom Resource Definition (CRD) and a controller that reconciles cluster state.

### Features

- **Declarative cluster management** via `FirebirdCluster` CRD
- **StatefulSet-based** deployment for stable pod identity and storage
- **Persistent storage** via PersistentVolumeClaims
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

  # Additional container environment variables (optional)
  env:
    - name: FIREBIRD_DATABASE
      value: mydb.fdb
```

### Status Fields

| Field | Description |
|-------|-------------|
| `phase` | Current cluster phase: `Creating`, `Running`, `Updating`, `Degraded`, `Deleting` |
| `instances` | Configured number of instances |
| `readyInstances` | Number of ready instances |
| `conditions` | Standard Kubernetes status conditions (`Ready`, `Progressing`, `Degraded`) |

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
│   ├── tests/              # Unit tests (Jest)
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

## License

Apache 2.0 — see [LICENSE](LICENSE).
