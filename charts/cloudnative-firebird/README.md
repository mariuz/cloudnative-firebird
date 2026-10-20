# cloudnative-firebird Helm chart

The [cloudnative-firebird](https://github.com/mariuz/cloudnative-firebird) operator: the same
Deployment, RBAC and admission webhook as the repository's `config/deploy`, with values, and the
CRDs in `crds/` (installed with the chart; `helm upgrade` does not upgrade them: apply the new
version's `config/crds` first).

```bash
helm install cloudnative-firebird oci://ghcr.io/mariuz/charts/cloudnative-firebird \
  --version <version> -n cloudnative-firebird-system --create-namespace
```

The names are fixed (`cloudnative-firebird-operator`, `cloudnative-firebird-webhook`: the
operator's own Lease, webhook Service and certificate Secret carry them), so one release per
cluster, in the namespace of your choice.

| Value | Default | What it is |
|-------|---------|------------|
| `replicaCount` | `2` | Operator replicas: one reconciles (leader election), every one serves the webhook |
| `image.repository`, `image.tag`, `image.pullPolicy` | `ghcr.io/mariuz/cloudnative-firebird`, the chart's `appVersion`, `IfNotPresent` | The operator image |
| `imagePullSecrets` | `[]` | For a private registry |
| `logLevel` | `info` | `debug`, `info`, `warn`, `error` |
| `firebirdDefaultImage` | `""` (`firebirdsql/firebird:latest`) | Firebird image of clusters without `spec.imageName` |
| `webhook.enabled` | `true` | The validating admission webhook (off, the operator still validates when it reconciles) |
| `segmentTLS.default`, `segmentTLS.migrate`, `segmentTLS.required` | `auto`, `""`, `false` | `SEGMENT_TLS_DEFAULT`, `SEGMENT_TLS_MIGRATE`, `SEGMENT_TLS_REQUIRED` (docs/segment-tls.md) |
| `primaryLease.default`, `primaryLease.migrate` | `auto`, `""` | `PRIMARY_LEASE_DEFAULT`, `PRIMARY_LEASE_MIGRATE` |
| `leaderElection` | `true` | Off: a single replica without the Lease |
| `extraEnv` | `[]` | More environment variables of the operator container (e.g. `RESYNC_INTERVAL_SECONDS`) |
| `resources` | 50m / 64Mi requests, 200m / 256Mi limits | The operator container's resources |
| `serviceAccount.annotations`, `podAnnotations`, `priorityClassName`, `nodeSelector`, `tolerations`, `affinity` | none; the replicas prefer different nodes | Scheduling |
| `podDisruptionBudget.enabled`, `podDisruptionBudget.maxUnavailable` | `true`, `1` | Keeps one replica through node drains |
| `podMonitor.enabled`, `podMonitor.interval` | `false`, `30s` | Scrape `/metrics` with the Prometheus Operator |
