# Object stores other than S3

Status: design, 2026-11-23. Tracked in [TODO.md](../TODO.md) ("Object stores other than S3").

## The gap

Every object store the operator uses speaks S3, through the `aws` CLI (`amazon/aws-cli`, or
`clientImage`). An S3-compatible endpoint works: MinIO, Ceph, Cloudflare R2, and Google Cloud
Storage's interoperability mode with HMAC keys. Azure Blob Storage has no S3 API. On GCS, the
interoperability mode cannot use workload identity, and it rules out the IAM setup most GKE
clusters have.

CloudNativePG's Barman Cloud writes to S3, Azure Blob and GCS natively
(`barmanObjectStore.s3Credentials`, `azureCredentials`, `googleCredentials`). This design brings
the same three stores to every place where this operator reads or writes objects.

ROADMAP.md lists "Cloud Object Storage Support (S3, GCS, Azure Blob)" as done since v0.4.0. Only
S3 was ever implemented. The entry is corrected with this work.

## Where objects are read and written

| Place | Field | Operations | Where it runs |
|---|---|---|---|
| Backups to a store | `FirebirdBackup.spec.s3`, `FirebirdScheduledBackup.spec.s3`, `backup.s3` of the cluster | upload (`cp`), the `.ctl` file of a physical backup | backup Job (`utils/backup.ts`) |
| Retention | the same | list, delete (`s3RetentionScript`) | backup Job |
| Restores | `FirebirdRestore.spec` source `s3` | download; for physical backups the level chain and its `.ctl` | restore Job |
| Bootstrap | `bootstrap.recovery.s3` | download | bootstrap Job or init container |
| Journal archive | `replication.journalArchiveS3` | list, `sync` up (cluster and `journals/<file>/`) | archive CronJob |
| Point-in-time recovery | `FirebirdRestore.spec.pointInTime` (`journalS3`) | list, download segments | restore Job |
| Replica clusters | `replica.journalS3` | list, download segments | `archive-follower.sh`, a sidecar of the instance |

That makes 21 `aws s3` command lines. Every one is one of six operations:

- `list`: the names under a prefix, one per line, which every caller reads with `awk '{ print $4 }'`;
- `upload` and `download` of one object;
- `sync`, a directory up to a prefix;
- `remove` of one object;
- `exists`.

## Design

### One client for Azure and GCS: rclone

The `aws` CLI stays for S3, so S3 users see no change: the same image, scripts and pods. Azure
Blob and GCS use [rclone](https://rclone.org) (`rclone/rclone`, about 60 MB), configured entirely
from environment variables:

- `RCLONE_CONFIG_STORE_TYPE=azureblob` or `google cloud storage`, plus the account, key, SAS or
  service account fields;
- `env_auth=true` for workload identity: Azure's default credential chain (AKS workload identity,
  managed identity), and Google's Application Default Credentials (GKE workload identity, the
  metadata server).

The six operations are the same rclone commands for both stores: `lsf`, `copyto` (both ways),
`copy` for `sync`, `deletefile`, and `lsf` of one name for `exists`. That is why it is one
client, not two.

The alternatives:

- **`az` and `gcloud` CLIs.** These are the providers' own clients, but `mcr.microsoft.com/azure-cli`
  and `google/cloud-sdk` are images of 1 GB or more. Their commands and listing formats differ, so
  each operation would be written twice more.
- **Barman Cloud's Python clients.** These are tied to PostgreSQL's WAL layout.
- **A store client in the Rust agent** (`fb-agent objects ...`). This would avoid a client image
  altogether. But it means implementing Azure Shared Key and SAS, Google's OAuth with
  service-account JWTs, and both workload-identity flows. It is a later option once the shape is
  settled; the operations below are what it would implement.

### The command builder

`utils/object-store.ts` returns the shell for each operation, for a given store:

```ts
interface ObjectStoreCommands {
  list(prefix: string): string;      // names under the prefix, one per line
  upload(file: string, key: string): string;
  download(key: string, file: string): string;
  sync(dir: string, prefix: string): string;
  remove(key: string): string;
  exists(key: string): string;       // exit status 0 when it exists
}
function objectStoreCommands(store: ObjectStore, id?: string): ObjectStoreCommands;
// plus objectStoreEnv / Volumes / Mounts / Image, as s3ClientEnv & co. today
```

The S3 implementation produces today's command lines byte for byte. `list` keeps the
`aws s3 ls | awk '{ print $4 }'` pipeline, which moves from the callers into the builder.

The callers above change once: they call the builder instead of building `aws` lines. The
unit tests compare every Job and container of an S3 configuration with the specs generated
before, so the refactor changes nothing for S3.

### The API

There is one new sibling beside every `s3` field. Exactly one of the three is set, which the
webhook enforces, as does a CEL rule where the schema allows:

```yaml
backup:
  azureBlob:
    storageAccount: myaccount        # or endpoint: https://myaccount.blob.core.windows.net (Azurite: http://azurite:10000/devstoreaccount1)
    container: firebird
    prefix: prod
    secretRef: { name: azure-creds } # AZURE_STORAGE_KEY, AZURE_STORAGE_SAS_TOKEN or AZURE_STORAGE_CONNECTION_STRING
    # no secretRef: workload identity (inheritFromAzureAD in CloudNativePG)
  gcs:
    bucket: firebird-backups
    prefix: prod
    secretRef: { name: gcs-creds }   # key APPLICATION_CREDENTIALS: a service account's JSON key
    # no secretRef: workload identity / Application Default Credentials (gkeEnvironment in CloudNativePG)
    endpoint: http://fake-gcs:4443   # tests only
```

| Today | Beside it |
|---|---|
| `FirebirdBackup.spec.s3`, `FirebirdScheduledBackup.spec.s3`, `backup.s3`, restore source `s3`, `bootstrap.recovery.s3` | `azureBlob`, `gcs` |
| `replication.journalArchiveS3` | `journalArchiveAzureBlob`, `journalArchiveGCS` |
| `replica.journalS3` | `replica.journalAzureBlob`, `replica.journalGCS` |
| `pointInTime.journalS3` (restores, and `bootstrap.recovery`'s point in time) | `pointInTime.journalAzureBlob`, `pointInTime.journalGCS` |

Both new types also take `clientImage`, defaulting to the pinned rclone image.

**Not carried over:** `region` and S3's encryption settings (`encryption`, `kmsKeyId`,
`sseCustomerKey`). Azure and GCS encrypt every object at rest by default. A customer-managed key
is a property of the storage account or the bucket, not of the request, so there is nothing to
pass. Setting one of these fields on another store is refused.

**Status and locations:** `status.location` of a backup becomes `azure://<account>/<container>/<key>`
or `gs://<bucket>/<key>`, as it is `s3://<bucket>/<key>` today. A restore from a `FirebirdBackup`
resolves its store from the backup, as it does now.

### Credentials

| Store | Static | Workload identity |
|---|---|---|
| S3 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (unchanged) | IRSA, Pod Identity, instance profile (unchanged) |
| Azure Blob | the Secret's `AZURE_STORAGE_KEY` (with `storageAccount`), `AZURE_STORAGE_SAS_TOKEN` or `AZURE_STORAGE_CONNECTION_STRING`, mapped to `RCLONE_CONFIG_STORE_KEY` / `_SAS_URL` / `_CONNECTION_STRING` | `RCLONE_CONFIG_STORE_ENV_AUTH=true`, with `spec.serviceAccountName` carrying the `azure.workload.identity/client-id` annotation and the pod label |
| GCS | the Secret's `APPLICATION_CREDENTIALS`, mounted as a file (`RCLONE_CONFIG_STORE_SERVICE_ACCOUNT_FILE`) | `RCLONE_CONFIG_STORE_ENV_AUTH=true`, with `spec.serviceAccountName` bound to a Google service account |

AKS workload identity also needs the label `azure.workload.identity/use: "true"` on the pod. The
Jobs and the archive-follower sidecar get it when `azureBlob` has no `secretRef`. This follows
CloudNativePG's `inheritFromAzureAD`.

### Testing

The kind CI already runs an S3 server in the cluster (rclone `serve s3`). It gains:

- [Azurite](https://github.com/Azure/Azurite), the Azure Storage emulator, used with its
  well-known development account and key;
- [fake-gcs-server](https://github.com/fsouza/fake-gcs-server), used with rclone's GCS endpoint
  override and anonymous access.

The existing backup, restore, journal archive, point-in-time and replica cluster steps repeat
once against each store. Each phase below runs its own steps for both stores, so neither store
falls behind.

## Phases

1. **The command builder, S3 only.** `utils/object-store.ts`, with every caller moved onto it. The
   unit tests prove byte-identical Jobs for S3, and there is no API change.
2. **Backups, restores and bootstrap on Azure Blob and GCS.** The `azureBlob` and `gcs` fields on
   backups, schedules, restores and `bootstrap.recovery`; the rclone builder; credentials;
   retention; the webhook rules. The kind CI runs Azurite and fake-gcs-server and takes a
   logical and a physical backup to each, restores them, bootstraps a cluster from one, and
   checks retention.
3. **The journal archive and point-in-time recovery on Azure Blob and GCS**, declarative
   databases' `journals/<file>/` included. The kind CI repeats its PITR step against each store.
4. **Replica clusters following an Azure Blob or GCS archive**: `archive-follower.sh` on the
   builder, and the replica-source refusal of v0.184.0 compares stores of any kind. The kind CI
   repeats the replica cluster step against each store.

Each phase is a release. The ROADMAP entry of v0.4.0 is corrected in phase 2.

## Open questions

- **Field names.** Recommended: siblings beside `s3` (above), which change nothing for existing
  manifests. The alternative is CloudNativePG's single `destinationPath` with credentials per
  provider. It is closer to Barman Cloud, but means a second way to configure S3, and the
  `s3://` / `azure://` / `gs://` parsing in every resource.
- **rclone for S3 too.** One client for all three would remove the `aws` CLI. But it would change
  every existing S3 user's pods and the encryption flags (rclone has SSE-C and SSE-KMS under
  other names). This design keeps the `aws` CLI; the question can be asked again once the rclone
  stores have run for a while.
- **A native client in the agent** (above). It is deferred, and phase 1's builder is its
  interface.
