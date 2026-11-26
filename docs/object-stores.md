# Object stores other than S3

Status: design, 2026-11-23 (revised: the cloud APIs directly, from the agent). Phase 1a,
`fb-agent objects`: v0.187.0. Phase 1b, `s3.client: agent`: v0.188.0. Phase 2, backups, restores, bootstrap and
retention on Azure Blob and GCS: v0.189.0. Phase 3, the journal archive and point-in-time
recovery on Azure Blob and GCS: v0.190.0. Tracked in [TODO.md](../TODO.md) ("Object stores other than S3").

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

### The cloud APIs directly, from the agent

`fb-agent` is a static binary that is already mounted in every instance pod and every Job
(`/opt/fb-agent/fb-agent`). It already speaks TLS (rustls with ring) and HTTP to the Kubernetes
API.

A new subcommand, `fb-agent objects`, performs the six operations against S3, Azure Blob or GCS
over their own REST APIs. It replaces all 21 `aws s3` command lines, and with them the client
image: no `amazon/aws-cli` (about 400 MB) and no third-party CLI. The containers that run it use
the instance's own image, with the agent mounted, as the other agent containers do.

The protocols come from Apache Arrow's
[`object_store`](https://github.com/apache/arrow-rs-object-store) (`=0.14.2`, MIT/Apache-2.0,
rust-version 1.85; the agent builds with 1.97). It is the object store layer of DataFusion,
InfluxDB and delta-rs. For each store it provides:

| | S3 | Azure Blob | GCS |
|---|---|---|---|
| Static credentials | access key and secret, session token | account key, SAS, client secret | service account JSON key |
| Workload identity | IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE`), EKS Pod Identity (`AWS_CONTAINER_CREDENTIALS_FULL_URI` and its token file), the instance profile (IMDSv2) | AKS workload identity (`AZURE_FEDERATED_TOKEN_FILE`), managed identity (IMDS) | Application Default Credentials, GKE workload identity through the metadata server |
| Encryption | SSE-S3 (`AES256`), SSE-KMS with a key id, SSE-C (`aws_sse_customer_key_base64`) | at rest by the service; a customer-provided key (`azure_storage_encryption_key`) | at rest by the service |
| Endpoint override | `aws_endpoint`, with `allow_http` (MinIO, the CI's server) | `azure_storage_endpoint`, `azure_storage_use_emulator` (Azurite) | `google_base_url` (fake-gcs-server) |

It also provides multipart uploads (S3) and the equivalents for the other two stores (Azure
block lists, GCS's multipart API). That matters for backups of many gigabytes. Listings are
paginated, and retries come with backoff.

**Built for the agent's targets.** The crate's `aws`, `azure` and `gcp` features bring in
`aws-lc-rs`, a C and assembly library the static musl builds would have to compile as well.
Instead, the agent uses the `*-base` features with the crate's `ring` crypto provider, and
`reqwest` 0.13 with `rustls-no-provider`, installing ring as rustls's provider. The agent already
does this for its own TLS, and the CI already compiles ring for both musl targets.

A probe with all three stores (the probe's own `Cargo.toml` is below) built to 4.9 MB, stripped,
with LTO and `opt-level = "s"`, against 2.5 MB for the agent today. With what they share (rustls,
ring, serde_json), expect the agent at 6 to 7 MB.

```toml
object_store = { version = "=0.14.2", default-features = false, features = ["aws-base", "azure-base", "gcp-base", "ring", "reqwest"] }
reqwest = { version = "0.13", default-features = false, features = ["rustls-no-provider", "http2"] }
tokio = { version = "1", features = ["rt"] }   # a current-thread runtime, in `objects` only
```

**What it costs:**

- **The async stack.** `object_store` is async: tokio, hyper, reqwest, about 170 crates in the
  probe's tree, against the agent's handful. `fb-agent objects` runs a current-thread tokio
  runtime of its own. The other subcommands stay synchronous and do not start it.
- **Pinning.** The crate is pinned (`=0.14.2`), as `firebird-wire` is. A version change is
  checked against the CI's three stores before it lands.

**Root certificates.** The cloud endpoints need the public roots. reqwest's platform verifier
reads the system's store, which an instance image may not have. The agent therefore also adds
the Mozilla roots (`webpki-roots`) through `ClientOptions::with_root_certificate`. A private
endpoint's CA (MinIO with an internal CA) is an optional `caSecretRef` beside `endpoint`, as an
open question below.

**The alternatives:**

- **The `aws` CLI with rclone for Azure and GCS** (this design's first version). It keeps two
  client images and two command sets.
- **The `az` and `gcloud` CLIs.** Their images are 1 GB or more, and their commands and listing
  formats differ.
- **The protocols written in the agent.** That means SigV4, Azure Shared Key and SAS, Google's
  service-account JWTs, three workload-identity token exchanges, multipart uploads and XML
  listings. It is a lot of security-sensitive code to own, and `object_store` owns it already.

### `fb-agent objects`

```
fb-agent objects list <prefix>        # names under the prefix, one per line (not recursive)
fb-agent objects put <file> <key>     # multipart above 64 MiB
fb-agent objects get <key> <file>     # to <file>.tmp, then renamed
fb-agent objects sync <dir> <prefix>  # each file of <dir> not listed under <prefix>
fb-agent objects rm <key>
fb-agent objects exists <key>         # exit status 0 when it exists, 1 when not, 2 on an error
```

The store comes from the environment: `OBJECT_STORE` (`s3`, `azure` or `gcs`), then the
crate's own variables, which its builders read (`from_env`). The `AWS_*` variables of today's
Jobs keep their names, so the S3 Secrets do not change. An SSE-C key file is read and base64
encoded for the crate. Errors go to stderr on one line, as the other subcommands do.

`sync` uploads what is not listed under the prefix yet. The journal segments are immutable and
named once, so this is `aws s3 sync`'s result for them. A key is relative to the configured
prefix, as `s3Uri` builds them today.

### The command builder

`utils/object-store.ts` returns the shell for each operation, for a given store and client:

```ts
interface ObjectStoreCommands {
  list(prefix: string): string;      // names under the prefix, one per line
  upload(file: string, key: string): string;
  download(key: string, file: string): string;
  sync(dir: string, prefix: string): string;
  remove(key: string): string;
  exists(key: string): string;
}
function objectStoreCommands(store: ObjectStore, id?: string): ObjectStoreCommands;
// plus objectStoreEnv / Volumes / Mounts / Container, as s3ClientEnv & co. today
```

The builder has two clients:

- **`aws`** produces today's command lines byte for byte. The unit tests compare every Job and
  container of an S3 configuration with the specs generated before.
- **`agent`** produces `fb-agent objects` lines. It is used for every Azure Blob and GCS store,
  and for an S3 store that opts in (`s3.client: agent`).

### The API

**S3:** `client: aws | agent` (default `aws`, the CLI as today). `agent` runs the transfers
through `fb-agent objects`, and `clientImage` is then ignored. The default moves to `agent` in
the last phase.

**Azure and GCS:** one new sibling beside every `s3` field. Exactly one of the three is set,
which the webhook enforces, as does a CEL rule where the schema allows:

```yaml
backup:
  azureBlob:
    storageAccount: myaccount        # or endpoint: https://myaccount.blob.core.windows.net (Azurite: http://azurite:10000/devstoreaccount1)
    container: firebird
    prefix: prod
    secretRef: { name: azure-creds } # AZURE_STORAGE_ACCOUNT_KEY or AZURE_STORAGE_SAS_KEY
    # no secretRef: workload identity or managed identity (inheritFromAzureAD in CloudNativePG)
  gcs:
    bucket: firebird-backups
    prefix: prod
    secretRef: { name: gcs-creds }   # key APPLICATION_CREDENTIALS: a service account's JSON key
    # no secretRef: Application Default Credentials / workload identity (gkeEnvironment in CloudNativePG)
    endpoint: http://fake-gcs:4443   # tests only
```

| Today | Beside it |
|---|---|
| `FirebirdBackup.spec.s3`, `FirebirdScheduledBackup.spec.s3`, `backup.s3`, restore source `s3`, `bootstrap.recovery.s3` | `azureBlob`, `gcs` |
| `replication.journalArchiveS3` | `journalArchiveAzureBlob`, `journalArchiveGCS` |
| `replica.journalS3` | `replica.journalAzureBlob`, `replica.journalGCS` |
| `pointInTime.journalS3` (restores, and `bootstrap.recovery`'s point in time) | `pointInTime.journalAzureBlob`, `pointInTime.journalGCS` |

**Not carried over:** S3's `region`, `encryption`, `kmsKeyId` and `clientImage`. Azure and GCS
encrypt every object at rest. A customer-managed key is a property of the storage account or
the bucket, not of the request. Setting one of these fields on another store is refused.
Azure's customer-provided key per request (the crate's `azure_storage_encryption_key`) could
come later, as `sseCustomerKey` did for S3.

**Status and locations:** `status.location` of a backup becomes `azure://<account>/<container>/<key>`
or `gs://<bucket>/<key>`, as it is `s3://<bucket>/<key>` today. A restore from a `FirebirdBackup`
resolves its store from the backup, as it does now.

### Credentials

| Store | Static (`secretRef`) | Without `secretRef` |
|---|---|---|
| S3 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (unchanged) | IRSA, Pod Identity, instance profile (unchanged): the variables EKS injects |
| Azure Blob | `AZURE_STORAGE_ACCOUNT_KEY` (with `storageAccount`) or `AZURE_STORAGE_SAS_KEY` (the crate also reads `AZURE_STORAGE_SAS_TOKEN`) | AKS workload identity (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, injected by its webhook), or managed identity |
| GCS | `APPLICATION_CREDENTIALS`, mounted as a file (`GOOGLE_APPLICATION_CREDENTIALS`) | Application Default Credentials: GKE workload identity through the metadata server |

AKS workload identity injects its variables only into pods labelled
`azure.workload.identity/use: "true"`. The Jobs and the archive-follower sidecar get that label
when `azureBlob` has no `secretRef` (CloudNativePG's `inheritFromAzureAD`). Their service account
is `spec.serviceAccountName`, as for S3's workload identity today.

### Testing

- **The Rust agent job.** It gains `hack/agent/objects.sh`, against three servers in Docker:
  - rclone `serve s3` (the server the kind CI already uses);
  - [Azurite](https://github.com/Azure/Azurite), the Azure Storage emulator, with its
    well-known development account and key;
  - [fake-gcs-server](https://github.com/fsouza/fake-gcs-server).

  For S3 and Azure it checks every operation: a multipart object of more than 64 MiB, a listing
  past the first page, `sync` of what is missing only, `exists` exit statuses, and a refused
  credential. For GCS it checks the reads only. `object_store` uploads through GCS's XML API,
  which fake-gcs-server accepts for signed URLs only and storage-testbench does not implement
  for multipart, so the test puts the objects through fake-gcs-server's JSON API. GCS uploads
  need a real bucket: a CI job with a GCS credential is an open question below.
- **Unit tests.** They prove the `aws` client's Jobs are byte-identical, and pin the `agent`
  client's command lines.
- **The kind CI.** It already runs an S3 server in the cluster. The backup, restore,
  journal-archive, PITR and replica-cluster steps run once with `s3.client: agent` on one
  track, then against Azurite and fake-gcs-server as their phases land.

## Phases

1. **`fb-agent objects` and the command builder, S3 opt-in.** *(1a, `fb-agent objects` and
   `hack/agent/objects.sh`: v0.187.0; 1b, the command builder and `s3.client: agent`: v0.188.0)* The subcommand on `object_store`,
   with all three stores built in and tested by `hack/agent/objects.sh`. Also
   `utils/object-store.ts`, with every caller moved onto it (the `aws` client byte-identical), and
   `s3.client: agent`. The kind CI runs its object store steps with the agent's client on one
   track.
2. **Backups, restores, bootstrap and retention on Azure Blob and GCS.** *(v0.189.0)* The
   `azureBlob` and `gcs` fields on backups, schedules, restores and `bootstrap.recovery`;
   credentials; the webhook rules and a CEL rule (one store). The kind CI runs Azurite and
   fake-gcs-server: a logical and a physical backup to Azurite, both restored, and retention
   deleting an expired backup there; from fake-gcs-server, a restore and a cluster bootstrapped
   from a backup put through its JSON API (no emulator takes object_store's GCS uploads). The
   ROADMAP entry of v0.4.0 is corrected. A point-in-time recovery reads the backup chain from
   any store and its journals from S3 until phase 3; a replica cluster still bootstraps from S3.
3. **The journal archive and point-in-time recovery on Azure Blob and GCS**, declarative
   databases' `journals/<file>/` included. *(v0.190.0)* `replication.journalArchiveAzureBlob` and
   `journalArchiveGCS`; `pointInTime.journalAzureBlob` and `journalGCS`; one archive per field
   group (the webhook and CEL); a recovery cannot read from the archive its own cluster writes,
   whatever the store. The kind CI repeats the PITR step against Azurite (the archive Job's sync
   and listing, a physical base backup, a recovery to a time); GCS has no emulator for uploads,
   so its archive is covered by the unit tests only.
4. **Replica clusters following an Azure Blob or GCS archive.** `archive-follower.sh` moves onto
   the builder, and the replica-source refusal of v0.184.0 compares stores of any kind. The kind
   CI repeats the replica cluster step against each store.
5. **The agent by default for S3.** `s3.client` defaults to `agent` once it has run for a
   release. `aws` stays selectable for a deprecation period, and is then removed with
   `clientImage`.

Each phase is a release.

## Open questions

- **Field names.** Recommended: siblings beside `s3` (above), which change nothing for existing
  manifests. The alternative is CloudNativePG's single `destinationPath` with credentials per
  provider. It is closer to Barman Cloud, but means a second way to configure S3, and the
  `s3://` / `azure://` / `gs://` parsing in every resource.
- **A private endpoint's CA.** A `caSecretRef` beside `endpoint`, for MinIO or Ceph behind an
  internal CA. The `aws` client has no such option today (it would be `AWS_CA_BUNDLE`), so this
  would be new for both clients.
- **GCS uploads in CI.** No emulator takes `object_store`'s XML API uploads (above). A CI job
  against a real bucket would need a GCS service account key as a repository secret, with a
  lifecycle rule deleting what the runs leave.
- **How long `aws` stays** after phase 5. Recommended: one minor release with a deprecation
  warning event on clusters that still select it, then removal.
