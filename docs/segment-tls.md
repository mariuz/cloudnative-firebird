# Segment TLS: encrypted segment shipping between instances

`spec.segmentTLS.enabled` encrypts and mutually authenticates every connection to a cluster's
**segment servers**: journal segments shipped from the primary to the replicas, the seed copies
that build a new replica, backup files fetched by backup and restore Jobs, and the operator's own
lag, health and isolation checks. This page explains why it exists, how it works, how to turn it on
and off safely, and what it does and does not protect.

- [Why it is needed](#why-it-is-needed)
- [Doesn't Firebird's SRP already encrypt this?](#doesnt-firebirds-srp-already-encrypt-this)
- [Why not TLS in the images (the proxy sidecar, until v0.171.0)](#why-not-tls-in-the-images-the-proxy-sidecar-until-v01710)
- [Architecture](#architecture)
- [Certificates](#certificates)
- [Enabling and disabling](#enabling-and-disabling)
- [Verifying it](#verifying-it)
- [Security properties and limits](#security-properties-and-limits)
- [Troubleshooting](#troubleshooting)
- [Reference](#reference)

## Why it is needed

Firebird clients (applications, `isql`, `gbak` over the network) are already encrypted by
Firebird's own wire protocol (WireCrypt, see *Encryption in Transit* in the README). Replication
in this operator does not use that protocol. Each instance runs a small **segment server** (the Rust
agent's, port 3051) next to Firebird, and the instances and Jobs exchange files with it:

| Traffic | From → to | Carries |
|---|---|---|
| Journal segments | replica's segment puller → primary | every committed change: row data, including whatever personal or secret data the tables hold |
| Seed copies | new replica's `replication-init` → primary or a ready replica | a complete copy of the database file |
| Backup files | backup / restore Jobs → instance (`backup-files` without replication) | full logical and physical backups |
| Control requests | operator, failover and switchover Jobs → every instance | lag, retention floor, isolation contact, `SYNC`, `REJOIN`, headers |

Until v0.75.0 all of this was plain TCP inside the cluster. The protections already in place
covered *who may ask*, not *who may read*:

- **Signed requests (v0.64.0).** Requests carry an HMAC-SHA256 over the request, its time and a
  nonce instead of the SYSDBA password. The password never crosses the network and a captured
  request cannot be replayed or altered. The **replies are not signed or encrypted**, so a reader
  on the path sees the whole database stream.
- **NetworkPolicy (`networkPolicy.enabled`).** It restricts which pods may *connect* to port 3051.
  It does nothing against anyone who can *observe* the traffic.

Pod-to-pod traffic can be observed more often than people assume:

- Most CNI plugins send pod traffic between nodes unencrypted, across the data-centre or cloud
  network, a shared VLAN or a VPN between sites (stretched clusters, multi-zone node pools).
- Anyone with `hostNetwork` access or root on a node, a compromised node, or a packet-capture
  DaemonSet sees the traffic of every pod on that node.
- Compliance regimes (PCI DSS, HIPAA, ISO 27001, many internal policies) require encryption in
  transit for this kind of data, including inside the cluster. "The database is encrypted for
  clients but the replica stream is plain text" is a finding in an audit.
- CloudNativePG, the operator this project follows, uses TLS between instances for streaming
  replication by default. Replication in Firebird clusters should give the same guarantee.

Segment TLS closes that gap. Everything that leaves an instance pod on the segment port is
encrypted with TLS 1.3. Both ends must prove that they hold a certificate from the cluster's own
CA.

## Doesn't Firebird's SRP already encrypt this?

No. SRP and WireCrypt protect only connections that use **Firebird's own network protocol**, and
journal segment shipping does not use it.

- **SRP** (Secure Remote Password, `AuthServer = Srp256`) lets a client prove that it knows the
  password without sending it, and gives both sides a shared session key.
- **WireCrypt** encrypts the connection with that key (ChaCha64, ChaCha or Arc4). Firebird 4 and
  later require it on the server by default (`WireCrypt = Required`).

Together they protect:

- applications, `isql` and network `gbak` connecting to port 3050;
- Firebird's own **synchronous** replication: with `sync_replica` the primary attaches to the
  standby's database over the Firebird protocol, so that connection already gets SRP and WireCrypt.

They do **not** protect the segment server. Firebird's **asynchronous** replication only writes
journal segments to files in the primary's journal archive; Firebird itself does not move them to
another machine. This operator ships them with its own segment server on port 3051, a separate plain
TCP service that Firebird knows nothing about. It carries the journal segments (every committed row
change), complete database copies when a replica is seeded, and backup files. None of that passes
through a Firebird connection, so SRP and WireCrypt never apply to it. Before segment TLS this
traffic was readable by anyone who could observe pod traffic, even with `WireCrypt = Required` and
Srp256 on every instance.

| Connection | Protocol | Authentication | Encryption |
|---|---|---|---|
| Client → Firebird (3050) | Firebird | SRP | WireCrypt (`tls.enabled` makes it strict) |
| Primary → synchronous standby (`sync_replica`) | Firebird | SRP | WireCrypt |
| Journal segments, seeds, backup files (3051) | segment server | signed requests (HMAC, since v0.64.0) | **none, unless `segmentTLS.enabled`** |
| Operator, Jobs → segment server (3051) | segment server | signed requests | **none, unless `segmentTLS.enabled`** |

Request signing borrows SRP's idea for the segment server: a request carries an HMAC instead of
the SYSDBA password, so the password never crosses the network. But like SRP without WireCrypt, it
only authenticates the request. The data coming back stays readable, and segment TLS is what
encrypts it.

## Why not TLS in the images (the proxy sidecar, until v0.171.0)

> **History.** Segment TLS started (v0.76.0) as a proxy sidecar beside the Perl scripts, for the
> reasons below. The Rust agent (docs/rust-agent-roadmap.md) replaced both: since v0.171.0 every
> process of the segment protocol is the agent, which does TLS itself, and there is no proxy. The
> [Architecture](#architecture) section describes what runs now.

The obvious approach, TLS inside the Perl segment server and its clients, is not possible with the
official `firebirdsql/firebird` images. Alternatives were measured or ruled out:

| Option | Result |
|---|---|
| TLS in Perl (`IO::Socket::SSL`) | not installed; the images ship `perl-base` only, and the operator does not modify them |
| `openssl s_client` / `stunnel` / `socat` | none are in the images (Debian trixie, only `libssl.so.3` is present) |
| A cipher in pure Perl (ChaCha20) | 0.47 MB/s measured: a 1 GB seed would take more than half an hour |
| Carrying the bytes over a Firebird connection (WireCrypt) | considered and rejected, see the note below |
| A service mesh (Istio, Linkerd mTLS) or an encrypting CNI (WireGuard) | works, and can be combined with this feature, but is outside the operator's control and not present in most clusters |
| **A proxy from the operator image** | Node.js with OpenSSL is already there, the image is already pulled by the cluster, and it runs as an unprivileged, read-only container |

> **Note: why not ship the segments over a Firebird connection instead?**
> It was considered, since it would reuse the SRP authentication and WireCrypt encryption that
> Firebird already has. It was rejected because it would need a Firebird client in every Job and a
> database attachment just to move files. Firebird has no API for transferring arbitrary files, so
> the bytes would have to be packed into BLOBs and unpacked on the other side through SQL. Seeding
> and physical backups copy raw database and journal files, which an attachment cannot read as
> files. And replication would depend on the database accepting connections: it would stop during
> a full shutdown or fencing, exactly when the segment server is needed. The TLS proxy is simpler,
> moves the bytes unchanged at full speed, and does not depend on Firebird's protocol.

The proxy (`operator/src/segment-tls.ts`, later `fb-agent segment-tls`; removed in v0.171.0) was
about 150 lines. It did not interpret the segment protocol. It relayed bytes in both directions, so the Perl
segment server and its clients kept their protocol, request signing and checks unchanged.

## Architecture

### Pods

With segment TLS on, the cluster's certificates (`/etc/segment-tls`) and the switch state
(`/etc/segment-tls-peers`) are mounted into the containers of the agent's processes, which do
mutual TLS themselves (`operator/agent/fb-agent/src/tls.rs`):

- **The segment server** (`segment-server`, or `backup-files` without replication) accepts mutual
  TLS on the segment port (`SEGMENT_TLS_LISTEN=0.0.0.0:3051`). Its plain listener is on
  `127.0.0.1:3061` only (`SEGMENT_LISTEN`), for the pod's own clients: the Lease holder, and
  `kubectl exec` checks.
- **Its clients** (`SEGMENT_TLS=true`): the segment puller, the replication init container's
  helpers (seed copies), the segment server's own requests to the other instances (the isolation
  check), and the programs of the backup, restore, switchover, failover and synchronous standby
  Jobs connect to an instance's segment port over mutual TLS.

```mermaid
flowchart LR
  subgraph P["Primary pod repl-0"]
    direction TB
    PFB[(Firebird<br/>journal archive)]
    PSS["segment-server (fb-agent)<br/>:3051 mTLS<br/>127.0.0.1:3061 plain"]
    PLH["lease-holder"]
    PFB --- PSS
    PLH -- "plain, loopback" --> PSS
  end

  subgraph R["Replica pod repl-1"]
    direction TB
    RPULL["segment-puller (fb-agent)"]
    RSS["segment-server (fb-agent)<br/>:3051 mTLS"]
  end

  subgraph J["Backup Job pod"]
    direction TB
    JC["fb-agent backup-file"]
  end

  OP["Operator<br/>(Node TLS client)"]

  RPULL == "TLS 1.3, mutual,<br/>cluster CA" ==> PSS
  JC == "TLS 1.3" ==> PSS
  OP == "TLS 1.3" ==> PSS
  OP == "TLS 1.3" ==> RSS
```

Only the thick edges cross the pod network, and all of them are TLS. The thin edge stays on the
pod's loopback interface.

### One request, end to end

A replica pulling the next journal segment from the primary:

```mermaid
sequenceDiagram
  autonumber
  participant Pull as segment-puller (replica)
  participant SS as segment-server (primary)

  Pull->>SS: TCP repl-0.repl-headless:3051
  Pull->>SS: TLS 1.3 handshake, both sides present a cert from the cluster CA
  Note over Pull,SS: each side rejects a peer whose chain does not end at the cluster CA
  Pull->>SS: "SIG1 <time> <nonce> <hmac> GET <segment>" (encrypted)
  SS->>SS: verify signature and nonce (unchanged)
  SS-->>Pull: "OK <size>" + segment bytes (encrypted)
```

The signed request is the same as without TLS. TLS adds confidentiality, integrity of the
replies, and authentication of the *peer*. The signature still authenticates the *request*.

### The operator

The operator opens TLS itself (`operator/src/utils/replication-lag.ts`) with the cluster's
certificate, which it reads from the Secret. It decides **per pod**: an instance is reached over
TLS when its pod mounts the certificates (the `segment-tls` volume; pods of before v0.171.0 also
ran the proxy sidecar), and in plain text otherwise. This keeps lag measurement, cut-off detection
and the isolation check correct while a rolling update moves the instances from one mode to the
other (see below).

## Certificates

```mermaid
flowchart TB
  CR["FirebirdCluster repl<br/>spec.segmentTLS.enabled: true"] -->|reconcile| OPR[Operator]
  OPR -->|"create / renew<br/>(ownerReference: the cluster)"| SEC["Secret repl-segment-tls<br/>type kubernetes.io/tls<br/>ca.crt · ca.key · tls.crt · tls.key"]
  SEC -->|"volume: ca.crt, tls.crt, tls.key<br/>(never ca.key)"| PODS["instance pods and Job pods<br/>/etc/segment-tls"]
  SEC -->|"read, cached 15 s"| OPC[Operator TLS client]
  PODS -->|"tls.crt mtime checked on each connection<br/>→ reloaded, no restart"| PX[the agent's processes]
```

- **One CA per cluster**, ECDSA P-256, valid for 10 years. A certificate from another cluster's CA
  is refused, so clusters in the same namespace cannot read each other's segments.
- **One certificate** signed by it, ECDSA P-256, valid for one year, with both `serverAuth` and
  `clientAuth`. All instances and Jobs of the cluster present it. Its names are
  `*.<cluster>-headless`, `*.<cluster>-headless.<namespace>.svc` and `…svc.cluster.local`. These
  names are informational only, see *host names* below.
- **Renewal**: on every reconcile the operator checks the Secret. It replaces the certificate when
  it has 30 days or less left or its names no longer match, and the CA when it has a year or less
  left. A renewed certificate is signed by the same CA, so pods with the old and new certificate
  accept each other while the kubelet propagates the update. The agent's processes notice the
  new files on their next connection and use them without a restart.
- **The CA's private key** stays in the Secret, which only the operator reads. Pods mount the three
  other keys through `items`. A compromised instance pod can therefore not mint certificates.
- **The Secret is owned by the cluster** and is deleted with it. Turning segment TLS off keeps the
  Secret, so turning it back on reuses the same CA.
- **Host names are not checked.** The clients address instances by short names
  (`repl-0.repl-headless`), and Node rejects wildcard certificates for two-label names. A peer is
  authenticated by its chain to the cluster's CA instead, which only this cluster's pods and the
  operator hold. In this setting that is a stronger check than a name.

To bring your own CA, there is no setting yet. Pre-creating the Secret (type `kubernetes.io/tls`) with
your own `ca.crt` and `ca.key` works: the operator keeps a CA that is valid for more than a year and
only issues the certificate from it.

## Enabling and disabling

### The default

Since v0.77.0 new clusters get segment TLS **on by default** when the API server is Kubernetes 1.29
or later. The operator writes the default into the cluster's spec on its first reconcile, before
it creates any pod, and records a `SegmentTLSDefaulted` event:

| Cluster | `spec.segmentTLS.enabled` written |
|---|---|
| new, Kubernetes 1.29 or later | `true` |
| new, older Kubernetes, or a version the operator could not read | `false` |
| existing (it already has a StatefulSet: created by an earlier operator version) | `false`: an upgrade never changes how an existing cluster ships its segments |
| a value set by you, `true` or `false` | left as it is |

Because the decision is stored in the spec, it never changes later, whether the operator's setting
changes or Kubernetes is upgraded. To move an existing cluster over, set `enabled: true` (the switch
is described below and does not stall replication).

The operator's `SEGMENT_TLS_DEFAULT` environment variable (`config/deploy/deployment.yaml`) changes
the default for new clusters: `auto` (the default: on from Kubernetes 1.29), `true` or `false`.
Admission warns when a cluster asks for segment TLS on a Kubernetes version without native sidecars,
where its pods would not start.

GitOps tools see the written field as set by a controller. Put `segmentTLS.enabled` in your
manifests to keep the decision in Git.

### Moving existing clusters

Since v0.79.0 the operator can move the clusters it pinned to plain segment shipping over to
segment TLS itself. When it pins an existing cluster, it marks it with the annotation
`firebird.cloudnative-firebird.io/segment-tls-migration: pinned`, which tells its own choice from an
owner's `enabled: false`. The operator's `SEGMENT_TLS_MIGRATE` setting decides what it moves:

| `SEGMENT_TLS_MIGRATE` | Clusters moved |
|---|---|
| unset (default) | none |
| `pinned` | those the operator pinned (annotation `pinned`) |
| `all` | every cluster with `enabled: false`, including those pinned by v0.77.0 and v0.78.0 (before the annotation) and those whose owners chose `false` |

A cluster is moved only when it is idle: phase `Running`, every instance ready, no rolling update,
switchover, failover or fencing in progress, not hibernated or suspended, and on Kubernetes 1.29 or
later. Only one cluster migrates at a time across the whole operator. The operator sets
`enabled: true` and the annotation `in-progress` in one patch, then the usual switch follows (the
instances restart one by one, the primary last, without replica lag). Once every instance serves
TLS, the annotation becomes `done`. Events record each step: `SegmentTLSMigrationStarted`,
`SegmentTLSMigrated`, and `SegmentTLSMigrationSkipped`.

To keep a cluster out of it, annotate it with `skip`:

```sh
kubectl annotate firebirdcluster <cluster> firebird.cloudnative-firebird.io/segment-tls-migration=skip --overwrite
```

An owner who turns segment TLS off again during the migration gets `skip` too. The operator does
not switch it back.

### Requiring segment TLS

Since v0.84.0 the operator can refuse plain segment shipping altogether. With
`SEGMENT_TLS_REQUIRED=true` on the operator (`config/deploy/deployment.yaml`):

- admission denies a `FirebirdCluster` created with `segmentTLS.enabled: false`, and an update that
  turns segment TLS off; a cluster that is already plain may still be changed otherwise (scaled,
  reconfigured), so nothing an owner runs breaks;
- new clusters default to `enabled: true` whatever the Kubernetes version (the operator warns at
  startup when the API server has no native sidecars, since their pods would not start);
- a cluster still on plain segment shipping (pinned on upgrade, or chosen before the setting) gets
  the condition `SegmentTLS: False` (reason `PlainSegmentShipping`), a `SegmentTLSRequired`
  warning event, and `firebird_cluster_segment_tls 0` on the metrics endpoint. The operator never
  restarts a cluster for this: move it with `SEGMENT_TLS_MIGRATE` or `enabled: true`.

```sh
kubectl get firebirdclusters -A -o custom-columns=NAME:.metadata.name,TLS:.spec.segmentTLS.enabled
# or alert on firebird_cluster_segment_tls == 0
```

The switch itself (the ConfigMap above, with its plain window while the instances restart) is
unchanged: it is how a plain cluster becomes a TLS one.

### Requirements


- Nothing about the Kubernetes version since v0.171.0: the proxy sidecar, which needed native
  sidecar containers (1.29), is gone. The default for new clusters and the migration of existing
  ones still follow the 1.29 rule (below).
- The operator's ClusterRole needs `create` and `update` on Secrets. They are in
  `config/deploy/rbac.yaml` since v0.75.0. Apply it again when upgrading.
- Instance and Job pods must be able to pull the **operator image**, which carries the agent's
  binary (the `agent-install` init container copies it).

```yaml
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdCluster
metadata:
  name: repl
spec:
  instances: 3
  replication:
    enabled: true
  segmentTLS:
    enabled: true
```

For a **new cluster** nothing else is needed: the Secret exists before the first pod starts, and
with the default above the setting is not even needed.

For an **existing cluster**, the change is a pod template change, and the normal rolling update
applies: replicas one at a time, the primary last. Between the first replica restart and the primary
restart, the cluster runs in **mixed mode**: some instances serve TLS, the others do not yet.
Since v0.76.0 replication goes on through it (before, the restarted replicas could not pull from the
plain primary and lagged until it restarted).

The operator publishes the instances' modes in a ConfigMap, `<cluster>-segment-tls-peers`, mounted
into every container with the certificates:

| Key | Meaning |
|---|---|
| `plain-peers` | instance pods that serve in plain text: a client connects to them without TLS |
| `accept-plain-until` | epoch milliseconds: until then, a segment server also accepts plain connections, from instances that do not serve TLS yet |

The segment server tells the two apart by the first byte of a connection: a TLS handshake record
starts with `0x16`, and a segment request starts with a letter. Plain connections still need a
signed request, as before segment TLS.

```mermaid
sequenceDiagram
  participant Op as Operator
  participant CM as ConfigMap repl-segment-tls-peers
  participant R1 as replica repl-1
  participant R2 as replica repl-2
  participant P as primary repl-0

  Note over R1,P: all plain
  Op->>CM: plain-peers: repl-0 repl-1 repl-2, accept plain for 5 min
  Op->>R1: restart with TLS
  R1->>P: repl-0 is listed plain: pull in plain text (no lag)
  Op->>CM: plain-peers: repl-0 repl-2
  Op->>R2: restart with TLS
  R2->>P: pull in plain text
  Op->>CM: plain-peers: repl-0
  Op->>P: restart the primary last
  Op->>CM: plain-peers: (none), accept plain for 5 more min
  R1->>P: pull over TLS
  R2->>P: pull over TLS
  Note over R1,P: after the grace period: TLS only
```

How the operator fills it (on every reconcile, before the StatefulSet, so a restarted instance reads
the current state when it starts):

- **Switching on:** `plain-peers` lists the instance pods that do not serve TLS yet. While any
  is listed, `accept-plain-until` is moved to 5 minutes ahead.
- **Switching off:** `plain-peers` lists every instance. The instances still serving TLS then talk to
  all instances in plain text, as the restarted ones do, and accept plain connections. The
  ConfigMap is deleted once no instance serves TLS any more.
- **After the switch** (on, every instance serving TLS): the list is empty and
  `accept-plain-until` is no longer moved. Once it has passed, the segment servers accept TLS only, and the
  steady state is the same as without the ConfigMap.

The 5-minute grace period covers the kubelet's delay in updating a mounted ConfigMap (up to about
two minutes). A client may still see the primary as plain for that long after it restarted with TLS,
and its plain connections are accepted meanwhile.

What to expect during the switch:

- **Replication goes on.** Each connection uses the mode of the instance it reaches, plain or TLS.
  Expect at most a few seconds of extra lag around each restart, as with any rolling update.
- **The operator keeps measuring** every instance in the mode its pod runs, so lag, cut-off
  detection and the isolation check stay accurate.
- **Jobs** (backups, restores, failover helpers) work during the switch as well: their programs read the
  same ConfigMap, and plain connections are accepted until the grace period ends.
- **The traffic is plain until the switch is over.** Connections to or from an instance that does
  not serve TLS yet are not encrypted, which is what the cluster had before. Once every instance
  serves TLS and the grace period has passed, plain connections are refused.
- **Synchronous replication** behaves as in any rolling update (`synchronous.detachForUpdates`).

Never edit the ConfigMap by hand: the operator rewrites it, and listing an instance that serves TLS
as plain makes the clients connect to it in plain text, which it refuses once the grace period is
over.

## Verifying it

```sh
# the Secret, created by the operator
kubectl get secret repl-segment-tls -o jsonpath='{.type}{"\n"}'          # kubernetes.io/tls

# every instance mounts the certificates
kubectl get pods -l firebird.cloudnative-firebird.io/cluster=repl \
  -o jsonpath='{range .items[*]}{.metadata.name} {.spec.volumes[?(@.name=="segment-tls")].name}{"\n"}{end}'

kubectl logs repl-0 -c segment-server   # listening on 127.0.0.1:3061; accepts mutual TLS itself on 0.0.0.0:3051

# a signed request over TLS is answered ...
kubectl exec repl-1 -c segment-server -- sh -c '$FB_AGENT request repl-0.repl-headless PING'
# ... the same request in plain text is not
kubectl exec repl-1 -c segment-server -- sh -c 'SEGMENT_TLS= $FB_AGENT request repl-0.repl-headless PING'

# replicas keep up (0 = caught up)
kubectl get firebirdcluster repl -o jsonpath='{.status.replicationStatus.replicas}'
```

The kind CI runs these checks, plus a backup Job over TLS, on every pull request.

## Security properties and limits

What segment TLS guarantees:

- **Confidentiality and integrity** of everything on the segment port, both directions: TLS 1.3
  only, with OpenSSL's TLS 1.3 cipher suites (AES-GCM, ChaCha20-Poly1305) and forward secrecy.
- **Mutual authentication**: a connection is accepted only when both ends present a certificate
  from the cluster's CA. Other clusters' pods, other workloads and plain clients are refused at the
  handshake, before the segment server sees a byte. The one exception is the switch itself: while segment TLS is
  being turned on or off, and for 5 minutes after, plain connections with a signed request are
  accepted (see *Enabling and disabling*).
- **The segment server is unreachable from the network**: it listens on the loopback only.
- **Request signing still applies** on top: even a holder of the cluster certificate needs the
  SYSDBA password to make the segment server do anything.

What it does not cover:

- **Loopback traffic inside a pod** (the Lease holder ↔ the segment server's plain listener) is
  plain. It
  never leaves the pod's network namespace.
- **Files at rest** (journal archive, backups on the volume) are not encrypted by this feature. Use
  encrypted storage classes and Firebird database encryption for that.
- **Client connections** to Firebird are protected by WireCrypt (`tls.enabled`), not by this
  feature.
- **The shared certificate**: all instances of a cluster present the same certificate, so an
  attacker with root in one instance pod can impersonate any instance of *that* cluster. That
  attacker can already read the database files, so the shared certificate adds no access.
- **No certificate revocation.** Rotating the CA means deleting `ca.crt` and `ca.key` from the
  Secret. The operator then issues a new CA and certificate, and pods pick them up within about a
  minute. During that time some connections fail until both ends have the new files.
- **Plain mode is still possible**: clusters created before v0.77.0, or on Kubernetes older than
  1.29, or with `segmentTLS.enabled: false`, ship segments in plain text. There is no setting that
  forbids it cluster-wide.

Resource cost: no container of its own since v0.171.0; TLS 1.3 with AES-GCM runs at several
hundred MB/s per core, so the segment stream, not the cipher, sets the pace.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Pods stay in `Init` with the `agent-install` container waiting | the operator image cannot be pulled: check `kubectl describe pod` events; set `OPERATOR_IMAGE` to a pullable image, or add the pull secret to the pods' ServiceAccount |
| Pods stay in `ContainerCreating` with `FailedMount: secret "<cluster>-segment-tls" not found` | the operator could not create it: check the operator log for a 403 and apply `config/deploy/rbac.yaml` (Secrets `create`, `update`) |
| Replica lag grows while switching | check `kubectl get configmap <cluster>-segment-tls-peers -o yaml`: the instances without the certificates must be listed in `plain-peers` and `accept-plain-until` must be in the future; the operator log shows why it could not write it |
| Plain requests are still answered after enabling | expected for 5 minutes after the last instance restarted with TLS (`accept-plain-until`) |
| Lag stays high after the rolling update | `kubectl logs <replica> -c segment-puller` for handshake errors; check that every pod mounts the same Secret |
| A backup Job fails during the switch | it reached an instance in the other mode: run it again once the rolling update is done |
| `segment TLS: no certificates in SEGMENT_TLS_DIR` from a client | `SEGMENT_TLS=true` in a container without the certificates mounted: a custom container; mount the `segment-tls` volume or unset `SEGMENT_TLS` for a local plain request |

## Reference

| Item | Value |
|---|---|
| Spec | `spec.segmentTLS.enabled` (boolean; written on a new cluster's first reconcile: `true` from Kubernetes 1.29, `false` for clusters that existed before v0.77.0) |
| Operator setting | `SEGMENT_TLS_DEFAULT`: `auto` (default), `true`, `false` |
| Switch state | ConfigMap `<cluster>-segment-tls-peers` (`plain-peers`, `accept-plain-until`), mounted at `/etc/segment-tls-peers`, written by the operator only while switching |
| Secret | `<cluster>-segment-tls`, type `kubernetes.io/tls`, keys `ca.crt`, `ca.key`, `tls.crt`, `tls.key` |
| Mount | `/etc/segment-tls` (`ca.crt`, `tls.crt`, `tls.key`), read-only |
| Containers | none of its own since v0.171.0 (the `segment-tls` proxy sidecar before) |
| Ports | 3051 TLS (pod network), 3061 segment server in plain text (loopback) |
| Environment | `SEGMENT_TLS_DIR`, `SEGMENT_TLS_PEERS_DIR`; the segment server `SEGMENT_TLS_LISTEN`, `SEGMENT_LISTEN`; the clients `SEGMENT_TLS=true` |
| Validity | CA 10 years (renewed with 1 year left), certificate 1 year (renewed with 30 days left) |
| Source | `operator/agent/fb-agent/src/tls.rs` (the agent's TLS), `operator/src/utils/segment-tls-pods.ts` (pod wiring), `operator/src/utils/segment-tls-client.ts` (Secret, operator client) |
