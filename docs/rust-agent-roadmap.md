# A Rust agent for the instance pods (roadmap proposal)

Status: **Phases 0, 1 and 2 done** (v0.154.0), **Phase 3 started** (v0.155.0: the segment server's
front, and pullers that wait instead of polling; v0.156.0: the database's state from a
persistent Firebird attachment; v0.157.0: the other requests about an instance's state;
v0.158.0: the backup files, also for instances without replication; v0.159.0: the seeds; v0.160.0: the fence, nbackup and synchronous replication). Results below. It answers
the question of whether a Rust core should replace the Perl scripts and the parts critical for
performance.

## Short answer

Yes, it can be done one component at a time, but only after measuring. Two points shape the plan:

- **Rust would not make the database faster.** Queries, commits and journaling run in the
  Firebird server. No Perl and no operator code sits between clients and the database. What
  Rust can improve is the machinery around it:
  - how fast segments reach the replicas;
  - how the segment server behaves under concurrent requests;
  - the cost of each check (a process per query today);
  - TLS, and the memory of the sidecars.
- **The biggest limit found is a design limit, not Perl's speed.** The segment server answers
  one request at a time (below), and that can be fixed in Perl too. The roadmap therefore starts
  with measurements and that fix, and adds Rust only where the numbers justify it.

The TypeScript operator stays as it is. Its work waits on the Kubernetes API, not on the CPU.

## What runs today

About 3,800 lines of Perl and sh in `operator/src/replication/`. They are shipped in the cluster
ConfigMap and run with the Firebird image's `perl-base`, or with the `aws-cli` image for the
archive follower.

| Component | Where | Lifetime | Calls out to |
|---|---|---|---|
| `segment-server.pl` (1,141 lines): journal archive, seeds, backup files, switchover and promotion, fencing | sidecar of every instance (Firebird image) | daemon | isql, fbsvcmgr, gfix, gstat, cp |
| `isolation-check.pl` | forked by the segment server on the primary | daemon, every 5 s | fbsvcmgr |
| `segment-puller.pl` | sidecar of every replica | daemon, every 5 s | none (sockets) |
| `metrics-exporter.pl` | sidecar with custom queries | serves each scrape | isql once per query |
| `archive-follower.sh` | sidecar of a replica cluster (aws-cli image) | daemon, every 30 s | aws |
| `init-instance.sh` | init container | one-shot | isql, gfix, gstat, nbackup |
| `switchover.pl`, `failover.pl`, `sync-standby.pl`, `fetch-segments.pl`, `backup-file.pl`, `pitr-plan.pl`, `pitr-restore.sh`, `major-upgrade.sh` | Jobs | one-shot | various |
| `set-repl-seq.pl`, `replica-control.pl` | offline helpers | one-shot | none (write Firebird's internal formats) |

Two Node sidecars from the operator image also run in the instance pods. The **lease holder**
renews the primary Lease, and **segment-tls** is the mutual TLS proxy in front of the segment
server. TLS moved there because a pure-Perl cipher reached under 1 MB/s (ROADMAP.md, segment TLS).

## Phase 0 results (2026-11-10)

Measured with `hack/bench/segment-server.sh` (Docker, one primary with its segment server and
isolation check, a client on the host), on Firebird 4.0.7, 5.0.4 and the 6 snapshot
(T6.0.0.2455):

| | Firebird 4 | Firebird 5 | Firebird 6 |
|---|---|---|---|
| PING: connection, pure-Perl signature, reply (median) | 2 ms | 2 ms | 2 ms |
| STATE: one `fbsvcmgr` per request (median) | 70 ms | 70 ms | 92 ms |
| GET of a 256 MB segment | 562 MB/s | 600 MB/s | 531 MB/s |

**The hypothesis was true.** A live seed of a 275 MB database, downloaded at 3 MB/s (91 s), on
the server before v0.154.0 (Firebird 5):

- none of the STATE and PING requests sent every second was answered within 10 s;
- after 62 s the isolation check put the primary's database into full shutdown ("neither the
  operator nor any replica has reached this primary for 62s").

With the fix of v0.154.0 (GET, SEED, TXNS and PLAN answered by child processes, the copies one
at a time under a lock), the same seed on all three versions: every probe answered (slowest
85 ms, 242 ms and 79 ms on Firebird 5, 6 and 4), and the primary not fenced.

**What the numbers say about Rust:**

- The pure-Perl signature and the 64 KiB copy loop are not bottlenecks. A signed request costs
  2 ms, and a segment is served at more than 500 MB/s, above what most pod networks carry.
  Hot spots 3 and 5 below do not justify a rewrite.
- The process per Firebird query is the measurable cost: 70–90 ms for each STATE, against
  0.2–0.7 ms over a persistent connection (the Firebird access spike of Phase 1). It matters for
  what asks often: the Lease holder every 5 s, the metrics exporter per query and scrape.
- Not measured yet: replication lag at a given commit rate, the sidecars' memory, and TXNS and
  PLAN on a large archive.

So the case for the later phases rests on persistent Firebird connections, fewer runtimes in
the pod (Node for TLS and the Lease holder) and typed, tested formats, not on raw speed.

## Phase 2 results (2026-11-10)

`fb-agent puller` is a port of `segment-puller.pl` with the same environment, files, flags and
log lines. `hack/bench/puller.sh` runs both against one real segment server, side by side, with
a 16 MB segment appearing in the primary's archive every 1 or 2 seconds (Firebird 5 image):

| | Perl puller | Rust puller |
|---|---|---|
| Archive to replica, poll every 5 s: median / longest | 2.48 s / 5.04 s | 2.59 s / 5.04 s |
| Archive to replica, poll every 1 s: median / longest | 0.65 s / 1.10 s | 0.69 s / 1.06 s |
| Peak memory | 8.4 MB | 0.5 MB |
| CPU for 30 segments (480 MB) | 0.17 s user + 0.46 s system | 0.05 s user + 0.51 s system |

**The port does not make replication faster, and was not expected to.** The delay is the poll
interval: a segment waits, on average, half of `POLL_SECONDS` before the next LIST sees it, and
the transfer itself is a small part. The CPU is the kernel's copy either way. What changes is
8 MB of memory per instance, and that the puller no longer depends on the image's Perl.

Lower lag needs a protocol change, not a faster client: the puller waiting on the server for
the next segment (a long poll) instead of asking every few seconds. With the segment server
answering requests concurrently since v0.154.0 that is possible, and it belongs to Phase 3,
where the server is ported. One request per connection stays as it is until then: the Perl
server closes after each reply.

## Where time goes

These come from reading the code. Phase 0 measured 1, 2, 3 and 5 (above).

1. **The segment server answers one request at a time.** It runs a single accept loop
   (`segment-server.pl` ~902–1141). Only FILE, STORE, PROMOTE and NBACKUP fork. GET (a segment),
   SEED (a whole database), TXNS and PLAN (which can wait up to 120 s) and STATE all run inline.
   So everything else waits behind a seed served by the primary: the replicas' pulls, the
   operator's checks, and the lease holder's STATE.
   - The lease holder stops renewing when STATE does not answer within 10 s
     (`lease-holder.ts`), and the Lease then expires.
   - The isolation check counts the requests the server *answers*. With `fenceWhenUnreached` it
     may fence a primary that nothing reached for `contactTimeoutSeconds` (60 s).
   - **Hypothesis to verify first:** a live seed of a large database (`allowLiveSeedFromPrimary`,
     the default) running longer than a minute could fence a healthy primary, or let its Lease
     expire.
2. **A process for each Firebird query.** `live_value` and `database_state` spawn `isql` or
   `fbsvcmgr` per request. The lease holder asks for STATE every 5 s, and the metrics exporter
   runs one `isql` per custom query per scrape.
3. **SHA-256 and HMAC in pure Perl** for every signed request (`segment-auth.pl`), because
   `perl-base` has no `Digest::SHA`. The puller opens a new connection, with a new signature, for
   LIST, SYNCTO and every GET.
4. **Segment headers read again and again.** ARCHIVED, POSITION, the pruning and the
   recovery-point sampler open every segment file; the sampler does it every second. TXNS and
   PLAN read every block header of the archive up to a sequence, which is the hot path of
   point-in-time recovery and live seeds.
5. **Copies in 64 KiB chunks** through Perl for GET, SEED and FILE, with no `sendfile` or
   `copy_file_range`.
6. **Polling sets the lag floor.** Replicas pull every 5 s, and replica clusters poll S3 every
   30 s, on top of the journal's own archive timeout.
7. **An extra hop and a Node runtime per pod for TLS**, and another Node runtime for the lease
   holder.

## What Rust would change, and what it would not

It would change:
- concurrency: each command class with its own limit, so a seed never blocks STATE or GET;
- TLS inside the agent (rustls), which removes the segment-tls sidecar and its hop;
- native SHA-256 and HMAC;
- persistent connections for the pullers;
- a header cache;
- zero-copy file transfers;
- one small static binary in place of a Node runtime per sidecar;
- Firebird's internal formats written from typed structures with tests.

It would not change:
- the database's own throughput or latency;
- what replication guarantees;
- the TypeScript operator;
- the orchestration scripts that drive Firebird's tools (`init-instance.sh`, `pitr-restore.sh`,
  `major-upgrade.sh`), where the time is spent in `nbackup`, `gbak` and the server.

## Design

- **One multi-call binary**, `fb-agent`, with subcommands: `segment-server`, `puller`,
  `follower`, `metrics`, `lease-holder`, `tls`, and the offline tools (`set-repl-seq`,
  `replica-control`, `header`, `pitr-plan`).
- **Delivery.** The binary is built static (musl) for amd64 and arm64 and shipped in the
  operator image, or in a small `cloudnative-firebird-agent` image. An init container copies it
  into an emptyDir, and the sidecars keep running in the Firebird image. That way they still
  have the Firebird tools (`fbsvcmgr`, `gfix`, `nbackup`) and the data volume. The operator
  already copies `/opt/firebird` into an emptyDir the same way.
- **Firebird access.** Decided by the spike of Phase 1
  ([rust-agent-firebird-access.md](rust-agent-firebird-access.md)): option (c) with the
  `firebird-wire` crate, option (b) as the fallback. The options were:
  - (a) keep calling `isql` and `fbsvcmgr`: simplest, but keeps hot spot 2;
  - (b) `libfbclient` through FFI: needs a glibc build that matches each image's libc and works
    with Firebird 4, 5 and 6;
  - (c) a pure-Rust implementation of the wire protocol (for example `rsfbclient`'s): to be
    evaluated for SRP authentication, wire encryption and the services API (shutdown, online,
    replica mode, statistics), which the promotion and fencing need.
- **The protocol stays the same.** The SIG1 request line, the commands and the replies remain
  wire-compatible. Perl, Node and Rust components can then mix during an upgrade and be
  replaced one at a time.
- **Opt-in first.** A cluster opts in with `firebird.cloudnative-firebird.io/agent: rust`. A later
  operator default for new clusters (`AGENT_DEFAULT`) and a migration of existing clusters one
  at a time when idle follow, as `SEGMENT_TLS_MIGRATE` and `PRIMARY_LEASE_MIGRATE` did. Perl stays
  the fallback for at least two releases after the default changes.
- **Restarts.** A change of the agent rolls the instances, as a change of the scripts does today
  (the replication configuration hash). The binary's version becomes part of that hash.

## Phases

**Phase 0: measure, and fix the sequential server (no Rust).** *Done in v0.154.0, except the
lag, memory and TXNS measurements.*
- Benchmarks:
  - segment GET throughput;
  - request latency under concurrency (seed plus pulls plus STATE);
  - replication lag at a given commit rate (`kubectl firebird bench` with a lag report);
  - CPU and memory of each sidecar;
  - the time TXNS and PLAN take on a large archive.
- A reproduction of hypothesis 1: a live seed of a multi-GB database with failover, the primary
  Lease and `fenceWhenUnreached` on. Watch the Lease age, the fence and the failover decisions.
- The fix in Perl: fork GET, SEED, TXNS and PLAN (as FILE already is), or answer STATE before
  queued work. It is worth doing whatever is decided about Rust.
- *Exit:* numbers in this document, and a go or no-go for each later phase.

**Phase 1: foundation.** *Done in v0.154.0:*
- *`operator/agent/`: the `sig1` crate and the `fb-agent` binary (`version`, `request`, and
  `serve`, which answers PING), 490 kB static, built into the operator image for amd64 and arm64
  without emulation.*
- *The conformance suite (`tests/agent-conformance.test.ts`): each client against each server,
  and the same refused lines with the same replies from the Perl and the Rust server.*
- *Delivery: the annotation `firebird.cloudnative-firebird.io/agent: rust` has an init container
  copy the binary into the instance pods. The kind CI runs it there against the pod's Perl
  segment server.*
- *Firebird access: `firebird-wire`, with `libfbclient` as the fallback.*

*The plan was:*
- A Cargo workspace in `operator/agent/` (inside the operator image's build context).
- CI: `fmt`, `clippy`, tests, static amd64 and arm64 builds.
- Packaging, and the init container that copies the binary.
- A SIG1 crate with a conformance suite that runs the same requests against the Perl, Node and
  Rust implementations.
- The Firebird access spike, which picks (a), (b) or (c).
- *Exit:* the binary reaches a pod in kind and answers PING; the conformance suite passes.

**Phase 2: the puller (lowest risk).** *Done in v0.154.0: clusters with the annotation run
`fb-agent puller`; `tests/puller-conformance.test.ts` runs eight scenarios against both pullers;
the kind CI replicates through it, with segment TLS. Persistent connections were left out (the
Perl server closes each connection), and `segment-request.pl` stays for the scripts that call
it.*
- `puller` and `segment-request` in Rust, with persistent connections and native HMAC. They are
  clients only and call no Firebird tools.
- Opt-in, plus a kind CI job with `agent: rust`.
- *Exit:* the same lag or better in the Phase 0 benchmark; the full kind matrix green with the
  agent on.

**Phase 3: the segment server (largest).** *In steps, designed in
[rust-agent-segment-server.md](rust-agent-segment-server.md). Step 1 is done in v0.155.0: a
front that answers PING, LIST, GET and the new WAIT and runs the Perl server behind it for the
rest. A segment now reaches a replica 0.08 s after it is archived instead of 2.4 s (median).
Step 2 (v0.156.0, v0.157.0): STATE from a service attachment that stays open, 2 ms instead of
71 ms, and the other requests about an instance's state. Step 3 (v0.158.0): the backup files. Step 4 (v0.159.0): the seeds, with the agent's first SQL. Step 5 began in v0.160.0: the fence, nbackup and
synchronous replication; the promotion is next. The plan was:*
- Every command in Rust, async, with a limit per command class.
- mTLS inside the agent, which replaces the segment-tls sidecar.
- A header cache, and `sendfile` or `copy_file_range` for GET, SEED and FILE.
- The promotion keeps its exact sequence, including the file lock of ISSUES.md issue 10.
- The existing script tests, the reproduction scripts (`hack/repro/*`) and the kind matrix all run
  against it.
- *Exit:* the Phase 0 concurrency benchmark shows STATE and GET unaffected by a seed; no change in
  the switchover, failover or PITR results.

**Phase 4: the lease holder and the metrics exporter.**
- The lease holder in the agent, with a Kubernetes client crate. Alternatively it stays in Node
  if Phase 0 shows its memory doesn't matter.
- The metrics exporter with persistent connections instead of `isql` per query.
- *Exit:* no Node runtime left in the instance pods; the same metrics.

**Phase 5: the offline tools.**
- `set-repl-seq`, `replica-control`, header parsing and `pitr-plan` become subcommands with typed
  formats.
- `hack/firebird-formats/verify.sh` checks them against the engines of Firebird 4, 5 and 6.
- *Exit:* the formats check passes on all three, and the PITR tests pass.

**Phase 6: default and removal.**
- The agent becomes the default for new clusters, then existing clusters are migrated.
- The Perl versions are removed after two releases.

Not planned: rewriting the orchestration shell scripts or the TypeScript operator.

## Costs and risks

- **A second toolchain.** It adds CI time and cross builds, and the image grows by a few MB.
- **Regressions.** The scripts carry many hard-won edge cases: the journal floor, the promotion's
  file lock, lineage and re-seed tokens. Rewriting them risks bringing old bugs back. The
  mitigations are the conformance suite, the opt-in and the unchanged protocol.
- **Firebird's internal formats stay internal.** The formats check stays mandatory for every
  Firebird version.
- **Firebird access.** Option (b) depends on each image's `libfbclient` and libc; option (c) on a
  third-party protocol implementation keeping up with Firebird 6.

## Next step

Phase 3, the rest of step 5 ([rust-agent-segment-server.md](rust-agent-segment-server.md)): the
in-place promotion (PROMOTE), then the isolation check and the recovery-point sampler as threads
of the agent.
