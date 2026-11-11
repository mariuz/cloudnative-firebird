# Moving the segment server to the Rust agent (Phase 3 design)

Phase 3 of the [Rust agent roadmap](rust-agent-roadmap.md). The segment server
(`segment-server.pl`, about 1,150 lines, more than 30 commands) is the centre of an instance: the
journal archive, seeds, backup files, the promotion, the fences. This note says how it moves,
in which order, and what the steps done so far contain (v0.155.0 to v0.160.0).

## The approach: a front, with the Perl server behind it

`fb-agent segment-server -- perl segment-server.pl` listens where the Perl server did. It reads
each request, checks its signature, and then either answers it itself or hands the connection,
with the request line as it came, to the Perl server it runs on a local port (3071).

```
puller / operator / Job ──► [TLS proxy] ──► fb-agent front ──► segment-server.pl (127.0.0.1:3071)
                                              answers: PING, LIST, GET, WAIT,     answers: the rest
                                                       STATE, VERSION, and
                                                       the instance's state,
                                                       the backup files,
                                                       the seeds, the fence,
                                                       nbackup, sync
```

Why a front instead of a second server or a rewrite in one go:

- **One command group at a time.** Each step moves a few commands behind the same port and the
  same protocol. Nothing else in the pod, the operator or the Jobs changes.
- **Every step can be compared.** The same requests go to the Perl server alone and to the
  front, and the replies must match (`tests/segment-server-front.test.ts`).
- **Going back is the annotation.** Without `firebird.cloudnative-firebird.io/agent: rust` a
  cluster runs the Perl server alone, as before.
- **The hard parts stay where they are proven** until their turn: the promotion with its file
  lock (ISSUES.md issue 10), the seeds, the isolation check and the recovery-point sampler, which
  the Perl server starts itself.

What the front does for every request:

- **Authentication.** It verifies the signature, the clock and the nonce as the Perl server does,
  and refuses with the same replies. A forwarded request is verified again by the Perl server,
  which keeps its own nonces: each sees a nonce once.
- **Contact.** The isolation check fences a primary that nothing has reached for
  `contactTimeoutSeconds`, from the time of the last authenticated request. The front records
  that time for the requests it answers itself; the Perl server does for the forwarded ones.
- **Forwarding.** The request line, anything the client already sent after it (an upload), then
  both directions until the Perl server closes. Uploads (STORE) and downloads (FILE, SEED) pass
  through unchanged.
- **Stopping.** In a container the signal reaches the front only. It passes SIGTERM on to the
  Perl server, which stops once the pod's Firebird server has stopped, and the front ends with
  it. If the Perl server dies, the front exits and the container restarts, as it did when the
  Perl server was the container's process.
- **Concurrency.** A thread per connection. An async runtime is not needed for a few dozen
  connections, and would be the largest dependency of the binary.

## Step 1 (v0.155.0): the journal's fast path, and waiting instead of polling

The front answers the four requests the pullers send all day:

| Request | Reply |
|---|---|
| `PING` | `OK` |
| `LIST` | the archived segments that are complete, one per line, then `.` |
| `GET <name>` | `OK <size>` and the bytes (the kernel copies the file to the socket) |
| `WAIT <seconds> [<name>]` | `OK new` as soon as a complete segment after `<name>` is archived, `OK timeout` after `<seconds>` (1 to 60) |

**WAIT is new, and it is what lowers replication lag.** A puller used to ask every
`POLL_SECONDS` (5): a segment waited 2.5 s on average before the next LIST saw it. The Rust
puller now asks the primary to tell it. Between two pulls it sends `WAIT <poll seconds> <last
segment>` instead of sleeping, and pulls as soon as the answer comes. Everything else it does on
each round is unchanged and still happens at least every poll interval: re-reading which
instance is the primary, the pause for a seed copy, the synchronous standby checks.

- A Perl segment server answers `ERR bad request` to WAIT. The puller then sleeps as before, and
  asks again about once a minute, since the primary may have been restarted with the front.
- The front looks at the archive every 100 ms while someone waits, once for all waiting pullers,
  and reads the directory again only when it changed.

Measured with `hack/bench/puller.sh` (Firebird 5 image, a 16 MB segment every 2 s, poll 5 s),
three replicas side by side on one archive:

| | Perl puller, Perl server | Rust puller, Perl server | Rust puller, front (WAIT) |
|---|---|---|---|
| Archive to replica: median | 2.40 s | 2.48 s | 0.08 s |
| Archive to replica: longest | 5.04 s | 5.01 s | 0.16 s |
| Puller peak memory | 8.4 MB | 0.6 MB | 0.6 MB |

On an archive of 5,000 segments: ten pullers waiting for ten seconds cost the front 0.07 s of
CPU, a LIST takes 42 ms (62 ms from the Perl server), and the front's peak memory is 1 MB.

This is the time from the primary's archive to the replica's source directory. The whole lag
also has the journal's own archive timeout before it (`archiveTimeoutSeconds`: how long a
commit may sit in the active segment) and the replica's apply interval after it, which this
step does not change.

### Complete segments only

Firebird archives a segment by running `journal_archive_command`, a plain `cp`. A listing made
during that copy saw the file, and a GET then sent what was there. Firebird's replica skips a
segment file shorter than the length in its header ("Skipping file ... as being too small"), and
the puller had recorded it as fetched: the replica would have waited for that segment for ever.
With a poll every five seconds the window was a few milliseconds per segment. With WAIT, which
answers the moment a file appears, it would have been hit regularly.

So a segment is listed, and ends a WAIT, only once its file holds the length its header
announces: the same rule the replica applies. An archived segment's file is exactly as long as
its header says (checked on Firebird 4, 5 and the 6 snapshot), and `cp` writes the header first.
A file without a segment header is listed as before. The front does this, and since v0.155.0
`segment-server.pl` does it for LIST too, so clusters without the agent are covered.

## Step 2, first part (v0.156.0): STATE from an attachment that stays open

STATE says whether the instance's database is online, shut down, or shut down by a fence. The
Lease holder asks every instance every few seconds, and the Perl server starts `fbsvcmgr` each
time. The front now keeps one service attachment to the pod's Firebird server and reads the
header statistics over it: the first use of the access layer chosen in
[rust-agent-firebird-access.md](rust-agent-firebird-access.md) (`firebird-wire`, pinned to one
version).

- **The same answers.** `OK online`; in full shutdown `OK shutdown`, or `OK fenced` /
  `OK fenced lease` when the fence's marker file is there. Firebird 6 refuses the statistics of
  a database in full shutdown ("database ... shutdown"); that is read as shut down, as the Perl
  server reads it.
- **When the front cannot tell, the Perl server is asked.** No attachment, an error it does not
  know, no answer within `LOCAL_QUERY_TIMEOUT_SECONDS` (10), or a call still in flight: the
  request is forwarded and answered with `fbsvcmgr` as before. So the front never answers
  worse than the Perl server, and a wrong guess is not possible: it answers only from statistics
  it received.
- **A deadline around every call.** The attachment lives in its own thread; a request waits for
  it at most the limit. The client library has no read timeout, so a hung server holds that
  thread, not the requests: they are forwarded while it is stuck.
- **Re-attaching.** The library closes the attachment after a server error, and a server restart
  breaks it. The next request attaches again (once per request), so a restart costs one
  forwarded or slower STATE.
- `NATIVE_STATE=false` in the container's environment turns it off (the front then forwards
  STATE). The operator does not set it.

`hack/agent/state.sh` runs the front with the Perl server behind it in the official images and
asks both, side by side, in every state: online, full shutdown, fenced by the Lease holder,
fenced by the isolation check, online again, online with a stale marker, the server stopped,
the server back. They agree on Firebird 4.0.7, 5.0.4 and the 6 snapshot. Time per STATE,
including the client process (about 2 ms):

| | Firebird 4 | Firebird 5 | Firebird 6 snapshot |
|---|---|---|---|
| Perl server (`fbsvcmgr` per request) | 72 ms | 71 ms | 90 ms |
| Front (persistent service attachment) | 2 ms | 2 ms | 21 ms |
| Front, after a server restart | 1 ms | 2 ms | 21 ms |

On the 6 snapshot the server opens the database for each statistics call when nothing else has
it open, which is the 21 ms; with another attachment open it is 5 ms. An instance in use is
the second case.

The binary grows from 0.5 MB to 1.0 MB (amd64; 0.9 MB arm64) and its dependency tree to 56
crates, all pure Rust.

## Step 2, second part (v0.157.0): the other requests about an instance

Twelve more requests are answered by the front. Eleven of them never asked Firebird: the Perl
server reads them from files on the instance's volume, and so does the front.

| Request | From |
|---|---|
| `HEADER` | the replication sequence in the database file's header page |
| `POSITION` | the primary's name, the promotion flag, the standby's flag, or the replica's control file and its fetched segments |
| `ARCHIVED` | the archived segments' headers and ages |
| `ISOLATION` | the fence's marker |
| `PRIMARYSEEN`, `FOLLOW`, `SYNCTO`, `LINEAGE`, `POINTS <segment>` | the small files the puller, the archive follower, the operator and the sampler keep |
| `UPLOADED <segment>`, `RETAIN <segment>\|none` | written: the numbers the Perl server's pruning reads |
| `VERSION` | the server's version string, over the service attachment |

- **VERSION without a database attachment.** The Perl server runs `isql` and asks
  `ENGINE_VERSION` (about 90 ms). The front reads the server's version string
  (`LI-V5.0.4.1812 Firebird 5.0`) over the attachment it already has and takes the same three
  numbers: 1 ms, the same answer on Firebird 4.0.7, 5.0.4 and the 6 snapshot
  (`hack/agent/state.sh`). One difference: the front can answer while the database is shut
  down, where the Perl server says `ERR cannot read the engine version`. Without an
  attachment the request goes to the Perl server.
- **No database attachment was needed after all.** The plan for this step expected SQL. Reading
  the Perl server showed only VERSION asked the database, and the services give that too. So
  the agent still holds nothing that a full shutdown or a promotion would have to wait for.
- **An argument the Perl server refuses is passed to it**, so the refusal is in its words.
- **Writes stay compatible.** UPLOADED and RETAIN write the same files, a temporary name and a
  rename, which the Perl server's pruning reads on its next round.

`tests/segment-server-front.test.ts` asks the Perl server alone and the front the same requests
in each state of an instance: an empty primary, a primary with its records, a replica without
and with a control file, a synchronous standby, promoted in place.

## Step 3 (v0.158.0): the backup files

Backup and restore Jobs copy nbackup and gbak files to and from an instance's data directory
through its segment server. The front answers those requests now:

| Request | Reply |
|---|---|
| `FILES` | the backup files (`*.nbk`, `*.fbk`, `*.nbk.ctl`), sorted, then `.` |
| `EXISTS <name>` | `OK yes` or `OK no` |
| `FILE <name>` | `OK <size>` and the bytes (the kernel copies the file to the socket) |
| `STORE <name> <size>` | reads `<size>` bytes into a hidden partial file, renames it, `OK`; `ERR short write ...` and nothing left behind when the client stops early |
| `REMOVE <name>` | `OK` (also when the file is not there) |

- **Instances without replication.** They have no segment server but a `backup-files`
  container: the Perl server in its files-only mode (`FILES_ONLY`). With the annotation that
  container runs the front too, in the same mode: PING and the backup files are its own, and
  everything else goes to the Perl server, which refuses it as it does alone.
- **Errors of the system stay the Perl server's.** A file that cannot be opened, created or
  removed is reported with the system's error text. The front does not imitate that text: it
  passes such a request on before it has sent or read anything.
- **Not a speed-up.** A 1 GB download and upload take the same time through either (0.9 to
  4.5 s on the test machine, the disk decides), and the files are identical. The step is here
  because the commands have to move before the Perl server can go, and because uploads and
  downloads no longer pass through two processes.

## Step 4 (v0.159.0): the seeds

A new or re-seeded replica starts from a copy of the database, which it asks an instance for
(SEED), and then asks the primary where the transactions open in that copy begin in the
archive (TXNS, PLAN). The front answers all three now.

| Seed | From | How |
|---|---|---|
| `offline` | the primary | its bootstrap seed file, while every segment after it is still archived |
| `live` | the primary, when `allowLiveSeedFromPrimary` | a copy of the database file under the backup lock |
| `replica` | a ready replica | its puller paused, everything received applied, then a copy under the backup lock, sent with the replica's control file |

- **The agent's first SQL.** The backup lock (`ALTER DATABASE BEGIN BACKUP`, `END BACKUP`),
  the replication sequence and the database's GUID go through `firebird-wire`, where the Perl
  server runs `isql`. Each statement gets its own short attachment and a deadline
  (`LOCAL_QUERY_TIMEOUT_SECONDS`, `LOCAL_COMMAND_TIMEOUT_SECONDS`), in a thread of its own since
  the library has no read timeout. No attachment is kept: nothing of the agent's is in the way
  of a full shutdown or a promotion.
- **The same sequence.** The order of the Perl server is kept step for step: the pause flag
  and its acknowledgement, the wait until everything received is applied, the lock, the copy,
  the unlock whatever happened, the reply, the cleanup.
- **One copy at a time, across both servers.** The lock file (`.exclusive.lock`) is the one the
  Perl server takes for its nbackup and its promotion, which it still answers: a seed in the
  front and a promotion behind it wait for each other as before.
- **When the local server does not answer the agent** before anything was done (the sequence,
  the GUID), the request is passed to the Perl server. After the backup lock is taken the front
  finishes or reports the failure itself, as the Perl server would.
- `NATIVE_SEED=false` in the container's environment leaves the seeds to the Perl server. The
  operator does not set it.

`hack/agent/seed.sh` runs the front with the Perl server behind it in the official images. It
asks each for a live, an offline and a replica seed with the replication init's own client
(`fetch-seed.pl`), opens every copy as a database and counts its rows, and checks that the
database is unlocked and writable afterwards, that the puller is not left paused and that no
copy is left behind. All pass on Firebird 4.0.7, 5.0.4 and the 6 snapshot, and the log shows
the front made its copies itself.

## Step 5, first part (v0.160.0): the fence, nbackup, synchronous replication

Five of the requests that change what an instance does. The promotion is kept apart: it is the
next part.

| Request | What it does |
|---|---|
| `FENCE [<reason>]` | the Lease holder's fence: the marker, then the full shutdown |
| `REJOIN` | lifts it: the database online, the marker removed |
| `NBACKUP <level> <file>` | an nbackup into the data directory; on a replica the puller is paused, everything received applied, and the control file written next to the backup |
| `SYNC none\|<hosts>` | the primary's synchronous replicas (`sync.conf`) |
| `STANDBY on`, `STANDBY off <segment>` | a replica becomes the synchronous standby, and an asynchronous replica again after that segment |

- **The services, without fbsvcmgr.** The full shutdown (forced at once), the online and the
  nbackup go to the local server's services over the agent's own attachments, one per call.
  The crate's own shutdown call asks for another mode, so the two property calls are built by
  hand, as the spike found.
- **A fence never depends on the agent's client alone.** When the server refuses a call at once
  (or cannot be reached), the front undoes what it did (the marker, the pause, the lock) and
  passes the request to the Perl server, which runs `fbsvcmgr`. When a call gets no answer
  within `LOCAL_COMMAND_TIMEOUT_SECONDS`, the server answers nobody: the front reports the
  failure as the Perl server would, without a second wait.
- **The same order.** The marker before the shutdown, so that a restarted sidecar never takes a
  fenced database for an online one; the pause flag before anything else on a replica; the
  lock shared with the seeds and the promotion.
- `NATIVE_ROLE=false` in the container's environment leaves these to the Perl server. The
  operator does not set it.

`hack/agent/role.sh` runs the front with the Perl server behind it in the official images, with
`fbsvcmgr` wrapped so that every call is recorded. The front fences and rejoins, takes level 0
and level 1 nbackups of a primary and one of a replica with its control file, without a single
call; the same requests to the Perl server need one each. The backups restore with their rows.
All pass on Firebird 4.0.7, 5.0.4 and the 6 snapshot.

## The next steps

Each is its own release, opt-in through the same annotation, with the comparison test extended
to its commands.

5. **The promotion.** PROMOTE keeps its exact sequence; `hack/repro/promoted-journal.sh` and
   `refused-attachment.sh` run against it. Then the isolation check and the recovery-point
   sampler, which become threads of the agent.
6. **TLS in the agent** (rustls), which retires the `segment-tls` sidecar and its hop.
7. **Without the Perl server.** The front no longer starts it; the scripts leave the ConfigMap.

Also open, and independent of the order above:

- The Perl puller could learn WAIT too. It is a few lines, but it restarts every cluster for a
  gain the agent already gives; it waits for a release that changes the scripts anyway.
- An atomic archive command (copy to a temporary name, then rename) would remove the incomplete
  window at its source. It changes `replication.conf`, which restarts the instances; the check
  on the header covers it meanwhile.
- The remaining lag is the archive timeout. Shipping the active segment's blocks as they are
  written would remove that too, but it is no longer journal *archiving*: it needs its own
  design, with Firebird's rules for partial segments.

## Risks

- **Two places verify a request.** A difference between the Rust and the Perl verification would
  show as a request accepted by the front and refused behind it. The conformance suite gives
  both the same refused lines.
- **The front is one more process in the path** of the forwarded requests. They cost one local
  connection more; the long ones (seeds, backup files) are copied through it. That goes away as
  the commands move.
- **A stuck front would hide a healthy Perl server.** The front has no state beyond the nonces
  and a cache of the newest segment, and each connection has its own thread.
