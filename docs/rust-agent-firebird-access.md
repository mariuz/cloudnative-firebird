# How the Rust agent reaches Firebird (spike, 2026-11-10)

Phase 1 of the [Rust agent roadmap](rust-agent-roadmap.md) has to pick how the agent talks to
the local Firebird server. Today the Perl scripts start `isql`, `fbsvcmgr` or `gfix` for every
query and every service call. This records a spike that tried the options against Firebird
4.0.7, 5.0.4 and the 6 snapshot (T6.0.0.2455), in the official images.

## Decision

**A pure-Rust wire protocol client, the `firebird-wire` crate, with `libfbclient` loaded at
runtime as the fallback.** It is the only pure-Rust client found that does SQL, wire encryption
and the services API, and all of it worked on the three versions. The decision is provisional
until Phase 3 has run it under load (below, "Not known yet").

## What the agent needs

- Small queries: `RDB$GET_CONTEXT('SYSTEM', 'REPLICATION_SEQUENCE')`, `MON$REPLICA_MODE`,
  `MON$GUID`, the backup lock (`ALTER DATABASE BEGIN/END BACKUP`).
- The services API: full shutdown and online (the promotion, the fences), header statistics
  (the database's state), the replica mode, nbackup.

## Results

Everything below was run, except where it says "source".

| Option | Connects (4 / 5 / 6) | Wire encryption | Server `WireCrypt = Required` | Queries | Services API |
|---|---|---|---|---|---|
| `firebird-wire` 0.1.16 (pure Rust) | yes, Srp256 and Srp | ChaCha | works | work | works: header statistics, full shutdown, online, replica mode |
| `rsfbclient` 0.29.0, `pure_rust` | yes, Srp256 (Srp only with encryption) | Arc4 only | works | work | not available |
| `firebirust` 0.6.4 (pure Rust) | yes, Srp256 (Srp only with encryption) | ChaCha64 | works | work | not available |
| `rsfbclient` 0.29.0 loading the image's `libfbclient` | yes, Srp256 | ChaCha64 | not run (the official client) | work | works, with hand-built service parameter blocks |
| `isql` / `fbsvcmgr` per call (today) | yes | (official client) | not run | work | works |

Cost per call, in milliseconds, inside each container against the local server:

| | Firebird 4 | Firebird 5 | Firebird 6 |
|---|---|---|---|
| `isql`, one process per query | 76.5 | 76.7 | 84.7 |
| `fbsvcmgr` header statistics, one process per call | 66.1 | 65.5 | 81.5 |
| `firebird-wire`: a query in its own transaction, persistent connection | 0.67 | 0.65 | 0.78 |
| `firebird-wire`: header statistics, persistent service attachment | 0.30 | 0.29 | 2.32 |
| `firebird-wire`: connect, query, close | 9.6 | 8.6 | 7.3 |
| `libfbclient` loaded at runtime: a query, persistent connection | 0.53 | 0.46 | 0.43 |

A persistent connection is about 100 times cheaper than a process per call, with either client.

Sizes: a small program with `firebird-wire` is a 920 kB static musl binary (33 crates in its
dependency tree); with `rsfbclient` pure, 2.0 MB (127 crates).

## Why not the others

- **`rsfbclient` pure and `firebirust`** have no services API, which the promotion and the
  fences need. `firebirust` also takes 44 ms per query (it never sets `TCP_NODELAY`; source) and
  does not build for musl.
- **`libfbclient`** works and is the official client, but the agent would have to be a glibc
  build matching each image (the images have glibc 2.41; a static musl binary cannot load it),
  and the services need hand-built parameter blocks either way. It stays the fallback: if
  `firebird-wire` falls behind a Firebird version, only the access layer changes.
- **A process per call** keeps the 65–85 ms per call that Phase 0 measured.

## What the agent has to handle itself

Found in the spike, each needs a test when the access layer is written:

1. **Firebird 6 refuses header statistics for a database in full shutdown** (`database ...
   shutdown`), from `fbsvcmgr` too. The Perl scripts already treat that reply as "shut down".
   After the error the service attachment is unusable: the agent re-attaches (about 5 ms).
2. **`firebird-wire` closes its service connection after any server error**: re-attach.
3. **The replica mode cannot be set in full shutdown**, only once the database is online. The
   in-place promotion already does it in that order.
4. **Two calls need raw parameter blocks**: the crate's `shutdown()` sends
   `prp_attachments_shutdown`, not `prp_force_shutdown`, and it has no replica mode constant.
   `ServiceManager::run` takes hand-built bytes; the spike used them for both.
5. **No read timeouts** in any of the pure-Rust crates (source). The agent needs its own
   deadline around each call, as the Perl bounds its commands with `timeout`.
6. `firebird-wire` has `nbackup()` (source, not run).

## Not known yet

- `firebird-wire` under load, with long-lived idle connections, across a server restart, and
  against a hung server.
- A shutdown with attachments open (the spike shut an idle database down).
- arm64 (only x86-64 was tried).
- The crate is young: first published 2026-06-23, about 500 downloads, releases roughly monthly
  (0.1.16 on 2026-10-21), MIT. Its issue tracker and CI were not looked at. The fallback above
  is the answer to that risk; pinning its version and vendoring it are options.
- How the agent gets its credentials: the spike passed SYSDBA and its password explicitly, as the
  sidecars' environment has them.

The spike's programs are not in the repository; they were four small crates, one per option,
each connecting, running the two queries and, where possible, the service calls.
