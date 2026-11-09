# An attachment refused for a full shutdown writes the header page

*Draft issue for [FirebirdSQL/firebird](https://github.com/FirebirdSQL/firebird/issues), ready to
file. Found while building journal replication for a Kubernetes operator
([ISSUES.md](../../ISSUES.md) issue 10).*

---

**Title:** An attachment refused because the database is in full shutdown still writes the header
page, overwriting offline changes with the copy it read

## Summary

`internalAttachDatabase` takes a new attachment id (`PAG_attachment_id`) before it checks the
shutdown state. `PAG_attachment_id` fetches the header page for write, increments
`hdr_attachment_id` and marks the page dirty. Only later does the attachment find the database
in full shutdown and fail with `isc_shutdown`. Releasing the `Database` then flushes the header
page. So every connection attempt to a database in full shutdown writes its header page, although
nothing is attached.

A full shutdown is the state in which a database file may be changed or copied offline. A header
change made with a tool that does not go through the engine, between the moment such an
attachment read the header page and the moment it flushes it, is overwritten with the old page.
Nothing reports it. We write the `HDR_repl_seq` clump offline while promoting a replica in place
(full shutdown, clump written, online, `-replica none`, publication), because a replica's header
has no replication sequence. A replication client retrying its attachment during the full
shutdown made the clump disappear. The promoted primary's journal then restarted at segment 1,
and every replica skipped its segments as already applied.

## Versions

4.0.7.3271, 5.0.4.1812 and 6.0.0.2455 (`master` at `67dbeb4`), the official Docker images
(`firebirdsql/firebird:4`, `:5`, `:6-snapshot`), Linux x64, SuperServer.

## Reproduce

Only Docker is needed. With the script from
[`hack/repro/`](https://github.com/mariuz/cloudnative-firebird/tree/main/hack/repro):

```sh
IMAGE=firebirdsql/firebird:5 ./refused-attachment.sh
```

1. **The counter.** The database is put in full shutdown, then five clients try to attach and
   are refused (`database ... shutdown`). `gstat -h` reads "Next attachment ID" from the file
   before and after.
2. **A header change lost.** The database is put in full shutdown, and four clients retry an
   attachment in a loop. A header clump is written offline (`HDR_repl_seq`, following
   `PAG_set_repl_sequence`), then the database is brought online. The clump is then read back
   from the file. The same is done again holding the file's `flock` during the write, taken once
   the server closed the file.

Result on 2026-11-09:

| Image | Next attachment ID, 5 refused attachments | Clump lost, write without the lock | Clump lost, write with the lock |
|---|---|---|---|
| 4.0.7.3271 | 85 → 90 | 9 of 10 | 0 of 10 |
| 5.0.4.1812 | 88 → 93 | 5 of 10 | 0 of 10 |
| 6.0.0.2455 (`67dbeb4`) | 88 → 93 | 4 of 10 | 0 of 10 |

In some cycles the clump was already gone when it was read back right after the write, before
the database went online. The page had been flushed by a refused attachment that read it earlier.

## Cause

In `src/jrd/jrd.cpp` (`master` at `67dbeb4`), `internalAttachDatabase`:

- line 1921: `PAG_attachment_id(tdbb);`
- lines 2038–2064: `if (dbb->isShutdown()) { ... if (dbb->isShutdown(shut_mode_full)) allow_access = false; ... ERR_post(Arg::Gds(isc_shutdown) << org_filename); }`

In `src/jrd/pag.cpp`, `PAG_attachment_id` (not read-only):

```cpp
header_page* header = (header_page*) CCH_FETCH(tdbb, &window, LCK_write, pag_header);
CCH_MARK(tdbb, &window);
attachment->att_attachment_id = ++header->hdr_attachment_id;
```

The dirty header page is written when the refused attachment's `Database` is released
(`JRD_shutdown_database`, `CCH_shutdown`). In SuperServer the file is open, and exclusively
`flock`ed (`lockDatabaseFile`), from the moment the page is read until the database is released.

## Expected

A refused attachment leaves the database file unchanged. In full shutdown in particular, nothing
should write to the file until it is brought online again. One way is to check the shutdown state
before taking an attachment id. Another is to take the id without writing the header page while
the database is shut down, as a read-only database does (`dbb->generateAttachmentId()`).

## Workaround

Before changing the file offline, take the exclusive `flock` the server holds while it has the
file open, and hold it until the change is written. The server cannot open the file meanwhile,
and once the lock is free no earlier attachment still holds a header page it could flush. This is
what the operator does since its v0.147.0.
