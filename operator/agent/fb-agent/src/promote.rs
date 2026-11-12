//! PROMOTE (docs/rust-agent-segment-server.md, step 5): this replica becomes the primary without
//! a restart, once the old primary is in full shutdown and this replica applied its last segment.
//! The sequence is segment-server.pl's, step for step: during a short full shutdown, while the
//! server has the database closed, the replication sequence is written to its header; then the
//! database goes online, stops being a replica and publishes. Until the promotion is complete the
//! replica's state is kept and any failure restores the replica, so the operator can still
//! promote it offline (a restart) instead.
//!
//! The server is reached over the agent's own attachments (the services for the shutdown, the
//! online and the replica mode; SQL for the replica mode's value, the sequence and the
//! publication). Two things are done on files no server has open, as the Perl server does them:
//! the header page is written directly, and the bootstrap seed copy is prepared with the image's
//! gfix and isql, which open it with the embedded engine. A client of the server cannot do that.

use crate::role::Answer;
use crate::seed::{exclusive, pause_until_applied, wait_for};
use crate::sql::{Database, Failed};
use crate::state::State;
use crate::status::{control_file, control_path, set_header_sequence, slurp, Files};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

/// The limits of segment-server.pl, from the same environment.
pub struct Limits {
    /// PROMOTE_SHUTDOWN_ATTEMPTS (5) and PROMOTE_SHUTDOWN_PAUSE (3 s): the full shutdown can fail
    /// while the server is busy
    pub shutdown_attempts: u32,
    pub shutdown_pause: Duration,
    /// PROMOTE_HEADER_ATTEMPTS (3): rounds of full shutdown, header and online until the server
    /// holds the sequence written
    pub header_attempts: u32,
    /// PROMOTE_CLOSE_TIMEOUT_SECONDS (30): how long the server may take to close the database
    pub close_timeout: u64,
    /// JOURNAL_DIR: the replica's own journal, cleared with the archive
    pub journal: Option<PathBuf>,
    /// SCRIPT_DIR/enable-publication.sql, for the bootstrap seed copy
    pub publication_script: PathBuf,
    /// The puller's position (STATE_FILE, or REPLICATION_DIR/.last-pulled)
    pub state_file: PathBuf,
}

const SEQUENCE: &str = "RDB$GET_CONTEXT('SYSTEM', 'REPLICATION_SEQUENCE')";

fn write_file(path: &Path, content: &str) -> bool {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    fs::write(&temporary, content).is_ok() && fs::rename(&temporary, path).is_ok()
}

fn remove_files(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for path in entries.filter_map(|entry| Some(entry.ok()?.path())).filter(|path| path.is_file()) {
        let _ = fs::remove_file(path);
    }
}

/// The service manager's message, on one line (it goes into the reply, the operator's event).
fn message(failed: &Failed, limit: Duration) -> String {
    match failed {
        Failed::Refused(why) => why.split_whitespace().collect::<Vec<_>>().join(" "),
        Failed::TimedOut => format!("timed out after {}s", limit.as_secs()),
    }
}

/// One of the image's tools on a file no server has open, under timeout(1) as the Perl server's.
fn tool(limit: Duration, program: &str, arguments: &[&str]) -> bool {
    Command::new("timeout")
        .args(["-k", "5", &limit.as_secs().to_string(), program])
        .args(arguments)
        .status()
        .is_ok_and(|status| status.success())
}

/// What a failed promotion has to undo.
#[derive(Default)]
struct Progress {
    shut: bool,
    writable: bool,
}

/// PROMOTE <archived>|none. `state`: the database's state from the local server.
/// Forward: the local server gave the agent no answer before anything was done.
pub fn promote(
    files: &Files,
    database: &Database,
    limits: &Limits,
    state: &dyn Fn() -> Option<State>,
    archived: Option<u64>,
    now: u64,
    client: &mut impl Write,
) -> Answer {
    let lock = match exclusive(&files.base) {
        Ok(lock) => lock,
        Err(error) => return Answer::Reply(format!("ERR {error}\n")),
    };
    let Ok(mode) = database.value("MON$REPLICA_MODE") else {
        drop(lock);
        return Answer::Forward;
    };
    let pause = files.base.join(".pause-pull");
    let journal_floor = files.base.join("journal-floor");
    let bootstrap_copy = files.base.join("bootstrap-seed.fdb.tmp");
    let mut progress = Progress::default();
    let done = run(files, database, limits, state, archived, now, &mode, &mut progress, client);
    if let Err(error) = &done {
        // back to a replica the offline promotion can still take over
        if progress.writable {
            let _ = database.replica_read_only();
        }
        let _ = fs::remove_file(&bootstrap_copy);
        let _ = fs::remove_file(&journal_floor);
        if progress.shut {
            let _ = database.bring_online();
        }
        let _ = writeln!(client, "ERR {error}");
        println!("promotion refused: {error}");
    }
    let _ = fs::remove_file(&pause);
    // the replies were written as the promotion went: nothing more to send
    Answer::Reply(String::new())
}

#[allow(clippy::too_many_arguments)]
fn run(
    files: &Files,
    database: &Database,
    limits: &Limits,
    state: &dyn Fn() -> Option<State>,
    archived: Option<u64>,
    now: u64,
    mode: &str,
    progress: &mut Progress,
    client: &mut impl Write,
) -> Result<(), String> {
    let promoted = files.base.join("promoted");
    if mode.trim().parse::<i64>() == Ok(0) {
        // promoted already (a repeated request)
        let sequence = database.value(SEQUENCE).ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        write_file(&promoted, &format!("{now}\n"));
        let _ = writeln!(client, "OK {sequence}");
        println!("already promoted (sequence {sequence})");
        return Ok(());
    }
    println!("promotion in place by the agent (its own attachments to the local server)");
    // the puller pauses first, as in the Perl server, whatever comes next
    let control = match control_path(&files.source) {
        Some(control) => control,
        None => {
            let _ = fs::remove_file(files.base.join(".pull-paused"));
            let _ = File::create(files.base.join(".pause-pull"));
            return Err(if wait_for(60, || files.base.join(".pull-paused").exists()) {
                "no replica control file".into()
            } else {
                "segment puller did not pause".into()
            });
        }
    };
    pause_until_applied(files, &control)?;
    let (mut sequence, _) = control_file(&control).ok_or("no replica control file")?;
    // a synchronous standby has every change up to the last segment archived on the old primary
    // (its puller records it, the control file stays where it was)
    if files.base.join("sync-standby").exists() {
        if let Ok(seen) = slurp(&files.base.join("sync-seen")).parse::<u64>() {
            sequence = sequence.max(seen);
        }
    }
    // segments sequence+1..archived of the archive are not in this lineage
    let mut lineage = None;
    if let Some(archived) = archived.filter(|archived| *archived > sequence) {
        lineage = Some(format!("{sequence} {archived}"));
        sequence = archived;
    }
    let bootstrap = files.base.join("bootstrap-seed.fdb");
    let bootstrap_copy = files.base.join("bootstrap-seed.fdb.tmp");
    let bootstrap_sequence = files.base.join("bootstrap-seed.seq");
    // The header is written while the server has the database closed, and read back through the
    // server once it is online again: a header page written back over the sequence (ISSUES.md
    // issue 10) gets the round again, the database still a replica.
    let mut rounds = 0;
    for round in 1..=limits.header_attempts {
        rounds = round;
        let mut failures = Vec::new();
        for attempt in 1..=limits.shutdown_attempts {
            match database.shut_down() {
                Ok(()) => {
                    progress.shut = true;
                    break;
                }
                Err(failed) => {
                    let why = Some(message(&failed, database.command_limit)).filter(|m| !m.is_empty());
                    failures.push(why.unwrap_or_else(|| "no message".into()));
                }
            }
            // a shutdown that went through despite the error
            if state() == Some(State::Shutdown) {
                progress.shut = true;
                break;
            }
            println!(
                "full shutdown attempt {attempt} of {} failed: {}",
                limits.shutdown_attempts,
                failures.last().map(String::as_str).unwrap_or("")
            );
            if attempt < limits.shutdown_attempts {
                thread::sleep(limits.shutdown_pause);
            }
        }
        if !progress.shut {
            return Err(format!(
                "full shutdown failed after {} attempts: {}",
                failures.len(),
                failures.join("; ")
            ));
        }
        // An attachment refused for the full shutdown still writes the header page (ISSUES.md,
        // issue 10): one that read the header before the sequence is written would write it back
        // without it, and the journal would restart at segment 1. The server holds an exclusive
        // lock (flock) on the file while it has it open: the header is written once that lock can
        // be taken, and the lock is held until the database goes online again.
        let closed = OpenOptions::new()
            .read(true)
            .open(&files.database)
            .map_err(|e| format!("cannot open {}: {e}", files.database.display()))?;
        let waited = Instant::now();
        // SAFETY: flock(2) on a descriptor this function owns
        if !wait_for(
            limits.close_timeout,
            || unsafe { libc::flock(closed.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        ) {
            return Err(format!(
                "the server still had the database open {}s after the full shutdown",
                limits.close_timeout
            ));
        }
        let waited = waited.elapsed().as_secs();
        if waited > 0 {
            println!("the server closed the database {waited}s after the full shutdown");
        }
        set_header_sequence(&files.database, sequence).map_err(|why| {
            println!("{why}");
            "cannot set the replication sequence".to_string()
        })?;
        // the journal floor: at its next start the init container raises a header that fell
        // behind it, so the journal never restarts below what the replicas applied
        if !write_file(&files.base.join("journal-floor"), &format!("{sequence}\n")) {
            return Err("cannot write the journal floor".into());
        }
        // the replica's journal and archive (the new journal starts empty) and the offline
        // bootstrap seed new replicas can start from, taken while nothing has the database open
        if let Some(journal) = &limits.journal {
            remove_files(journal);
        }
        remove_files(&files.archive);
        let _ = fs::remove_file(&bootstrap);
        let _ = fs::remove_file(&bootstrap_sequence);
        fs::copy(&files.database, &bootstrap_copy).map_err(|_| "cannot copy the bootstrap seed")?;
        drop(closed);
        database.bring_online().map_err(|failed| {
            let why = message(&failed, database.command_limit);
            format!(
                "cannot bring the database online{}",
                if why.is_empty() { String::new() } else { format!(": {why}") }
            )
        })?;
        progress.shut = false;
        let online = database.value(SEQUENCE).ok().filter(|value| !value.is_empty());
        if online
            .as_ref()
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|value| value >= sequence)
        {
            break;
        }
        let got = online.unwrap_or_else(|| "unreadable".into());
        if round == limits.header_attempts {
            return Err(format!(
                "the server kept a replication sequence of {got} instead of {sequence} after {} attempts",
                limits.header_attempts
            ));
        }
        println!(
            "online with replication sequence {got} instead of {sequence} (attempt {round} of {}): writing the header again",
            limits.header_attempts
        );
    }
    database.replica_none().map_err(|_| "cannot set replica mode none")?;
    progress.writable = true;
    for statement in ["ALTER DATABASE ENABLE PUBLICATION", "ALTER DATABASE INCLUDE ALL TO PUBLICATION"] {
        database.execute(statement).map_err(|why| {
            println!("{statement}: {why}");
            "cannot enable publication".to_string()
        })?;
    }
    // the server journals after the sequence it read from the header: below it the replicas
    // would skip every new segment as applied already (the offline promotion takes over)
    let live = database
        .value(SEQUENCE)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or("cannot read the replication sequence")?;
    if live < sequence {
        return Err(format!("the journal would continue after segment {live}, not {sequence}"));
    }
    // complete: the replica's state goes (the new journal continues after the sequence)
    write_file(&files.base.join("promoted"), &format!("{now}\n"));
    if let Some(lineage) = lineage {
        let recorded = files.base.join("lineage");
        if !slurp(&recorded).lines().any(|line| line == lineage) {
            if let Ok(mut file) = OpenOptions::new().append(true).create(true).open(&recorded) {
                let _ = writeln!(file, "{lineage}");
            }
        }
    }
    remove_files(&files.source);
    for gone in ["sync-seen", "sync-standby"] {
        let _ = fs::remove_file(files.base.join(gone));
    }
    let _ = fs::remove_file(&limits.state_file);
    // the rounds the header took (more than one: the server went online without it, issue 10)
    let _ = writeln!(client, "OK {sequence} {rounds}");
    let _ = client.flush();
    println!(
        "promoted in place: the journal continues after segment {sequence}{}",
        if rounds > 1 {
            format!(" (header written {rounds} times)")
        } else {
            String::new()
        }
    );
    // the copy taken in full shutdown becomes what the offline promotion's seed is: online, not a
    // replica, publishing; without it new replicas seed from a ready replica
    let copy = bootstrap_copy.to_string_lossy().into_owned();
    let script = limits.publication_script.to_string_lossy().into_owned();
    let limit = database.command_limit;
    if tool(limit, "gfix", &["-online", "normal", &copy])
        && tool(limit, "gfix", &["-replica", "none", &copy])
        && tool(limit, "isql", &["-q", "-b", "-i", &script, &copy])
        && write_file(&bootstrap_sequence, &format!("{sequence}\n"))
        && fs::rename(&bootstrap_copy, &bootstrap).is_ok()
    {
        println!("offline bootstrap seed written (sequence {sequence})");
    } else {
        let _ = fs::remove_file(&bootstrap_copy);
        let _ = fs::remove_file(&bootstrap_sequence);
        println!("no offline bootstrap seed: the copy could not be prepared");
    }
    Ok(())
}
