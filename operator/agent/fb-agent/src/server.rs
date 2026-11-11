//! The segment server's front (docs/rust-agent-segment-server.md). It listens where
//! segment-server.pl would, answers the commands it has taken over, and forwards every other
//! request, unchanged, to the Perl server it runs behind it on a local port.
//!
//! Its own commands (each connection carries one signed request, as before):
//!   PING                    -> "OK"
//!   LIST                    -> the archived segment names that are complete, one per line, then "."
//!   GET <name>              -> "OK <size>" + the segment's bytes
//!   WAIT <seconds> [<name>] -> "OK new" as soon as a complete segment after <name> (any segment
//!                              without one) is archived, "OK timeout" after <seconds> (1 to 60).
//!                              The pullers wait here instead of asking every few seconds.
//!   STATE                   -> "OK online", "OK shutdown", "OK fenced" (shut down by the isolation
//!                              check) or "OK fenced lease" (by the Lease holder), from a service
//!                              attachment that stays open (state.rs). Passed to the Perl server
//!                              when that cannot tell.
//!   VERSION                 -> "OK <engine version>", from the same attachment; else the Perl server.
//!   HEADER, ISOLATION, POSITION, PRIMARYSEEN, FOLLOW, SYNCTO, POINTS <segment>, LINEAGE,
//!   ARCHIVED, UPLOADED <segment>, RETAIN <segment>|none
//!                           -> from the files on the instance's volume, as the Perl server
//!                              answers them (status.rs).
//!   FILE <name>, STORE <name> <size>, REMOVE <name>, FILES, EXISTS <name>
//!                           -> the backup files in the data directory (backups.rs).
//!   SEED, TXNS <upto> <ids>, PLAN <upto> <next> <ids>
//!                           -> a replica's seed copy and where its transactions begin in the
//!                              archive (seed.rs). NATIVE_SEED=false: the Perl server's.
//!   SYNC, STANDBY on|off <segment>, FENCE [<reason>], REJOIN, NBACKUP <level> <file>
//!                           -> the changes of what the instance does (role.rs). NATIVE_ROLE=false:
//!                              the Perl server's. PROMOTE is the Perl server's.
//!
//! With FILES_ONLY=true (instances without replication) only PING and the backup files are its
//! own; the Perl server refuses the rest, as it does alone.
//!
//! A segment is complete once its file holds the length its header announces. Firebird archives a
//! segment with a plain copy, and its replica skips a file shorter than its header says, so a
//! segment listed while it is still being copied would be fetched short and never applied.
//!
//! Environment: ARCHIVE_DIR, REPLICATION_DIR, SOURCE_DIR, PRIMARY_FILE, POD_NAME, ISC_PASSWORD, SEGMENT_LISTEN (host:port) or
//! SEGMENT_PORT, AUTH_WINDOW_SECONDS, BACKEND_PORT (3071; 0: a free port); for STATE:
//! DATABASE_PATH, ISC_USER (SYSDBA), FIREBIRD_PORT (3050), LOCAL_QUERY_TIMEOUT_SECONDS (10),
//! NATIVE_STATE (false: always the Perl server). The backend command
//! follows "--" on the command line and gets SEGMENT_LISTEN=127.0.0.1:<BACKEND_PORT>.

use crate::backups;
use crate::client::{now, var};
use crate::role::{self, Answer};
use crate::seed::{self, Seeds};
use crate::sql::Database;
use crate::state::{Probe, State};
use crate::status::Files;
use sig1::Verifier;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

/// As the Perl server: a client that sends nothing is dropped after this long.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
/// How often WAIT looks at the archive.
const WAIT_STEP: Duration = Duration::from_millis(100);

/// `^[A-Za-z0-9._-]+\.journal-\d+$`
pub fn segment_name(name: &str) -> bool {
    let Some((base, sequence)) = name.rsplit_once(".journal-") else {
        return false;
    };
    !base.is_empty()
        && base.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && !sequence.is_empty()
        && sequence.bytes().all(|b| b.is_ascii_digit())
}

/// Whether a segment file holds all of its segment: a 48-byte header {char[12] "FBCHANGELOG",
/// u16 version, u16 state, guid[16], u64 sequence, u64 length} and at least `length` bytes. A file
/// without such a header is left to the replica to judge, as before.
pub fn segment_complete(path: &Path) -> bool {
    let mut header = [0u8; 48];
    let Ok(mut file) = File::open(path) else { return false };
    if file.read_exact(&mut header).is_err() || !header.starts_with(b"FBCHANGELOG") {
        return true;
    }
    let length = u64::from_le_bytes(header[40..48].try_into().expect("8 bytes"));
    file.metadata().map(|m| m.len() >= length).unwrap_or(false)
}

/// The names in the archive that look like segments, sorted.
fn segment_names(dir: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(dir) else { return Vec::new() };
    let mut names: Vec<String> = entries
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| segment_name(name))
        .collect();
    names.sort();
    names
}

fn servable(dir: &Path, name: &str) -> bool {
    let path = dir.join(name);
    path.is_file() && segment_complete(&path)
}

/// The archived segments that can be served, sorted by name.
fn segments(dir: &Path) -> Vec<String> {
    segment_names(dir).into_iter().filter(|name| servable(dir, name)).collect()
}

struct Newest {
    looked: Instant,
    /// the archive directory's modification time at the lookup: a new or removed file changes it
    changed: Option<SystemTime>,
    /// a newer file was still being copied: looked at again whatever the directory says
    copying: bool,
    name: Option<String>,
}

struct Front {
    archive: PathBuf,
    /// REPLICATION_DIR/last-contact: when an authenticated request was last answered, which the
    /// isolation check reads (a primary nothing reaches fences itself)
    contact: Option<PathBuf>,
    contacted: Mutex<u64>,
    /// What the requests that wait share: the newest servable segment, looked up when the archive
    /// directory changes
    newest: Mutex<Newest>,
    verifier: Mutex<Verifier>,
    backend: SocketAddr,
    /// The local server's state, with REPLICATION_DIR/self-fenced: who shut the database down
    state: Option<Probe>,
    self_fenced: Option<PathBuf>,
    /// The instance's files, for the requests answered from them
    files: Option<Files>,
    /// Whether the journal's requests are served (not with FILES_ONLY)
    journal: bool,
    /// For the seeds: the local server's database and whether a live copy is allowed
    seeds: Option<Seeds>,
    /// Whether the changes of role are answered here (not with NATIVE_ROLE=false)
    role: bool,
    /// The puller's position (STATE_FILE, or REPLICATION_DIR/.last-pulled)
    state_file: PathBuf,
    /// The data directory, where the backup files are
    data: Option<PathBuf>,
}

impl Front {
    fn contacted(&self) {
        let Some(path) = &self.contact else { return };
        let at = now();
        let mut last = self.contacted.lock().unwrap_or_else(|p| p.into_inner());
        if *last == at && path.exists() {
            return;
        }
        *last = at;
        let _ = fs::write(path, format!("{at}\n"));
    }

    /// The newest servable segment. The archive is read at most once per WAIT_STEP whatever the
    /// number of pullers waiting, and only when the directory changed since, or while its newest
    /// file is being copied.
    fn newest(&self) -> Option<String> {
        let mut newest = self.newest.lock().unwrap_or_else(|p| p.into_inner());
        if newest.looked.elapsed() >= WAIT_STEP {
            newest.looked = Instant::now();
            let changed = fs::metadata(&self.archive).and_then(|m| m.modified()).ok();
            // a directory changed in the last seconds is read again: its time may be coarse
            let recent = changed.is_none_or(|at| at.elapsed().map_or(true, |age| age < Duration::from_secs(3)));
            if newest.copying || recent || changed != newest.changed {
                let names = segment_names(&self.archive);
                newest.name = names.iter().rev().find(|name| servable(&self.archive, name)).cloned();
                newest.copying = names.last() != newest.name.as_ref();
                newest.changed = changed;
            }
        }
        newest.name.clone()
    }

    fn answer(&self, stream: TcpStream) -> io::Result<()> {
        stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut line = String::new();
        (&mut reader).take(65536).read_line(&mut line)?;
        let request_line = line.trim_end_matches(['\r', '\n']).to_string();
        let verified = self.verifier.lock().unwrap_or_else(|p| p.into_inner()).verify(&request_line, now());
        let mut client = stream;
        let request = match verified {
            Ok(request) => request,
            Err(denied) => return writeln!(client, "ERR unauthorized ({denied})"),
        };
        let argument = request.argument.as_deref();
        match (request.command.as_str(), argument) {
            ("PING", _) => {
                self.contacted();
                writeln!(client, "OK")
            }
            ("LIST", _) if self.journal => {
                self.contacted();
                let mut reply = String::new();
                for name in segments(&self.archive) {
                    reply.push_str(&name);
                    reply.push('\n');
                }
                reply.push_str(".\n");
                client.write_all(reply.as_bytes())
            }
            ("GET", Some(name)) if self.journal && segment_name(name) && self.archive.join(name).is_file() => {
                self.contacted();
                let Ok(mut file) = File::open(self.archive.join(name)) else {
                    return writeln!(client, "ERR cannot read");
                };
                writeln!(client, "OK {}", file.metadata()?.len())?;
                io::copy(&mut file, &mut client).map(|_| ())
            }
            ("WAIT", Some(argument)) if self.journal => {
                self.contacted();
                self.wait(&mut client, argument)
            }
            // the Lease holder's, from this pod: no contact from outside
            ("STATE", _) if self.state.is_some() => match self.state.as_ref().and_then(Probe::state) {
                Some(State::Online) => writeln!(client, "OK online"),
                Some(State::Shutdown) => {
                    // a fence leaves its marker: the Lease holder's says "lease" (it lifts its own),
                    // the isolation check's does not (the operator lifts that)
                    let marker = self.self_fenced.as_ref().filter(|path| path.is_file());
                    match marker.map(|path| fs::read_to_string(path).unwrap_or_default()) {
                        Some(text) if text.split(|c: char| !c.is_alphanumeric() && c != '_').any(|word| word == "lease") => {
                            writeln!(client, "OK fenced lease")
                        }
                        Some(_) => writeln!(client, "OK fenced"),
                        None => writeln!(client, "OK shutdown"),
                    }
                }
                // not told: the Perl server asks with fbsvcmgr
                None => self.forward(client, reader, &line),
            },
            (command @ ("TXNS" | "PLAN"), Some(argument)) if self.seeds.is_some() => {
                self.contacted();
                match seed::transactions(&self.archive, command, argument) {
                    Some(reply) => client.write_all(reply.as_bytes()),
                    None => self.forward(client, reader, &line),
                }
            }
            (command @ ("SYNC" | "STANDBY" | "FENCE" | "REJOIN" | "NBACKUP"), _)
                if self.role && self.seeds.is_some() && self.files.is_some() =>
            {
                // the Lease holder's requests come from this pod: no contact from outside
                if !matches!(command, "FENCE" | "REJOIN") {
                    self.contacted();
                }
                match self.change(command, argument) {
                    Some(Answer::Reply(reply)) => client.write_all(reply.as_bytes()),
                    // refused, or undone: the Perl server answers
                    Some(Answer::Forward) | None => self.forward(client, reader, &line),
                }
            }
            ("SEED", _) if self.seeds.is_some() && self.files.is_some() => {
                self.contacted();
                let (seeds, files) = (self.seeds.as_ref().expect("checked"), self.files.as_ref().expect("checked"));
                match seed::seed(seeds, files, &mut client) {
                    Some(done) => done,
                    // the local server gave no answer here: the Perl server asks with isql
                    None => self.forward(client, reader, &line),
                }
            }
            (command @ ("FILE" | "STORE" | "REMOVE" | "FILES" | "EXISTS"), _) if self.data.is_some() => {
                let data = self.data.as_deref().expect("checked by the guard");
                self.contacted();
                match self.backup_files(data, command, argument, &mut client, &mut reader) {
                    Some(done) => done,
                    // refused, or an error of the system's: in the Perl server's words
                    None => self.forward(client, reader, &line),
                }
            }
            ("VERSION", _) if self.state.is_some() => {
                self.contacted();
                match self.state.as_ref().and_then(Probe::version) {
                    Some(version) => writeln!(client, "OK {version}"),
                    // not told: the Perl server asks with isql
                    None => self.forward(client, reader, &line),
                }
            }
            (command, _) if self.files.is_some() => match self.files.as_ref().and_then(|files| from_files(files, command, argument)) {
                Some(reply) => {
                    self.contacted();
                    client.write_all(reply.as_bytes())
                }
                // not taken over yet: the Perl server answers, with the request as it came
                None => self.forward(client, reader, &line),
            },
            _ => self.forward(client, reader, &line),
        }
    }

    /// SYNC, STANDBY, FENCE, REJOIN and NBACKUP. None: an argument the Perl server refuses.
    fn change(&self, command: &str, argument: Option<&str>) -> Option<Answer> {
        let (files, database) = (self.files.as_ref()?, &self.seeds.as_ref()?.database);
        match (command, argument) {
            ("SYNC", Some(hosts)) => role::sync(files, database, hosts),
            ("STANDBY", Some("on")) => Some(role::standby_on(files, now())),
            ("STANDBY", Some(argument)) => {
                let segment = argument.strip_prefix("off ")?;
                let digits = !segment.is_empty() && segment.bytes().all(|b| b.is_ascii_digit());
                digits.then(|| role::standby_off(files, &self.state_file, segment))
            }
            ("FENCE", reason) => Some(role::fence(files, database, reason, now())),
            ("REJOIN", _) => Some(role::rejoin(files, database)),
            ("NBACKUP", Some(argument)) => role::nbackup(files, database, self.data.as_deref()?, argument),
            _ => None,
        }
    }

    /// The backup file requests. None: nothing was sent or read, and the Perl server is to answer.
    fn backup_files(
        &self,
        data: &Path,
        command: &str,
        argument: Option<&str>,
        client: &mut TcpStream,
        reader: &mut BufReader<TcpStream>,
    ) -> Option<io::Result<()>> {
        let reply = match (command, argument) {
            ("FILES", _) => backups::list(data),
            ("EXISTS", Some(name)) if backups::plain_name(name) => backups::exists(data, name),
            ("REMOVE", Some(name)) if backups::backup_name(name) => backups::remove(data, name)?,
            ("FILE", Some(name)) if backups::backup_name(name) && data.join(name).is_file() => {
                return backups::send(data, name, client);
            }
            ("STORE", Some(argument)) => {
                let (name, size) = argument.split_once(' ')?;
                let digits = !size.is_empty() && size.bytes().all(|b| b.is_ascii_digit());
                if !backups::backup_name(name) || !digits {
                    return None;
                }
                backups::store(data, name, size.parse().ok()?, reader)?
            }
            _ => return None,
        };
        Some(client.write_all(reply.as_bytes()))
    }

    fn wait(&self, client: &mut TcpStream, argument: &str) -> io::Result<()> {
        let (seconds, after) = match argument.split_once(' ') {
            Some((seconds, after)) => (seconds, Some(after)),
            None => (argument, None),
        };
        let seconds = seconds.parse::<u64>().ok().filter(|s| (1..=60).contains(s));
        let (Some(seconds), true) = (seconds, after.is_none_or(segment_name)) else {
            return writeln!(client, "ERR bad request");
        };
        let deadline = Instant::now() + Duration::from_secs(seconds);
        loop {
            if self
                .newest()
                .is_some_and(|newest| after.is_none_or(|after| newest.as_str() > after))
            {
                return writeln!(client, "OK new");
            }
            if Instant::now() >= deadline {
                return writeln!(client, "OK timeout");
            }
            thread::sleep(WAIT_STEP);
        }
    }

    /// Hands the connection to the backend: the request line as it came (the backend verifies it
    /// again), what the client sent after it, then both directions until the backend is done.
    fn forward(&self, mut client: TcpStream, reader: BufReader<TcpStream>, line: &str) -> io::Result<()> {
        // the backend may still be starting
        let mut attempts = 0;
        let mut backend = loop {
            match TcpStream::connect_timeout(&self.backend, Duration::from_secs(5)) {
                Ok(backend) => break backend,
                Err(_) if attempts < 50 => {
                    attempts += 1;
                    thread::sleep(Duration::from_millis(100));
                }
                Err(err) => return writeln!(client, "ERR segment server backend: {err}"),
            }
        };
        backend.write_all(line.as_bytes())?;
        if !line.ends_with('\n') {
            backend.write_all(b"\n")?;
        }
        backend.write_all(reader.buffer())?;
        let mut upload_from = reader.into_inner();
        let mut upload_to = backend.try_clone()?;
        // uploads (STORE) for as long as the client sends; ends when either side closes
        thread::spawn(move || {
            let _ = io::copy(&mut upload_from, &mut upload_to);
            let _ = upload_to.shutdown(Shutdown::Write);
        });
        let copied = io::copy(&mut backend, &mut client);
        let _ = client.shutdown(Shutdown::Both);
        copied.map(|_| ())
    }
}

/// The reply to a request that is answered from the instance's files; None for any other, and for
/// an argument the Perl server would refuse (it then says so).
fn from_files(files: &Files, command: &str, argument: Option<&str>) -> Option<String> {
    let number = argument.filter(|a| !a.is_empty() && a.bytes().all(|b| b.is_ascii_digit()));
    Some(match command {
        "HEADER" => files.header(),
        "ISOLATION" => files.isolation(),
        "POSITION" => files.position(),
        "PRIMARYSEEN" => files.primary_seen(now()),
        "FOLLOW" => files.follow(),
        "SYNCTO" => files.sync_to(),
        "LINEAGE" => files.lineage(),
        "ARCHIVED" => files.archived(now()),
        "POINTS" => files.points(number?),
        "UPLOADED" => files.record("uploaded-floor", Some(number?)),
        "RETAIN" if argument == Some("none") => files.record("retain-floor", None),
        "RETAIN" => files.record("retain-floor", Some(number?)),
        _ => return None,
    })
}

/// A positive number of seconds from the environment, or the default.
fn seconds(name: &str, default: u64) -> Duration {
    Duration::from_secs(var(name).parse().ok().filter(|s| *s > 0).unwrap_or(default))
}

fn free_port() -> io::Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

pub fn run(backend_command: &[String]) -> io::Result<()> {
    let Some((program, arguments)) = backend_command.split_first() else {
        return Err(io::Error::other("usage: fb-agent segment-server -- <backend command...>"));
    };
    let files_only = var("FILES_ONLY") == "true";
    let archive = var("ARCHIVE_DIR");
    if archive.is_empty() && !files_only {
        return Err(io::Error::other("ARCHIVE_DIR is required"));
    }
    let listen = var("SEGMENT_LISTEN");
    let listener = if listen.is_empty() {
        TcpListener::bind(("0.0.0.0", var("SEGMENT_PORT").parse::<u16>().unwrap_or(3051)))?
    } else {
        TcpListener::bind(listen.as_str())?
    };
    let backend_port = match var("BACKEND_PORT").parse::<u16>() {
        Ok(0) => free_port()?,
        Ok(port) => port,
        Err(_) => 3071,
    };
    let backend = SocketAddr::from(([127, 0, 0, 1], backend_port));
    let base = var("REPLICATION_DIR");
    let front = Arc::new(Front {
        archive: PathBuf::from(archive),
        contact: (!base.is_empty()).then(|| Path::new(&base).join("last-contact")),
        contacted: Mutex::new(0),
        newest: Mutex::new(Newest {
            looked: Instant::now() - WAIT_STEP,
            changed: None,
            copying: true,
            name: None,
        }),
        verifier: Mutex::new(Verifier::new(
            var("ISC_PASSWORD").as_bytes(),
            var("AUTH_WINDOW_SECONDS").parse().unwrap_or(300),
        )),
        backend,
        state: (!files_only && var("NATIVE_STATE") != "false" && !var("DATABASE_PATH").is_empty()).then(|| {
            Probe::start(
                var("DATABASE_PATH"),
                var("FIREBIRD_PORT").parse().unwrap_or(3050),
                Some(var("ISC_USER"))
                    .filter(|user| !user.is_empty())
                    .unwrap_or_else(|| "SYSDBA".into()),
                var("ISC_PASSWORD"),
                seconds("LOCAL_QUERY_TIMEOUT_SECONDS", 10),
            )
        }),
        self_fenced: (!base.is_empty()).then(|| Path::new(&base).join("self-fenced")),
        journal: !files_only,
        role: var("NATIVE_ROLE") != "false",
        state_file: Some(var("STATE_FILE"))
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(&base).join(".last-pulled")),
        seeds: (!files_only && var("NATIVE_SEED") != "false" && !var("DATABASE_PATH").is_empty()).then(|| Seeds {
            database: Database {
                path: var("DATABASE_PATH"),
                port: var("FIREBIRD_PORT").parse().unwrap_or(3050),
                user: Some(var("ISC_USER"))
                    .filter(|user| !user.is_empty())
                    .unwrap_or_else(|| "SYSDBA".into()),
                password: var("ISC_PASSWORD"),
                query_limit: seconds("LOCAL_QUERY_TIMEOUT_SECONDS", 10),
                command_limit: seconds("LOCAL_COMMAND_TIMEOUT_SECONDS", 60),
            },
            allow_live: var("ALLOW_LIVE_SEED") == "true",
        }),
        // the database's directory ("." for a bare file name, as the Perl server's)
        data: Some(var("DATABASE_PATH"))
            .filter(|path| !path.is_empty())
            .map(|path| match path.rsplit_once('/') {
                Some((dir, _)) if !dir.is_empty() => PathBuf::from(dir),
                _ => PathBuf::from("."),
            }),
        files: (!files_only
            && [var("DATABASE_PATH"), var("SOURCE_DIR"), base.clone(), var("PRIMARY_FILE")]
                .iter()
                .all(|value| !value.is_empty()))
        .then(|| Files {
            database: PathBuf::from(var("DATABASE_PATH")),
            archive: PathBuf::from(var("ARCHIVE_DIR")),
            source: PathBuf::from(var("SOURCE_DIR")),
            base: PathBuf::from(&base),
            primary_file: PathBuf::from(var("PRIMARY_FILE")),
            pod: var("POD_NAME"),
        }),
    });
    println!(
        "{} (fb-agent {}) listening on {}, serving {}; other requests go to {backend}",
        if files_only { "backup file server" } else { "segment server" },
        env!("CARGO_PKG_VERSION"),
        listener.local_addr()?,
        if files_only {
            front.data.as_deref().unwrap_or(Path::new("")).display()
        } else {
            front.archive.display()
        }
    );
    let mut child = Command::new(program)
        .args(arguments)
        .env("SEGMENT_LISTEN", backend.to_string())
        .spawn()?;

    // SIGTERM reaches this process only (the container's PID 1): it is passed on to the backend,
    // which stops once the pod's Firebird server has stopped, and this front stops with it
    let stopping = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(signal, Arc::clone(&stopping))?;
    }
    thread::spawn(move || {
        let mut passed_on = false;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                println!("segment server backend ended ({status})");
                process::exit(status.code().unwrap_or(1));
            }
            if stopping.load(Ordering::Relaxed) && !passed_on {
                passed_on = true;
                // SAFETY: kill(2) with the id of a child this process has not waited for yet
                unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
            }
            thread::sleep(Duration::from_millis(200));
        }
    });

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let front = Arc::clone(&front);
        thread::spawn(move || {
            let _ = front.answer(stream);
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(length: u64, size: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; size.max(48)];
        bytes[..11].copy_from_slice(b"FBCHANGELOG");
        bytes[40..48].copy_from_slice(&length.to_le_bytes());
        bytes.truncate(size);
        bytes
    }

    #[test]
    fn recognizes_segment_names() {
        assert!(segment_name("mydb.fdb.journal-000000007"));
        assert!(segment_name("a.journal-1"));
        assert!(!segment_name("mydb.fdb.journal-"));
        assert!(!segment_name("mydb.fdb.journal-7x"));
        assert!(!segment_name(".journal-7"));
        assert!(!segment_name("../x.journal-7"));
        assert!(!segment_name("mydb.fdb"));
    }

    #[test]
    fn serves_only_segments_that_hold_their_whole_length() {
        let dir = std::env::temp_dir().join(format!("fb-agent-segments-{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("db.fdb.journal-000000001"), segment(1000, 1000)).unwrap();
        // still being copied: the header says more than the file holds
        fs::write(dir.join("db.fdb.journal-000000002"), segment(1000, 400)).unwrap();
        // longer than its length (a preallocated file) is complete
        fs::write(dir.join("db.fdb.journal-000000003"), segment(100, 4096)).unwrap();
        // no segment header: listed as before, the replica decides
        fs::write(dir.join("db.fdb.journal-000000004"), b"x").unwrap();
        fs::write(dir.join("not-a-segment"), segment(10, 100)).unwrap();
        fs::create_dir(dir.join("db.fdb.journal-000000005")).unwrap();
        assert_eq!(
            segments(&dir),
            ["db.fdb.journal-000000001", "db.fdb.journal-000000003", "db.fdb.journal-000000004"]
        );
        // WAIT: the newest that can be served, also while a newer file is being copied
        let front = Front {
            archive: dir.clone(),
            contact: None,
            contacted: Mutex::new(0),
            newest: Mutex::new(Newest {
                looked: Instant::now() - WAIT_STEP,
                changed: None,
                copying: true,
                name: None,
            }),
            verifier: Mutex::new(Verifier::new(b"", 300)),
            backend: SocketAddr::from(([127, 0, 0, 1], 1)),
            state: None,
            self_fenced: None,
            files: None,
            journal: true,
            seeds: None,
            role: false,
            state_file: PathBuf::new(),
            data: None,
        };
        assert_eq!(front.newest().as_deref(), Some("db.fdb.journal-000000004"));
        fs::write(dir.join("db.fdb.journal-000000006"), segment(1000, 400)).unwrap();
        thread::sleep(WAIT_STEP);
        assert_eq!(front.newest().as_deref(), Some("db.fdb.journal-000000004"));
        // the copy completes without the directory changing: seen all the same
        fs::write(dir.join("db.fdb.journal-000000006"), segment(1000, 1000)).unwrap();
        thread::sleep(WAIT_STEP);
        assert_eq!(front.newest().as_deref(), Some("db.fdb.journal-000000006"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
