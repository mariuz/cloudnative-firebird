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
//!
//! A segment is complete once its file holds the length its header announces. Firebird archives a
//! segment with a plain copy, and its replica skips a file shorter than its header says, so a
//! segment listed while it is still being copied would be fetched short and never applied.
//!
//! Environment: ARCHIVE_DIR, REPLICATION_DIR, ISC_PASSWORD, SEGMENT_LISTEN (host:port) or
//! SEGMENT_PORT, AUTH_WINDOW_SECONDS, BACKEND_PORT (3071; 0: a free port). The backend command
//! follows "--" on the command line and gets SEGMENT_LISTEN=127.0.0.1:<BACKEND_PORT>.

use crate::client::{now, var};
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
            ("LIST", _) => {
                self.contacted();
                let mut reply = String::new();
                for name in segments(&self.archive) {
                    reply.push_str(&name);
                    reply.push('\n');
                }
                reply.push_str(".\n");
                client.write_all(reply.as_bytes())
            }
            ("GET", Some(name)) if segment_name(name) && self.archive.join(name).is_file() => {
                self.contacted();
                let Ok(mut file) = File::open(self.archive.join(name)) else {
                    return writeln!(client, "ERR cannot read");
                };
                writeln!(client, "OK {}", file.metadata()?.len())?;
                io::copy(&mut file, &mut client).map(|_| ())
            }
            ("WAIT", Some(argument)) => {
                self.contacted();
                self.wait(&mut client, argument)
            }
            // not taken over yet: the Perl server answers, with the request as it came
            _ => self.forward(client, reader, &line),
        }
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

fn free_port() -> io::Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

pub fn run(backend_command: &[String]) -> io::Result<()> {
    let Some((program, arguments)) = backend_command.split_first() else {
        return Err(io::Error::other("usage: fb-agent segment-server -- <backend command...>"));
    };
    let archive = var("ARCHIVE_DIR");
    if archive.is_empty() {
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
    });
    println!(
        "segment server (fb-agent {}) listening on {}, serving {}; other requests go to {backend}",
        env!("CARGO_PKG_VERSION"),
        listener.local_addr()?,
        front.archive.display()
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
