//! Replica-side journal shipping, as segment-puller.pl: polls the primary's segment server and
//! places new archived segments into journal_source_directory, where the replica applies them.
//! The primary host is re-read from PRIMARY_FILE on every poll, so it follows the operator's view
//! of the current primary; the puller idles on the primary itself.
//!
//! Environment: SOURCE_DIR, STATE_FILE, PRIMARY_FILE (required), REPLICATION_DIR, POD_NAME,
//! ISC_PASSWORD, SEGMENT_PORT, SEGMENT_PROXY, POLL_SECONDS (5), FIREBIRD_PORT (3050).

use crate::client::{now, open, var};
use crate::server::segment_name;
use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// The content of a file without its trailing whitespace; empty when it cannot be read.
fn slurp(path: &Path) -> String {
    fs::read_to_string(path).map(|s| s.trim_end().to_string()).unwrap_or_default()
}

/// Whether `host` names this instance: its pod name, alone or before a dot.
fn names(host: &str, pod: &str) -> bool {
    host.strip_prefix(pod).is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

/// Writes `content` through a temporary name, so a reader never sees a partial file.
fn write_atomic(path: &Path, content: &str) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    fs::write(&tmp, content)?;
    fs::rename(&tmp, path)
}

fn other(message: String) -> io::Error {
    io::Error::other(message)
}

struct Puller {
    source: PathBuf,
    state: PathBuf,
    primary_file: PathBuf,
    /// REPLICATION_DIR: the flags shared with the local segment server; without it they are not used
    base: Option<PathBuf>,
    pod: String,
}

impl Puller {
    fn flag(&self, name: &str) -> Option<PathBuf> {
        self.base.as_ref().map(|base| base.join(name))
    }

    /// The replicas the primary replicates to synchronously ("h1,h2" or "none"); None when it cannot tell.
    fn sync_target(&self, primary: &str) -> Option<String> {
        let mut line = String::new();
        open(primary, "SYNCTO", Some(REQUEST_TIMEOUT)).ok()?.read_line(&mut line).ok()?;
        line.strip_prefix("OK ")?.split_whitespace().next().map(str::to_string)
    }

    /// Fetches what is new. Some(primary) when it asked that primary, None when it had nothing to ask
    /// (paused, promoted, the primary itself, a synchronous standby).
    fn pull_once(&self) -> io::Result<Option<String>> {
        // the local segment server pauses pulling while it takes a seed copy of this replica
        if let (Some(flag), Some(ack)) = (self.flag(".pause-pull"), self.flag(".pull-paused")) {
            if flag.exists() {
                let _ = File::create(ack);
                return Ok(None);
            }
            let _ = fs::remove_file(ack);
        }
        // promoted in place (segment-server.pl PROMOTE) before the ConfigMap file names this instance
        if self.flag("promoted").is_some_and(|p| p.exists()) {
            return Ok(None);
        }
        let primary = slurp(&self.primary_file);
        if primary.is_empty() || names(&primary, &self.pod) {
            return Ok(None); // this instance is the primary
        }
        let mut last = slurp(&self.state);
        let mut segments = Vec::new();
        for line in open(&primary, "LIST", Some(REQUEST_TIMEOUT))?.lines() {
            let line = line?;
            let line = line.trim_end_matches('\r');
            if line == "." {
                break;
            }
            if line.starts_with("ERR") {
                return Err(other(format!("server: {line}")));
            }
            if segment_name(line) {
                segments.push(line.to_string());
            }
        }
        if let Some(seen) = self.flag("primary-seen") {
            let _ = write_atomic(&seen, &format!("{} {primary}\n", now()));
        }
        // Synchronous replication: a replica the primary replicates to directly must not apply the
        // journal too (every change would be applied twice). Neither may a standby the primary no
        // longer names without having repositioned it (STANDBY off): it may hold changes after its
        // replica control file position, so it waits to be re-seeded.
        let named = self
            .sync_target(&primary)
            .is_some_and(|hosts| hosts.split(',').any(|host| names(host, &self.pod)));
        let standby_seen = self.flag("sync-seen");
        let flagged = self.flag("sync-standby").is_some_and(|p| p.exists());
        if named || flagged {
            if named && flagged {
                let seen = segments
                    .iter()
                    .filter_map(|n| n.rsplit_once(".journal-")?.1.parse::<u64>().ok())
                    .max();
                if let (Some(seen), Some(path)) = (seen, &standby_seen) {
                    if seen.to_string() != slurp(path) {
                        let _ = write_atomic(path, &format!("{seen}\n"));
                    }
                }
            } else if named {
                println!("the primary replicates to this replica synchronously but it is not the standby: not applying the journal");
            } else {
                if let Some(path) = &standby_seen {
                    let _ = fs::remove_file(path);
                }
                println!("synchronous standby no longer attached and not repositioned: not applying the journal (re-seed this replica)");
            }
            return Ok(None);
        }
        if let Some(path) = &standby_seen {
            let _ = fs::remove_file(path);
        }
        segments.sort();
        for name in segments {
            if !last.is_empty() && name <= last {
                continue;
            }
            self.fetch(&primary, &name)?;
            write_atomic(&self.state, &format!("{name}\n")).map_err(|e| other(format!("write {}: {e}", self.state.display())))?;
            println!("fetched {name} from {primary}");
            last = name;
        }
        Ok(Some(primary))
    }

    /// Waits on the primary's segment server for a segment after the last one fetched, for at most
    /// `seconds` (WAIT, answered by the fb-agent segment server only). False when the server does
    /// not know the request or cannot be asked: the caller then waits out its poll interval.
    fn wait_for_segment(&self, primary: &str, seconds: u64) -> bool {
        let last = slurp(&self.state);
        let request = if last.is_empty() {
            format!("WAIT {seconds}")
        } else {
            format!("WAIT {seconds} {last}")
        };
        let mut line = String::new();
        let asked = open(primary, &request, Some(Duration::from_secs(seconds + 10))).and_then(|mut reply| reply.read_line(&mut line));
        asked.is_ok() && line.starts_with("OK")
    }

    /// GET: the segment into a temporary name, then renamed (an atomic hand-off to the replica).
    fn fetch(&self, primary: &str, name: &str) -> io::Result<()> {
        let mut reply = open(primary, &format!("GET {name}"), Some(REQUEST_TIMEOUT))?;
        let mut header = String::new();
        reply.read_line(&mut header)?;
        let size: u64 = header
            .strip_prefix("OK ")
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|size| size.parse().ok())
            .ok_or_else(|| other(format!("server: {}", header.trim_end())))?;
        let part = self.source.join(format!(".{name}.part"));
        let mut out = File::create(&part).map_err(|e| other(format!("write {}: {e}", part.display())))?;
        let copied = io::copy(&mut reply.take(size), &mut out)?;
        if copied != size {
            return Err(other(format!("short read for {name}")));
        }
        out.flush()?;
        drop(out);
        fs::rename(&part, self.source.join(name)).map_err(|e| other(format!("rename {name}: {e}")))
    }
}

/// The pod's Firebird server no longer accepts connections.
fn firebird_stopped() -> bool {
    let port: u16 = var("FIREBIRD_PORT").parse().unwrap_or(3050);
    TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], port)), Duration::from_secs(2)).is_err()
}

fn required(name: &str) -> io::Result<PathBuf> {
    let value = var(name);
    if value.is_empty() {
        return Err(other(format!("{name} is required")));
    }
    Ok(PathBuf::from(value))
}

pub fn run() -> io::Result<()> {
    let base = var("REPLICATION_DIR");
    let puller = Puller {
        source: required("SOURCE_DIR")?,
        state: required("STATE_FILE")?,
        primary_file: required("PRIMARY_FILE")?,
        base: (!base.is_empty()).then(|| PathBuf::from(base)),
        pod: var("POD_NAME"),
    };
    let interval: u64 = var("POLL_SECONDS").parse().unwrap_or(5);
    // SIGTERM for a sidecar that runs as its container's PID 1: it keeps working until the pod's
    // Firebird server has stopped (the smart shutdown waits for clients first), then exits
    let stopping = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(signal, Arc::clone(&stopping))?;
    }
    let stopped = || stopping.load(Ordering::Relaxed) && firebird_stopped();
    println!(
        "segment puller: source={} primary-file={}",
        puller.source.display(),
        puller.primary_file.display()
    );
    // the primary that did not answer WAIT (a Perl segment server), and the polls since: it is
    // asked again now and then, as it may have been restarted with the fb-agent server
    let mut no_wait: Option<(String, u32)> = None;
    loop {
        if stopped() {
            break;
        }
        let asked = match puller.pull_once() {
            Ok(asked) => asked,
            Err(err) => {
                println!("pull failed: {err}");
                None
            }
        };
        if stopped() {
            break;
        }
        // instead of sleeping: wait on the primary for the next segment, where it can tell
        if let Some(primary) = asked.filter(|_| !stopping.load(Ordering::Relaxed) && interval > 0) {
            let skip = match &mut no_wait {
                Some((host, polls)) if *host == primary => {
                    *polls += 1;
                    *polls % 12 != 0
                }
                _ => false,
            };
            if !skip {
                if puller.wait_for_segment(&primary, interval.min(60)) {
                    no_wait = None;
                    continue;
                }
                let polls = match no_wait.take() {
                    Some((host, polls)) if host == primary => polls,
                    _ => 0,
                };
                no_wait = Some((primary, polls));
            }
        }
        // in short steps: a signal is noticed at once, then the server is checked every second
        let mut left = Duration::from_secs(interval);
        while !left.is_zero() && !stopping.load(Ordering::Relaxed) {
            let step = left.min(Duration::from_millis(200));
            thread::sleep(step);
            left -= step;
        }
        if stopping.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
        }
    }
    println!("stopping: the Firebird server has stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_its_own_name() {
        assert!(names("db-1", "db-1"));
        assert!(names("db-1.db-headless", "db-1"));
        assert!(!names("db-10.db-headless", "db-1"));
        assert!(!names("db-0.db-headless", "db-1"));
        // without a pod name, only a host that starts with a dot (as the Perl puller)
        assert!(!names("db-0", ""));
    }
}
