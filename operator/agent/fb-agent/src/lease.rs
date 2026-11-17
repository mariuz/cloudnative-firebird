//! The primary Lease holder (`fb-agent lease-holder`): a native sidecar of every instance pod
//! that keeps the cluster's Lease alive while its pod is the primary, as CloudNativePG's
//! instance manager does. The operator's lease-holder.ts, with the same decisions, requests and
//! log lines; clusters that run the agent run this one.
//!
//! Every LEASE_DURATION_SECONDS / 3 it checks whether this pod is the primary (the cluster
//! ConfigMap's primary entry, or the "promoted" marker of an instance promoted in place) and
//! whether its database is online (the segment server's STATE). While both hold it renews the
//! Lease (renewTime, with itself as the holder); a database shut down stops the renewals, so the
//! Lease expires and the operator may fail over.
//!
//! It fences the database (the segment server's FENCE) when the Lease names another instance
//! (the operator promoted a replica) or when it could not renew it for the renew deadline, two
//! thirds of the Lease's duration: it cannot reach the API server, and the operator may promote
//! a replica once the Lease expires. The deadline counts from when the last successful renewal
//! was sent, and a check every second enforces it whatever a tick is waiting for. A database
//! fenced this way comes back online once this pod holds the Lease again: the holder re-acquires
//! it (with the version it read, so a failover that took the Lease over meanwhile wins) and asks
//! the segment server to bring the database back (REJOIN). A database the isolation check fenced
//! is left to the operator.
//!
//! Environment: POD_NAME, POD_NAMESPACE, LEASE_NAME, LEASE_DURATION_SECONDS, PRIMARY_FILE,
//! REPLICATION_DIR, TOKEN_DIR (the projected ServiceAccount token and the API server's CA),
//! KUBERNETES_SERVICE_HOST / PORT, SEGMENT_SERVER (host:port of this pod's segment server),
//! ISC_PASSWORD (the requests to it are signed with it), FIREBIRD_PORT.

use crate::client::{self, var};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde_json::{json, Value};
use std::fs;
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Online, shut down, fenced by this holder, or isolated (fenced by the isolation check: the
/// operator lifts that). None: not answering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Database {
    Online,
    Shutdown,
    Fenced,
    Isolated,
}

/// The Lease as it was read: its holder, and the whole object, which a renewal writes back.
#[derive(Debug, Clone, PartialEq)]
pub struct Lease {
    pub holder: String,
    pub object: Value,
}

/// What the holder looks at and acts on.
pub trait World {
    fn is_primary(&self) -> bool;
    fn database(&self) -> Option<Database>;
    /// The Lease, or None when it does not exist
    fn read(&self) -> Result<Option<Lease>, String>;
    /// Renews the Lease that was read, for this holder, with the version that was read: a write
    /// that landed meanwhile (the operator moving it) makes it fail
    fn renew(&self, lease: &Lease, holder: &str, duration: u64) -> Result<(), String>;
    fn fence(&self, reason: &str) -> Result<(), String>;
    /// Brings a fenced database back online (the segment server's REJOIN)
    fn rejoin(&self) -> Result<(), String>;
    /// Milliseconds
    fn now(&self) -> u64;
    fn log(&self, message: &str);
}

#[derive(Default)]
struct Held {
    /// When the last successful renewal was sent, or this pod started holding the Lease; None
    /// while not holding
    last_renewed: Option<u64>,
    /// Counts the deadline's fences: a renewal sent before one does not count as holding again
    fences: u64,
    last_action: String,
}

/// One holder: its ticks decide what to do from the pod's role, the database's state and the
/// Lease. The deadline's check runs beside them, from another thread.
pub struct Holder<W: World> {
    world: W,
    name: String,
    duration: u64,
    held: Mutex<Held>,
}

pub fn renew_interval(duration: u64) -> u64 {
    (duration / 3).max(1)
}

/// How long the primary may go without a successful renewal before it fences: two thirds of the
/// duration (10 of 15 s, as CloudNativePG's renewDeadline), so the fence comes before the Lease
/// expires and the operator may promote a replica. Always above the renew interval.
pub fn renew_deadline(duration: u64) -> u64 {
    (renew_interval(duration) + 1).max(duration * 2 / 3)
}

/// Milliseconds to whole seconds, rounded as lease-holder.ts does.
fn seconds(milliseconds: u64) -> u64 {
    (milliseconds + 500) / 1000
}

impl<W: World> Holder<W> {
    pub fn new(world: W, name: &str, duration: u64) -> Holder<W> {
        Holder {
            world,
            name: name.to_string(),
            duration,
            held: Mutex::new(Held::default()),
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Held> {
        self.held.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Logged when it differs from the last one.
    fn say(&self, action: impl Into<String>) -> String {
        let action = action.into();
        let changed = {
            let mut held = self.held();
            let changed = held.last_action != action;
            if changed {
                held.last_action = action.clone();
            }
            changed
        };
        if changed {
            self.world.log(&action);
        }
        action
    }

    fn fence(&self, reason: &str) {
        match self.world.fence(reason) {
            Ok(()) => self.held().last_renewed = None,
            // still counted from the last renewal: the deadline's check tries again within a second
            Err(why) => self.world.log(&format!("cannot fence the database ({why}); retrying")),
        }
    }

    /// A read or a renewal failed while holding: fenced at the deadline, counted down before it.
    fn failed(&self, what: &str, why: &str, fence_reason: impl Fn(u64) -> String) -> String {
        let Some(last) = self.held().last_renewed else {
            return self.say("fenced: the Lease could not be renewed");
        };
        let since = seconds(self.world.now().saturating_sub(last));
        let deadline = renew_deadline(self.duration);
        if since >= deadline {
            self.fence(&fence_reason(since));
            return self.say("fenced: the Lease could not be renewed");
        }
        self.say(format!("cannot {what} the Lease ({why}): fencing in {}s", deadline - since))
    }

    pub fn tick(&self) -> String {
        // a fence by the deadline's check while this tick waits makes its renewal not count
        let fences = self.held().fences;
        if !self.world.is_primary() {
            self.held().last_renewed = None;
            return self.say("not the primary: not holding the Lease");
        }
        let state = self.world.database();
        if state == Some(Database::Fenced) {
            // fenced by this holder: back online once this pod holds the Lease again, which the
            // operator leaves to it unless a failover took the Lease over
            self.held().last_renewed = None;
            let lease = match self.world.read() {
                Ok(lease) => lease,
                Err(why) => return self.say(format!("fenced; cannot read the Lease ({why})")),
            };
            let lease = match lease {
                Some(lease) if lease.holder.is_empty() || lease.holder == self.name => lease,
                Some(lease) => return self.say(format!("fenced; the Lease is held by {}", lease.holder)),
                None => return self.say("fenced; the Lease is held by nobody"),
            };
            let sent = self.world.now();
            if let Err(why) = self.world.renew(&lease, &self.name, self.duration) {
                return self.say(format!("fenced; cannot re-acquire the Lease ({why})"));
            }
            if let Err(why) = self.world.rejoin() {
                return self.say(format!(
                    "re-acquired the Lease; cannot bring the database back online ({why}); retrying"
                ));
            }
            self.held().last_renewed = Some(sent);
            return self.say("re-acquired the Lease: database back online");
        }
        if state != Some(Database::Online) {
            // down (shut down by a Job, or not answering yet): the Lease expires on its own
            self.held().last_renewed = None;
            let state = match state {
                Some(Database::Shutdown) => "shutdown",
                Some(Database::Isolated) => "isolated",
                _ => "not answering",
            };
            return self.say(format!("database {state}: not renewing the Lease"));
        }
        {
            let now = self.world.now();
            self.held().last_renewed.get_or_insert(now);
        }
        let lease = match self.world.read() {
            Ok(lease) => lease,
            Err(why) => {
                return self.failed("read", &why, |since| {
                    format!("cannot renew the Lease: the API server has not answered for {since}s")
                })
            }
        };
        let Some(lease) = lease else {
            return self.say("the Lease does not exist yet");
        };
        if !lease.holder.is_empty() && lease.holder != self.name {
            // the operator moved the Lease: a replica is being promoted, this database must not take writes
            self.fence(&format!("the Lease is held by {}", lease.holder));
            return self.say(format!("fenced: the Lease is held by {}", lease.holder));
        }
        // the renewal writes renewTime as it is sent: the deadline counts from there
        let sent = self.world.now();
        match self.world.renew(&lease, &self.name, self.duration) {
            Ok(()) => {
                let mut held = self.held();
                // the deadline's check fenced the database while this tick was on its way: the
                // next tick finds it fenced and brings it back with the Lease
                if held.fences != fences {
                    drop(held);
                    return self.say("fenced: the Lease could not be renewed in time");
                }
                held.last_renewed = Some(sent);
                drop(held);
                self.say(format!(
                    "holding the Lease {}as the primary, renewed every {}s",
                    if lease.holder == self.name { "" } else { "(acquired) " },
                    renew_interval(self.duration)
                ))
            }
            Err(why) => self.failed("renew", &why, |since| format!("cannot renew the Lease: {why} (for {since}s)")),
        }
    }

    /// The renew deadline, checked every second beside the ticks: a tick waiting on a slow API
    /// server (or the segment server) must not hold the fence past it. Fences once no renewal
    /// succeeded for the deadline while holding the Lease; what it did, if anything.
    pub fn check_deadline(&self) -> Option<String> {
        let since = {
            let mut held = self.held();
            let since = seconds(self.world.now().saturating_sub(held.last_renewed?));
            if since < renew_deadline(self.duration) {
                return None;
            }
            held.fences += 1;
            since
        };
        self.fence(&format!(
            "cannot renew the Lease: no renewal succeeded for {since}s (renew deadline)"
        ));
        Some(self.say("fenced: the Lease could not be renewed"))
    }
}

/// How long an API request may take: a primary cut off from the API server must notice within
/// the renew deadline.
const API_TIMEOUT: Duration = Duration::from_secs(5);

/// The Lease API through the pod's projected ServiceAccount token.
#[derive(Clone)]
struct Api {
    host: String,
    port: u16,
    /// TOKEN_DIR: token (read again for every request: it rotates) and ca.crt
    directory: String,
    path: String,
}

/// "2026-11-16T06:50:44.123456Z" (a Kubernetes MicroTime) for a time since the epoch.
fn micro_time(since_epoch: Duration) -> String {
    let (days, rest) = (since_epoch.as_secs() / 86_400, since_epoch.as_secs() % 86_400);
    // the civil date of a day count (Howard Hinnant's algorithm)
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 { shifted_month + 3 } else { shifted_month - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        since_epoch.subsec_micros()
    )
}

/// The body of an HTTP/1.1 response read to its end, with its status.
fn response(raw: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let end = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or("no HTTP response")?;
    let head = String::from_utf8_lossy(&raw[..end]);
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or("no HTTP status")?;
    let chunked = lines.any(|line| {
        line.split_once(':')
            .is_some_and(|(name, value)| name.eq_ignore_ascii_case("transfer-encoding") && value.to_ascii_lowercase().contains("chunked"))
    });
    let mut rest = &raw[end + 4..];
    if !chunked {
        return Ok((status, rest.to_vec()));
    }
    let mut body = Vec::new();
    loop {
        let line_end = rest.windows(2).position(|w| w == b"\r\n").ok_or("a chunk without its size")?;
        let size = String::from_utf8_lossy(&rest[..line_end]);
        let size = usize::from_str_radix(size.split(';').next().unwrap_or("").trim(), 16).map_err(|_| "a chunk without its size")?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Ok((status, body));
        }
        if rest.len() < size + 2 {
            return Err("a chunk cut short".into());
        }
        body.extend(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

/// A connection to the first address that accepts one, each in turn: a name with an IPv6 and
/// an IPv4 address ("localhost", a dual-stack Service) may be served on one of them only.
fn connect_any(host: &str, addresses: impl Iterator<Item = std::net::SocketAddr>) -> Result<TcpStream, String> {
    let mut refused = format!("{host}: no address");
    for address in addresses {
        match TcpStream::connect_timeout(&address, API_TIMEOUT) {
            Ok(socket) => return Ok(socket),
            Err(e) => refused = format!("connect {address}: {e}"),
        }
    }
    Err(refused)
}

impl Api {
    /// One request on its own connection, verified against the API server's CA.
    fn request(&self, method: &str, body: Option<&str>) -> Result<(u16, Vec<u8>), String> {
        let directory = Path::new(&self.directory);
        let token = fs::read_to_string(directory.join("token")).map_err(|e| format!("the ServiceAccount token: {e}"))?;
        let mut roots = RootCertStore::empty();
        for certificate in CertificateDer::pem_file_iter(directory.join("ca.crt")).map_err(|e| format!("the API server's CA: {e}"))? {
            roots
                .add(certificate.map_err(|e| format!("the API server's CA: {e}"))?)
                .map_err(|e| format!("the API server's CA: {e}"))?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = ServerName::try_from(self.host.clone()).map_err(|e| format!("{}: {e}", self.host))?;
        let session = ClientConnection::new(Arc::new(config), name).map_err(|e| e.to_string())?;
        let addresses = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| format!("{}: {e}", self.host))?;
        let socket = connect_any(&self.host, addresses)?;
        let _ = socket.set_nodelay(true);
        socket.set_read_timeout(Some(API_TIMEOUT)).map_err(|e| e.to_string())?;
        socket.set_write_timeout(Some(API_TIMEOUT)).map_err(|e| e.to_string())?;
        let mut stream = StreamOwned::new(session, socket);
        let host = if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        };
        let mut request = format!(
            "{method} {} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {}\r\nAccept: application/json\r\nUser-Agent: fb-agent/{}\r\nConnection: close\r\n",
            self.path,
            token.trim(),
            env!("CARGO_PKG_VERSION")
        );
        if let Some(body) = body {
            request.push_str(&format!("Content-Type: application/json\r\nContent-Length: {}\r\n", body.len()));
        }
        request.push_str("\r\n");
        request.push_str(body.unwrap_or(""));
        stream.write_all(request.as_bytes()).map_err(|e| e.to_string())?;
        stream.flush().map_err(|e| e.to_string())?;
        let mut raw = Vec::new();
        match stream.read_to_end(&mut raw) {
            Ok(_) => {}
            // closed without TLS's own end, after the whole response
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && !raw.is_empty() => {}
            Err(e) => return Err(e.to_string()),
        }
        response(&raw)
    }

    /// The request within the API timeout as a whole, in its own thread: a server that answers a
    /// byte now and then must not hold the holder.
    fn within(&self, what: &'static str, method: &'static str, body: Option<String>) -> Result<(u16, Vec<u8>), String> {
        let (done, answer) = mpsc::channel();
        let api = self.clone();
        thread::spawn(move || {
            let _ = done.send(api.request(method, body.as_deref()));
        });
        answer
            .recv_timeout(API_TIMEOUT)
            .unwrap_or_else(|_| Err(format!("{what} timed out after {}s", API_TIMEOUT.as_secs())))
    }

    /// The API server's own message for a refused request.
    fn refused(status: u16, body: &[u8]) -> String {
        let message = serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|status| status.get("message").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_default();
        if message.is_empty() {
            format!("HTTP {status}")
        } else {
            format!("HTTP {status}: {message}")
        }
    }

    fn read(&self) -> Result<Option<Lease>, String> {
        let (status, body) = self.within("reading the Lease", "GET", None)?;
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            return Err(Api::refused(status, &body));
        }
        let object: Value = serde_json::from_slice(&body).map_err(|e| format!("the Lease: {e}"))?;
        let holder = object
            .pointer("/spec/holderIdentity")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        Ok(Some(Lease { holder, object }))
    }

    fn renew(&self, lease: &Lease, holder: &str, duration: u64) -> Result<(), String> {
        let body = renewed(
            &lease.object,
            holder,
            duration,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default(),
        );
        // with the resourceVersion that was read: a conflict (409) means the operator moved it meanwhile
        let (status, answer) = self.within("renewing the Lease", "PUT", Some(body.to_string()))?;
        if !(200..300).contains(&status) {
            return Err(Api::refused(status, &answer));
        }
        Ok(())
    }
}

/// The Lease that was read, renewed for `holder`: everything else as it was.
fn renewed(object: &Value, holder: &str, duration: u64, at: Duration) -> Value {
    let mut object = object.clone();
    if !object.get("spec").is_some_and(Value::is_object) {
        object["spec"] = json!({});
    }
    let spec = &mut object["spec"];
    spec["holderIdentity"] = json!(holder);
    spec["leaseDurationSeconds"] = json!(duration);
    spec["renewTime"] = json!(micro_time(at));
    object
}

/// Whether this pod is the primary: promoted in place (the marker), or named by the ConfigMap.
fn is_primary(replication_directory: &str, primary_file: &str, pod: &str) -> bool {
    if !replication_directory.is_empty() && Path::new(replication_directory).join("promoted").exists() {
        return true;
    }
    let Ok(primary) = fs::read_to_string(primary_file) else {
        return false;
    };
    let primary = primary.trim();
    primary.is_empty() || primary == pod || primary.strip_prefix(pod).is_some_and(|rest| rest.starts_with('.'))
}

/// The reason of a fence as the segment server takes it: `[\w .:,()/-]`, 200 characters at most.
fn plain_reason(reason: &str) -> String {
    reason
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '.' | ':' | ',' | '(' | ')' | '/' | '-') {
                c
            } else {
                ' '
            }
        })
        .take(200)
        .collect()
}

/// The pod, its segment server and the Lease API.
struct Pod {
    api: Api,
    /// SEGMENT_SERVER: this pod's segment server
    server: (String, u16),
    started: Instant,
}

impl Pod {
    /// The first line of the segment server's reply.
    fn ask(&self, request: &str) -> Result<String, String> {
        let mut reply =
            client::open_at(&self.server.0, self.server.1, request, Some(Duration::from_secs(10))).map_err(|e| e.to_string())?;
        let mut line = String::new();
        reply.read_line(&mut line).map_err(|e| e.to_string())?;
        if line.is_empty() {
            return Err("no reply".into());
        }
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }
}

impl World for Pod {
    fn is_primary(&self) -> bool {
        is_primary(&var("REPLICATION_DIR"), &var("PRIMARY_FILE"), &var("POD_NAME"))
    }

    fn database(&self) -> Option<Database> {
        // online, shut down by something else (a Job), by this holder's fence ("fenced lease"),
        // or by the isolation check ("fenced": the operator lifts that once it reaches this primary)
        match self.ask("STATE").ok()?.as_str() {
            "OK online" => Some(Database::Online),
            "OK shutdown" => Some(Database::Shutdown),
            "OK fenced lease" => Some(Database::Fenced),
            "OK fenced" => Some(Database::Isolated),
            _ => None,
        }
    }

    fn read(&self) -> Result<Option<Lease>, String> {
        self.api.read()
    }

    fn renew(&self, lease: &Lease, holder: &str, duration: u64) -> Result<(), String> {
        self.api.renew(lease, holder, duration)
    }

    fn fence(&self, reason: &str) -> Result<(), String> {
        let reply = self.ask(&format!("FENCE {}", plain_reason(reason)))?;
        if reply.starts_with("OK") {
            Ok(())
        } else {
            Err(reply)
        }
    }

    fn rejoin(&self) -> Result<(), String> {
        let reply = self.ask("REJOIN")?;
        if reply == "OK" {
            Ok(())
        } else {
            Err(reply)
        }
    }

    fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn log(&self, message: &str) {
        println!("lease holder: {message}");
    }
}

pub fn run() -> io::Result<()> {
    // SIGTERM reaches this process only (the container's PID 1), and is caught from the start, so
    // one that comes as it starts is not fatal: the holder keeps working until
    // the pod's Firebird server has stopped, then exits
    let stopping = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(signal, Arc::clone(&stopping))?;
    }
    let (pod, lease) = (var("POD_NAME"), var("LEASE_NAME"));
    if pod.is_empty() || lease.is_empty() {
        return Err(io::Error::other("lease holder: POD_NAME and LEASE_NAME are required"));
    }
    let namespace = Some(var("POD_NAMESPACE"))
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "default".into());
    let duration = var("LEASE_DURATION_SECONDS").parse::<u64>().ok().filter(|d| *d > 0).unwrap_or(15);
    let server = Some(var("SEGMENT_SERVER"))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:3051".into());
    let (server_host, server_port) = server.rsplit_once(':').unwrap_or((&server, "3051"));
    let world = Pod {
        api: Api {
            host: var("KUBERNETES_SERVICE_HOST"),
            port: var("KUBERNETES_SERVICE_PORT").parse().unwrap_or(443),
            directory: Some(var("TOKEN_DIR"))
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| "/var/run/primary-lease".into()),
            path: format!("/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases/{lease}"),
        },
        server: (server_host.to_string(), server_port.parse().unwrap_or(3051)),
        started: Instant::now(),
    };
    let holder = Arc::new(Holder::new(world, &pod, duration));
    println!(
        "lease holder: {pod} watching Lease {namespace}/{lease} (duration {duration}s, renew deadline {}s)",
        renew_deadline(duration)
    );

    let deadline = Arc::clone(&holder);
    thread::spawn(move || {
        let mut said = false;
        loop {
            thread::sleep(Duration::from_secs(1));
            deadline.check_deadline();
            if stopping.load(Ordering::Relaxed) {
                if !said {
                    said = true;
                    println!("lease holder: stopping once the Firebird server has stopped");
                }
                if crate::puller::firebird_stopped() {
                    println!("lease holder: the Firebird server has stopped, exiting");
                    process::exit(0);
                }
            }
        }
    });
    // a tick every interval from the start of the last one, so the time a tick takes does not
    // stretch the renewals towards the deadline
    loop {
        let started = Instant::now();
        holder.tick();
        thread::sleep(Duration::from_secs(renew_interval(duration)).saturating_sub(started.elapsed()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    /// What the tests set and read: the pod, its database, the Lease, and what was asked.
    struct Fake {
        primary: Cell<bool>,
        database: Cell<Option<Database>>,
        /// Err: the API server does not answer
        lease: RefCell<Result<Option<Lease>, String>>,
        /// the next renewals: an error, or the seconds one takes to answer
        renewals: RefCell<Vec<Result<u64, String>>>,
        fences_failing: Cell<u32>,
        rejoins_failing: Cell<u32>,
        now: Cell<u64>,
        renewed: RefCell<Vec<Lease>>,
        fenced: RefCell<Vec<String>>,
        rejoined: Cell<u32>,
        log: RefCell<Vec<String>>,
        /// run inside the next renewal (the deadline's check, from the other thread)
        during_renewal: RefCell<Option<Box<dyn FnOnce()>>>,
    }

    fn lease(holder: &str) -> Lease {
        let object = if holder.is_empty() {
            json!({ "spec": {} })
        } else {
            json!({ "spec": { "holderIdentity": holder } })
        };
        Lease {
            holder: holder.into(),
            object,
        }
    }

    impl World for &Fake {
        fn is_primary(&self) -> bool {
            self.primary.get()
        }
        fn database(&self) -> Option<Database> {
            self.database.get()
        }
        fn read(&self) -> Result<Option<Lease>, String> {
            self.lease.borrow().clone()
        }
        fn renew(&self, lease: &Lease, _holder: &str, _duration: u64) -> Result<(), String> {
            if let Some(check) = self.during_renewal.borrow_mut().take() {
                check();
            }
            let next = if self.renewals.borrow().is_empty() {
                Ok(0)
            } else {
                self.renewals.borrow_mut().remove(0)
            };
            self.now.set(self.now.get() + next? * 1000);
            self.renewed.borrow_mut().push(lease.clone());
            Ok(())
        }
        fn fence(&self, reason: &str) -> Result<(), String> {
            self.fenced.borrow_mut().push(reason.into());
            if self.fences_failing.get() > 0 {
                self.fences_failing.set(self.fences_failing.get() - 1);
                return Err("no reply".into());
            }
            Ok(())
        }
        fn rejoin(&self) -> Result<(), String> {
            self.rejoined.set(self.rejoined.get() + 1);
            if self.rejoins_failing.get() > 0 {
                self.rejoins_failing.set(self.rejoins_failing.get() - 1);
                return Err("ERR cannot bring online".into());
            }
            Ok(())
        }
        fn now(&self) -> u64 {
            self.now.get()
        }
        fn log(&self, message: &str) {
            self.log.borrow_mut().push(message.into());
        }
    }

    fn fake(database: Option<Database>, held_by: &str) -> Fake {
        Fake {
            primary: Cell::new(true),
            database: Cell::new(database),
            lease: RefCell::new(Ok(Some(lease(held_by)))),
            renewals: RefCell::new(Vec::new()),
            fences_failing: Cell::new(0),
            rejoins_failing: Cell::new(0),
            now: Cell::new(1_000_000),
            renewed: RefCell::new(Vec::new()),
            fenced: RefCell::new(Vec::new()),
            rejoined: Cell::new(0),
            log: RefCell::new(Vec::new()),
            during_renewal: RefCell::new(None),
        }
    }

    fn holder(world: &Fake) -> Holder<&Fake> {
        Holder::new(world, "db-0", 15)
    }

    fn advance(world: &Fake, seconds: u64) {
        world.now.set(world.now.get() + seconds * 1000);
    }

    #[test]
    fn renews_while_the_pod_is_the_primary_and_its_database_online() {
        let world = fake(Some(Database::Online), "db-0");
        let h = holder(&world);
        assert_eq!(h.tick(), "holding the Lease as the primary, renewed every 5s");
        assert_eq!(world.renewed.borrow().as_slice(), &[lease("db-0")]);
        world.database.set(Some(Database::Shutdown));
        assert_eq!(h.tick(), "database shutdown: not renewing the Lease");
        world.database.set(None);
        assert_eq!(h.tick(), "database not answering: not renewing the Lease");
        world.database.set(Some(Database::Online));
        world.primary.set(false);
        assert_eq!(h.tick(), "not the primary: not holding the Lease");
        assert_eq!(world.renewed.borrow().len(), 1);
        assert!(world.fenced.borrow().is_empty());
        // an empty Lease (nobody holds it) is acquired
        world.primary.set(true);
        *world.lease.borrow_mut() = Ok(Some(lease("")));
        assert_eq!(h.tick(), "holding the Lease (acquired) as the primary, renewed every 5s");
        assert_eq!(world.renewed.borrow().len(), 2);
        // a Lease that does not exist is the operator's to create
        *world.lease.borrow_mut() = Ok(None);
        assert_eq!(h.tick(), "the Lease does not exist yet");
        // each action is logged once
        assert_eq!(world.log.borrow().len(), 6);
        h.tick();
        assert_eq!(world.log.borrow().len(), 6);
        assert_eq!((renew_interval(15), renew_interval(5), renew_interval(2)), (5, 1, 1));
    }

    #[test]
    fn fences_when_the_lease_names_another_instance() {
        let world = fake(Some(Database::Online), "db-2");
        let h = holder(&world);
        assert_eq!(h.tick(), "fenced: the Lease is held by db-2");
        assert_eq!(world.fenced.borrow().as_slice(), &["the Lease is held by db-2"]);
        assert!(world.renewed.borrow().is_empty());
        // fenced: nothing more while another instance holds the Lease
        world.database.set(Some(Database::Fenced));
        assert_eq!(h.tick(), "fenced; the Lease is held by db-2");
        assert_eq!(world.fenced.borrow().len(), 1);
        assert_eq!(world.rejoined.get(), 0);
        *world.lease.borrow_mut() = Ok(None);
        assert_eq!(h.tick(), "fenced; the Lease is held by nobody");
    }

    #[test]
    fn reacquires_its_lease_and_brings_a_fenced_database_back() {
        let world = fake(Some(Database::Fenced), "db-0");
        let h = holder(&world);
        assert_eq!(h.tick(), "re-acquired the Lease: database back online");
        assert_eq!(world.renewed.borrow().as_slice(), &[lease("db-0")]);
        assert_eq!(world.rejoined.get(), 1);
        world.database.set(Some(Database::Online));
        assert!(h.tick().starts_with("holding the Lease"));
        // the API server still unreachable: it stays fenced
        let cut = fake(Some(Database::Fenced), "db-0");
        *cut.lease.borrow_mut() = Err("timed out".into());
        assert_eq!(holder(&cut).tick(), "fenced; cannot read the Lease (timed out)");
        assert_eq!(cut.rejoined.get(), 0);
        // the operator took the Lease over between the read and the renewal (a conflict)
        let taken = fake(Some(Database::Fenced), "db-0");
        taken.renewals.borrow_mut().push(Err("409 conflict".into()));
        assert_eq!(holder(&taken).tick(), "fenced; cannot re-acquire the Lease (409 conflict)");
        assert_eq!(taken.rejoined.get(), 0);
        // an expired Lease nobody holds is acquired too
        let free = fake(Some(Database::Fenced), "");
        assert_eq!(holder(&free).tick(), "re-acquired the Lease: database back online");
        // a database shut down for another reason (a Job) is left alone
        let job = fake(Some(Database::Shutdown), "db-0");
        assert_eq!(holder(&job).tick(), "database shutdown: not renewing the Lease");
        assert_eq!(job.rejoined.get(), 0);
        // one the isolation check fenced is the operator's to lift
        let isolated = fake(Some(Database::Isolated), "db-0");
        assert_eq!(holder(&isolated).tick(), "database isolated: not renewing the Lease");
        assert_eq!(isolated.rejoined.get(), 0);
        assert!(isolated.renewed.borrow().is_empty());
        // a failed rejoin is retried on the next tick
        let retried = fake(Some(Database::Fenced), "db-0");
        retried.rejoins_failing.set(1);
        let h = holder(&retried);
        assert_eq!(
            h.tick(),
            "re-acquired the Lease; cannot bring the database back online (ERR cannot bring online); retrying"
        );
        assert_eq!(h.tick(), "re-acquired the Lease: database back online");
    }

    #[test]
    fn fences_once_the_lease_could_not_be_renewed_for_the_deadline() {
        assert_eq!((renew_deadline(15), renew_deadline(5), renew_deadline(120)), (10, 3, 80));
        for duration in [5, 6, 7, 15, 30, 120] {
            assert!(renew_deadline(duration) > renew_interval(duration) && renew_deadline(duration) < duration);
        }
        let world = fake(Some(Database::Online), "db-0");
        let h = holder(&world);
        h.tick();
        *world.lease.borrow_mut() = Err("connect ETIMEDOUT".into());
        advance(&world, 5);
        assert_eq!(h.tick(), "cannot read the Lease (connect ETIMEDOUT): fencing in 5s");
        assert!(world.fenced.borrow().is_empty());
        advance(&world, 5);
        assert_eq!(h.tick(), "fenced: the Lease could not be renewed");
        assert_eq!(
            world.fenced.borrow().as_slice(),
            &["cannot renew the Lease: the API server has not answered for 10s"]
        );
        // a failing renewal counts the same way
        let refused = fake(Some(Database::Online), "db-0");
        refused
            .renewals
            .borrow_mut()
            .extend([Err("403".to_string()), Err("403".to_string())]);
        let h = holder(&refused);
        assert_eq!(h.tick(), "cannot renew the Lease (403): fencing in 10s");
        advance(&refused, 10);
        assert_eq!(h.tick(), "fenced: the Lease could not be renewed");
        assert_eq!(refused.fenced.borrow().as_slice(), &["cannot renew the Lease: 403 (for 10s)"]);
        // a fence that fails is retried on the next tick
        let failing = fake(Some(Database::Online), "db-1");
        failing.fences_failing.set(1);
        let h = holder(&failing);
        h.tick();
        assert!(failing
            .log
            .borrow()
            .contains(&"cannot fence the database (no reply); retrying".to_string()));
        h.tick();
        assert_eq!(failing.fenced.borrow().len(), 2);
    }

    #[test]
    fn counts_the_deadline_from_when_the_last_renewal_was_sent() {
        let world = fake(Some(Database::Online), "db-0");
        // a renewal that takes 4s to answer: its renewTime is when it was sent
        world.renewals.borrow_mut().push(Ok(4));
        let h = holder(&world);
        h.tick();
        *world.lease.borrow_mut() = Err("connect ETIMEDOUT".into());
        advance(&world, 2);
        assert_eq!(h.tick(), "cannot read the Lease (connect ETIMEDOUT): fencing in 4s");
    }

    #[test]
    fn enforces_the_deadline_whatever_a_tick_is_waiting_for() {
        let world = fake(Some(Database::Online), "db-0");
        let h = holder(&world);
        assert_eq!(h.check_deadline(), None);
        h.tick();
        advance(&world, 9);
        assert_eq!(h.check_deadline(), None);
        // a tick stuck on a renewal: the deadline passes and its check fences meanwhile, as
        // from the other thread
        let checked = std::rc::Rc::new(RefCell::new(None));
        {
            let (world_at, holder_at, checked) = (&world as *const Fake as usize, &h as *const Holder<&Fake> as usize, checked.clone());
            *world.during_renewal.borrow_mut() = Some(Box::new(move || {
                // SAFETY: the world and the holder live until the end of the test, on this thread
                let (world, h) = unsafe { (&*(world_at as *const Fake), &*(holder_at as *const Holder<&Fake>)) };
                advance(world, 1);
                *checked.borrow_mut() = Some(h.check_deadline());
            }));
        }
        // the renewal answers after the fence: it does not count
        assert_eq!(h.tick(), "fenced: the Lease could not be renewed in time");
        assert_eq!(
            checked.borrow().clone(),
            Some(Some("fenced: the Lease could not be renewed".to_string()))
        );
        assert_eq!(
            world.fenced.borrow().as_slice(),
            &["cannot renew the Lease: no renewal succeeded for 10s (renew deadline)"]
        );
        // nothing more until the Lease is held again
        advance(&world, 1);
        assert_eq!(h.check_deadline(), None);
        assert_eq!(world.fenced.borrow().len(), 1);
        // the next tick brings the database back
        world.database.set(Some(Database::Fenced));
        assert_eq!(h.tick(), "re-acquired the Lease: database back online");
        assert_eq!(world.rejoined.get(), 1);
        // a fence that fails is retried at the next check
        let failing = fake(Some(Database::Online), "db-0");
        let h = holder(&failing);
        h.tick();
        failing.fences_failing.set(1);
        advance(&failing, 10);
        h.check_deadline();
        assert!(failing
            .log
            .borrow()
            .contains(&"cannot fence the database (no reply); retrying".to_string()));
        advance(&failing, 1);
        assert_eq!(h.check_deadline(), Some("fenced: the Lease could not be renewed".to_string()));
        assert_eq!(failing.fenced.borrow().len(), 2);
        // never while not holding the Lease: a replica, or a database down
        let replica = fake(Some(Database::Online), "db-0");
        replica.primary.set(false);
        let h = holder(&replica);
        h.tick();
        advance(&replica, 60);
        assert_eq!(h.check_deadline(), None);
        let down = fake(Some(Database::Shutdown), "db-0");
        let h = holder(&down);
        h.tick();
        advance(&down, 60);
        assert_eq!(h.check_deadline(), None);
        assert!(replica.fenced.borrow().is_empty() && down.fenced.borrow().is_empty());
    }

    #[test]
    fn reads_the_pods_role_from_the_configmap_file_or_the_promoted_marker() {
        let dir = std::env::temp_dir().join(format!("fb-agent-lease-{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (file, repl) = (dir.join("primary"), dir.join("repl"));
        let role = || is_primary(&repl.to_string_lossy(), &file.to_string_lossy(), "db-1");
        assert!(!role());
        fs::write(&file, "db-0.db-headless\n").unwrap();
        assert!(!role());
        fs::write(&file, "db-1.db-headless\n").unwrap();
        assert!(role());
        fs::write(&file, "db-10.db-headless\n").unwrap();
        assert!(!role());
        fs::write(&file, "db-1\n").unwrap();
        assert!(role());
        fs::write(&file, "\n").unwrap();
        assert!(role());
        fs::write(&file, "db-0.db-headless\n").unwrap();
        fs::create_dir_all(&repl).unwrap();
        fs::write(repl.join("promoted"), "1\n").unwrap();
        assert!(role());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn writes_the_lease_back_whole_with_its_renewal() {
        let read = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": "db-lease", "resourceVersion": "42", "labels": { "a": "b" }, "ownerReferences": [{ "uid": "u" }] },
            "spec": { "holderIdentity": "db-0", "leaseTransitions": 3, "acquireTime": "2026-01-01T00:00:00.000000Z" }
        });
        let at = Duration::new(1_794_811_844, 123_456_000);
        let written = renewed(&read, "db-0", 15, at);
        assert_eq!(written["metadata"], read["metadata"]);
        assert_eq!(written["spec"]["leaseTransitions"], 3);
        assert_eq!(written["spec"]["acquireTime"], "2026-01-01T00:00:00.000000Z");
        assert_eq!(written["spec"]["holderIdentity"], "db-0");
        assert_eq!(written["spec"]["leaseDurationSeconds"], 15);
        assert_eq!(written["spec"]["renewTime"], "2026-11-16T06:50:44.123456Z");
        // a Lease without a spec gets one
        assert_eq!(renewed(&json!({ "metadata": {} }), "db-1", 5, at)["spec"]["holderIdentity"], "db-1");
        // dates: the epoch, a leap day, the end of a year
        assert_eq!(micro_time(Duration::ZERO), "1970-01-01T00:00:00.000000Z");
        assert_eq!(micro_time(Duration::from_secs(1_709_164_800)), "2024-02-29T00:00:00.000000Z");
        assert_eq!(micro_time(Duration::from_secs(1_767_225_599)), "2025-12-31T23:59:59.000000Z");
    }

    #[test]
    fn connects_to_the_address_that_answers() {
        use std::net::{SocketAddr, TcpListener};
        // a server on the IPv4 loopback only, under a name that also has the IPv6 one, first
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let both = [
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], port)),
            SocketAddr::from(([127, 0, 0, 1], port)),
        ];
        let socket = connect_any("localhost", both.into_iter()).unwrap();
        assert_eq!(socket.peer_addr().unwrap(), both[1]);
        // nobody anywhere: the last refusal, with its address
        drop(listener);
        let refused = connect_any("localhost", both.into_iter()).unwrap_err();
        assert!(refused.starts_with(&format!("connect 127.0.0.1:{port}: ")), "{refused}");
        assert_eq!(connect_any("nowhere", std::iter::empty()).unwrap_err(), "nowhere: no address");
    }

    #[test]
    fn reads_http_responses_and_cleans_the_reason_of_a_fence() {
        let plain = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(response(plain), Ok((200, b"{}".to_vec())));
        let chunked = b"HTTP/1.1 409 Conflict\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{\"a\"\r\n3;x=y\r\n:1}\r\n0\r\n\r\n";
        assert_eq!(response(chunked), Ok((409, b"{\"a\":1}".to_vec())));
        assert!(response(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n9\r\nshort").is_err());
        assert!(response(b"nothing").is_err());
        assert_eq!(
            Api::refused(409, br#"{"message":"the object has been modified"}"#),
            "HTTP 409: the object has been modified"
        );
        assert_eq!(Api::refused(503, b"upstream"), "HTTP 503");
        assert_eq!(
            plain_reason("cannot renew the Lease: HTTP 409: \"x\" (for 10s)"),
            "cannot renew the Lease: HTTP 409:  x  (for 10s)"
        );
        assert_eq!(plain_reason(&"a".repeat(300)).len(), 200);
    }
}
