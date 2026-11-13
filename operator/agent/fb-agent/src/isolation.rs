//! The primary's isolation check (docs/rust-agent-segment-server.md, step 5), with the rules of
//! isolation-check.pl. Every CHECK_INTERVAL seconds, while this instance is the primary:
//!
//! - It looks for the Kubernetes API server (a TCP connection) or the segment server of another
//!   instance (the addresses of the headless Service, other than this pod's). When neither has
//!   been reachable for ISOLATION_TIMEOUT_SECONDS, the primary is cut off from the rest of the
//!   cluster, where the operator may be promoting a replica: the database is put into full
//!   shutdown, and the marker file records it.
//! - A primary that neither the operator nor any replica has reached for CONTACT_TIMEOUT_SECONDS
//!   (the segment server records each authenticated request) is cut off on the other side of a
//!   partition. It fences itself too, when the headless Service lists other instances.
//! - The other instances are found through cluster DNS, which can fail as well. The lookup has a
//!   time limit, and every answer is remembered. When it fails, the check goes by the addresses
//!   it knew and those the operator publishes, and by the operator's list of ready replicas: a
//!   primary nothing has reached is then fenced only when none of the known addresses answers.
//!
//! The database stays shut down until the operator asks the segment server to bring it online
//! again (REJOIN); once it is online, the marker is removed and the check starts over. Without
//! peers or an API server address to check, nothing is ever fenced.
//!
//! The local server is asked over the agent's own service attachments. When that is refused at
//! once (or the server cannot be reached that way), fbsvcmgr is run instead, under timeout(1) as
//! the Perl check runs it: the fence must not depend on one client alone.

use crate::client::var;
use crate::sql::{Database, Failed};
use crate::state::{Probe, State};
use crate::status::slurp;
use std::collections::BTreeSet;
use std::ffi::CString;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn seconds(name: &str, default: u64) -> u64 {
    var(name).parse().unwrap_or(default)
}

/// The check's settings, from the environment isolation-check.pl reads.
pub struct Settings {
    pub database: Database,
    pub primary_file: PathBuf,
    /// REPLICATION_DIR/promoted: promoted in place before the ConfigMap names this instance
    pub promoted: Option<PathBuf>,
    pub marker: PathBuf,
    pub timeout: u64,
    pub contact_timeout: u64,
    pub contact_file: Option<PathBuf>,
    pub interval: u64,
    pub pod: String,
    pub pod_ip: String,
    pub peers: String,
    pub peer_port: u16,
    pub api: Option<(String, u16)>,
    pub connect_timeout: u64,
    pub dns_timeout: u64,
    pub peers_cache: PathBuf,
    pub seed_sources: Option<PathBuf>,
    pub peer_addresses: Option<PathBuf>,
    pub once: bool,
    /// LAST_CONNECTED, LAST_CONTACT_BASE (tests): backdate the last good check and the time
    /// this instance became the primary
    pub last_connected: Option<u64>,
    pub primary_since: Option<u64>,
}

impl Settings {
    /// `marker` and `contact_file`: the segment server's, when it starts the check itself.
    pub fn from_environment(marker: Option<PathBuf>, contact_file: Option<PathBuf>) -> Result<Settings, String> {
        let path = |name: &str| Some(var(name)).filter(|v| !v.is_empty()).map(PathBuf::from);
        let marker = marker.or_else(|| path("SELF_FENCED_FILE")).ok_or("SELF_FENCED_FILE is required")?;
        let mut peers_cache = marker.clone().into_os_string();
        peers_cache.push(".peers");
        let limit = |name: &str, default: u64| Duration::from_secs(var(name).parse().ok().filter(|s| *s > 0).unwrap_or(default));
        Ok(Settings {
            database: Database {
                path: Some(var("DATABASE_PATH"))
                    .filter(|v| !v.is_empty())
                    .ok_or("DATABASE_PATH is required")?,
                port: var("FIREBIRD_PORT").parse().unwrap_or(3050),
                user: Some(var("ISC_USER")).filter(|v| !v.is_empty()).unwrap_or_else(|| "SYSDBA".into()),
                password: var("ISC_PASSWORD"),
                query_limit: limit("LOCAL_QUERY_TIMEOUT_SECONDS", 10),
                command_limit: limit("LOCAL_COMMAND_TIMEOUT_SECONDS", 60),
            },
            primary_file: path("PRIMARY_FILE").ok_or("PRIMARY_FILE is required")?,
            promoted: path("REPLICATION_DIR").map(|dir| dir.join("promoted")),
            marker,
            timeout: seconds("ISOLATION_TIMEOUT_SECONDS", 0),
            contact_timeout: seconds("CONTACT_TIMEOUT_SECONDS", 0),
            contact_file: contact_file.or_else(|| path("LAST_CONTACT_FILE")),
            interval: seconds("CHECK_INTERVAL", 5),
            pod: var("POD_NAME"),
            pod_ip: var("POD_IP"),
            peers: var("PEERS_SERVICE"),
            peer_port: var("SEGMENT_PORT").parse().unwrap_or(3051),
            api: Some(var("KUBERNETES_SERVICE_HOST"))
                .filter(|host| !host.is_empty())
                .map(|host| (host, var("KUBERNETES_SERVICE_PORT").parse().unwrap_or(443))),
            connect_timeout: seconds("CONNECT_TIMEOUT", 3),
            dns_timeout: seconds("DNS_TIMEOUT", 2),
            peers_cache: path("PEERS_CACHE_FILE").unwrap_or_else(|| PathBuf::from(peers_cache)),
            seed_sources: path("SEED_SOURCES_FILE"),
            peer_addresses: path("PEER_ADDRESSES_FILE"),
            once: var("ONCE") == "true",
            last_connected: var("LAST_CONNECTED").parse().ok(),
            primary_since: var("LAST_CONTACT_BASE").parse().ok(),
        })
    }
}

fn reachable(host: &str, port: u16, timeout: u64) -> bool {
    let Ok(mut addresses) = (host, port).to_socket_addrs() else {
        return false;
    };
    addresses.any(|address: SocketAddr| TcpStream::connect_timeout(&address, Duration::from_secs(timeout.max(1))).is_ok())
}

/// What a lookup of the headless Service said.
enum Lookup {
    /// DNS answered: the addresses (none for "no such name")
    Answered(Vec<String>),
    /// No answer, or a failure
    Failed,
}

/// The IPv4 addresses of a name. getaddrinfo itself, for its error code: "no such name" is an
/// answer, anything else is not.
fn addresses(name: &str) -> Lookup {
    // tests: a DNS server that fails, or never answers
    match var("TEST_DNS").as_str() {
        "fail" => return Lookup::Failed,
        "hang" => thread::sleep(Duration::from_secs(60)),
        _ => {}
    }
    let Ok(node) = CString::new(name) else { return Lookup::Failed };
    // SAFETY: zeroed is a valid addrinfo for hints; the result list is walked while it is
    // non-null and freed once, and only sockaddr_in entries (AF_INET was asked for) are read
    unsafe {
        let mut hints: libc::addrinfo = std::mem::zeroed();
        hints.ai_family = libc::AF_INET;
        hints.ai_socktype = libc::SOCK_STREAM;
        let mut list: *mut libc::addrinfo = std::ptr::null_mut();
        let code = libc::getaddrinfo(node.as_ptr(), std::ptr::null(), &hints, &mut list);
        if code != 0 {
            return if code == libc::EAI_NONAME {
                Lookup::Answered(Vec::new())
            } else {
                Lookup::Failed
            };
        }
        let mut found = Vec::new();
        let mut entry = list;
        while !entry.is_null() {
            if (*entry).ai_family == libc::AF_INET && !(*entry).ai_addr.is_null() {
                let address = (*((*entry).ai_addr as *const libc::sockaddr_in)).sin_addr.s_addr;
                found.push(Ipv4Addr::from(u32::from_be(address)).to_string());
            }
            entry = (*entry).ai_next;
        }
        libc::freeaddrinfo(list);
        Lookup::Answered(found)
    }
}

fn dotted(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit() || b == b'.')
}

/// How the check reaches the local server.
pub trait Local {
    /// The database's state, or None without an answer
    fn state(&self) -> Option<State>;
    /// The full shutdown; false when it failed
    fn shut_down(&self) -> bool;
}

/// The agent's service attachments first, fbsvcmgr when they are refused at once.
pub struct Server {
    pub database: Database,
    pub probe: Option<Probe>,
}

impl Server {
    /// `timeout -k 5 <limit> fbsvcmgr localhost:service_mgr <arguments>`: (succeeded, output)
    fn fbsvcmgr(&self, limit: Duration, what: &str, arguments: &[&str]) -> (bool, String) {
        let ran = Command::new("timeout")
            .args(["-k", "5", &limit.as_secs().to_string(), "fbsvcmgr", "localhost:service_mgr"])
            .args(arguments)
            .stdin(Stdio::null())
            .output();
        let Ok(output) = ran else { return (false, String::new()) };
        if matches!(output.status.code(), Some(124) | Some(137)) {
            println!("fbsvcmgr {what} timed out after {}s", limit.as_secs());
        }
        let text = String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr);
        (output.status.success(), text)
    }
}

impl Local for Server {
    fn state(&self) -> Option<State> {
        if let Some(state) = self.probe.as_ref().and_then(Probe::state) {
            return Some(state);
        }
        let (ok, text) = self.fbsvcmgr(
            self.database.query_limit,
            "action_db_stats",
            &["action_db_stats", "dbname", &self.database.path, "sts_hdr_pages"],
        );
        // Firebird 6 refuses header statistics for a database in full shutdown
        if text.lines().any(|line| line.starts_with("database ") && line.contains(" shutdown")) {
            return Some(State::Shutdown);
        }
        ok.then(|| {
            if text.contains("shutdown") {
                State::Shutdown
            } else {
                State::Online
            }
        })
    }

    fn shut_down(&self) -> bool {
        match self.database.shut_down() {
            Ok(()) => true,
            Err(Failed::TimedOut) => {
                println!("the full shutdown timed out after {}s", self.database.command_limit.as_secs());
                false
            }
            Err(Failed::Refused(_)) => {
                let arguments = [
                    "action_properties",
                    "dbname",
                    &self.database.path,
                    "prp_shutdown_mode",
                    "prp_sm_full",
                    "prp_force_shutdown",
                    "0",
                ];
                self.fbsvcmgr(self.database.command_limit, "action_properties", &arguments).0
            }
        }
    }
}

/// The other instances as one check sees them: (DNS answered, addresses).
type View = (bool, Vec<String>);

pub struct Check<L: Local> {
    settings: Settings,
    local: L,
    last_ok: u64,
    primary_since: u64,
    dns_failing: bool,
    view: Option<View>,
}

impl<L: Local> Check<L> {
    pub fn new(settings: Settings, local: L) -> Check<L> {
        let (last_ok, primary_since) = (
            settings.last_connected.unwrap_or_else(now),
            settings.primary_since.unwrap_or_else(now),
        );
        Check {
            settings,
            local,
            last_ok,
            primary_since,
            dns_failing: false,
            view: None,
        }
    }

    fn is_primary(&self) -> bool {
        if self.settings.promoted.as_ref().is_some_and(|flag| flag.exists()) {
            return true;
        }
        let primary = slurp(&self.settings.primary_file);
        primary.is_empty()
            || primary
                .strip_prefix(self.settings.pod.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
    }

    /// The headless Service's addresses other than this pod's, within DNS_TIMEOUT: a lookup DNS
    /// never answers must not hold the check up.
    fn resolve_peers(&self) -> Lookup {
        if self.settings.peers.is_empty() {
            return Lookup::Answered(Vec::new());
        }
        let (done, answer) = mpsc::channel();
        let name = self.settings.peers.clone();
        thread::spawn(move || {
            let _ = done.send(addresses(&name));
        });
        match answer.recv_timeout(Duration::from_secs(self.settings.dns_timeout)) {
            Ok(Lookup::Answered(found)) => {
                let others: BTreeSet<String> = found.into_iter().filter(|ip| *ip != self.settings.pod_ip).collect();
                Lookup::Answered(others.into_iter().collect())
            }
            _ => Lookup::Failed,
        }
    }

    fn known_peers(&self) -> Vec<String> {
        fs::read_to_string(&self.settings.peers_cache)
            .map(|text| {
                text.lines()
                    .map(|line| line.trim_end().to_string())
                    .filter(|ip| dotted(ip))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn published_peers(&self) -> Vec<String> {
        let Some(file) = &self.settings.peer_addresses else {
            return Vec::new();
        };
        slurp(file).split_whitespace().filter(|ip| dotted(ip)).map(str::to_string).collect()
    }

    fn remember_peers(&self, ips: &[String]) {
        if self.known_peers() == ips {
            return;
        }
        let mut temporary = self.settings.peers_cache.clone().into_os_string();
        temporary.push(".tmp");
        let content: String = ips.iter().map(|ip| format!("{ip}\n")).collect();
        if fs::write(&temporary, content).is_ok() {
            let _ = fs::rename(&temporary, &self.settings.peers_cache);
        }
    }

    /// Whether the operator lists ready replicas (the cluster ConfigMap, mounted: no DNS involved)
    fn seed_sources_listed(&self) -> bool {
        self.settings
            .seed_sources
            .as_ref()
            .is_some_and(|file| !slurp(file).trim().is_empty())
    }

    /// The other instances, once per check: from DNS, or those known when DNS failed.
    fn peer_view(&mut self) -> View {
        if let Some(view) = &self.view {
            return view.clone();
        }
        let view = match self.resolve_peers() {
            Lookup::Answered(ips) => {
                if self.dns_failing {
                    println!("cluster DNS answers again");
                }
                self.dns_failing = false;
                self.remember_peers(&ips);
                (true, ips)
            }
            Lookup::Failed => {
                // the last answer, and the addresses the operator publishes, which follow peers
                // that restarted with a new address meanwhile
                let known: BTreeSet<String> = self
                    .known_peers()
                    .into_iter()
                    .chain(self.published_peers())
                    .filter(|ip| *ip != self.settings.pod_ip)
                    .collect();
                if !self.dns_failing {
                    println!(
                        "cluster DNS did not answer for {}: using the {} peer address(es) known from the last answer and the operator",
                        self.settings.peers,
                        known.len()
                    );
                }
                self.dns_failing = true;
                (false, known.into_iter().collect())
            }
        };
        self.view = Some(view.clone());
        view
    }

    /// Whether other instances could be failed over to while nothing reaches this primary.
    fn cut_off_from_peers(&mut self) -> bool {
        let (answered, ips) = self.peer_view();
        if answered {
            return !ips.is_empty();
        }
        // DNS failed: only when none of the known peers answers either (not a DNS outage alone);
        // with a short timeout each, as the fence must come before the operator's failover
        if !ips.is_empty() {
            return !ips.iter().any(|ip| reachable(ip, self.settings.peer_port, 1));
        }
        self.seed_sources_listed()
    }

    fn connected(&mut self) -> bool {
        if let Some((host, port)) = &self.settings.api {
            if reachable(host, *port, self.settings.connect_timeout) {
                return true;
            }
        }
        let (_, ips) = self.peer_view();
        ips.iter()
            .any(|ip| reachable(ip, self.settings.peer_port, self.settings.connect_timeout))
    }

    /// Seconds since the operator or a replica last reached this instance's segment server,
    /// counted from when it became the primary at the earliest; None when the check is off.
    fn unreached_for(&self) -> Option<u64> {
        let file = self.settings.contact_file.as_ref().filter(|_| self.settings.contact_timeout > 0)?;
        let contacted = fs::metadata(file)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
            .map(|at| at.as_secs());
        let last = contacted.filter(|at| *at > self.primary_since).unwrap_or(self.primary_since);
        Some(now().saturating_sub(last))
    }

    fn fence(&self, since: u64, reason: Option<String>) {
        let marker = &self.settings.marker;
        // the marker first: a restarted sidecar must not take a fenced database for an online one
        let mut temporary = marker.clone().into_os_string();
        temporary.push(".tmp");
        if let Err(error) = fs::write(&temporary, format!("{}\n", now())).and_then(|()| fs::rename(&temporary, marker)) {
            println!("cannot write {}: {error}", marker.display());
            return;
        }
        let reason = reason.unwrap_or_else(|| {
            format!(
                "neither the Kubernetes API server nor another instance reachable for {}s",
                now().saturating_sub(since)
            )
        });
        println!("isolated: {reason}; fencing the primary (database in full shutdown)");
        if !self.local.shut_down() {
            let _ = fs::remove_file(marker);
            println!("full shutdown failed; retrying on the next check");
        }
    }

    /// One check.
    pub fn check(&mut self) {
        self.view = None;
        let primary = self.is_primary();
        let fenced = self.settings.marker.is_file();
        // the primary looks its peers up on every check, so that they are known when DNS fails
        if primary && !fenced {
            self.peer_view();
        }
        let unreached = self.unreached_for();
        if !primary {
            self.last_ok = now();
            self.primary_since = now();
        } else if fenced {
            // fenced: wait until the database is online again (REJOIN, or an administrator)
            self.last_ok = now();
            if self.local.state() == Some(State::Online) {
                let _ = fs::remove_file(&self.settings.marker);
                println!("database online again: isolation fence lifted");
            }
        } else if unreached.is_some_and(|seconds| seconds >= self.settings.contact_timeout) && self.cut_off_from_peers() {
            let unreached = unreached.unwrap_or(0);
            let dns = if self.dns_failing {
                " (cluster DNS failing, and no known peer answers)"
            } else {
                ""
            };
            self.fence(
                now().saturating_sub(unreached),
                Some(format!(
                    "neither the operator nor any replica has reached this primary for {unreached}s{dns}"
                )),
            );
        } else if self.connected() {
            self.last_ok = now();
        } else if now().saturating_sub(self.last_ok) >= self.settings.timeout {
            self.fence(self.last_ok, None);
        } else {
            println!(
                "isolated for {}s (fencing after {}s)",
                now().saturating_sub(self.last_ok),
                self.settings.timeout
            );
        }
    }

    /// Check after check; once with ONCE=true. Nothing at all when the check is off.
    pub fn run(mut self) {
        if self.settings.timeout == 0 {
            return;
        }
        loop {
            self.check();
            if self.settings.once {
                return;
            }
            thread::sleep(Duration::from_secs(self.settings.interval.max(1)));
        }
    }
}

/// `fb-agent isolation-check`: the check as its own process, as isolation-check.pl is.
pub fn command() -> Result<(), String> {
    let settings = Settings::from_environment(None, None)?;
    let database = settings.database.clone();
    let probe = Probe::start(
        database.path.clone(),
        database.port,
        database.user.clone(),
        database.password.clone(),
        database.query_limit,
    );
    let local = Server {
        database,
        probe: Some(probe),
    };
    Check::new(settings, local).run();
    Ok(())
}

/// The check beside the segment server's front, started again if it ever ends.
pub fn start(marker: PathBuf, contact_file: PathBuf) {
    thread::spawn(move || loop {
        let (marker, contact_file) = (marker.clone(), contact_file.clone());
        let ended = thread::spawn(move || match Settings::from_environment(Some(marker), Some(contact_file)) {
            Ok(settings) => {
                let limit = settings.database.query_limit;
                let database = settings.database.clone();
                let probe = Probe::start(
                    database.path.clone(),
                    database.port,
                    database.user.clone(),
                    database.password.clone(),
                    limit,
                );
                Check::new(
                    settings,
                    Server {
                        database,
                        probe: Some(probe),
                    },
                )
                .run();
            }
            Err(reason) => println!("cannot start the isolation check: {reason}"),
        })
        .join();
        if ended.is_err() {
            println!("the isolation check ended unexpectedly: starting it again");
        }
        // off, or it ended: looked at again as the Perl server restarts its own
        thread::sleep(Duration::from_secs(if ended.is_err() { 1 } else { 3600 }));
    });
}
