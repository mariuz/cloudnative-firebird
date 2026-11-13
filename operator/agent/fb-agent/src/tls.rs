//! TLS for the segment servers (spec.segmentTLS, docs/rust-agent-segment-server.md, step 6): the
//! proxy that runs beside the instances and in the Jobs that talk to them, as
//! `fb-agent segment-tls`. It does what the operator image's segment-tls.js does, with the same
//! environment:
//!
//! - server side (SERVER_LISTEN, SERVER_TARGET): accepts mutual TLS on the segment port and
//!   forwards each connection to the segment server, which then listens on localhost only;
//! - client side (CLIENT_LISTEN, on localhost): a client connects here and sends one line,
//!   "CONNECT <host> <port>", followed by its request as before; the proxy opens mutual TLS to
//!   that host's segment port and relays both ways.
//!
//! The certificates come from the cluster's Secret, mounted in SEGMENT_TLS_DIR (ca.crt, tls.crt,
//! tls.key): one CA per cluster, which signs the certificate all of its instances present. A
//! valid chain to the cluster's CA is what authenticates a peer, so host names are not checked
//! (the clients address instances by short names). TLS 1.3 only. The files are read again when
//! they change (rotation).
//!
//! While segment TLS is switched on or off the instances restart one by one. The operator
//! publishes those that still serve in plain text in SEGMENT_TLS_PEERS_DIR:
//!
//! - `plain-peers`: the client side connects to these instances in plain text;
//! - `accept-plain-until` (epoch milliseconds): until then the server side also accepts plain
//!   connections, told apart from TLS by the first byte (a handshake record starts with 0x16).
//!
//! Without them, or once the switch is over: TLS only. A failed handshake never falls back to
//! plain text.

use crate::client::var;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use rustls::{
    ClientConfig, ClientConnection, Connection, DigitallySignedStruct, RootCertStore, ServerConfig, ServerConnection, SignatureScheme,
    StreamOwned,
};
use std::collections::HashSet;
use std::fs;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONNECT_LINE_MAX: usize = 512;
/// First byte of a TLS handshake record
pub const TLS_HANDSHAKE: u8 = 0x16;
/// How long the server side waits for a connection's first byte
pub const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(30);
/// What one direction of a relay holds at most before it stops reading the other side
const RELAY_BUFFER: usize = 256 * 1024;

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

/// A peer is authenticated by a valid chain to the cluster's CA; its name is not checked.
#[derive(Debug)]
struct ChainToCa {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for ChainToCa {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let certificate = ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &certificate,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

/// Both sides' TLS settings: mutual TLS, TLS 1.3, the cluster's CA only.
pub struct Settings {
    pub server: Arc<ServerConfig>,
    pub client: Arc<ClientConfig>,
}

pub fn settings(dir: &Path) -> io::Result<Settings> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let certificates = |name: &str| -> io::Result<Vec<CertificateDer<'static>>> {
        CertificateDer::pem_file_iter(dir.join(name))
            .map_err(other)?
            .collect::<Result<_, _>>()
            .map_err(other)
    };
    let mut roots = RootCertStore::empty();
    for ca in certificates("ca.crt")? {
        roots.add(ca).map_err(other)?;
    }
    let roots = Arc::new(roots);
    let chain = certificates("tls.crt")?;
    let key = || PrivateKeyDer::from_pem_file(dir.join("tls.key")).map_err(other);
    let clients = WebPkiClientVerifier::builder_with_provider(Arc::clone(&roots), Arc::clone(&provider))
        .build()
        .map_err(other)?;
    let server = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(other)?
        .with_client_cert_verifier(clients)
        .with_single_cert(chain.clone(), key()?)
        .map_err(other)?;
    let client = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(other)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ChainToCa { roots, provider }))
        .with_client_auth_cert(chain, key()?)
        .map_err(other)?;
    Ok(Settings {
        server: Arc::new(server),
        client: Arc::new(client),
    })
}

/// What the operator allows during a switch of segment TLS.
#[derive(Default, Debug, PartialEq)]
pub struct Peers {
    /// Instance pod names reached in plain text
    pub plain: HashSet<String>,
    /// Until when (epoch milliseconds) plain connections are accepted
    pub accept_plain_until: u128,
}

/// `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
fn pod_name(name: &str) -> bool {
    let plain = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let bytes = name.as_bytes();
    !bytes.is_empty() && plain(bytes[0]) && plain(bytes[bytes.len() - 1]) && bytes.iter().all(|b| plain(*b) || *b == b'-')
}

/// The ConfigMap's files; anything missing or malformed means TLS only.
fn parse_peers(plain_peers: Option<&str>, accept_plain_until: Option<&str>) -> Peers {
    Peers {
        plain: plain_peers
            .unwrap_or_default()
            .split_whitespace()
            .filter(|name| pod_name(name))
            .map(str::to_string)
            .collect(),
        accept_plain_until: accept_plain_until.and_then(|until| until.trim().parse().ok()).unwrap_or(0),
    }
}

pub fn read_peers(dir: &Path) -> Peers {
    let read = |name: &str| fs::read_to_string(dir.join(name)).ok();
    parse_peers(read("plain-peers").as_deref(), read("accept-plain-until").as_deref())
}

/// The instance a segment server address names: its first label (<pod>.<cluster>-headless...).
pub fn peer_name(host: &str) -> &str {
    host.split('.').next().unwrap_or(host)
}

pub fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
}

/// The certificates and the operator's peers files of a process that does its own TLS (the
/// segment server's front, the puller): the settings are built again when the certificate
/// changed, the peers are read when they are asked for.
pub struct Material {
    dir: PathBuf,
    peers: Option<PathBuf>,
    built: Mutex<Option<(Option<SystemTime>, Arc<Settings>)>>,
}

impl Material {
    /// From SEGMENT_TLS_DIR and SEGMENT_TLS_PEERS_DIR; None without the first.
    pub fn from_environment() -> Option<Material> {
        let dir = Some(var("SEGMENT_TLS_DIR")).filter(|dir| !dir.is_empty())?;
        Some(Material {
            dir: PathBuf::from(dir),
            peers: Some(var("SEGMENT_TLS_PEERS_DIR")).filter(|dir| !dir.is_empty()).map(PathBuf::from),
            built: Mutex::new(None),
        })
    }

    pub fn settings(&self) -> io::Result<Arc<Settings>> {
        let stamp = fs::metadata(self.dir.join("tls.crt")).and_then(|m| m.modified()).ok();
        let mut built = self.built.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((at, settings)) = built.as_ref() {
            if *at == stamp {
                return Ok(Arc::clone(settings));
            }
        }
        match settings(&self.dir) {
            Ok(fresh) => {
                let fresh = Arc::new(fresh);
                *built = Some((stamp, Arc::clone(&fresh)));
                Ok(fresh)
            }
            // being replaced: those in use stay
            Err(error) => built.as_ref().map(|(_, settings)| Arc::clone(settings)).ok_or(error),
        }
    }

    pub fn peers(&self) -> Peers {
        self.peers.as_deref().map(read_peers).unwrap_or_default()
    }
}

/// A connection of the segment protocol: plain, or the TLS side of this process. One request
/// and its reply go over it in turn, so one owner reads and writes.
pub enum Stream {
    Plain(TcpStream),
    Server(Box<StreamOwned<ServerConnection, TcpStream>>),
    Client(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Stream {
    /// The end of what this side sends: TLS says so before the connection closes, so the peer
    /// can tell a complete reply from a cut one.
    pub fn finish(&mut self) {
        match self {
            Stream::Plain(stream) => {
                let _ = stream.shutdown(Shutdown::Write);
            }
            Stream::Server(tls) => {
                tls.conn.send_close_notify();
                let _ = tls.conn.complete_io(&mut tls.sock);
                let _ = tls.sock.shutdown(Shutdown::Write);
            }
            Stream::Client(tls) => {
                tls.conn.send_close_notify();
                let _ = tls.conn.complete_io(&mut tls.sock);
                let _ = tls.sock.shutdown(Shutdown::Write);
            }
        }
    }
}

/// A peer that closes without TLS's own end (a proxy that was stopped) reads as the end.
fn or_end(read: io::Result<usize>) -> io::Result<usize> {
    match read {
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(0),
        other => other,
    }
}

impl Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(stream) => stream.read(buffer),
            Stream::Server(tls) => or_end(tls.read(buffer)),
            Stream::Client(tls) => or_end(tls.read(buffer)),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(stream) => stream.write(buffer),
            Stream::Server(tls) => tls.write(buffer),
            Stream::Client(tls) => tls.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(stream) => stream.flush(),
            Stream::Server(tls) => tls.flush(),
            Stream::Client(tls) => tls.flush(),
        }
    }
}

/// The server side of a connection accepted on a TLS port: TLS after the handshake, plain text
/// while the operator allows it (told apart by the first byte), None for anything else.
pub fn accepted(material: &Material, connection: TcpStream) -> Option<Stream> {
    // nothing sent: a bare TCP probe, or a client that gave up
    let _ = connection.set_read_timeout(Some(FIRST_BYTE_TIMEOUT));
    let mut first = [0u8; 1];
    if !matches!(connection.peek(&mut first), Ok(1)) {
        return None;
    }
    if first[0] != TLS_HANDSHAKE {
        return (now_ms() < material.peers().accept_plain_until).then_some(Stream::Plain(connection));
    }
    let session = ServerConnection::new(Arc::clone(&material.settings().ok()?.server)).ok()?;
    let mut session = Connection::Server(session);
    if !handshake(&mut session, &connection) {
        return None;
    }
    let Connection::Server(session) = session else { return None };
    Some(Stream::Server(Box::new(StreamOwned::new(session, connection))))
}

/// The client side to an instance's segment port: plain text to the instances the operator lists
/// as plain, mutual TLS to the others.
pub fn connected(material: &Material, host: &str, connection: TcpStream) -> io::Result<Stream> {
    if material.peers().plain.contains(peer_name(host)) {
        return Ok(Stream::Plain(connection));
    }
    // the name is not checked (the chain to the cluster's CA authenticates the peer)
    let name = ServerName::try_from(host.to_string()).unwrap_or_else(|_| ServerName::try_from("segment-server").expect("a valid name"));
    let session = ClientConnection::new(Arc::clone(&material.settings()?.client), name).map_err(other)?;
    let mut session = Connection::Client(session);
    if !handshake(&mut session, &connection) {
        return Err(other(format!(
            "{host} is not an instance of this cluster (the TLS handshake failed)"
        )));
    }
    let Connection::Client(session) = session else {
        return Err(other("not a client connection"));
    };
    Ok(Stream::Client(Box::new(StreamOwned::new(session, connection))))
}

/// What both sides share and what is read again while they run.
struct Proxy {
    settings: RwLock<Arc<Settings>>,
    peers: RwLock<Peers>,
}

impl Proxy {
    fn settings(&self) -> Arc<Settings> {
        Arc::clone(&self.settings.read().unwrap_or_else(|p| p.into_inner()))
    }
}

/// Two plain connections, both ways, until both are done.
fn relay_plain(a: TcpStream, b: TcpStream) {
    let (Ok(mut a_read), Ok(mut b_write)) = (a.try_clone(), b.try_clone()) else {
        return;
    };
    let forward = thread::spawn(move || {
        let _ = io::copy(&mut a_read, &mut b_write);
        let _ = b_write.shutdown(Shutdown::Write);
    });
    let (mut b_read, mut a_write) = (b, a);
    let _ = io::copy(&mut b_read, &mut a_write);
    let _ = a_write.shutdown(Shutdown::Write);
    let _ = forward.join();
}

fn poll(descriptors: &mut [libc::pollfd]) -> bool {
    // SAFETY: a valid array of pollfd and its length
    unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as libc::nfds_t, 60_000) >= 0 }
}

/// A TLS connection and a plain one, both ways, in one thread: neither direction waits for the
/// other, whatever the two ends do (a large upload while the reply is being sent). `pending`:
/// what the plain side sent before the relay started.
pub fn relay_tls(mut tls: Connection, encrypted: TcpStream, plain: TcpStream, pending: Vec<u8>) {
    if encrypted.set_nonblocking(true).is_err() || plain.set_nonblocking(true).is_err() {
        return;
    }
    let (mut encrypted, mut plain) = (encrypted, plain);
    // to the plain side, decrypted; from the plain side, not yet taken by the TLS connection
    let (mut to_plain, mut from_plain) = (Vec::<u8>::new(), pending);
    let (mut tls_done, mut plain_done, mut plain_closed, mut notified) = (false, false, false, false);
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        // what the TLS connection has decrypted (also what arrived with the end of the handshake)
        while !tls_done && to_plain.len() < RELAY_BUFFER {
            match tls.reader().read(&mut buffer) {
                Ok(0) => tls_done = true,
                Ok(read) => to_plain.extend_from_slice(&buffer[..read]),
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(_) => tls_done = true,
            }
        }
        // what the plain side sent goes into the TLS connection as far as it takes it
        while !from_plain.is_empty() {
            match tls.writer().write(&from_plain) {
                Ok(0) | Err(_) => break,
                Ok(taken) => {
                    from_plain.drain(..taken);
                }
            }
        }
        if plain_done && from_plain.is_empty() && !notified {
            tls.send_close_notify();
            notified = true;
        }
        if tls_done && to_plain.is_empty() && !plain_closed {
            let _ = plain.shutdown(Shutdown::Write);
            plain_closed = true;
        }
        if tls_done && plain_closed && plain_done && !tls.wants_write() {
            let _ = encrypted.shutdown(Shutdown::Both);
            return;
        }
        let mut wanted = [
            libc::pollfd {
                fd: encrypted.as_raw_fd(),
                events: 0,
                revents: 0,
            },
            libc::pollfd {
                fd: plain.as_raw_fd(),
                events: 0,
                revents: 0,
            },
        ];
        if !tls_done && to_plain.len() < RELAY_BUFFER {
            wanted[0].events |= libc::POLLIN;
        }
        if tls.wants_write() {
            wanted[0].events |= libc::POLLOUT;
        }
        if !plain_done && from_plain.is_empty() {
            wanted[1].events |= libc::POLLIN;
        }
        if !to_plain.is_empty() {
            wanted[1].events |= libc::POLLOUT;
        }
        if wanted[0].events == 0 && wanted[1].events == 0 {
            return;
        }
        if !poll(&mut wanted) {
            return;
        }
        let ready = |d: &libc::pollfd, events: libc::c_short| d.revents & (events | libc::POLLERR | libc::POLLHUP) != 0;
        if wanted[0].events & libc::POLLIN != 0 && ready(&wanted[0], libc::POLLIN) {
            match tls.read_tls(&mut encrypted) {
                Ok(0) => tls_done = true,
                Ok(_) => {
                    if tls.process_new_packets().is_err() {
                        // an alert may be queued for the peer: sent, then both ends closed
                        let _ = tls.write_tls(&mut encrypted);
                        let _ = plain.shutdown(Shutdown::Both);
                        return;
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => tls_done = true,
            }
        }
        if wanted[0].events & libc::POLLOUT != 0 && ready(&wanted[0], libc::POLLOUT) {
            match tls.write_tls(&mut encrypted) {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => {
                    let _ = plain.shutdown(Shutdown::Both);
                    return;
                }
            }
        }
        if wanted[1].events & libc::POLLIN != 0 && ready(&wanted[1], libc::POLLIN) {
            match plain.read(&mut buffer) {
                Ok(0) => plain_done = true,
                Ok(read) => from_plain.extend_from_slice(&buffer[..read]),
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => plain_done = true,
            }
        }
        if wanted[1].events & libc::POLLOUT != 0 && ready(&wanted[1], libc::POLLOUT) {
            match plain.write(&to_plain) {
                Ok(written) => {
                    to_plain.drain(..written);
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => {
                    let _ = encrypted.shutdown(Shutdown::Both);
                    return;
                }
            }
        }
    }
}

/// The TLS handshake, within the time a first byte gets; false when the peer is not one of this
/// cluster (the alert, if any, is sent).
pub fn handshake(tls: &mut Connection, mut encrypted: &TcpStream) -> bool {
    let _ = encrypted.set_read_timeout(Some(FIRST_BYTE_TIMEOUT));
    let _ = encrypted.set_write_timeout(Some(FIRST_BYTE_TIMEOUT));
    while tls.is_handshaking() {
        if tls.complete_io(&mut encrypted).is_err() {
            let _ = tls.write_tls(&mut encrypted);
            return false;
        }
    }
    let _ = encrypted.set_read_timeout(None);
    let _ = encrypted.set_write_timeout(None);
    true
}

/// Server side: one accepted connection. TLS is forwarded to the segment server; plain text too
/// while the operator allows it.
fn accept(proxy: &Proxy, connection: TcpStream, target: &str) {
    // nothing sent: the isolation check's bare TCP probe, or a client that gave up
    let _ = connection.set_read_timeout(Some(FIRST_BYTE_TIMEOUT));
    let mut first = [0u8; 1];
    if !matches!(connection.peek(&mut first), Ok(1)) {
        return;
    }
    let _ = connection.set_read_timeout(None);
    let tls = first[0] == TLS_HANDSHAKE;
    if !tls && now_ms() >= proxy.peers.read().unwrap_or_else(|p| p.into_inner()).accept_plain_until {
        return;
    }
    let upstream = || {
        let upstream = TcpStream::connect(target).ok()?;
        let _ = upstream.set_nodelay(true);
        Some(upstream)
    };
    if tls {
        // the segment server is reached only for a peer of this cluster: after the handshake
        let Ok(session) = ServerConnection::new(Arc::clone(&proxy.settings().server)) else {
            return;
        };
        let mut session = Connection::Server(session);
        if !handshake(&mut session, &connection) {
            return;
        }
        if let Some(upstream) = upstream() {
            relay_tls(session, connection, upstream, Vec::new());
        }
    } else if let Some(upstream) = upstream() {
        relay_plain(connection, upstream);
    }
}

/// Client side: "CONNECT <host> <port>" first, then relayed over mutual TLS, or in plain text
/// to the instances the operator lists as plain.
fn connect(proxy: &Proxy, mut local: TcpStream) {
    let mut received = Vec::new();
    let mut buffer = [0u8; 1024];
    let newline = loop {
        if let Some(at) = received.iter().position(|b| *b == b'\n') {
            break at;
        }
        if received.len() > CONNECT_LINE_MAX {
            return;
        }
        match local.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(read) => received.extend_from_slice(&buffer[..read]),
        }
    };
    let rest = received.split_off(newline + 1);
    let line = String::from_utf8_lossy(&received[..newline]).trim_end_matches('\r').to_string();
    let target = line
        .strip_prefix("CONNECT ")
        .and_then(|target| target.split_once(' '))
        .filter(|(host, port)| {
            !host.is_empty() && !host.contains(char::is_whitespace) && !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
        });
    let Some((host, port)) = target else {
        let _ = local.write_all(b"ERR expected CONNECT <host> <port>\n");
        return;
    };
    let Ok(port) = port.parse::<u16>() else { return };
    let plain = proxy
        .peers
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .plain
        .contains(peer_name(host));
    let Ok(mut remote) = TcpStream::connect((host, port)) else { return };
    let _ = remote.set_nodelay(true);
    if plain {
        if remote.write_all(&rest).is_ok() {
            relay_plain(local, remote);
        }
        return;
    }
    // the name is not checked (the chain to the cluster's CA authenticates the peer)
    let name = ServerName::try_from(host.to_string()).unwrap_or_else(|_| ServerName::try_from("segment-server").expect("a valid name"));
    let Ok(session) = ClientConnection::new(Arc::clone(&proxy.settings().client), name) else {
        return;
    };
    let mut session = Connection::Client(session);
    if handshake(&mut session, &remote) {
        relay_tls(session, remote, local, rest);
    }
}

fn listen(address: &str, default_host: &str) -> io::Result<TcpListener> {
    match address.rsplit_once(':') {
        Some((host, port)) => TcpListener::bind((host, port.parse::<u16>().map_err(other)?)),
        None => TcpListener::bind((default_host, address.parse::<u16>().map_err(other)?)),
    }
}

/// Whether nothing listens on the local Firebird port.
fn firebird_stopped() -> bool {
    let port = var("FIREBIRD_PORT").parse().unwrap_or(3050);
    TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(2)).is_err()
}

pub fn run() -> io::Result<()> {
    let dir = Some(var("SEGMENT_TLS_DIR"))
        .filter(|dir| !dir.is_empty())
        .unwrap_or_else(|| "/etc/segment-tls".into());
    let dir = PathBuf::from(dir);
    let peers_dir = Some(var("SEGMENT_TLS_PEERS_DIR")).filter(|dir| !dir.is_empty()).map(PathBuf::from);
    let proxy = Arc::new(Proxy {
        settings: RwLock::new(Arc::new(settings(&dir)?)),
        peers: RwLock::new(peers_dir.as_deref().map(read_peers).unwrap_or_default()),
    });
    let (server_listen, server_target, client_listen) = (var("SERVER_LISTEN"), var("SERVER_TARGET"), var("CLIENT_LISTEN"));
    let serving = !server_listen.is_empty() && !server_target.is_empty();
    if !serving && client_listen.is_empty() {
        return Err(other("segment TLS: neither SERVER_LISTEN/SERVER_TARGET nor CLIENT_LISTEN is set"));
    }
    if serving {
        let listener = listen(&server_listen, "0.0.0.0")?;
        let target = if server_target.contains(':') {
            server_target.clone()
        } else {
            format!("127.0.0.1:{server_target}")
        };
        println!("segment TLS: accepting on {}, forwarding to {target}", listener.local_addr()?);
        let proxy = Arc::clone(&proxy);
        thread::spawn(move || {
            for connection in listener.incoming().flatten() {
                let (proxy, target) = (Arc::clone(&proxy), target.clone());
                thread::spawn(move || accept(&proxy, connection, &target));
            }
        });
    }
    if !client_listen.is_empty() {
        let listener = listen(&client_listen, "127.0.0.1")?;
        println!("segment TLS: client proxy on {}", listener.local_addr()?);
        let proxy = Arc::clone(&proxy);
        thread::spawn(move || {
            for connection in listener.incoming().flatten() {
                let proxy = Arc::clone(&proxy);
                thread::spawn(move || connect(&proxy, connection));
            }
        });
    }

    // SIGTERM for a sidecar that is its container's first process: it keeps working until the
    // pod's Firebird server has stopped, then exits. Where no server runs (a Job) that is at once.
    let stopping = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(signal, Arc::clone(&stopping))?;
    }
    let modified = |dir: &Path| fs::metadata(dir.join("tls.crt")).and_then(|m| m.modified()).ok();
    let mut stamp = modified(&dir);
    let (mut said, mut tick) = (false, 0u64);
    // SEGMENT_TLS_RELOAD_SECONDS (tests): how often the certificate files are looked at
    let reload = var("SEGMENT_TLS_RELOAD_SECONDS")
        .parse::<u64>()
        .ok()
        .filter(|s| *s > 0)
        .unwrap_or(30);
    loop {
        thread::sleep(Duration::from_secs(1));
        tick += 1;
        if stopping.load(Ordering::Relaxed) {
            if !said {
                println!("segment TLS: stopping once the Firebird server has stopped");
                said = true;
            }
            if firebird_stopped() {
                println!("segment TLS: the Firebird server has stopped, exiting");
                return Ok(());
            }
        }
        // the peers every five seconds (ConfigMap volume updates)
        if tick % 5 == 0 {
            if let Some(peers_dir) = &peers_dir {
                *proxy.peers.write().unwrap_or_else(|p| p.into_inner()) = read_peers(peers_dir);
            }
        }
        // the certificates once they changed (Secret volume updates); while they are being
        // replaced the ones in use stay
        if tick % reload == 0 {
            let latest = modified(&dir);
            if latest != stamp {
                if let Ok(renewed) = settings(&dir) {
                    stamp = latest;
                    *proxy.settings.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(renewed);
                    println!("segment TLS: the certificates were read again");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_peers_the_operator_publishes() {
        let peers = parse_peers(Some("db-0\ndb-2\n"), Some("1700000000000"));
        assert_eq!(peers.plain, HashSet::from(["db-0".to_string(), "db-2".to_string()]));
        assert_eq!(peers.accept_plain_until, 1_700_000_000_000);
        assert_eq!(parse_peers(None, None), Peers::default());
        // anything that is not a pod name is ignored, and a time that is none means TLS only
        let odd = parse_peers(Some("db-0 ../x DB-1 -a a-"), Some("soon"));
        assert_eq!(odd.plain, HashSet::from(["db-0".to_string()]));
        assert_eq!(odd.accept_plain_until, 0);
        assert_eq!(peer_name("db-1.db-headless.prod.svc"), "db-1");
        assert_eq!(peer_name("localhost"), "localhost");
    }
}
