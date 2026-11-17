//! TLS for the segment protocol (spec.segmentTLS): the segment server accepts mutual TLS on its
//! port (`accepted`), and its clients (the puller, the replication init's helpers, the Jobs'
//! programs, the segment server's own requests) connect over it (`connected`). Until v0.171.0
//! this module also ran the proxy sidecar `fb-agent segment-tls`, of the clients that had no TLS.
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
//! - `plain-peers`: a client connects to these instances in plain text;
//! - `accept-plain-until` (epoch milliseconds): until then the server also accepts plain
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
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// First byte of a TLS handshake record
pub const TLS_HANDSHAKE: u8 = 0x16;
/// How long the server side waits for a connection's first byte
pub const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(30);

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
