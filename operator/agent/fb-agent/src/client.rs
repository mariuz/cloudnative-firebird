//! The client side of the segment servers' protocol: one signed request per connection.

use crate::tls::{self, Material, Stream};
use std::env;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

pub fn var(name: &str) -> String {
    env::var(name).unwrap_or_default()
}

/// 16 random bytes in hex, from the kernel.
fn nonce() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn connect(host: &str, port: u16) -> io::Result<TcpStream> {
    let mut last = io::Error::new(io::ErrorKind::NotFound, "no address");
    for address in (host, port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(err) => last = err,
        }
    }
    Err(last)
}

/// A plain connection to the segment server at `host`:`port` with `request` signed and sent: the
/// pod's own server, from its other containers.
pub fn open_at(host: &str, port: u16, request: &str, timeout: Option<Duration>) -> io::Result<BufReader<Stream>> {
    let mut stream = connect(host, port).map_err(|err| io::Error::new(err.kind(), format!("connect {host}:{port}: {err}")))?;
    stream.set_read_timeout(timeout)?;
    // one write: the line and its end in one segment
    stream.write_all(format!("{}\n", sig1::sign(var("ISC_PASSWORD").as_bytes(), request, now(), &nonce()?)).as_bytes())?;
    stream.flush()?;
    Ok(BufReader::new(Stream::Plain(stream)))
}

/// Connects to the segment server of `host` (SEGMENT_PORT, 3051 by default) and sends `request`,
/// signed with ISC_PASSWORD. With segment TLS (SEGMENT_TLS=true) the connection is mutual TLS to
/// that host with the cluster's certificates (SEGMENT_TLS_DIR), or plain text to an instance the
/// operator lists as still plain while the cluster switches (tls.rs). `timeout` bounds each read
/// of the reply.
pub fn open(host: &str, request: &str, timeout: Option<Duration>) -> io::Result<BufReader<Stream>> {
    static MATERIAL: OnceLock<Option<Material>> = OnceLock::new();
    let port: u16 = var("SEGMENT_PORT").parse().unwrap_or(3051);
    let failed = |target: String| move |err: io::Error| io::Error::new(err.kind(), format!("connect {target}: {err}"));
    let mut stream = if var("SEGMENT_TLS") == "true" {
        // never plain text instead: without the certificates the request is not sent
        let material = MATERIAL
            .get_or_init(Material::from_environment)
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "segment TLS: no certificates in SEGMENT_TLS_DIR"))?;
        let connection = connect(host, port).map_err(failed(format!("{host}:{port}")))?;
        let _ = connection.set_nodelay(true);
        let stream = tls::connected(material, host, connection)?;
        if let Stream::Client(tls) = &stream {
            tls.sock.set_read_timeout(timeout)?;
        }
        stream
    } else {
        Stream::Plain(connect(host, port).map_err(failed(format!("{host}:{port}")))?)
    };
    if let Stream::Plain(plain) = &stream {
        plain.set_read_timeout(timeout)?;
    }
    // one write: the line and its end in one segment
    stream.write_all(format!("{}\n", sig1::sign(var("ISC_PASSWORD").as_bytes(), request, now(), &nonce()?)).as_bytes())?;
    stream.flush()?;
    Ok(BufReader::new(stream))
}
