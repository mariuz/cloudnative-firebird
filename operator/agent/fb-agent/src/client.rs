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

/// Connects to the segment server of `host` (SEGMENT_PORT, 3051 by default) and sends `request`,
/// signed with ISC_PASSWORD. With segment TLS (SEGMENT_PROXY is set) the connection is mutual TLS
/// to that host: made here when this process has the cluster's certificates (SEGMENT_TLS_DIR),
/// else through the local proxy named by SEGMENT_PROXY, which is told the host on a first line.
/// `timeout` bounds each read of the reply.
pub fn open(host: &str, request: &str, timeout: Option<Duration>) -> io::Result<BufReader<Stream>> {
    static MATERIAL: OnceLock<Option<Material>> = OnceLock::new();
    let port: u16 = var("SEGMENT_PORT").parse().unwrap_or(3051);
    let failed = |target: String| move |err: io::Error| io::Error::new(err.kind(), format!("connect {target}: {err}"));
    let proxy = var("SEGMENT_PROXY");
    let material = MATERIAL
        .get_or_init(Material::from_environment)
        .as_ref()
        .filter(|_| !proxy.is_empty());
    let mut stream = match (material, proxy.rsplit_once(':')) {
        (Some(material), _) => {
            let connection = connect(host, port).map_err(failed(format!("{host}:{port}")))?;
            let _ = connection.set_nodelay(true);
            let stream = tls::connected(material, host, connection)?;
            if let Stream::Client(tls) = &stream {
                tls.sock.set_read_timeout(timeout)?;
            }
            stream
        }
        (None, Some((proxy_host, proxy_port))) => {
            let proxy = format!("to the segment TLS proxy {proxy_host}:{proxy_port}");
            let proxy_port = proxy_port
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SEGMENT_PROXY: bad port"))?;
            let mut stream = connect(proxy_host, proxy_port).map_err(failed(proxy))?;
            writeln!(stream, "CONNECT {host} {port}")?;
            Stream::Plain(stream)
        }
        (None, None) => Stream::Plain(connect(host, port).map_err(failed(format!("{host}:{port}")))?),
    };
    if let Stream::Plain(plain) = &stream {
        plain.set_read_timeout(timeout)?;
    }
    writeln!(stream, "{}", sig1::sign(var("ISC_PASSWORD").as_bytes(), request, now(), &nonce()?))?;
    stream.flush()?;
    Ok(BufReader::new(stream))
}
