//! The client side of the segment servers' protocol: one signed request per connection.

use std::env;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
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
/// signed with ISC_PASSWORD. With SEGMENT_PROXY (segment TLS) the connection goes through the
/// local proxy, which opens mutual TLS to the host named on the first line. `timeout` bounds each
/// read of the reply.
pub fn open(host: &str, request: &str, timeout: Option<Duration>) -> io::Result<BufReader<TcpStream>> {
    let port: u16 = var("SEGMENT_PORT").parse().unwrap_or(3051);
    let failed = |target: String| move |err: io::Error| io::Error::new(err.kind(), format!("connect {target}: {err}"));
    let mut stream = match var("SEGMENT_PROXY").rsplit_once(':') {
        Some((proxy_host, proxy_port)) => {
            let proxy = format!("to the segment TLS proxy {proxy_host}:{proxy_port}");
            let proxy_port = proxy_port
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SEGMENT_PROXY: bad port"))?;
            let mut stream = connect(proxy_host, proxy_port).map_err(failed(proxy))?;
            writeln!(stream, "CONNECT {host} {port}")?;
            stream
        }
        None => connect(host, port).map_err(failed(format!("{host}:{port}")))?,
    };
    stream.set_read_timeout(timeout)?;
    writeln!(stream, "{}", sig1::sign(var("ISC_PASSWORD").as_bytes(), request, now(), &nonce()?))?;
    Ok(BufReader::new(stream))
}
