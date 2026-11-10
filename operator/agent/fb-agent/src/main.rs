//! The instance pods' agent (docs/rust-agent-roadmap.md). Phase 1: the signed request protocol.
//!
//!   fb-agent version
//!   fb-agent request <host> <request...>   one request to a segment server, the reply on stdout
//!                                          (as segment-request.pl); ISC_PASSWORD, SEGMENT_PORT,
//!                                          SEGMENT_PROXY (the segment TLS proxy's client side)
//!   fb-agent serve                         answers PING to signed requests, on SEGMENT_LISTEN
//!                                          (host:port) or SEGMENT_PORT (0: a free port);
//!                                          ISC_PASSWORD, AUTH_WINDOW_SECONDS

use sig1::Verifier;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// As the Perl server: a client that sends nothing is dropped after this long.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn var(name: &str) -> String {
    env::var(name).unwrap_or_default()
}

/// 16 random bytes in hex, from the kernel.
fn nonce() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn connect(host: &str, port: u16) -> io::Result<TcpStream> {
    let mut last = io::Error::new(io::ErrorKind::NotFound, format!("{host}: no address"));
    for address in (host, port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(err) => last = err,
        }
    }
    Err(last)
}

fn request(host: &str, request: &str) -> io::Result<()> {
    let port: u16 = var("SEGMENT_PORT").parse().unwrap_or(3051);
    // segment TLS: through the local proxy, which opens mutual TLS to the host named on the first line
    let mut stream = match var("SEGMENT_PROXY").rsplit_once(':') {
        Some((proxy_host, proxy_port)) => {
            let proxy_port = proxy_port
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SEGMENT_PROXY: bad port"))?;
            let mut stream = connect(proxy_host, proxy_port)?;
            writeln!(stream, "CONNECT {host} {port}")?;
            stream
        }
        None => connect(host, port)?,
    };
    writeln!(stream, "{}", sig1::sign(var("ISC_PASSWORD").as_bytes(), request, now(), &nonce()?))?;
    io::copy(&mut stream, &mut io::stdout().lock())?;
    Ok(())
}

fn answer(mut stream: TcpStream, verifier: &Mutex<Verifier>) -> io::Result<()> {
    stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
    let mut line = String::new();
    BufReader::new(stream.try_clone()?).take(65536).read_line(&mut line)?;
    let line = line.trim_end_matches(['\r', '\n']);
    let verified = verifier.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).verify(line, now());
    match verified {
        Err(denied) => writeln!(stream, "ERR unauthorized ({denied})"),
        Ok(request) if request.command == "PING" => writeln!(stream, "OK"),
        Ok(_) => writeln!(stream, "ERR not available in fb-agent yet"),
    }
}

fn serve() -> io::Result<()> {
    let listen = var("SEGMENT_LISTEN");
    let port = var("SEGMENT_PORT");
    let listener = if listen.is_empty() {
        TcpListener::bind(("0.0.0.0", port.parse::<u16>().unwrap_or(3051)))?
    } else {
        TcpListener::bind(listen.as_str())?
    };
    let window = var("AUTH_WINDOW_SECONDS").parse().unwrap_or(300);
    let verifier = Arc::new(Mutex::new(Verifier::new(var("ISC_PASSWORD").as_bytes(), window)));
    println!("fb-agent {} listening on {}", env!("CARGO_PKG_VERSION"), listener.local_addr()?);
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let verifier = Arc::clone(&verifier);
        thread::spawn(move || {
            let _ = answer(stream, &verifier);
        });
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let result = match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["version"] => {
            println!("fb-agent {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        ["request", host, words @ ..] if !words.is_empty() => request(host, &words.join(" ")),
        ["serve"] => serve(),
        _ => {
            eprintln!("usage: fb-agent version | request <host> <request...> | serve");
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("fb-agent: {err}");
            ExitCode::FAILURE
        }
    }
}
