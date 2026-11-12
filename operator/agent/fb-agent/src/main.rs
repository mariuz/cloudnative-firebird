//! The instance pods' agent (docs/rust-agent-roadmap.md): the signed request protocol (Phase 1)
//! the replicas' segment puller (Phase 2) and the segment server's front (Phase 3).
//!
//!   fb-agent version
//!   fb-agent request <host> <request...>   one request to a segment server, the reply on stdout
//!                                          (as segment-request.pl); ISC_PASSWORD, SEGMENT_PORT,
//!                                          SEGMENT_PROXY (the segment TLS proxy's client side)
//!   fb-agent serve                         answers PING to signed requests, on SEGMENT_LISTEN
//!                                          (host:port) or SEGMENT_PORT (0: a free port);
//!                                          ISC_PASSWORD, AUTH_WINDOW_SECONDS
//!   fb-agent puller                        the replicas' segment puller (puller.rs)
//!   fb-agent segment-server -- <command>   the segment server's front, with the Perl server
//!                                          behind it (server.rs)

mod background;
mod backups;
mod client;
mod promote;
mod puller;
mod role;
mod seed;
mod server;
mod sql;
mod state;
mod status;

use client::{now, var};
use sig1::Verifier;
use std::env;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// As the Perl server: a client that sends nothing is dropped after this long.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

fn request(host: &str, request: &str) -> io::Result<()> {
    let mut reply = client::open(host, request, None)?;
    io::copy(&mut reply, &mut io::stdout().lock())?;
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
        ["puller"] => puller::run(),
        ["segment-server", "--", backend @ ..] => server::run(&backend.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
        _ => {
            eprintln!("usage: fb-agent version | request <host> <request...> | serve | puller | segment-server -- <backend command...>");
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
