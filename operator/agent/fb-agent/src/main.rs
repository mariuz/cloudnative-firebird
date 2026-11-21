//! The instance pods' agent (docs/rust-agent-roadmap.md): the signed request protocol (Phase 1)
//! the replicas' segment puller (Phase 2) and the segment server's front (Phase 3).
//!
//!   fb-agent version
//!   fb-agent request <host> <request...>   one request to a segment server, the reply on stdout
//!                                          ISC_PASSWORD, SEGMENT_PORT, SEGMENT_TLS (with
//!                                          SEGMENT_TLS_DIR: mutual TLS)
//!   fb-agent serve                         answers PING to signed requests, on SEGMENT_LISTEN
//!                                          (host:port) or SEGMENT_PORT (0: a free port);
//!                                          ISC_PASSWORD, AUTH_WINDOW_SECONDS
//!   fb-agent puller                        the replicas' segment puller (puller.rs)
//!   fb-agent fetch-seed | replica-control | set-repl-seq | reposition
//!                                          the replication init container's helpers (init.rs)
//!   fb-agent lease-holder                  the primary Lease holder sidecar (lease.rs)
//!   fb-agent metrics-exporter              the custom metrics exporter sidecar (metrics.rs)
//!   fb-agent backup-file | fetch-segments | pitr-plan | cut-segment | switchover | failover |
//!            sync-standby                  the Jobs' programs (jobs.rs)
//!   fb-agent segment-server                the segment server (server.rs)

mod background;
mod backups;
mod client;
mod databases;
mod init;
mod isolation;
mod jobs;
mod lease;
mod metrics;
mod promote;
mod puller;
mod role;
mod seed;
mod server;
mod sql;
mod state;
mod status;
mod tls;

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

/// `fb-agent request`: with SEGMENT_DATABASE, about that declarative database (databases.rs)
fn request(host: &str, request: &str) -> io::Result<()> {
    let mut reply = client::open(host, &databases::request_for_environment(request), None)?;
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

/// How a Job's program ends: its message as the Perl script's `die` printed it, and its status.
fn job(done: Result<(), jobs::Exit>) -> ExitCode {
    let _ = io::stdout().flush();
    match done {
        Ok(()) => ExitCode::SUCCESS,
        Err(exit) => {
            eprint!("{}", exit.message);
            ExitCode::from(exit.code)
        }
    }
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
        ["isolation-check"] => isolation::command().map_err(io::Error::other),
        ["lease-holder"] => lease::run(),
        ["metrics-exporter"] => metrics::run(),
        // the Jobs' programs: their own messages and exit status (jobs.rs)
        ["backup-file", rest @ ..] => return job(jobs::backup_file(rest)),
        ["fetch-segments"] => return job(jobs::fetch_segments()),
        ["pitr-plan", rest @ ..] => return job(jobs::pitr_plan(rest)),
        ["cut-segment", rest @ ..] => return job(jobs::cut_segment(rest)),
        ["switchover"] => return job(jobs::switchover()),
        ["failover"] => return job(jobs::failover()),
        ["sync-standby"] => return job(jobs::sync_standby()),
        ["fetch-seed", host, target] => init::fetch_seed(host, target),
        // the declarative databases REPLICATED_DATABASES_FILE names, one file name per line, as
        // the segment server and the puller read them (init-databases.sh runs the init for each)
        ["databases"] => {
            for database in databases::from_environment() {
                println!("{}", database.file);
            }
            Ok(())
        }
        ["replica-control", arguments @ ..] => init::replica_control(arguments),
        ["set-repl-seq", database, sequence] => init::set_sequence(database, sequence),
        ["reposition", sequence, control] => init::reposition(sequence, control),
        ["segment-server"] => server::run(),
        _ => {
            eprintln!("usage: fb-agent version | request <host> <request...> | serve | puller | isolation-check | fetch-seed <host> <file> | databases | replica-control ... | set-repl-seq <database> <sequence> | reposition <U> <control> | lease-holder | metrics-exporter | backup-file ... | fetch-segments | pitr-plan ... | cut-segment <file> <length> | switchover | failover | sync-standby | segment-server");
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
