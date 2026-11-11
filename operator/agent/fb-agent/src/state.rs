//! The database's state from the local Firebird server, over one service attachment that stays
//! open (docs/rust-agent-segment-server.md, step 2). segment-server.pl starts `fbsvcmgr` for
//! every STATE, which the Lease holder asks every few seconds.
//!
//! The attachment lives in its own thread. A request waits for it at most the query limit
//! (LOCAL_QUERY_TIMEOUT_SECONDS, 10 s as the Perl server's `timeout`): the client library has no
//! read timeout, and a hung server must not hold the request. Whenever this cannot tell the
//! state (no attachment, an error it does not know, the limit, a call still in flight), the
//! caller passes the request to the Perl server, which asks with `fbsvcmgr` as before.

use firebird_wire::{ConnectConfig, ServiceManager, WireCrypt};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TrySendError};
use std::thread;
use std::time::Duration;

/// `isc_spb_sts_hdr_pages`: the header page only, as `fbsvcmgr ... sts_hdr_pages`.
const HEADER_PAGES: u32 = 0x04;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Online,
    Shutdown,
}

/// What the header statistics say, as segment-server.pl reads the output of fbsvcmgr: shut down
/// when the attributes say so. Firebird 6 refuses the statistics of a database in full shutdown
/// with "database ... shutdown", which is the same answer.
fn state_of(statistics: Result<String, String>) -> Option<State> {
    match statistics {
        Ok(text) if text.contains("shutdown") => Some(State::Shutdown),
        Ok(_) => Some(State::Online),
        Err(error) if error.lines().any(|line| line.contains("database") && line.contains("shutdown")) => Some(State::Shutdown),
        Err(_) => None,
    }
}

pub struct Probe {
    requests: SyncSender<Sender<Option<State>>>,
    limit: Duration,
}

impl Probe {
    /// `database`: the database file; `port`: the local server's; the credentials of the sidecar.
    pub fn start(database: String, port: u16, user: String, password: String, limit: Duration) -> Probe {
        // room for one request: with one waiting already, the attachment is stuck
        let (requests, queue) = mpsc::sync_channel::<Sender<Option<State>>>(1);
        thread::spawn(move || serve(queue, database, port, user, password));
        Probe { requests, limit }
    }

    /// The state, or None when it cannot be told within the limit.
    pub fn state(&self) -> Option<State> {
        let (reply, answer) = mpsc::channel();
        match self.requests.try_send(reply) {
            Ok(()) => answer.recv_timeout(self.limit).ok().flatten(),
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => None,
        }
    }
}

fn serve(queue: Receiver<Sender<Option<State>>>, database: String, port: u16, user: String, password: String) {
    let config = ConnectConfig::new()
        .host("127.0.0.1")
        .port(port)
        .user(user)
        .password(password)
        .wire_crypt(WireCrypt::Enabled)
        .connect_timeout(Duration::from_secs(5));
    let mut service: Option<ServiceManager> = None;
    let mut said = false;
    for reply in queue {
        let mut state = None;
        // a second attempt with a new attachment: the one kept may be from before a server restart
        for _ in 0..2 {
            if service.is_none() {
                match ServiceManager::attach(&config) {
                    Ok(attached) => {
                        if !said {
                            println!("database state from a service attachment to the local server (no fbsvcmgr per request)");
                            said = true;
                        }
                        service = Some(attached);
                    }
                    Err(error) => {
                        if said {
                            println!("cannot attach to the local server's services ({error}): the state is asked with fbsvcmgr");
                            said = false;
                        }
                        break;
                    }
                }
            }
            let Some(attached) = service.as_mut() else { break };
            let statistics = attached.statistics(&database, HEADER_PAGES).map_err(|error| error.to_string());
            // the client library closes the attachment after a server error: a new one next time
            if statistics.is_err() {
                service = None;
            }
            state = state_of(statistics);
            if state.is_some() {
                break;
            }
        }
        let _ = reply.send(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_state_as_the_perl_server() {
        let online = "Database header page information:\n\tFlags\t\t\t0\n\tAttributes\t\tforce write\n";
        let shut = "Database header page information:\n\tAttributes\t\tforce write, full shutdown\n";
        assert_eq!(state_of(Ok(online.into())), Some(State::Online));
        assert_eq!(state_of(Ok(shut.into())), Some(State::Shutdown));
        // Firebird 6 refuses the statistics of a database in full shutdown
        assert_eq!(
            state_of(Err("database /var/lib/firebird/data/mydb.fdb shutdown (gds 335544528)".into())),
            Some(State::Shutdown)
        );
        // anything else is unknown: the Perl server is asked
        assert_eq!(state_of(Err("connection is closed".into())), None);
        assert_eq!(state_of(Err("Error writing data to the connection".into())), None);
    }

    #[test]
    fn gives_up_without_a_server() {
        // nothing listens on port 1: no attachment, no state, and no waiting for the limit
        let probe = Probe::start("/nowhere.fdb".into(), 1, "SYSDBA".into(), "x".into(), Duration::from_secs(10));
        let started = std::time::Instant::now();
        assert_eq!(probe.state(), None);
        assert_eq!(probe.state(), None);
        assert!(started.elapsed() < Duration::from_secs(8));
    }
}
