//! Statements for the pod's Firebird server, each on its own short database attachment
//! (docs/rust-agent-firebird-access.md), as the Perl scripts run isql for each. No attachment is
//! kept: one held through a full shutdown or a promotion would be in their way.
//!
//! The service calls (full shutdown, online, nbackup) attach to the server's services the same
//! way, where the Perl scripts run fbsvcmgr.
//!
//! Every call has a deadline, except an nbackup, which takes as long as the database is large. The client library has no read timeout, so the call runs in its
//! own thread; when the server does not answer in time the caller goes on without it (the thread
//! ends when the server does answer, or with the process).

use firebird_wire::{ConnectConfig, Connection, ServiceManager, Value, WireCrypt};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// The local server and its database, with the limits of segment-server.pl: reads
/// (LOCAL_QUERY_TIMEOUT_SECONDS, 10) and changes (LOCAL_COMMAND_TIMEOUT_SECONDS, 60).
#[derive(Clone)]
pub struct Database {
    pub path: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub query_limit: Duration,
    pub command_limit: Duration,
}

impl Database {
    fn config(&self) -> ConnectConfig {
        ConnectConfig::new()
            .host("127.0.0.1")
            .port(self.port)
            .database(self.path.clone())
            .user(self.user.clone())
            .password(self.password.clone())
            .wire_crypt(WireCrypt::Enabled)
            .connect_timeout(Duration::from_secs(5))
    }

    /// `SELECT <expression> FROM MON$DATABASE`: the value as text, or the reason it has none.
    pub fn value(&self, expression: &str) -> Result<String, String> {
        let (config, sql) = (self.config(), format!("SELECT {expression} FROM MON$DATABASE"));
        within(self.query_limit, move || {
            let mut connection = Connection::connect(&config).map_err(|e| e.to_string())?;
            let answer = (|| {
                let transaction = connection.begin()?;
                let mut statement = connection.prepare(&transaction, &sql)?;
                statement.execute(&mut connection, &transaction, &[])?;
                let row = statement.fetch(&mut connection)?;
                statement.drop_statement(&mut connection)?;
                transaction.commit(&mut connection)?;
                Ok::<_, firebird_wire::Error>(row)
            })()
            .map_err(|e| e.to_string());
            let _ = connection.close();
            match answer?.and_then(|row| row.into_values().into_iter().next()) {
                Some(Value::Text(text)) => Ok(text.trim_end().to_string()),
                Some(Value::Bytes(bytes)) => Ok(String::from_utf8_lossy(&bytes).trim_end().to_string()),
                Some(Value::BigInt(n)) => Ok(n.to_string()),
                Some(Value::Int(n)) => Ok(n.to_string()),
                Some(Value::Short(n)) => Ok(n.to_string()),
                other => Err(format!("no value ({other:?})")),
            }
        })
    }

    /// A statement without rows, committed (the backup lock: ALTER DATABASE BEGIN / END BACKUP).
    pub fn execute(&self, statement: &str) -> Result<(), String> {
        let (config, sql) = (self.config(), statement.to_string());
        within(self.command_limit, move || {
            let mut connection = Connection::connect(&config).map_err(|e| e.to_string())?;
            let done = connection.exec_immediate(None, &sql).map_err(|e| e.to_string());
            let _ = connection.close();
            done
        })
    }
}

/// Why a call to the local server gave nothing.
#[derive(Debug, PartialEq, Eq)]
pub enum Failed {
    /// The server refused or could not be reached: another client (fbsvcmgr) may still get through
    Refused(String),
    /// No answer within the limit: the server is not answering anyone
    TimedOut,
}

// isc_action_svc_properties and its parameters (ibase.h)
const ACTION_PROPERTIES: u8 = 8;
const SPB_DBNAME: u8 = 106;
const PRP_FORCE_SHUTDOWN: u8 = 41;
const PRP_SHUTDOWN_MODE: u8 = 44;
const PRP_ONLINE_MODE: u8 = 45;
const PRP_REPLICA_MODE: u8 = 46;
const PRP_RM_NONE: u8 = 0;
const PRP_RM_READ_ONLY: u8 = 1;
const PRP_SM_NORMAL: u8 = 0;
const PRP_SM_FULL: u8 = 3;

impl Database {
    fn services(&self) -> ConnectConfig {
        ConnectConfig::new()
            .host("127.0.0.1")
            .port(self.port)
            .user(self.user.clone())
            .password(self.password.clone())
            .wire_crypt(WireCrypt::Enabled)
            .connect_timeout(Duration::from_secs(5))
    }

    /// `fbsvcmgr action_properties dbname <database> ...`, within the command limit.
    fn properties(&self, parameters: &[u8]) -> Result<(), Failed> {
        let mut request = vec![ACTION_PROPERTIES, SPB_DBNAME];
        request.extend((self.path.len() as u16).to_le_bytes());
        request.extend(self.path.as_bytes());
        request.extend(parameters);
        let config = self.services();
        let (done, answer) = mpsc::channel();
        thread::spawn(move || {
            let run = ServiceManager::attach(&config).and_then(|mut service| {
                let ran = service.run(&request).map(|_| ());
                let _ = service.close();
                ran
            });
            let _ = done.send(run.map_err(|e| e.to_string()));
        });
        match answer.recv_timeout(self.command_limit) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(reason)) => Err(Failed::Refused(reason)),
            Err(_) => Err(Failed::TimedOut),
        }
    }

    /// Full shutdown, forced at once (`prp_shutdown_mode prp_sm_full prp_force_shutdown 0`).
    pub fn shut_down(&self) -> Result<(), Failed> {
        self.properties(&[PRP_SHUTDOWN_MODE, PRP_SM_FULL, PRP_FORCE_SHUTDOWN, 0, 0, 0, 0])
    }

    /// Online (`prp_online_mode prp_sm_normal`).
    pub fn bring_online(&self) -> Result<(), Failed> {
        self.properties(&[PRP_ONLINE_MODE, PRP_SM_NORMAL])
    }

    /// Not a replica any more (`gfix -replica none`). Only once the database is online.
    pub fn replica_none(&self) -> Result<(), Failed> {
        self.properties(&[PRP_REPLICA_MODE, PRP_RM_NONE])
    }

    /// A read-only replica again (`gfix -replica read_only`).
    pub fn replica_read_only(&self) -> Result<(), Failed> {
        self.properties(&[PRP_REPLICA_MODE, PRP_RM_READ_ONLY])
    }

    /// An nbackup of the database into `file`, by the server (`action_nbak`). Not bounded.
    pub fn nbackup(&self, file: &str, level: u32) -> Result<(), String> {
        let mut service = ServiceManager::attach(&self.services()).map_err(|e| e.to_string())?;
        let done = service.nbackup(&self.path, file, level, 0).map(|_| ()).map_err(|e| e.to_string());
        let _ = service.close();
        done
    }
}

fn within<T: Send + 'static>(limit: Duration, call: impl FnOnce() -> Result<T, String> + Send + 'static) -> Result<T, String> {
    let (done, answer) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(call());
    });
    answer
        .recv_timeout(limit)
        .unwrap_or_else(|_| Err(format!("no answer from the local server within {} s", limit.as_secs())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gives_up_without_a_server_and_at_the_deadline() {
        let database = Database {
            path: "/nowhere.fdb".into(),
            port: 1,
            user: "SYSDBA".into(),
            password: "x".into(),
            query_limit: Duration::from_secs(10),
            command_limit: Duration::from_secs(10),
        };
        assert!(database.value("1").is_err());
        assert!(database.execute("ALTER DATABASE END BACKUP").is_err());
        assert!(matches!(database.shut_down(), Err(Failed::Refused(_))));
        assert!(matches!(database.bring_online(), Err(Failed::Refused(_))));
        assert!(database.nbackup("/nowhere.nbk", 0).is_err());
        let slow = within(Duration::from_millis(50), || {
            thread::sleep(Duration::from_secs(2));
            Ok(1)
        });
        assert_eq!(slow, Err("no answer from the local server within 0 s".into()));
    }
}
