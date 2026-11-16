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
use std::io::Write;
use std::process::{Command, Stdio};
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
    /// Without the Perl server behind the agent: when the server refuses the agent's own
    /// attachment, the image's isql and fbsvcmgr are asked, as the Perl server asked them
    pub tools: bool,
}

/// What a tool wrote, on one line.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

impl Database {
    /// One of the image's client tools against the local server, under timeout(1) when it has a
    /// limit, as segment-server.pl ran them: what it wrote, or why it failed.
    fn tool(&self, limit: Option<Duration>, program: &str, arguments: &[&str], input: Option<&str>) -> Result<String, String> {
        let mut command = match limit {
            Some(limit) => {
                let mut command = Command::new("timeout");
                command.args(["-k", "5", &limit.as_secs().to_string(), program]);
                command
            }
            None => Command::new(program),
        };
        command
            .args(arguments)
            .env("ISC_USER", &self.user)
            .env("ISC_PASSWORD", &self.password)
            .stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|e| format!("{program}: {e}"))?;
        if let (Some(input), Some(mut stdin)) = (input, child.stdin.take()) {
            let _ = stdin.write_all(input.as_bytes());
        }
        let output = child.wait_with_output().map_err(|e| format!("{program}: {e}"))?;
        let text = String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            return Ok(text);
        }
        let timed_out = matches!(output.status.code(), Some(124) | Some(137));
        if let (true, Some(limit)) = (timed_out, limit) {
            println!("{program} timed out after {}s", limit.as_secs());
            if text.trim().is_empty() {
                return Err(format!("timed out after {}s", limit.as_secs()));
            }
        }
        Err(one_line(&text))
    }

    /// `fbsvcmgr ... action_db_stats ... sts_hdr_pages`: whether it succeeded, and what it wrote.
    pub fn tool_statistics(&self) -> (bool, String) {
        let arguments = ["localhost:service_mgr", "action_db_stats", "dbname", &self.path, "sts_hdr_pages"];
        match self.tool(Some(self.query_limit), "fbsvcmgr", &arguments, None) {
            Ok(text) => (true, text),
            Err(text) => (false, text),
        }
    }

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
        let asked = within(self.query_limit, move || {
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
        });
        match asked {
            Err(why) if self.tools => {
                // isql, as the Perl server's live_value; a value printed before the limit is not trusted
                let sql = format!("SET LIST ON; SELECT {expression} AS V FROM MON$DATABASE;\n");
                let target = format!("localhost:{}", self.path);
                let written = self.tool(Some(self.query_limit), "isql", &["-q", &target], Some(&sql));
                let value = written.ok().and_then(|text| {
                    text.lines()
                        .filter_map(|line| line.strip_prefix('V').filter(|rest| rest.starts_with([' ', '\t'])))
                        .filter_map(|rest| rest.split_whitespace().next().map(str::to_string))
                        .next_back()
                });
                value.ok_or(why)
            }
            asked => asked,
        }
    }

    /// A statement without rows, committed (the backup lock: ALTER DATABASE BEGIN / END BACKUP).
    pub fn execute(&self, statement: &str) -> Result<(), String> {
        let (config, sql) = (self.config(), statement.to_string());
        let done = within(self.command_limit, move || {
            let mut connection = Connection::connect(&config).map_err(|e| e.to_string())?;
            let done = connection.exec_immediate(None, &sql).map_err(|e| e.to_string());
            let _ = connection.close();
            done
        });
        match done {
            Err(why) if self.tools => {
                // isql -b, as the Perl server's live_sql
                let sql = format!("{};\n", statement.trim_end().trim_end_matches(';'));
                let target = format!("localhost:{}", self.path);
                self.tool(Some(self.command_limit), "isql", &["-q", "-b", &target], Some(&sql))
                    .map(|_| ())
                    .map_err(|tool| format!("{why}; isql: {tool}"))
            }
            done => done,
        }
    }
}

/// Why a call to the local server gave nothing.
#[derive(Debug, PartialEq, Eq)]
pub enum Failed {
    /// The server refused or could not be reached: another client (fbsvcmgr) may still get through.
    /// With `tools`, fbsvcmgr was asked too, and this is what it said
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

    /// `fbsvcmgr action_properties dbname <database> ...`, within the command limit. `words`: the
    /// same for fbsvcmgr, or (with "gfix" first) for gfix, which the Perl server ran for these.
    fn properties(&self, parameters: &[u8], words: &[&str]) -> Result<(), Failed> {
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
            Ok(Err(_)) if self.tools => {
                let target = format!("localhost:{}", self.path);
                let (program, arguments) = match words.split_first() {
                    Some((&"gfix", options)) => ("gfix", [options, &[target.as_str()]].concat()),
                    _ => (
                        "fbsvcmgr",
                        [&["localhost:service_mgr", "action_properties", "dbname", self.path.as_str()], words].concat(),
                    ),
                };
                self.tool(Some(self.command_limit), program, &arguments, None)
                    .map(|_| ())
                    .map_err(Failed::Refused)
            }
            Ok(Err(reason)) => Err(Failed::Refused(reason)),
            Err(_) => Err(Failed::TimedOut),
        }
    }

    /// Full shutdown, forced at once (`prp_shutdown_mode prp_sm_full prp_force_shutdown 0`).
    pub fn shut_down(&self) -> Result<(), Failed> {
        self.properties(
            &[PRP_SHUTDOWN_MODE, PRP_SM_FULL, PRP_FORCE_SHUTDOWN, 0, 0, 0, 0],
            &["prp_shutdown_mode", "prp_sm_full", "prp_force_shutdown", "0"],
        )
    }

    /// Online (`prp_online_mode prp_sm_normal`).
    pub fn bring_online(&self) -> Result<(), Failed> {
        self.properties(&[PRP_ONLINE_MODE, PRP_SM_NORMAL], &["prp_online_mode", "prp_sm_normal"])
    }

    /// Not a replica any more (`gfix -replica none`). Only once the database is online.
    pub fn replica_none(&self) -> Result<(), Failed> {
        self.properties(&[PRP_REPLICA_MODE, PRP_RM_NONE], &["gfix", "-replica", "none"])
    }

    /// A read-only replica again (`gfix -replica read_only`).
    pub fn replica_read_only(&self) -> Result<(), Failed> {
        self.properties(&[PRP_REPLICA_MODE, PRP_RM_READ_ONLY], &["gfix", "-replica", "read_only"])
    }

    /// An nbackup of the database into `file`, by the server (`action_nbak`). Not bounded.
    pub fn nbackup(&self, file: &str, level: u32) -> Result<(), String> {
        let taken = ServiceManager::attach(&self.services())
            .map_err(|e| e.to_string())
            .and_then(|mut service| {
                let done = service.nbackup(&self.path, file, level, 0).map(|_| ()).map_err(|e| e.to_string());
                let _ = service.close();
                done
            });
        match taken {
            Err(why) if self.tools => {
                let level = level.to_string();
                let arguments = [
                    "localhost:service_mgr",
                    "action_nbak",
                    "dbname",
                    &self.path,
                    "nbk_file",
                    file,
                    "nbk_level",
                    &level,
                ];
                self.tool(None, "fbsvcmgr", &arguments, None)
                    .map(|_| ())
                    .map_err(|tool| format!("{why}; fbsvcmgr: {tool}"))
            }
            taken => taken,
        }
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
            tools: false,
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
