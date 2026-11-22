//! The custom metrics exporter (`fb-agent metrics-exporter`), metrics-exporter.pl with the
//! same environment, output and log lines: serves GET /metrics in the Prometheus text format.
//! Every scrape reads the queries the operator rendered from the cluster's ConfigMaps and
//! Secrets (QUERIES_FILE, a mounted Secret: a changed query applies at the next scrape, without
//! a restart), runs each one with isql against the local server, and turns the rows into
//! metrics: firebird_<query>_<column>, with the LABEL columns as labels. Queries marked primary
//! run only where the database is not a replica.
//!
//! The queries run through isql by default: the values are what it prints, as the Perl exporter
//! served them. With QUERY_CLIENT=agent (monitoring.customQueriesClient: agent, since v0.186.0)
//! they run over the agent's own Firebird client instead: one attachment for the whole scrape,
//! each statement in its own read-only read-committed transaction, no process started. The values
//! are typed, which changes their text where isql printed something else (`cell`): a boolean is 1
//! or 0, a date or timestamp the seconds since the Unix epoch (UTC), a time the seconds since
//! midnight, a float its shortest exact form, a text blob its text; a NULL is no value.
//!
//! QUERIES_FILE, one block per query:
//!   query <name>
//!   primary <0|1>
//!   sql <statement; "\n" a newline, "\\" a backslash>
//!   metric <column> <GAUGE|COUNTER|LABEL|DISCARD> <description>
//!   end
//!
//! Besides: firebird_up (the database answers), firebird_exporter_query_error and
//! firebird_exporter_query_duration_seconds per query.
//!
//! Environment: DATABASE_PATH, QUERIES_FILE (/custom-queries/queries), METRICS_PORT (9187),
//! QUERY_TIMEOUT_SECONDS (10), QUERY_CLIENT (isql, or agent), ISQL (isql), TMPDIR (/tmp),
//! FIREBIRD_PORT (3050), ISC_USER (SYSDBA), ISC_PASSWORD.

use crate::client::var;
use firebird_wire::{ColumnMeta, ConnectConfig, Connection, IsolationLevel, TransactionBuilder, Value, WireCrypt};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{self, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq)]
struct Metric {
    column: String,
    usage: String,
    description: String,
}

#[derive(Debug, PartialEq)]
struct Query {
    name: String,
    primary: bool,
    sql: String,
    metrics: Vec<Metric>,
}

/// The queries of the file's text; a block without a statement is left out.
fn queries(text: &str) -> Vec<Query> {
    let mut queries = Vec::new();
    let mut current: Option<Query> = None;
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(name) = line
            .strip_prefix("query ")
            .filter(|n| !n.is_empty() && !n.contains(char::is_whitespace))
        {
            current = Some(Query {
                name: name.to_string(),
                primary: false,
                sql: String::new(),
                metrics: Vec::new(),
            });
            continue;
        }
        let Some(query) = current.as_mut() else { continue };
        if line == "primary 0" || line == "primary 1" {
            query.primary = line.ends_with('1');
        } else if let Some(escaped) = line.strip_prefix("sql ") {
            // "\n" a newline, a backslash before anything else that character
            let mut sql = String::new();
            let mut characters = escaped.chars();
            while let Some(character) = characters.next() {
                match character {
                    '\\' => match characters.next() {
                        Some('n') => sql.push('\n'),
                        Some(other) => sql.push(other),
                        None => sql.push('\\'),
                    },
                    other => sql.push(other),
                }
            }
            query.sql = sql;
        } else if let Some(rest) = line.strip_prefix("metric ") {
            // ^metric (\S+) (GAUGE|COUNTER|LABEL|DISCARD) ?(.*)$
            let Some((column, rest)) = rest.split_once(' ') else { continue };
            if column.is_empty() || column.contains(char::is_whitespace) {
                continue;
            }
            let usage = ["GAUGE", "COUNTER", "LABEL", "DISCARD"]
                .into_iter()
                .find(|usage| rest.starts_with(usage));
            if let Some(usage) = usage {
                let description = &rest[usage.len()..];
                query.metrics.push(Metric {
                    column: column.to_string(),
                    usage: usage.to_string(),
                    description: description.strip_prefix(' ').unwrap_or(description).to_string(),
                });
            }
        } else if line == "end" {
            if let Some(query) = current.take().filter(|query| !query.sql.is_empty()) {
                queries.push(query);
            }
        }
    }
    queries
}

type Row = HashMap<String, String>;

/// The rows of isql's LIST output: hashes of upper-case column names.
fn rows(output: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    let mut row: Option<Row> = None;
    for line in output.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.trim().is_empty() {
            rows.extend(row.take());
            continue;
        }
        // ^(\S+)\s+(.*?)\s*$ or ^(\S+)$: a line that begins with a blank is not a column
        if line.starts_with(char::is_whitespace) {
            continue;
        }
        let (column, value) = match line.split_once(char::is_whitespace) {
            Some((column, value)) => (column, value.trim()),
            None => (line, ""),
        };
        row.get_or_insert_with(Row::new).insert(column.to_uppercase(), value.to_string());
    }
    rows.extend(row);
    rows
}

/// Days from the Firebird epoch (1858-11-17) to the Unix epoch (1970-01-01)
const UNIX_EPOCH_DAY: i64 = 40_587;

/// A number with a decimal scale (NUMERIC, DECIMAL: the mantissa and a negative scale), as isql prints it.
fn scaled(mantissa: i128, scale: i32) -> String {
    if scale >= 0 {
        return (mantissa * 10i128.pow(scale as u32)).to_string();
    }
    let digits = (-scale) as usize;
    let text = mantissa.unsigned_abs().to_string();
    let text = format!("{text:0>width$}", width = digits + 1);
    let (whole, fraction) = text.split_at(text.len() - digits);
    format!("{}{whole}.{fraction}", if mantissa < 0 { "-" } else { "" })
}

/// Seconds, with the fraction only when there is one (the server's unit is 1/10000 s).
fn seconds(ten_thousandths: i64) -> String {
    if ten_thousandths % 10_000 == 0 {
        (ten_thousandths / 10_000).to_string()
    } else {
        format!("{}", ten_thousandths as f64 / 10_000.0)
    }
}

/// The text of a typed value (QUERY_CLIENT=agent); None for a NULL and what has no text (a binary blob, an array).
fn cell(value: &Value, scale: i32) -> Option<String> {
    let day = |days: i32| (i64::from(days) - UNIX_EPOCH_DAY) * 86_400 * 10_000;
    Some(match value {
        Value::Null | Value::Blob(_) | Value::Array(_) => return None,
        Value::Bool(b) => u8::from(*b).to_string(),
        Value::Short(n) => scaled(i128::from(*n), scale),
        Value::Int(n) => scaled(i128::from(*n), scale),
        Value::BigInt(n) => scaled(i128::from(*n), scale),
        Value::Int128(n) => scaled(*n, scale),
        Value::Float(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::DecFloat(d) => d.to_string(),
        Value::Text(text) => text.clone(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Value::Date(days) => seconds(day(*days)),
        Value::Time(time) => seconds(i64::from(*time)),
        Value::Timestamp(days, time) => seconds(day(*days) + i64::from(*time)),
        Value::TimeTz(t) => seconds(i64::from(t.utc_time)),
        Value::TimestampTz(t) => seconds(day(t.utc_date) + i64::from(t.utc_time)),
    })
}

/// A fetched row: upper-case column names (the alias, else the field) and their text.
fn typed_row(columns: &[ColumnMeta], values: &[Value], blobs: &HashMap<u64, String>) -> Row {
    columns
        .iter()
        .zip(values)
        .filter_map(|(column, value)| {
            let text = match value {
                Value::Blob(id) => blobs.get(id).cloned(),
                value => cell(value, column.scale),
            }?;
            Some((column.name().to_uppercase(), text))
        })
        .collect()
}

/// BLOB SUB_TYPE TEXT, whose content is read for its text
const BLOB_TEXT: i32 = 1;
const SQL_BLOB: i32 = 520;

/// Whether the column is a text blob; the type as the server describes it keeps the nullable bit (521).
fn text_blob(column: &ColumnMeta) -> bool {
    column.sql_type & !1 == SQL_BLOB && column.sub_type == BLOB_TEXT
}

/// One statement in its own read-only transaction: the rows, text blobs read.
fn query(connection: &mut Connection, sql: &str) -> Result<Vec<Row>, String> {
    let read_only = TransactionBuilder::new()
        .isolation(IsolationLevel::ReadCommittedRecordVersion)
        .read_only();
    let transaction = connection.begin_with(&read_only).map_err(|e| e.to_string())?;
    let ran = (|| {
        let mut statement = connection.prepare(&transaction, sql)?;
        statement.execute(connection, &transaction, &[])?;
        let columns = statement.columns().to_vec();
        let fetched = statement.fetch_all(connection)?;
        statement.drop_statement(connection)?;
        let mut rows = Vec::with_capacity(fetched.len());
        for row in fetched {
            let values = row.into_values();
            let mut blobs = HashMap::new();
            for (column, value) in columns.iter().zip(&values) {
                if let (Value::Blob(id), true) = (value, text_blob(column)) {
                    let content = connection.read_blob(&transaction, *id)?;
                    blobs.insert(*id, String::from_utf8_lossy(&content).into_owned());
                }
            }
            rows.push(typed_row(&columns, &values, &blobs));
        }
        Ok::<_, firebird_wire::Error>(rows)
    })();
    match ran {
        Ok(rows) => {
            transaction.commit(connection).map_err(|e| e.to_string())?;
            Ok(rows)
        }
        Err(error) => {
            let _ = transaction.rollback(connection);
            Err(error.to_string())
        }
    }
}

type Answer = Result<Vec<Row>, String>;

/// The scrape's attachment (QUERY_CLIENT=agent): a thread that holds it and runs the statements it
/// is sent. The client has no read timeout, so a statement that takes longer than the limit is left
/// to that thread, which ends with its answer, and the next statement gets a new attachment.
struct Session {
    config: ConnectConfig,
    limit: Duration,
    worker: Option<(Sender<String>, Receiver<Answer>)>,
}

impl Session {
    fn new(config: ConnectConfig, limit: Duration) -> Session {
        Session {
            config,
            limit,
            worker: None,
        }
    }

    fn start(config: ConnectConfig) -> (Sender<String>, Receiver<Answer>) {
        let (statements, inbox) = mpsc::channel::<String>();
        let (outbox, answers) = mpsc::channel::<Answer>();
        thread::spawn(move || {
            let mut connection = match Connection::connect(&config) {
                Ok(connection) => connection,
                Err(error) => {
                    for _ in inbox {
                        if outbox.send(Err(error.to_string())).is_err() {
                            break;
                        }
                    }
                    return;
                }
            };
            for sql in inbox {
                if outbox.send(query(&mut connection, &sql)).is_err() {
                    break;
                }
            }
            let _ = connection.close();
        });
        (statements, answers)
    }

    fn run(&mut self, sql: &str) -> Answer {
        let config = &self.config;
        let (statements, answers) = self.worker.get_or_insert_with(|| Session::start(config.clone()));
        let sent = statements.send(sql.to_string());
        let answer = match sent.map(|_| answers.recv_timeout(self.limit)) {
            Ok(Ok(answer)) => return answer,
            Ok(Err(RecvTimeoutError::Timeout)) => Err(format!("timed out after {}s", self.limit.as_secs())),
            _ => Err("the attachment was lost".to_string()),
        };
        self.worker = None;
        answer
    }
}

enum Client {
    Isql { isql: String, temporary: String },
    Agent { config: ConnectConfig },
}

struct Exporter {
    database: String,
    queries_file: String,
    timeout: String,
    client: Client,
}

impl Exporter {
    /// Runs one statement (isql -b, LIST mode): the rows, or the error.
    fn run_isql(&self, isql: &str, temporary: &str, sql: &str) -> Result<Vec<Row>, String> {
        let file = format!("{temporary}/metrics-query-{}.sql", process::id());
        let statement = sql.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
        fs::write(&file, format!("SET LIST ON;\n{statement};\n")).map_err(|e| format!("cannot write {file}: {e}"))?;
        // what isql says on either stream, in the order it says it
        let ran = Command::new("sh")
            .args([
                "-c",
                "exec \"$@\" 2>&1",
                "sh",
                "timeout",
                &self.timeout,
                isql,
                "-q",
                "-b",
                "-i",
                &file,
            ])
            .arg(format!("localhost:{}", self.database))
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output();
        let _ = fs::remove_file(&file);
        let output = ran.map_err(|e| format!("cannot fork: {e}"))?;
        let text = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            let message = text
                .split('\n')
                .map(|line| line.strip_suffix('\r').unwrap_or(line))
                .filter(|line| !line.trim().is_empty())
                .collect::<Vec<_>>()
                .join(" ");
            if !message.is_empty() {
                return Err(message);
            }
            let code = output.status.code().unwrap_or(0);
            return Err(if code == 124 {
                format!("timed out after {}s", self.timeout)
            } else {
                format!("isql exited with {code}")
            });
        }
        Ok(rows(&text))
    }

    /// One scrape: the text of /metrics.
    fn scrape(&self) -> String {
        let limit = Duration::from_secs(self.timeout.parse().unwrap_or(10));
        let mut session = match &self.client {
            Client::Agent { config } => Some(Session::new(config.clone(), limit)),
            Client::Isql { .. } => None,
        };
        let mut run_sql = |sql: &str| match (&self.client, session.as_mut()) {
            (_, Some(session)) => session.run(sql),
            (Client::Isql { isql, temporary }, None) => self.run_isql(isql, temporary, sql),
            (Client::Agent { .. }, None) => Err("no attachment".into()),
        };
        let mut lines: Vec<String> = Vec::new();
        let state = run_sql("SELECT MON$REPLICA_MODE AS REPLICA_MODE FROM MON$DATABASE");
        let up = state.as_ref().is_ok_and(|rows| !rows.is_empty());
        let replica = up
            && state
                .as_ref()
                .ok()
                .and_then(|rows| rows[0].get("REPLICA_MODE"))
                .is_some_and(|mode| mode != "0");
        lines.push("# HELP firebird_up Whether the database answers a query".into());
        lines.push("# TYPE firebird_up gauge".into());
        lines.push(format!("firebird_up {}", u8::from(up)));
        if let Err(error) = &state {
            if !error.is_empty() {
                println!("the database does not answer: {error}");
            }
        }
        let (mut errors, mut durations) = (Vec::new(), Vec::new());
        // name -> (help, type, samples), in the order of the names
        let mut families: BTreeMap<String, (String, String, Vec<String>)> = BTreeMap::new();
        let text = fs::read_to_string(&self.queries_file).unwrap_or_default();
        for query in queries(&text) {
            if query.primary && replica {
                continue;
            }
            let start = Instant::now();
            let answer = if up {
                run_sql(&query.sql)
            } else {
                Err("the database does not answer".into())
            };
            let label = label_value(&query.name);
            durations.push(format!(
                "firebird_exporter_query_duration_seconds{{query=\"{label}\"}} {:.3}",
                start.elapsed().as_secs_f64()
            ));
            errors.push(format!(
                "firebird_exporter_query_error{{query=\"{label}\"}} {}",
                u8::from(answer.is_err())
            ));
            let rows = match answer {
                Ok(rows) => rows,
                Err(error) => {
                    if up {
                        println!("query {}: {error}", query.name);
                    }
                    continue;
                }
            };
            for row in &rows {
                let labels: Vec<String> = query
                    .metrics
                    .iter()
                    .filter(|metric| metric.usage == "LABEL")
                    .filter_map(|metric| {
                        let value = row.get(&metric.column.to_uppercase()).filter(|value| *value != "<null>")?;
                        Some(format!("{}=\"{}\"", metric_name(&metric.column), label_value(value)))
                    })
                    .collect();
                let labels = if labels.is_empty() {
                    String::new()
                } else {
                    format!("{{{}}}", labels.join(","))
                };
                for metric in query.metrics.iter().filter(|m| m.usage == "GAUGE" || m.usage == "COUNTER") {
                    let Some(value) = row.get(&metric.column.to_uppercase()).filter(|value| numeric(value)) else {
                        continue;
                    };
                    let name = format!("firebird_{}_{}", metric_name(&query.name), metric_name(&metric.column));
                    let family = families
                        .entry(name.clone())
                        .or_insert_with(|| (metric.description.clone(), metric.usage.to_lowercase(), Vec::new()));
                    family.2.push(format!("{name}{labels} {}", value.trim()));
                }
            }
        }
        for (name, (help, kind, samples)) in &families {
            let help = if help.is_empty() { name } else { help };
            lines.push(format!("# HELP {name} {}", help.replace('\\', "\\\\").replace('\n', "\\n")));
            lines.push(format!("# TYPE {name} {kind}"));
            lines.extend(samples.iter().cloned());
        }
        if !errors.is_empty() {
            lines.push("# HELP firebird_exporter_query_error Whether the custom query failed in this scrape".into());
            lines.push("# TYPE firebird_exporter_query_error gauge".into());
            lines.extend(errors);
            lines.push("# HELP firebird_exporter_query_duration_seconds How long the custom query took".into());
            lines.push("# TYPE firebird_exporter_query_duration_seconds gauge".into());
            lines.extend(durations);
        }
        lines.join("\n") + "\n"
    }
}

fn label_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Lower case, anything but [a-z0-9_] an underscore.
fn metric_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$`
fn numeric(value: &str) -> bool {
    let value = value.trim();
    let value = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(at) => (&value[..at], Some(&value[at + 1..])),
        None => (value, None),
    };
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = match mantissa.split_once('.') {
        Some((whole, fraction)) => (digits(whole) && (fraction.is_empty() || digits(fraction))) || (whole.is_empty() && digits(fraction)),
        None => digits(mantissa),
    };
    mantissa_ok && exponent.is_none_or(|exponent| digits(exponent.strip_prefix(['-', '+']).unwrap_or(exponent)))
}

pub fn run() -> io::Result<()> {
    // SIGTERM reaches this process only (the container's PID 1), and is caught from the start, so
    // one that comes as it starts is not fatal: the exporter keeps answering
    // until the pod's Firebird server has stopped, then exits
    let stopping = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(signal, Arc::clone(&stopping))?;
    }
    let database = var("DATABASE_PATH");
    if database.is_empty() {
        return Err(io::Error::other("DATABASE_PATH is required"));
    }
    let or = |name: &str, default: &str| Some(var(name)).filter(|v| !v.is_empty()).unwrap_or_else(|| default.into());
    let client = match or("QUERY_CLIENT", "isql").as_str() {
        "agent" => Client::Agent {
            config: ConnectConfig::new()
                .host("127.0.0.1")
                .port(var("FIREBIRD_PORT").parse().unwrap_or(3050))
                .database(database.clone())
                .user(or("ISC_USER", "SYSDBA"))
                .password(var("ISC_PASSWORD"))
                .wire_crypt(WireCrypt::Enabled)
                .connect_timeout(Duration::from_secs(5)),
        },
        "isql" => Client::Isql {
            isql: or("ISQL", "isql"),
            temporary: or("TMPDIR", "/tmp"),
        },
        other => return Err(io::Error::other(format!("QUERY_CLIENT: isql or agent, not {other}"))),
    };
    let client_name = if matches!(client, Client::Agent { .. }) { "agent" } else { "isql" };
    let exporter = Exporter {
        database,
        queries_file: or("QUERIES_FILE", "/custom-queries/queries"),
        timeout: or("QUERY_TIMEOUT_SECONDS", "10"),
        client,
    };
    let port: u16 = or("METRICS_PORT", "9187")
        .parse()
        .map_err(|_| io::Error::other("METRICS_PORT: not a port"))?;
    let listener = TcpListener::bind(("0.0.0.0", port)).map_err(|e| io::Error::other(format!("cannot listen on {port}: {e}")))?;
    println!("metrics on :{port}/metrics");
    if client_name == "agent" {
        println!("queries over the agent's own attachment (QUERY_CLIENT=agent)");
    }

    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));
        if stopping.load(Ordering::Relaxed) && crate::puller::firebird_stopped() {
            println!("stopping: the Firebird server has stopped");
            process::exit(0);
        }
    });

    // one scrape at a time, as the Perl exporter
    for client in listener.incoming() {
        let Ok(mut client) = client else { continue };
        let _ = client.set_read_timeout(Some(Duration::from_secs(10)));
        let _ = client.set_write_timeout(Some(Duration::from_secs(10)));
        let mut reader = BufReader::new(match client.try_clone() {
            Ok(clone) => clone,
            Err(_) => continue,
        });
        let mut request = String::new();
        if reader.read_line(&mut request).is_err() {
            continue;
        }
        loop {
            let mut header = String::new();
            match reader.read_line(&mut header) {
                Ok(0) | Err(_) => break,
                Ok(_) if header == "\r\n" || header == "\n" => break,
                Ok(_) => {}
            }
        }
        let path = request
            .strip_prefix("GET ")
            .and_then(|rest| rest.split_once(" HTTP"))
            .map(|(path, _)| path);
        let metrics = |path: &str| path == "/metrics" || path.strip_prefix("/metrics?").is_some_and(|q| !q.contains(char::is_whitespace));
        let (status, kind, body) = match path {
            Some(path) if metrics(path) => ("200 OK", "text/plain; version=0.0.4", exporter.scrape()),
            Some("/healthz") => ("200 OK", "text/plain", "ok\n".to_string()),
            _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
        };
        let _ = write!(
            client,
            "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_queries_file() {
        let text = "ignored\nquery attachments\nprimary 0\nsql SELECT A,\\n B \\\\ FROM T\nmetric username LABEL The user\nmetric total GAUGE Attachments per user\nmetric skipped DISCARD\nmetric bad OTHER x\nend\n\
                    query empty\nprimary 1\nend\nquery tx\r\nprimary 1\r\nsql SELECT 1\r\nmetric n COUNTER\r\nend\r\nquery unfinished\nsql SELECT 2\n";
        let parsed = queries(text);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].name, "attachments");
        assert!(!parsed[0].primary);
        assert_eq!(parsed[0].sql, "SELECT A,\n B \\ FROM T");
        let metrics: Vec<(&str, &str, &str)> = parsed[0]
            .metrics
            .iter()
            .map(|m| (m.column.as_str(), m.usage.as_str(), m.description.as_str()))
            .collect();
        assert_eq!(
            metrics,
            [
                ("username", "LABEL", "The user"),
                ("total", "GAUGE", "Attachments per user"),
                ("skipped", "DISCARD", "")
            ]
        );
        assert_eq!(
            (parsed[1].name.as_str(), parsed[1].primary, parsed[1].sql.as_str()),
            ("tx", true, "SELECT 1")
        );
        assert_eq!(parsed[1].metrics[0].description, "");
    }

    #[test]
    fn reads_the_rows_isql_lists() {
        let listed = "\nUSERNAME                        SYSDBA\nTOTAL                           2\n\nUSERNAME                        APP \"x\"  \nTOTAL                           5\nEMPTY\n   continued\n";
        let read = rows(listed);
        assert_eq!(read.len(), 2);
        assert_eq!(read[0]["USERNAME"], "SYSDBA");
        assert_eq!(read[0]["TOTAL"], "2");
        assert_eq!(read[1]["USERNAME"], "APP \"x\"");
        assert_eq!(read[1]["EMPTY"], "");
        assert_eq!(read[1].len(), 3);
        assert!(rows("\n\n").is_empty());
        assert_eq!(rows("lower 1")[0]["LOWER"], "1");
    }

    #[test]
    fn formats_typed_values_as_metrics_and_labels() {
        use firebird_wire::{TimeTz, TimestampTz};
        // NUMERIC and DECIMAL: the mantissa at its scale, as isql prints them
        assert_eq!(cell(&Value::Int(1250), -2), Some("12.50".into()));
        assert_eq!(cell(&Value::BigInt(-5), -3), Some("-0.005".into()));
        assert_eq!(cell(&Value::Short(7), 0), Some("7".into()));
        assert_eq!(
            cell(&Value::Int128(-123456789012345678901234567), -4),
            Some("-12345678901234567890123.4567".into())
        );
        assert_eq!(cell(&Value::Int(12), 2), Some("1200".into()));
        assert_eq!(cell(&Value::Bool(true), 0), Some("1".into()));
        assert_eq!(cell(&Value::Bool(false), 0), Some("0".into()));
        assert_eq!(cell(&Value::Double(0.25), 0), Some("0.25".into()));
        assert_eq!(cell(&Value::Double(3.0), 0), Some("3".into()));
        assert_eq!(cell(&Value::Text("SYSDBA".into()), 0), Some("SYSDBA".into()));
        assert_eq!(cell(&Value::Null, 0), None);
        assert_eq!(cell(&Value::Array(1), 0), None);
        // dates and times: seconds since the Unix epoch (UTC), a time since midnight
        assert_eq!(cell(&Value::Date(40_587), 0), Some("0".into()));
        assert_eq!(cell(&Value::Date(40_588), 0), Some("86400".into()));
        assert_eq!(cell(&Value::Timestamp(40_587, 15_000), 0), Some("1.5".into()));
        assert_eq!(cell(&Value::Time(36_000_000), 0), Some("3600".into()));
        let tz = TimestampTz {
            utc_date: 40_588,
            utc_time: 10_000,
            zone: 0,
            offset: 120,
        };
        assert_eq!(cell(&Value::TimestampTz(tz), 0), Some("86401".into()));
        let time = TimeTz {
            utc_time: 20_000,
            zone: 0,
            offset: 0,
        };
        assert_eq!(cell(&Value::TimeTz(time), 0), Some("2".into()));
        // every number the typed client gives is one the exporter serves
        for value in ["12.50", "-0.005", "1", "0", "0.25", "86400", "1.5"] {
            assert!(numeric(value), "{value}");
        }
    }

    #[test]
    fn names_typed_rows_by_alias() {
        let column = |alias: &str, field: &str, scale: i32| ColumnMeta {
            index: 0,
            sql_type: 496,
            sub_type: 0,
            scale,
            length: 4,
            nullable: true,
            field: field.into(),
            relation: String::new(),
            alias: alias.into(),
            owner: String::new(),
        };
        let columns = [
            column("total", "COUNT", 0),
            column("", "MON$PAGE_SIZE", 0),
            column("ratio", "R", -1),
            column("gone", "G", 0),
            column("note", "N", 0),
        ];
        let values = [Value::BigInt(3), Value::Int(8192), Value::Int(5), Value::Null, Value::Blob(9)];
        let blobs = HashMap::from([(9u64, "a text blob".to_string())]);
        let row = typed_row(&columns, &values, &blobs);
        assert_eq!(row["TOTAL"], "3");
        assert_eq!(row["MON$PAGE_SIZE"], "8192");
        assert_eq!(row["RATIO"], "0.5");
        assert_eq!(row["NOTE"], "a text blob");
        assert!(!row.contains_key("GONE"));
        assert_eq!(row.len(), 4);
        // a text blob, nullable (521, as the server describes it) or not; not a binary blob nor a text column
        let blob = |sql_type: i32, sub_type: i32| ColumnMeta {
            sql_type,
            sub_type,
            ..column("note", "N", 0)
        };
        assert!(text_blob(&blob(521, 1)));
        assert!(text_blob(&blob(520, 1)));
        assert!(!text_blob(&blob(521, 0)));
        assert!(!text_blob(&blob(449, 1)));
    }

    #[test]
    fn a_session_without_a_server_answers_each_statement() {
        let config = ConnectConfig::new()
            .host("127.0.0.1")
            .port(1)
            .database("/nowhere.fdb".to_string())
            .user("SYSDBA".to_string())
            .password("x".to_string())
            .connect_timeout(Duration::from_secs(1));
        let mut session = Session::new(config, Duration::from_secs(5));
        assert!(session.run("SELECT 1 FROM RDB$DATABASE").is_err());
        assert!(session.run("SELECT 2 FROM RDB$DATABASE").is_err());
    }

    #[test]
    fn names_values_and_numbers() {
        assert_eq!(metric_name("Next-TX.count"), "next_tx_count");
        assert_eq!(label_value("a\\b \"c\"\nd"), "a\\\\b \\\"c\\\"\\nd");
        for value in ["0", "12", "-3", "+4.5", "6.", ".7", "1e9", "1.5E-3", " 8 ", "-.5e+2"] {
            assert!(numeric(value), "{value}");
        }
        for value in ["", "<null>", "abc", "1.2.3", ".", "e5", "1e", "1e+", "--1", "0x10", "1 2", "-"] {
            assert!(!numeric(value), "{value}");
        }
    }
}
