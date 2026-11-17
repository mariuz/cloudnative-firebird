//! The custom metrics exporter (`fb-agent metrics-exporter`), metrics-exporter.pl with the
//! same environment, output and log lines: serves GET /metrics in the Prometheus text format.
//! Every scrape reads the queries the operator rendered from the cluster's ConfigMaps and
//! Secrets (QUERIES_FILE, a mounted Secret: a changed query applies at the next scrape, without
//! a restart), runs each one with isql against the local server, and turns the rows into
//! metrics: firebird_<query>_<column>, with the LABEL columns as labels. Queries marked primary
//! run only where the database is not a replica.
//!
//! isql stays: the values are what it prints, as the Perl exporter served them.
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
//! QUERY_TIMEOUT_SECONDS (10), ISQL (isql), TMPDIR (/tmp), FIREBIRD_PORT.

use crate::client::var;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{self, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
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

struct Exporter {
    database: String,
    queries_file: String,
    timeout: String,
    isql: String,
    temporary: String,
}

impl Exporter {
    /// Runs one statement (isql -b, LIST mode): the rows, or the error.
    fn run_sql(&self, sql: &str) -> Result<Vec<Row>, String> {
        let file = format!("{}/metrics-query-{}.sql", self.temporary, process::id());
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
                &self.isql,
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
        let mut lines: Vec<String> = Vec::new();
        let state = self.run_sql("SELECT MON$REPLICA_MODE AS REPLICA_MODE FROM MON$DATABASE");
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
                self.run_sql(&query.sql)
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
    let exporter = Exporter {
        database,
        queries_file: or("QUERIES_FILE", "/custom-queries/queries"),
        timeout: or("QUERY_TIMEOUT_SECONDS", "10"),
        isql: or("ISQL", "isql"),
        temporary: or("TMPDIR", "/tmp"),
    };
    let port: u16 = or("METRICS_PORT", "9187")
        .parse()
        .map_err(|_| io::Error::other("METRICS_PORT: not a port"))?;
    let listener = TcpListener::bind(("0.0.0.0", port)).map_err(|e| io::Error::other(format!("cannot listen on {port}: {e}")))?;
    println!("metrics on :{port}/metrics");

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
