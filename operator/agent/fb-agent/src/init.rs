//! What the replication init container (init-instance.sh) ran Perl for, as commands of the
//! agent with the same arguments, files and output:
//!
//!   fb-agent fetch-seed <host> <target-file>                        fetch-seed.pl
//!   fb-agent replica-control --adopt <source-control> <db-sequence> <control-file>
//!   fb-agent replica-control [--next <id>] <primary-host> <S> <db-sequence> <control-file> [id ...]
//!                                                                   replica-control.pl
//!   fb-agent set-repl-seq <database> <sequence>                     set-repl-seq.pl
//!   fb-agent reposition <U> <control-file>                          pitr-plan.pl --reposition
//!
//! The requests to a segment server go through client.rs: with segment TLS, over TLS made here
//! when the container has the cluster's certificates, without the proxy sidecar.
//!
//! The replica control file (src/remote/server/ReplServer.cpp, ControlFile::DataV1):
//! char[10] "FBREPLCTL", u16 version = 1, u32 txn_count, u64 sequence, u32 offset, 4 bytes of
//! padding, u64 db_sequence, then txn_count x {u64 tra_id, u64 sequence} sorted by tra_id.

use crate::client;
use crate::status::set_header_sequence;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::time::Duration;

fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn number(text: &str, what: &str) -> io::Result<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(other(format!("{what}: not a number: {text}")));
    }
    text.parse().map_err(|_| other(format!("{what}: not a number: {text}")))
}

/// Through a temporary name, so that a file is never seen half written.
fn write_whole(path: &str, suffix: &str, content: &[u8]) -> io::Result<()> {
    let temporary = format!("{path}{suffix}");
    fs::write(&temporary, content).map_err(|e| other(format!("write {temporary}: {e}")))?;
    fs::rename(&temporary, path).map_err(|e| other(format!("rename {path}: {e}")))
}

fn control(count: u32, sequence: u64, db_sequence: u64) -> Vec<u8> {
    let mut data = b"FBREPLCTL\0".to_vec();
    data.extend(1u16.to_le_bytes());
    data.extend(count.to_le_bytes());
    data.extend(sequence.to_le_bytes());
    data.extend(0u32.to_le_bytes());
    data.extend([0u8; 4]);
    data.extend(db_sequence.to_le_bytes());
    data
}

fn read_control(path: &str) -> io::Result<Vec<u8>> {
    let data = fs::read(path).map_err(|e| other(format!("read {path}: {e}")))?;
    if data.len() < 40 || !data.starts_with(b"FBREPLCTL") {
        return Err(other(format!("{path} is not a replica control file")));
    }
    Ok(data)
}

/// SEED from `host`: the database to `target`, the replica control file (when the source sent
/// one) to `target`.ctl and the seed kind (replica | offline | live) to `target`.kind.
pub fn fetch_seed(host: &str, target: &str) -> io::Result<()> {
    let mut reply = client::open(
        host,
        &crate::databases::request_for_environment("SEED"),
        Some(Duration::from_secs(900)),
    )?;
    let mut header = String::new();
    reply.read_line(&mut header)?;
    let refused = || other(format!("{host}: {}", header.trim_end()));
    let fields: Vec<&str> = header.trim_end_matches(['\r', '\n']).split(' ').collect();
    let [ok, database_size, control_size, kind] = fields[..] else {
        return Err(refused());
    };
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    if ok != "OK" || !digits(database_size) || !digits(control_size) || !matches!(kind, "replica" | "offline" | "live") {
        return Err(refused());
    }
    let (database_size, control_size): (u64, u64) = (
        database_size.parse().map_err(|_| refused())?,
        control_size.parse().map_err(|_| refused())?,
    );
    let mut receive = |path: &str, size: u64| -> io::Result<()> {
        let part = format!("{path}.part");
        let mut file = File::create(&part).map_err(|e| other(format!("write {part}: {e}")))?;
        let got = io::copy(&mut (&mut reply).take(size), &mut file)?;
        if got != size {
            return Err(other(format!("short read ({got} of {size} bytes)")));
        }
        file.flush()?;
        drop(file);
        fs::rename(&part, path).map_err(|e| other(format!("rename {path}: {e}")))
    };
    let _ = fs::remove_file(format!("{target}.ctl"));
    receive(target, database_size)?;
    if control_size > 0 {
        receive(&format!("{target}.ctl"), control_size)?;
    }
    fs::write(format!("{target}.kind"), format!("{kind}\n")).map_err(|e| other(format!("write {target}.kind: {e}")))?;
    println!("fetched {kind} seed copy ({database_size} bytes) from {host}");
    Ok(())
}

/// One request to the primary's segment server: its lines up to "."; None when the server does
/// not know the command.
fn ask(host: &str, request: &str) -> io::Result<Option<Vec<String>>> {
    let reply = client::open(
        host,
        &crate::databases::request_for_environment(request),
        Some(Duration::from_secs(300)),
    )?;
    let mut lines = Vec::new();
    for line in reply.lines() {
        let line = line?;
        let line = line.trim_end_matches('\r');
        if line == "." {
            break;
        }
        if line == "ERR bad request" {
            return Ok(None);
        }
        if line.starts_with("ERR") {
            return Err(other(format!("server: {line}")));
        }
        lines.push(line.to_string());
    }
    Ok(Some(lines))
}

/// The control file of a freshly seeded replica (the arguments of replica-control.pl).
pub fn replica_control(arguments: &[&str]) -> io::Result<()> {
    if let ["--adopt", rest @ ..] = arguments {
        // a seed copied from another replica: its position and active transactions, with the
        // copy's own header sequence
        let [from, db_sequence, target] = rest else {
            return Err(other(
                "usage: replica-control --adopt <source-control> <db-sequence> <control-file>",
            ));
        };
        let mut data = read_control(from)?;
        data[32..40].copy_from_slice(&number(db_sequence, "db-sequence")?.to_le_bytes());
        write_whole(target, ".tmp", &data)?;
        let count = u32::from_le_bytes(data[12..16].try_into().expect("4 bytes"));
        let sequence = u64::from_le_bytes(data[16..24].try_into().expect("8 bytes"));
        println!("replica control: adopted position after segment {sequence} with {count} active transaction(s)");
        return Ok(());
    }
    let (next, rest) = match arguments {
        ["--next", next, rest @ ..] => (Some(number(next, "--next")?), rest),
        rest => (None, rest),
    };
    let [host, sequence, db_sequence, target, candidates @ ..] = rest else {
        return Err(other(
            "usage: replica-control [--next <id>] <host> <S> <db-sequence> <control-file> [ids...]",
        ));
    };
    let (sequence, db_sequence) = (number(sequence, "S")?, number(db_sequence, "db-sequence")?);
    for candidate in candidates {
        number(candidate, "candidate")?;
    }
    let ids = candidates.join(",");
    // where each transaction that is not committed in the copy begins in the journal
    let mut start: BTreeMap<u64, u64> = BTreeMap::new();
    // with --next (live seeds), PLAN also covers the transactions numbered <next> or above with
    // blocks in segments <= S, and tells whether each first block begins its transaction; an
    // older segment server only knows TXNS (the candidates' first segments)
    let plan = match next {
        Some(next) => ask(host, &format!("PLAN {sequence} {next} {ids}"))?,
        None => None,
    };
    let numbers = |line: &str| line.split(' ').map(|field| field.parse::<u64>().ok()).collect::<Option<Vec<u64>>>();
    if let Some(plan) = plan {
        for line in &plan {
            if let Some([transaction, first, begins @ (0 | 1)]) = numbers(line).as_deref() {
                if *begins == 0 {
                    return Err(other(format!(
                        "transaction {transaction} began before the oldest archived segment of {host} ({first} is its first); cannot seed from this copy"
                    )));
                }
                start.insert(*transaction, *first);
            }
        }
    } else if !candidates.is_empty() {
        let lines = ask(host, &format!("TXNS {sequence} {ids}"))?.ok_or_else(|| other("server: TXNS not supported"))?;
        for line in &lines {
            if let Some([transaction, first]) = numbers(line).as_deref() {
                start.insert(*transaction, *first);
            }
        }
    }
    let mut data = control(start.len() as u32, sequence, db_sequence);
    for (transaction, first) in &start {
        data.extend(transaction.to_le_bytes());
        data.extend(first.to_le_bytes());
    }
    write_whole(target, ".tmp", &data)?;
    let replayed: Vec<String> = start
        .iter()
        .map(|(transaction, first)| format!("{transaction} from segment {first}"))
        .collect();
    println!(
        "replica control: continue after segment {sequence}, replay {} transaction(s){}",
        start.len(),
        if replayed.is_empty() {
            String::new()
        } else {
            format!(" ({})", replayed.join(", "))
        }
    );
    Ok(())
}

/// The replication sequence in the header of a database no server has open.
pub fn set_sequence(database: &str, sequence: &str) -> io::Result<()> {
    let sequence = number(sequence, "sequence")?;
    // (it says what it set)
    set_header_sequence(Path::new(database), sequence).map_err(other)
}

/// Across a failover in the source (a replica cluster): the control file continues after the
/// archive's segment U with no active transaction, keeping db_sequence.
pub fn reposition(sequence: &str, path: &str) -> io::Result<()> {
    let sequence = number(sequence, "U")?;
    let data = read_control(path)?;
    let db_sequence = u64::from_le_bytes(data[32..40].try_into().expect("8 bytes"));
    let mut rewritten = control(0, sequence, db_sequence);
    // the file's own signature and version
    rewritten[..12].copy_from_slice(&data[..12]);
    write_whole(path, ".tmp", &rewritten)?;
    println!("the replay continues after segment {sequence}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("fb-agent-init-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn adopts_and_repositions_a_control_file() {
        let dir = scratch("control");
        let (from, target) = (format!("{dir}/from"), format!("{dir}/target"));
        let mut source = control(1, 7, 3);
        source.extend(40u64.to_le_bytes());
        source.extend(6u64.to_le_bytes());
        fs::write(&from, &source).unwrap();
        replica_control(&["--adopt", &from, "9", &target]).unwrap();
        let adopted = fs::read(&target).unwrap();
        // the position and the active transaction stay, db_sequence is the copy's
        assert_eq!(adopted.len(), 56);
        assert_eq!(&adopted[..32], &source[..32]);
        assert_eq!(&adopted[32..40], &9u64.to_le_bytes());
        assert_eq!(&adopted[40..], &source[40..]);
        assert!(!Path::new(&format!("{target}.tmp")).exists());
        // repositioned: after segment 12, no active transaction, the same db_sequence
        reposition("12", &target).unwrap();
        assert_eq!(fs::read(&target).unwrap(), control(0, 12, 9));
        // not control files, not numbers
        fs::write(&from, "short").unwrap();
        assert!(replica_control(&["--adopt", &from, "9", &target]).is_err());
        assert!(reposition("12", &from).is_err());
        assert!(reposition("x", &target).is_err());
        assert!(replica_control(&["--adopt", &from]).is_err());
        assert!(replica_control(&["host", "1", "x", &target]).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn lays_the_control_file_out_as_the_server_reads_it() {
        let data = control(2, 0x0102030405060708, 9);
        assert_eq!(data.len(), 40);
        assert_eq!(&data[..10], b"FBREPLCTL\0");
        assert_eq!(&data[10..12], &[1, 0]);
        assert_eq!(&data[12..16], &[2, 0, 0, 0]);
        assert_eq!(&data[16..24], &[8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(&data[24..32], &[0; 8]);
        assert_eq!(&data[32..40], &[9, 0, 0, 0, 0, 0, 0, 0]);
    }
}
