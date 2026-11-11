//! Seeds (docs/rust-agent-segment-server.md, step 4): SEED, the copy of the database a new
//! replica starts from, and TXNS and PLAN, which tell where in the archive the transactions of
//! such a copy begin. Each with the replies and the sequence of segment-server.pl.
//!
//! One copy at a time: the lock file is the one the Perl server takes for its nbackup and its
//! promotion, which it still answers, so a seed here and those there wait for each other.

use crate::server::segment_name;
use crate::sql::Database;
use crate::status::{control_file, header_sequence, segment_sequence, Files};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

/// What a seed needs beside the instance's files.
pub struct Seeds {
    pub database: Database,
    /// ALLOW_LIVE_SEED: a copy of the running primary under the backup lock
    pub allow_live: bool,
}

/// Checks at once, then every second, for at most `seconds`.
pub fn wait_for(seconds: u64, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if check() {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_secs(1));
    }
}

/// The archive's segment files with a header: (sequence, path), by sequence.
fn archived(archive: &Path) -> Vec<(u64, PathBuf)> {
    let Ok(entries) = fs::read_dir(archive) else { return Vec::new() };
    let mut names: Vec<String> = entries
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| segment_name(name))
        .collect();
    names.sort();
    let mut segments: Vec<(u64, PathBuf)> = names
        .into_iter()
        .map(|name| archive.join(name))
        .filter(|path| path.is_file())
        .filter_map(|path| Some((segment_sequence(&path)?, path)))
        .collect();
    segments.sort_by_key(|(sequence, _)| *sequence);
    segments
}

/// The first archived segment up to `upto` holding a block of each wanted transaction (and,
/// with `next`, of every transaction numbered `next` or above), and whether that block begins
/// the transaction: id -> (segment, begins). A segment is a 48-byte header (its length at 40)
/// and blocks {u64 transaction, u16 protocol, u16 flags, u32 length} + payload.
fn first_segments(archive: &Path, upto: u64, wanted: &HashSet<u64>, next: Option<u64>) -> BTreeMap<u64, (u64, u8)> {
    let mut first = BTreeMap::new();
    for (sequence, path) in archived(archive).into_iter().filter(|(sequence, _)| *sequence <= upto) {
        let Ok(file) = File::open(&path) else { continue };
        let mut header = [0u8; 48];
        if file.read_exact_at(&mut header, 0).is_err() {
            continue;
        }
        let length = u64::from_le_bytes(header[40..48].try_into().expect("8 bytes"));
        let mut at = 48u64;
        while at + 16 <= length {
            let mut block = [0u8; 16];
            if file.read_exact_at(&mut block, at).is_err() {
                break;
            }
            let transaction = u64::from_le_bytes(block[0..8].try_into().expect("8 bytes"));
            let flags = u16::from_le_bytes(block[10..12].try_into().expect("2 bytes"));
            let size = u32::from_le_bytes(block[12..16].try_into().expect("4 bytes"));
            if transaction != 0 && (wanted.contains(&transaction) || next.is_some_and(|next| transaction >= next)) {
                first.entry(transaction).or_insert((sequence, (flags & 1) as u8));
            }
            at += 16 + u64::from(size);
        }
    }
    first
}

fn numbers(list: &str) -> Option<HashSet<u64>> {
    list.split(',').filter(|n| !n.is_empty()).map(|n| n.parse().ok()).collect()
}

fn all_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

/// TXNS <upto> <ids> and PLAN <upto> <next> <ids>: None for an argument the Perl server refuses.
/// Waits up to two minutes for segment <upto>: the one active when a copy was locked is archived
/// shortly after.
pub fn transactions(archive: &Path, command: &str, argument: &str) -> Option<String> {
    if !argument.bytes().all(|b| b.is_ascii_digit() || b == b' ' || b == b',') {
        return None;
    }
    let mut parts = argument.split(' ');
    let upto = parts.next().filter(|n| all_digits(n))?;
    let next = match command {
        "PLAN" => Some(parts.next().filter(|n| all_digits(n))?.parse::<u64>().ok()?),
        _ => None,
    };
    let wanted = numbers(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    let upto_number = upto.parse::<u64>().ok()?;
    if !wait_for(120, || {
        archived(archive).last().is_some_and(|(sequence, _)| *sequence >= upto_number)
    }) {
        return Some(format!("ERR segment {upto} not archived yet\n"));
    }
    let mut reply = String::new();
    for (transaction, (segment, begins)) in first_segments(archive, upto_number, &wanted, next) {
        reply.push_str(&match command {
            "PLAN" => format!("{transaction} {segment} {begins}\n"),
            _ => format!("{transaction} {segment}\n"),
        });
    }
    Some(reply + ".\n")
}

/// Held until dropped: the lock the seeds, the nbackup and the promotion share.
pub struct Exclusive(#[allow(dead_code)] File);

pub fn exclusive(base: &Path) -> Result<Exclusive, String> {
    let path = base.join(".exclusive.lock");
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .map_err(|e| format!("cannot open the lock {}: {e}", path.display()))?;
    // SAFETY: flock(2) on a descriptor this function owns
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(format!("cannot lock {}: {}", path.display(), io::Error::last_os_error()));
    }
    Ok(Exclusive(file))
}

/// "OK <size> <control size> <kind>", the copy's bytes, then the control file's.
fn send(client: &mut impl Write, copy: &Path, control: Option<&[u8]>, kind: &str) -> io::Result<()> {
    let Ok(mut file) = File::open(copy) else {
        return writeln!(client, "ERR cannot read seed copy");
    };
    let control = control.unwrap_or_default();
    writeln!(client, "OK {} {} {kind}", file.metadata()?.len(), control.len())?;
    io::copy(&mut file, client)?;
    client.write_all(control)
}

/// The backup lock is taken through the local server: writes go to the delta file while the
/// main file is copied, and the database is always unlocked again.
fn locked_copy(seeds: &Seeds, files: &Files, copy: &Path, on_locked: impl FnOnce()) -> bool {
    let _ = fs::remove_file(copy);
    let locked = seeds.database.execute("ALTER DATABASE BEGIN BACKUP");
    if let Err(error) = &locked {
        println!("backup lock: {error}");
    }
    let locked = locked.is_ok();
    let copied = locked && fs::copy(&files.database, copy).is_ok();
    if copied {
        on_locked();
    }
    let unlocked = !locked || seeds.database.execute("ALTER DATABASE END BACKUP").is_ok();
    let flag = |yes: bool| if yes { "1" } else { "" };
    println!(
        "locked copy (fb-agent): locked={} copied={} unlocked={}",
        flag(locked),
        flag(copied),
        flag(unlocked)
    );
    copied && unlocked
}

/// Pauses the segment puller (its flag, then its acknowledgement) and waits until the replica has
/// applied everything it received: no segment beyond the control file's position. The caller
/// removes the flag (`.pause-pull`) when it is done.
pub fn pause_until_applied(files: &Files, control: &Path) -> Result<(), &'static str> {
    let (pause, paused) = (files.base.join(".pause-pull"), files.base.join(".pull-paused"));
    let _ = fs::remove_file(&paused);
    let _ = File::create(&pause);
    if !wait_for(60, || paused.exists()) {
        return Err("segment puller did not pause");
    }
    let applied = || {
        let Some((sequence, offset)) = control_file(control) else {
            return false;
        };
        offset == 0 && !archived(&files.source).iter().any(|(fetched, _)| *fetched > sequence)
    };
    if !wait_for(300, applied) {
        return Err("replica did not finish applying received segments");
    }
    Ok(())
}

/// SEED. None: nothing was done or sent, and the Perl server is to answer (the local server
/// gave this process no answer; isql may get one).
pub fn seed(seeds: &Seeds, files: &Files, client: &mut impl Write) -> Option<io::Result<()>> {
    if files.is_primary() {
        from_primary(seeds, files, client)
    } else {
        from_replica(seeds, files, client)
    }
}

fn from_primary(seeds: &Seeds, files: &Files, client: &mut impl Write) -> Option<io::Result<()>> {
    let bootstrap = files.base.join("bootstrap-seed.fdb");
    if bootstrap.is_file() {
        let seed_sequence = header_sequence(&bootstrap).unwrap_or(0);
        let current = seeds.database.value("RDB$GET_CONTEXT('SYSTEM', 'REPLICATION_SEQUENCE')").ok()?;
        let current = current.parse::<u64>().unwrap_or(0);
        let oldest = archived(&files.archive).first().map(|(sequence, _)| *sequence);
        // usable while every segment after the seed is still archived
        if current <= seed_sequence || oldest.is_some_and(|oldest| oldest <= seed_sequence + 1) {
            let sent = send(client, &bootstrap, None, "offline");
            println!("served offline bootstrap seed (sequence {seed_sequence})");
            return Some(sent);
        }
        println!("bootstrap seed (sequence {seed_sequence}) is older than the archived segments");
    }
    if !seeds.allow_live {
        println!("seed refused: no usable bootstrap seed and live seeding is not allowed");
        return Some(writeln!(
            client,
            "ERR no safe seed source on the primary; add a replica seed source, restart the primary cleanly for a fresh offline seed, or allow live seeds (allowLiveSeedFromPrimary)"
        ));
    }
    let _lock = match exclusive(&files.base) {
        Ok(lock) => lock,
        Err(error) => return Some(writeln!(client, "ERR {error}")),
    };
    let copy = files.base.join("seed.copy");
    let sent = if locked_copy(seeds, files, &copy, || ()) {
        let sent = send(client, &copy, None, "live");
        println!("served live seed copy of the primary");
        sent
    } else {
        writeln!(client, "ERR seed copy failed")
    };
    let _ = fs::remove_file(&copy);
    Some(sent)
}

fn from_replica(seeds: &Seeds, files: &Files, client: &mut impl Write) -> Option<io::Result<()>> {
    let guid = seeds.database.value("MON$GUID").ok()?;
    let control = files.source.join(&guid);
    let _lock = match exclusive(&files.base) {
        Ok(lock) => lock,
        Err(error) => return Some(writeln!(client, "ERR {error}")),
    };
    let (pause, copy) = (files.base.join(".pause-pull"), files.base.join("seed.copy"));
    let copied = (|| {
        pause_until_applied(files, &control)?;
        let mut position = None;
        if !locked_copy(seeds, files, &copy, || position = fs::read(&control).ok()) {
            return Err("locked copy failed");
        }
        position.ok_or("the replica control file cannot be read")
    })();
    let sent = match copied {
        Ok(position) => {
            let sent = send(client, &copy, Some(&position), "replica");
            println!("served replica seed copy");
            sent
        }
        Err(reason) => {
            println!("replica seed refused: {reason}");
            writeln!(client, "ERR {reason}")
        }
    };
    let _ = fs::remove_file(&pause);
    let _ = fs::remove_file(&copy);
    Some(sent)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// a segment: its header, then blocks of (transaction, flags, payload size)
    fn segment(sequence: u64, blocks: &[(u64, u16, u32)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (transaction, flags, size) in blocks {
            body.extend(transaction.to_le_bytes());
            body.extend(1u16.to_le_bytes());
            body.extend(flags.to_le_bytes());
            body.extend(size.to_le_bytes());
            body.extend(vec![0u8; *size as usize]);
        }
        let mut bytes = b"FBCHANGELOG\0".to_vec();
        bytes.extend([0u8; 20]);
        bytes.extend(sequence.to_le_bytes());
        bytes.extend((48 + body.len() as u64).to_le_bytes());
        bytes.extend(body);
        bytes
    }

    #[test]
    fn finds_where_transactions_begin() {
        let dir = std::env::temp_dir().join(format!("fb-agent-seed-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("db.fdb.journal-000000003"),
            segment(3, &[(10, 1, 5), (11, 0, 0), (10, 0, 7)]),
        )
        .unwrap();
        fs::write(
            dir.join("db.fdb.journal-000000004"),
            segment(4, &[(11, 0, 3), (12, 1, 2), (0, 0, 4), (20, 1, 0)]),
        )
        .unwrap();
        fs::write(dir.join("db.fdb.journal-000000005"), segment(5, &[(13, 1, 0)])).unwrap();
        fs::write(dir.join("db.fdb.journal-000000006"), "x").unwrap();
        assert_eq!(transactions(&dir, "TXNS", "4 10,12,13").as_deref(), Some("10 3\n12 4\n.\n"));
        assert_eq!(transactions(&dir, "TXNS", "5 ").as_deref(), Some(".\n"));
        // with a next transaction: every one from it on too, and whether its first block begins it
        assert_eq!(
            transactions(&dir, "PLAN", "5 12 11").as_deref(),
            Some("11 3 0\n12 4 1\n13 5 1\n20 4 1\n.\n")
        );
        for (command, refused) in [
            ("TXNS", "4"),
            ("TXNS", "x 1"),
            ("TXNS", "4 1 2"),
            ("TXNS", "4 a"),
            ("PLAN", "4 1"),
            ("PLAN", "4 x 1"),
            ("TXNS", " 1"),
        ] {
            assert_eq!(transactions(&dir, command, refused), None, "{command} {refused}");
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sends_a_copy_with_its_control_file() {
        let dir = std::env::temp_dir().join(format!("fb-agent-seedcopy-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("seed.copy"), "database").unwrap();
        let mut out = Vec::new();
        send(&mut out, &dir.join("seed.copy"), Some(b"ctl"), "replica").unwrap();
        assert_eq!(out, b"OK 8 3 replica\ndatabasectl");
        let mut out = Vec::new();
        send(&mut out, &dir.join("seed.copy"), None, "offline").unwrap();
        assert_eq!(out, b"OK 8 0 offline\ndatabase");
        let mut out = Vec::new();
        send(&mut out, &dir.join("none"), None, "live").unwrap();
        assert_eq!(out, b"ERR cannot read seed copy\n");
        // the lock is free again once dropped
        drop(exclusive(&dir).unwrap());
        drop(exclusive(&dir).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }
}
