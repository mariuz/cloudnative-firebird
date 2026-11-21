//! The Jobs' programs, as commands of the agent with the arguments, environment, output, files
//! and exit status of the Perl scripts they replace:
//!
//!   fb-agent backup-file get|put|remove|list|nbackup ...   backup-file.pl
//!   fb-agent fetch-segments                                fetch-segments.pl
//!   fb-agent pitr-plan [--describe|--reposition] ...       pitr-plan.pl
//!   fb-agent cut-segment <file> <length>                   pitr-restore.sh's Perl one-liner
//!   fb-agent switchover                                    switchover.pl
//!   fb-agent failover                                      failover.pl
//!   fb-agent sync-standby                                  sync-standby.pl
//!
//! A failure is a message on stderr and a non-zero exit, as a Perl `die`; pitr-plan exits 3 when
//! it needs earlier segments. The requests to the segment servers go through client.rs (with
//! segment TLS, through the proxy or over TLS of its own). The Firebird servers are reached with
//! the image's fbsvcmgr and isql, as the scripts did.

use crate::client::{self, now, var};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

/// How a command ends when it does not succeed: its message (as `die` prints it) and status.
#[derive(Debug)]
pub struct Exit {
    pub code: u8,
    pub message: String,
}

impl From<String> for Exit {
    fn from(message: String) -> Exit {
        Exit { code: 1, message }
    }
}

impl From<&str> for Exit {
    fn from(message: &str) -> Exit {
        Exit::from(message.to_string())
    }
}

type Done = Result<(), Exit>;

fn die<T>(message: impl Into<String>) -> Result<T, Exit> {
    Err(Exit::from(message.into()))
}

fn digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

fn number(text: &str) -> Option<u64> {
    digits(text).then(|| text.parse().ok()).flatten()
}

/// Perl's `$ENV{NAME} // default` for a number: a value that is not one counts as 0.
fn seconds(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value.trim().parse::<f64>().map(|v| v.max(0.0) as u64).unwrap_or(0),
        Err(_) => default,
    }
}

fn or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn required(name: &str) -> Result<String, Exit> {
    let value = var(name);
    if value.is_empty() || value == "0" {
        return die(format!("{name} is required\n"));
    }
    Ok(value)
}

/// One request to a segment server, the connection left open for the reply.
fn open(host: &str, request: &str, timeout: u64) -> Result<BufReader<crate::tls::Stream>, Exit> {
    client::open(host, request, Some(Duration::from_secs(timeout))).map_err(|e| Exit::from(format!("{e}\n")))
}

/// A line of the reply, with its end of line; "" at the end.
fn line(reply: &mut impl BufRead) -> String {
    let mut text = String::new();
    let _ = reply.read_line(&mut text);
    text
}

fn chomp(text: &str) -> &str {
    text.strip_suffix('\n').map(|t| t.strip_suffix('\r').unwrap_or(t)).unwrap_or(text)
}

/// "server: <reply>", or "server: no reply"
fn server(reply: &str) -> Exit {
    Exit::from(format!("server: {}", if reply.is_empty() { "no reply\n" } else { reply }))
}

/// Up to `size` bytes of the reply into `path` (through `part`); false when it ended before.
fn receive(reply: &mut impl Read, part: &str, path: &str, size: u64) -> Result<u64, Exit> {
    let mut file = File::create(part).map_err(|e| Exit::from(format!("write {path}: {e}\n")))?;
    let got = io::copy(&mut reply.take(size), &mut file).unwrap_or(0);
    file.flush().map_err(|e| Exit::from(format!("write {path}: {e}\n")))?;
    Ok(got)
}

// ---------------------------------------------------------------------------------------------
// backup-file.pl

/// backup-file get <name> <file> | put <file> <name> | remove <name>... | list | nbackup <level> <name>
pub fn backup_file(arguments: &[&str]) -> Done {
    let host = required("FIREBIRD_HOST")?;
    let request = |line: &str| open(&host, line, 600);
    match arguments {
        ["get", name, file] => {
            let mut reply = request(&format!("FILE {name}"))?;
            let header = line(&mut reply);
            let Some(size) = chomp(&header)
                .strip_prefix("OK ")
                .and_then(|s| number(s.split(' ').next().unwrap_or("")))
            else {
                return Err(server(&header));
            };
            let part = format!("{file}.part");
            let got = receive(&mut reply, &part, file, size)?;
            if got < size {
                return die(format!("short read for {name} ({got} of {size} bytes)\n"));
            }
            fs::rename(&part, file).map_err(|e| Exit::from(format!("rename {file}: {e}\n")))?;
            println!("copied {name} ({size} bytes) from {host}");
        }
        ["put", file, name] => {
            let Ok(metadata) = fs::metadata(file) else {
                return die(format!("cannot read {file}\n"));
            };
            let size = metadata.len();
            let mut source = File::open(file).map_err(|e| Exit::from(format!("read {file}: {e}\n")))?;
            let mut reply = request(&format!("STORE {name} {size}"))?;
            io::copy(&mut source, reply.get_mut()).map_err(|e| Exit::from(format!("send {name}: {e}\n")))?;
            let _ = reply.get_mut().flush();
            let answer = line(&mut reply);
            if !answer.starts_with("OK") {
                return Err(server(&answer));
            }
            println!("copied {file} ({size} bytes) to {name} on {host}");
        }
        ["nbackup", level, name] => {
            // about a declarative database with SEGMENT_DATABASE (a physical backup of a
            // FirebirdDatabase on a replica); the files themselves are the instance's
            let mut reply = request(&crate::databases::request_for_environment(&format!("NBACKUP {level} {name}")))?;
            let answer = line(&mut reply);
            if !answer.starts_with("OK") {
                return Err(server(&answer));
            }
            // ^OK(?: replica (\d+))?
            let replica = answer[2..]
                .strip_prefix(" replica ")
                .map(|rest| rest.bytes().take_while(u8::is_ascii_digit).map(char::from).collect::<String>());
            match replica.filter(|position| !position.is_empty()) {
                Some(position) => {
                    println!("nbackup {name} written on {host} at replica position {position}\nreplica position {position}")
                }
                None => println!("nbackup {name} written on {host}"),
            }
        }
        ["list"] => {
            let mut reply = request("FILES")?;
            loop {
                let text = line(&mut reply);
                if text.is_empty() {
                    return die("server: incomplete listing\n");
                }
                let entry = chomp(&text);
                if entry == "." {
                    break;
                }
                if entry.starts_with("ERR") {
                    return die(format!("server: {entry}\n"));
                }
                println!("{entry}");
            }
        }
        ["remove", names @ ..] if !names.is_empty() => {
            for name in names {
                let mut reply = request(&format!("REMOVE {name}"))?;
                let answer = line(&mut reply);
                if !answer.starts_with("OK") {
                    return Err(server(&answer));
                }
                println!("removed {name} on {host}");
            }
        }
        _ => {
            return die("usage: backup-file.pl get <name> <file> | put <file> <name> | remove <name>... | list | nbackup <level> <name>\n")
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// fetch-segments.pl

/// ^[A-Za-z0-9._-]+\.journal-(\d+)$: the sequence
fn journal_sequence(name: &str) -> Option<u64> {
    let (base, digits_part) = name.rsplit_once(".journal-")?;
    let plain = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    (plain(base) && digits(digits_part)).then(|| digits_part.parse().ok()).flatten()
}

/// ^[A-Za-z0-9._-]+\.lineage-\d+-\d+$
fn lineage_name(name: &str) -> bool {
    let Some((base, rest)) = name.rsplit_once(".lineage-") else {
        return false;
    };
    let plain = !base.is_empty() && base.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    plain && rest.split_once('-').is_some_and(|(a, b)| digits(a) && digits(b))
}

/// (year, month, day, hour, minute, second) of a time since the epoch, in UTC.
fn utc(seconds: i64) -> (i64, i64, i64, i64, i64, i64) {
    let (days, rest) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted + 2) / 5 + 1;
    let month = if shifted < 10 { shifted + 3 } else { shifted - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day, rest / 3600, rest % 3600 / 60, rest % 60)
}

pub fn fetch_segments() -> Done {
    let host = required("FIREBIRD_HOST")?;
    let out = required("OUT_DIR")?;
    let (skip, listed_file) = (var("SKIP_FILE"), var("LISTED_FILE"));
    // with SEGMENT_DATABASE, a declarative database's archive (DB <file> LIST, GET, ...)
    let request = |line: &str| open(&host, &crate::databases::request_for_environment(line), 60);

    if var("REPORT") == "true" {
        let max = if listed_file.is_empty() {
            String::new()
        } else {
            fs::read_to_string(&listed_file)
                .map(|text| text.split_inclusive('\n').next().unwrap_or("").to_string())
                .unwrap_or_default()
        };
        let max = max.trim_end();
        if !digits(max) {
            println!("nothing listed, nothing to report");
            return Ok(());
        }
        let mut reply = request(&format!("UPLOADED {max}"))?;
        let answer = line(&mut reply);
        if !answer.starts_with("OK") {
            return Err(server(&answer));
        }
        println!("reported segments up to {max} as uploaded to {host}");
        return Ok(());
    }

    let done: HashSet<String> = if skip.is_empty() {
        HashSet::new()
    } else {
        fs::read_to_string(&skip)
            .unwrap_or_default()
            .split('\n')
            .map(|l| l.trim_end().to_string())
            .filter(|l| !l.is_empty())
            .collect()
    };
    let _ = fs::create_dir(&out);

    let mut names = Vec::new();
    let mut listing = request("LIST")?;
    loop {
        let text = line(&mut listing);
        if text.is_empty() {
            break;
        }
        let entry = chomp(&text);
        if entry == "." {
            break;
        }
        if entry.starts_with("ERR") {
            return die(format!("server: {entry}\n"));
        }
        if journal_sequence(entry).is_some() {
            names.push(entry.to_string());
        }
    }
    drop(listing);
    let max = names.iter().filter_map(|n| journal_sequence(n)).max();
    if !listed_file.is_empty() {
        let text = max.map(|m| format!("{m}\n")).unwrap_or_default();
        fs::write(&listed_file, text).map_err(|e| Exit::from(format!("write {listed_file}: {e}\n")))?;
    }
    let result = var("RESULT_FILE");
    if let (false, Some(max)) = (result.is_empty(), max) {
        let _ = fs::write(&result, format!("listed={max}"));
    }

    // lineage markers: a server without LINEAGE answers ERR, none then
    let mut lineage = 0;
    let mut markers = request("LINEAGE")?;
    loop {
        let text = line(&mut markers);
        let entry = chomp(&text);
        if text.is_empty() || entry == "." || entry.starts_with("ERR") {
            break;
        }
        if !lineage_name(entry) || done.contains(entry) {
            continue;
        }
        File::create(format!("{out}/{entry}")).map_err(|e| Exit::from(format!("write {entry}: {e}\n")))?;
        lineage += 1;
    }
    drop(markers);
    if lineage > 0 {
        println!("{lineage} new lineage marker(s)");
    }

    // the archive time of each segment, from its age on the primary
    let mut archived: HashMap<u64, String> = HashMap::new();
    let mut ages = request("ARCHIVED")?;
    let at = now() as i64;
    loop {
        let text = line(&mut ages);
        let entry = chomp(&text);
        if text.is_empty() || entry == "." {
            break;
        }
        if entry.starts_with("ERR") {
            return die(format!("server: {entry}\n"));
        }
        let Some((sequence, age)) = entry.split_once(' ') else { continue };
        let age_ok = digits(age.strip_prefix('-').unwrap_or(age));
        if let (Some(sequence), true) = (number(sequence), age_ok) {
            let age: i64 = age.parse().unwrap_or(0);
            let (y, mo, d, h, mi, s) = utc(at - age.max(0));
            archived.insert(sequence, format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z"));
        }
    }
    drop(ages);

    let mut fetched = 0;
    let mut sorted = names.clone();
    sorted.sort();
    for name in &sorted {
        if done.contains(name) {
            continue;
        }
        let mut reply = request(&format!("GET {name}"))?;
        let header = line(&mut reply);
        if header.starts_with("ERR") {
            // pruned meanwhile
            print!("skipping {name}: {header}");
            continue;
        }
        let Some(size) = chomp(&header)
            .strip_prefix("OK ")
            .and_then(|s| number(s.split(' ').next().unwrap_or("")))
        else {
            return Err(Exit::from(format!("server: {header}")));
        };
        let (part, path) = (format!("{out}/.{name}.part"), format!("{out}/{name}"));
        let got = receive(&mut reply, &part, name, size)?;
        if got < size {
            return die(format!("short read for {name}\n"));
        }
        drop(reply);
        fs::rename(&part, &path).map_err(|e| Exit::from(format!("rename {name}: {e}\n")))?;
        let sequence = journal_sequence(name);
        if let Some(sequence) = sequence {
            // its recovery points ("<epoch> <length>"), when the primary sampled any
            let mut points = request(&format!("POINTS {sequence}"))?;
            let mut kept = String::new();
            loop {
                let text = line(&mut points);
                if text.is_empty() || text.starts_with('.') || text.starts_with("ERR") {
                    break;
                }
                let point = text.strip_suffix('\n').unwrap_or("");
                if text.ends_with('\n') && point.split_once(' ').is_some_and(|(a, b)| digits(a) && digits(b)) {
                    kept.push_str(&text);
                }
            }
            if !kept.is_empty() {
                fs::write(format!("{path}.points"), kept).map_err(|e| Exit::from(format!("write points of {name}: {e}\n")))?;
            }
            if let Some(time) = archived.get(&sequence) {
                File::create(format!("{path}.archived-{time}")).map_err(|e| Exit::from(format!("write marker for {name}: {e}\n")))?;
            }
        }
        fetched += 1;
    }
    println!("fetched {fetched} new segment(s) of {} archived on {host}", names.len());
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// pitr-plan.pl

const BLOCK_BEGIN_TRANS: u16 = 1;

fn control_file(path: &str) -> Result<Vec<u8>, Exit> {
    let data = fs::read(path).map_err(|e| Exit::from(format!("read {path}: {e}\n")))?;
    if data.len() < 40 || !data.starts_with(b"FBREPLCTL") {
        return die(format!("{path} is not a replica control file\n"));
    }
    Ok(data)
}

fn u64_at(data: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(data[at..at + 8].try_into().expect("8 bytes"))
}

fn control(count: usize, sequence: u64, db_sequence: u64, active: &[(u64, u64)]) -> Vec<u8> {
    let mut data = b"FBREPLCTL\0".to_vec();
    data.extend(1u16.to_le_bytes());
    data.extend((count as u32).to_le_bytes());
    data.extend(sequence.to_le_bytes());
    data.extend(0u32.to_le_bytes());
    data.extend([0u8; 4]);
    data.extend(db_sequence.to_le_bytes());
    for (transaction, first) in active {
        data.extend(transaction.to_le_bytes());
        data.extend(first.to_le_bytes());
    }
    data
}

fn write_through(path: &str, data: &[u8]) -> Done {
    let temporary = format!("{path}.tmp");
    fs::write(&temporary, data).map_err(|e| Exit::from(format!("write {temporary}: {e}\n")))?;
    fs::rename(&temporary, path).map_err(|e| Exit::from(format!("rename {path}: {e}\n")))
}

/// pitr-plan <dir> <S> <OAT> <next> <candidates> <L> <control> | --describe <control> | --reposition <U> <control>
pub fn pitr_plan(arguments: &[&str]) -> Done {
    match arguments {
        ["--describe", rest @ ..] => {
            let [control_path] = rest else {
                return die("usage: pitr-plan.pl --describe <control file>\n");
            };
            let data = control_file(control_path)?;
            let count = u32::from_le_bytes(data[12..16].try_into().expect("4 bytes")) as usize;
            let (sequence, offset) = (u64_at(&data, 16), u32::from_le_bytes(data[24..28].try_into().expect("4 bytes")));
            if data.len() < 40 + 16 * count {
                return die(format!("{control_path} is truncated\n"));
            }
            let first = (0..count).map(|i| u64_at(&data, 48 + 16 * i)).fold(sequence, u64::min);
            println!("{sequence} {offset} {first}");
            Ok(())
        }
        ["--reposition", rest @ ..] => {
            let [sequence, control_path] = rest else {
                return die("usage: pitr-plan.pl --reposition <U> <control file>\n");
            };
            let Some(sequence) = number(sequence) else {
                return die("usage: pitr-plan.pl --reposition <U> <control file>\n");
            };
            let data = control_file(control_path)?;
            let mut rewritten = control(0, sequence, u64_at(&data, 32), &[]);
            rewritten[..12].copy_from_slice(&data[..12]);
            write_through(control_path, &rewritten)?;
            println!("the replay continues after segment {sequence}");
            Ok(())
        }
        [dir, base, oat, next, candidates, last, control_path] => plan(dir, base, oat, next, candidates, last, control_path),
        _ => die("usage: pitr-plan.pl <dir> <S> <OAT> <next> <candidates> <L> <control>\n"),
    }
}

fn plan(dir: &str, base: &str, oat: &str, next: &str, candidates: &str, last: &str, control_path: &str) -> Done {
    let mut values = [0u64; 4];
    for (i, text) in [base, oat, next, last].iter().enumerate() {
        values[i] = number(text).ok_or_else(|| Exit::from(format!("not a number: {text}\n")))?;
    }
    let [base, oat, next, last] = values;
    if last < base {
        return die(format!("the last segment ({last}) is before the backup's segment ({base})\n"));
    }
    let text = fs::read_to_string(candidates).map_err(|e| Exit::from(format!("read {candidates}: {e}\n")))?;
    let candidate: HashSet<u64> = text.split('\n').filter_map(|l| number(l.trim())).collect();
    let open_in_copy = |t: u64| t >= oat && (t >= next || candidate.contains(&t));

    let entries = fs::read_dir(dir).map_err(|e| Exit::from(format!("read {dir}: {e}\n")))?;
    let mut files: BTreeMap<u64, String> = BTreeMap::new();
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else { continue };
        let Some((_, digits_part)) = name.rsplit_once(".journal-") else {
            continue;
        };
        if !digits(digits_part) || !entry.path().is_file() {
            continue;
        }
        let Ok(sequence) = digits_part.parse::<u64>() else { continue };
        if sequence > last {
            continue;
        }
        if files.insert(sequence, name).is_some() {
            return die(format!("two files for segment {sequence}\n"));
        }
    }
    let sequences: Vec<u64> = files.keys().copied().collect();
    if sequences.last() != Some(&last) {
        return die(format!("segment {last} is not available\n"));
    }
    for pair in sequences.windows(2) {
        if pair[1] != pair[0] + 1 {
            return die(format!("segment {} is missing\n", pair[0] + 1));
        }
    }
    let first_present = sequences[0];
    if first_present > base && base > 0 {
        println!("need {}", first_present - 1);
        return Err(Exit {
            code: 3,
            message: String::new(),
        });
    }

    let (mut first, mut begins): (BTreeMap<u64, u64>, HashMap<u64, bool>) = (BTreeMap::new(), HashMap::new());
    for (&sequence, name) in &files {
        let path = format!("{dir}/{name}");
        let mut file = File::open(&path).map_err(|e| Exit::from(format!("read {path}: {e}\n")))?;
        let mut header = [0u8; 48];
        if read_full(&mut file, &mut header) != 48 {
            return die(format!("{path}: short header\n"));
        }
        if &header[..11] != b"FBCHANGELOG" {
            return die(format!("{path} is not a journal segment\n"));
        }
        let (held, length) = (u64_at(&header, 32), u64_at(&header, 40));
        if held != sequence {
            return die(format!("{path} holds segment {held}\n"));
        }
        let mut at = 48u64;
        while at < length {
            let mut block = [0u8; 16];
            if read_full(&mut file, &mut block) != 16 {
                return die(format!("{path}: truncated block at {at}\n"));
            }
            let transaction = u64_at(&block, 0);
            let flags = u16::from_le_bytes([block[10], block[11]]);
            let size = u32::from_le_bytes(block[12..16].try_into().expect("4 bytes")) as u64;
            if size > 0 {
                file.seek(SeekFrom::Current(size as i64))
                    .map_err(|e| Exit::from(format!("{path}: seek: {e}\n")))?;
                if transaction > 0 {
                    first.entry(transaction).or_insert_with(|| {
                        begins.insert(transaction, flags & BLOCK_BEGIN_TRANS != 0);
                        sequence
                    });
                    if sequence > base && !open_in_copy(transaction) {
                        return die(format!(
                            "transaction {transaction} is complete in the backup but has changes in segment {sequence}, after its segment {base}\n"
                        ));
                    }
                }
            }
            at += 16 + size;
        }
    }

    let incomplete: Vec<String> = first
        .keys()
        .filter(|t| open_in_copy(**t) && !begins[*t])
        .map(u64::to_string)
        .collect();
    if !incomplete.is_empty() {
        println!("need {}", first_present - 1);
        return Err(Exit {
            code: 3,
            message: format!("transaction(s) {} started before segment {first_present}\n", incomplete.join(", ")),
        });
    }
    let active: Vec<(u64, u64)> = first
        .iter()
        .filter(|(t, f)| open_in_copy(**t) && **f <= base)
        .map(|(t, f)| (*t, *f))
        .collect();
    write_through(control_path, &control(active.len(), base, base, &active))?;
    let listed: Vec<String> = active.iter().map(|(t, f)| format!("{t} from segment {f}")).collect();
    println!(
        "replay: {}{} transaction(s) open in the backup{}",
        if last > base {
            format!("segments {} to {last}, and ", base + 1)
        } else {
            String::new()
        },
        active.len(),
        if listed.is_empty() {
            String::new()
        } else {
            format!(" ({})", listed.join(", "))
        }
    );
    Ok(())
}

fn read_full(file: &mut File, buffer: &mut [u8]) -> usize {
    let mut got = 0;
    while got < buffer.len() {
        match file.read(&mut buffer[got..]) {
            Ok(0) | Err(_) => break,
            Ok(n) => got += n,
        }
    }
    got
}

/// cut-segment <file> <length>: the segment's header says <length>, and the file ends there.
pub fn cut_segment(arguments: &[&str]) -> Done {
    let [path, length] = arguments else {
        return die("usage: cut-segment <file> <length>\n");
    };
    let length = number(length).ok_or_else(|| Exit::from(format!("not a number: {length}\n")))?;
    let fail = |e: io::Error| Exit::from(format!("{path}: {e}\n"));
    let mut file = OpenOptions::new().read(true).write(true).open(path).map_err(fail)?;
    file.seek(SeekFrom::Start(40)).map_err(fail)?;
    file.write_all(&length.to_le_bytes()).map_err(fail)?;
    file.set_len(length).map_err(fail)?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The segment server requests of switchover.pl, failover.pl and sync-standby.pl

/// The reply's lines, up to "." or the first OK or ERR line; None when the server cannot be reached.
fn request(host: &str, line_text: &str, timeout: u64) -> Option<Vec<String>> {
    let mut reply = client::open(host, line_text, Some(Duration::from_secs(timeout))).ok()?;
    let mut lines = Vec::new();
    loop {
        let text = line(&mut reply);
        if text.is_empty() {
            break;
        }
        let entry = chomp(&text).to_string();
        let end = entry == "." || entry.starts_with("OK") || entry.starts_with("ERR");
        lines.push(entry);
        if end {
            break;
        }
    }
    Some(lines)
}

fn first(lines: &Option<Vec<String>>) -> Option<&str> {
    lines.as_ref().and_then(|l| l.first()).map(String::as_str)
}

/// "OK <seq> <offset> <pending>": (seq, pending)
fn position(reply: &str) -> Option<(u64, u64)> {
    let mut fields = reply.strip_prefix("OK ")?.split(' ');
    let (sequence, offset, pending) = (fields.next()?, fields.next()?, fields.next()?);
    (digits(offset) && fields.next().is_none()).then_some(())?;
    Some((number(sequence)?, number(pending)?))
}

fn wait_until(what: &str, timeout: u64, report: bool, mut check: impl FnMut() -> bool) -> Done {
    let deadline = now() + timeout;
    let mut next_report = now() + 10;
    while now() < deadline {
        if check() {
            return Ok(());
        }
        if report && now() >= next_report {
            println!("waiting for {what}");
            next_report = now() + 10;
        }
        thread::sleep(Duration::from_secs(2));
    }
    die(format!("timed out waiting for {what}\n"))
}

/// One of the image's client tools, as a shell's backticks with `2>&1`: (succeeded, output).
fn tool(program: &str, arguments: &[&str], input: Option<&str>) -> (bool, String) {
    let mut command = Command::new("sh");
    command
        .args(["-c", "exec \"$@\" 2>&1", "sh", program])
        .args(arguments)
        .stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let Ok(mut child) = command.spawn() else {
        return (false, String::new());
    };
    if let (Some(input), Some(mut stdin)) = (input, child.stdin.take()) {
        let _ = stdin.write_all(input.as_bytes());
    }
    match child.wait_with_output() {
        Ok(output) => (output.status.success(), String::from_utf8_lossy(&output.stdout).into_owned()),
        Err(_) => (false, String::new()),
    }
}

/// `system(...) == 0`: the tool's output goes where this process's goes.
fn system(program: &str, arguments: &[&str]) -> bool {
    Command::new(program).args(arguments).status().is_ok_and(|s| s.success())
}

/// The header statistics of `host`'s database; None when the server refuses them in full
/// shutdown (Firebird 6).
fn header(host: &str, database: &str) -> Result<Option<String>, Exit> {
    let service = format!("{host}:service_mgr");
    let (ok, out) = tool(
        "fbsvcmgr",
        &[&service, "action_db_stats", "dbname", database, "sts_hdr_pages"],
        None,
    );
    if ok {
        return Ok(Some(out));
    }
    let refused = out
        .lines()
        .any(|l| l.starts_with("database ") && l.trim_end().ends_with(" shutdown"));
    if refused {
        return Ok(None);
    }
    die(format!("cannot read the header of {host}: {out}"))
}

fn shut_down(host: &str, database: &str) -> bool {
    let service = format!("{host}:service_mgr");
    system(
        "fbsvcmgr",
        &[
            &service,
            "action_properties",
            "dbname",
            database,
            "prp_shutdown_mode",
            "prp_sm_full",
            "prp_force_shutdown",
            "0",
        ],
    )
}

fn result_file(text: &str) {
    let path = or("RESULT_FILE", "/dev/termination-log");
    let _ = fs::write(path, text);
}

/// The final replication sequence: from the header statistics, or through the segment server
/// when they are refused.
fn final_sequence(host: &str, statistics: Option<String>, port_timeout: u64) -> Result<u64, Exit> {
    final_sequence_of(host, None, statistics, port_timeout)
}

/// The final replication sequence of `database` (None: the cluster database; Some: a declarative
/// one, asked with `DB <file>`).
fn final_sequence_of(host: &str, database: Option<&str>, statistics: Option<String>, port_timeout: u64) -> Result<u64, Exit> {
    if let Some(statistics) = statistics {
        let sequence = statistics
            .split("Replication sequence:")
            .nth(1)
            .and_then(|rest| rest.trim_start().split(|c: char| !c.is_ascii_digit()).next().and_then(number));
        return Ok(sequence.unwrap_or(0));
    }
    let reply = request(host, &crate::databases::request_about(database, "HEADER"), port_timeout)
        .ok_or_else(|| Exit::from(format!("cannot reach the segment server of {host}\n")))?;
    let answer = reply.first().cloned();
    answer
        .as_deref()
        .and_then(|a| a.strip_prefix("OK "))
        .and_then(number)
        .ok_or_else(|| {
            Exit::from(format!(
                "cannot read the header of {host}: {}\n",
                answer.as_deref().unwrap_or("no reply")
            ))
        })
}

// ---------------------------------------------------------------------------------------------
// switchover.pl

pub fn switchover() -> Done {
    let old = required("OLD_PRIMARY")?;
    let target = required("TARGET")?;
    let others: Vec<String> = var("REPLICAS").split_whitespace().map(str::to_string).collect();
    let database = required("DATABASE_PATH")?;
    let timeout = seconds("TIMEOUT_SECONDS", 300);
    let delay = std::env::var("SWITCHOVER_DELAY_SECONDS").ok().and_then(|d| number(&d)).unwrap_or(0);
    let poll = seconds("SWITCHOVER_POLL_SECONDS", 2);

    let remote_attachments = || {
        let sql = "SELECT 'remote=' || COUNT(*) FROM MON$ATTACHMENTS WHERE MON$SYSTEM_FLAG = 0 AND MON$ATTACHMENT_ID <> CURRENT_CONNECTION AND MON$REMOTE_ADDRESS IS NOT NULL AND MON$REMOTE_ADDRESS NOT STARTING WITH '127.' AND MON$REMOTE_ADDRESS <> '::1' AND MON$REMOTE_ADDRESS NOT STARTING WITH '::ffff:127.';\n";
        let (_, out) = tool("isql", &["-q", &format!("{old}:{database}")], Some(sql));
        let count = out.split("remote=").nth(1).and_then(|rest| {
            let digits_part: String = rest.chars().take_while(char::is_ascii_digit).collect();
            number(&digits_part)
        });
        match count {
            Some(count) => (Some(count), String::new()),
            None => (None, out),
        }
    };

    let before = header(&old, &database)?;
    if before.as_ref().is_none_or(|b| b.contains("full shutdown")) {
        println!("{old} already shut down");
    } else {
        let started = now();
        let mut left: Option<u64> = None;
        if delay > 0 {
            println!("stopping writes on {old}: its clients have up to {delay}s to finish");
            let deadline = started + delay;
            loop {
                let (count, error) = remote_attachments();
                left = count;
                match left {
                    None => {
                        let why: String = error.split_whitespace().collect::<Vec<_>>().join(" ");
                        let trailing = if error.ends_with(char::is_whitespace) && !why.is_empty() {
                            " "
                        } else {
                            ""
                        };
                        let leading = if error.starts_with(char::is_whitespace) && !why.is_empty() {
                            " "
                        } else {
                            ""
                        };
                        println!("cannot count the clients of {old} ({leading}{why}{trailing}); waiting");
                    }
                    Some(0) => {
                        println!("no client attached to {old}");
                        break;
                    }
                    Some(_) => {}
                }
                let remaining = deadline as i64 - now() as i64;
                if remaining <= 0 {
                    match left {
                        Some(count) => println!("{count} client(s) still attached to {old} after {delay}s: disconnecting them"),
                        None => println!("the clients of {old} could not be counted for {delay}s: stopping writes"),
                    }
                    break;
                }
                if let Some(count) = left {
                    println!("{count} client(s) attached to {old}, waiting ({remaining}s left)");
                }
                thread::sleep(Duration::from_secs((remaining as u64).min(poll)));
            }
        }
        if !shut_down(&old, &database) {
            return die(format!("cannot shut down {old}\n"));
        }
        let outcome = match left {
            None => "its clients were not counted".to_string(),
            Some(0) => "its clients disconnected by themselves".to_string(),
            Some(count) => format!("{count} client(s) disconnected"),
        };
        let after = if delay > 0 {
            format!(" after {}s ({outcome})", now() - started)
        } else {
            String::new()
        };
        println!("writes stopped: {old} is in full shutdown{after}");
    }

    // the declarative databases (DATABASES, docs/multi-database-replication.md): writes stopped
    // on each too, beside the cluster database
    let declarative: Vec<String> = var("DATABASES").split_whitespace().map(str::to_string).collect();
    let directory = match database.rsplit_once('/') {
        Some((dir, _)) if !dir.is_empty() => dir.to_string(),
        _ => ".".to_string(),
    };
    let path_of = |file: &str| format!("{directory}/{file}");
    for file in &declarative {
        let before = header(&old, &path_of(file))?;
        if before.as_ref().is_some_and(|b| !b.contains("full shutdown")) && !shut_down(&old, &path_of(file)) {
            return die(format!("cannot shut down {file} on {old}\n"));
        }
        println!("writes stopped: {file} on {old} is in full shutdown");
    }

    let statistics = header(&old, &database)?;
    let last = final_sequence(&old, statistics, 60)?;
    println!("final replication sequence of {old}: {last}");
    let mut finals: Vec<(String, u64)> = Vec::new();
    for file in &declarative {
        let statistics = header(&old, &path_of(file))?;
        let last = final_sequence_of(&old, Some(file), statistics, 60)?;
        println!("final replication sequence of {file} on {old}: {last}");
        finals.push((file.clone(), last));
    }
    for host in std::iter::once(&target).chain(others.iter()) {
        wait_until(&format!("{host} to apply segment {last}"), timeout, false, || {
            let reply = request(host, "POSITION", 60);
            let answer = first(&reply).unwrap_or("");
            if host == &target && answer == "OK primary" {
                return true;
            }
            position(answer).is_some_and(|(sequence, pending)| sequence >= last && pending == 0)
        })?;
        println!("{host} has applied everything up to segment {last}");
        for (file, last) in &finals {
            let ask = format!("DB {file} POSITION");
            wait_until(&format!("{host} to apply segment {last} of {file}"), timeout, false, || {
                let reply = request(host, &ask, 60);
                let answer = first(&reply).unwrap_or("");
                if host == &target && answer == "OK primary" {
                    return true;
                }
                position(answer).is_some_and(|(sequence, pending)| sequence >= *last && pending == 0)
            })?;
            println!("{host} has applied everything up to segment {last} of {file}");
        }
    }
    println!("switchover ready: promote {target} after segment {last}");

    let mut outcome = "restart in-place promotion not requested".to_string();
    if var("PROMOTE_IN_PLACE") == "true" {
        let archived = Some(var("ARCHIVED")).filter(|a| digits(a)).unwrap_or_else(|| "none".into());
        let reply = request(&target, &format!("PROMOTE {archived}"), 60);
        let answer = match &reply {
            Some(lines) => lines.first().cloned().unwrap_or_else(|| "no reply".into()),
            None => "segment server not reachable".into(),
        };
        // ^OK (\d+)(?: (\d+))?$
        let promoted = answer.strip_prefix("OK ").and_then(|rest| {
            let mut fields = rest.split(' ');
            let sequence = fields.next().filter(|s| digits(s))?;
            let rounds = fields.next();
            if fields.next().is_some() || rounds.is_some_and(|r| !digits(r)) {
                return None;
            }
            Some((sequence.to_string(), rounds.unwrap_or("1").to_string()))
        });
        match promoted {
            Some((sequence, rounds)) => {
                outcome = format!("inplace {sequence} {rounds}");
                println!("{target} promoted in place: its journal continues after segment {sequence}");
                // then each declarative database; one that cannot be promoted in place has the
                // target restarted, whose init promotes it offline (and leaves the others as they are)
                for file in &declarative {
                    let reply = request(&target, &format!("DB {file} PROMOTE none"), 60);
                    let answer = match &reply {
                        Some(lines) => lines.first().cloned().unwrap_or_else(|| "no reply".into()),
                        None => "segment server not reachable".into(),
                    };
                    if answer.starts_with("OK ") {
                        println!("{file} promoted in place on {target}: {answer}");
                    } else {
                        outcome = format!("restart {file}: {answer}");
                        println!("in-place promotion of {file} on {target} failed ({answer}): the target is promoted offline instead");
                        break;
                    }
                }
            }
            None => {
                outcome = format!("restart {answer}");
                println!("in-place promotion of {target} failed ({answer}): it is promoted offline instead");
            }
        }
    }
    result_file(&outcome);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// failover.pl

pub fn failover() -> Done {
    let candidates: Vec<String> = var("CANDIDATES").split_whitespace().map(str::to_string).collect();
    if candidates.is_empty() {
        return die("no candidate replicas\n");
    }
    let settle = seconds("SETTLE_SECONDS", 60);
    let declarative: Vec<String> = var("DATABASES").split_whitespace().map(str::to_string).collect();
    let position_in = |host: &str, request: &str| {
        let mut reply = client::open(host, request, Some(Duration::from_secs(30))).ok()?;
        let answer = line(&mut reply);
        // ^OK (\d+) (\d+) (\d+)
        let mut fields = answer.strip_prefix("OK ")?.splitn(3, ' ');
        let sequence = fields.next().and_then(number)?;
        fields.next().filter(|o| digits(o))?;
        let rest = fields.next()?;
        let pending: String = rest.chars().take_while(char::is_ascii_digit).collect();
        Some((sequence, number(&pending)?))
    };
    let position_of = |host: &str| position_in(host, "POSITION");
    // a candidate's least position among the declarative databases (DATABASES): None when one of
    // them has no position there (not seeded yet), which ranks it below one that has them all
    let least_of = |host: &str| -> Option<u64> {
        declarative
            .iter()
            .map(|file| position_in(host, &format!("DB {file} POSITION")).map(|(sequence, _)| sequence))
            .try_fold(u64::MAX, |least, sequence| sequence.map(|s| least.min(s)))
    };
    let mut positions: HashMap<String, u64> = HashMap::new();
    let deadline = now() + settle;
    for host in &candidates {
        let mut found;
        loop {
            found = position_of(host);
            if found.is_some_and(|(_, pending)| pending == 0) || now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_secs(2));
        }
        match found {
            Some((sequence, pending)) => {
                positions.insert(host.clone(), sequence);
                let still = if pending > 0 {
                    format!(" ({pending} segment(s) still pending)")
                } else {
                    String::new()
                };
                println!("{host}: applied up to segment {sequence}{still}");
            }
            None => println!("{host}: no position (unreachable or not a replica), not a candidate"),
        }
    }
    let ranked: Vec<&String> = candidates.iter().filter(|h| positions.contains_key(*h)).collect();
    let Some(mut best) = ranked.first().copied() else {
        return die("no candidate reported a position\n");
    };
    // the cluster database's position first, then the least among the declarative databases;
    // ties keep the listed order
    let least: HashMap<String, Option<u64>> = ranked.iter().map(|h| ((*h).clone(), least_of(h))).collect();
    let rank = |host: &String| (positions[host], least[host]);
    for host in &ranked {
        if rank(host) > rank(best) {
            best = host;
        }
    }
    let listed: Vec<String> = ranked.iter().map(|h| format!("{h}:{}", positions[*h])).collect();
    let mut summary = format!("target={best} sequence={} positions={}", positions[best], listed.join(","));
    if !declarative.is_empty() {
        let shown: Vec<String> = ranked
            .iter()
            .map(|h| format!("{h}:{}", least[*h].map_or("none".to_string(), |s| s.to_string())))
            .collect();
        summary.push_str(&format!(" databases={}", shown.join(",")));
    }
    println!("{summary}");
    result_file(&summary);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// sync-standby.pl

pub fn sync_standby() -> Done {
    let action = var("ACTION");
    let primary = required("PRIMARY")?;
    let standby = required("STANDBY")?;
    let database = required("DATABASE_PATH")?;
    let timeout = seconds("TIMEOUT_SECONDS", 300);
    let result = or("RESULT_FILE", "/dev/termination-log");
    let others: Vec<String> = var("OTHERS").split(',').filter(|h| !h.is_empty()).map(str::to_string).collect();
    // the SYNC argument: the standbys that stay attached, with or without this one
    let sync_list = |with: Option<&str>| {
        let hosts: Vec<&str> = others.iter().map(String::as_str).chain(with).collect();
        if hosts.is_empty() {
            "none".to_string()
        } else {
            hosts.join(",")
        }
    };
    if action != "attach" && action != "detach" {
        return die("ACTION must be attach or detach\n");
    }
    let ok = |host: &str, line_text: &str| first(&request(host, line_text, 60)) == Some("OK");
    let mut paused: Option<u64> = None;
    let online = |paused_at: Option<u64>, paused: &mut Option<u64>| {
        let service = format!("{primary}:service_mgr");
        if system(
            "fbsvcmgr",
            &[
                &service,
                "action_properties",
                "dbname",
                &database,
                "prp_online_mode",
                "prp_sm_normal",
            ],
        ) {
            if let Some(at) = paused_at {
                *paused = Some(now() - at);
            }
            if let Some(p) = paused {
                println!("writes were paused for {p}s");
            }
        } else {
            println!("could not bring {primary} back online");
        }
    };
    let report = |outcome: &str, paused: Option<u64>| {
        println!("{outcome}");
        let message = format!("{outcome}{}", paused.map(|p| format!("\npaused {p}s")).unwrap_or_default());
        let _ = fs::write(&result, message);
    };

    // 1. stop writes
    let started = now();
    let before = header(&primary, &database)?;
    let paused_at;
    if before.as_ref().is_none_or(|b| b.contains("full shutdown")) {
        println!("{primary} already shut down");
        paused_at = Some(started);
    } else {
        if !shut_down(&primary, &database) {
            return die(format!("cannot shut down {primary}\n"));
        }
        paused_at = Some(now());
        println!("writes stopped: {primary} is in full shutdown");
    }

    let mut standby_on: Option<u64> = None;
    let outcome: Result<&str, Exit> = (|| {
        // 2. the final sequence, from the header page on disk through the segment server
        let reply = request(&primary, "HEADER", 60).ok_or_else(|| Exit::from(format!("cannot reach the segment server of {primary}\n")))?;
        let answer = reply.first().cloned();
        let last = answer
            .as_deref()
            .and_then(|a| a.strip_prefix("OK "))
            .and_then(number)
            .ok_or_else(|| {
                Exit::from(format!(
                    "cannot read the header of {primary}: {}\n",
                    answer.as_deref().unwrap_or("no reply")
                ))
            })?;
        println!("final replication sequence of {primary}: {last}");
        if action == "detach" {
            let reached = ok(&standby, &format!("STANDBY off {last}"));
            if !ok(&primary, &format!("SYNC {}", sync_list(None))) {
                // still attached: the standby must not apply the journal
                if reached {
                    ok(&standby, "STANDBY on");
                }
                return die(format!("cannot turn synchronous replication off on {primary}\n"));
            }
            println!("synchronous replication off on {primary}");
            if reached {
                println!("{standby} continues from the journal after segment {last}");
                return Ok("detached");
            }
            println!("{standby} cannot be reached: it has to be re-seeded");
            return Ok("detached unreachable");
        }
        wait_until(&format!("{standby} to apply segment {last}"), timeout, true, || {
            let reply = request(&standby, "POSITION", 60);
            position(first(&reply).unwrap_or("")).is_some_and(|(sequence, pending)| sequence >= last && pending == 0)
        })?;
        println!("{standby} has applied everything up to segment {last}");
        if !ok(&primary, &format!("SYNC {}", sync_list(Some(&standby)))) {
            return die(format!("cannot turn synchronous replication on on {primary}\n"));
        }
        println!("synchronous replication to {standby} on {primary} (from its next opening)");
        standby_on = Some(last);
        if !ok(&standby, "STANDBY on") {
            return die(format!("{standby} refused to become the synchronous standby\n"));
        }
        Ok("attached")
    })();

    match outcome {
        Ok(outcome) => {
            online(paused_at, &mut paused);
            println!("{primary} is online");
            report(outcome, paused);
            Ok(())
        }
        Err(error) => {
            // back as it was: no synchronous standby; "failed clean" when the standby never
            // received a change synchronously
            let mut clean = true;
            if action == "attach" {
                if !ok(&primary, &format!("SYNC {}", sync_list(None))) {
                    clean = false;
                }
                if let Some(at) = standby_on {
                    if !ok(&standby, &format!("STANDBY off {at}")) {
                        clean = false;
                    }
                }
            }
            online(paused_at, &mut paused);
            report(if clean { "failed clean" } else { "failed" }, paused);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_times() {
        assert_eq!(journal_sequence("mydb.fdb.journal-000000012"), Some(12));
        for name in [
            "mydb.fdb.journal-",
            "mydb.fdb.journal-1x",
            "my db.journal-1",
            ".journal-1",
            "a/b.journal-1",
        ] {
            assert_eq!(journal_sequence(name), None, "{name}");
        }
        assert!(lineage_name("mydb.fdb.lineage-5-7"));
        assert!(!lineage_name("mydb.fdb.lineage-5") && !lineage_name("mydb.fdb.lineage-5-x") && !lineage_name(".lineage-1-2"));
        assert_eq!(utc(0), (1970, 1, 1, 0, 0, 0));
        assert_eq!(utc(1_709_164_800 + 3661), (2024, 2, 29, 1, 1, 1));
        assert_eq!(position("OK 12 0 3"), Some((12, 3)));
        assert_eq!(position("OK primary"), None);
        assert_eq!(position("OK 12 0 3 4"), None);
    }

    #[test]
    fn plans_and_describes_a_replay() {
        let dir = std::env::temp_dir().join(format!("fb-agent-jobs-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("seg")).unwrap();
        let segment = |sequence: u64, blocks: &[(u64, u16, u32)]| {
            let mut body = Vec::new();
            for (t, f, s) in blocks {
                body.extend(t.to_le_bytes());
                body.extend(1u16.to_le_bytes());
                body.extend(f.to_le_bytes());
                body.extend(s.to_le_bytes());
                body.extend(vec![0u8; *s as usize]);
            }
            let mut bytes = b"FBCHANGELOG\0".to_vec();
            bytes.extend([0u8; 20]);
            bytes.extend(sequence.to_le_bytes());
            bytes.extend((48 + body.len() as u64).to_le_bytes());
            bytes.extend(body);
            bytes
        };
        let seg = dir.join("seg");
        fs::write(seg.join("db.journal-000000005"), segment(5, &[(40, 1, 4), (41, 1, 2)])).unwrap();
        fs::write(seg.join("db.journal-000000006"), segment(6, &[(41, 0, 3), (50, 1, 1)])).unwrap();
        fs::write(dir.join("candidates"), "41\n").unwrap();
        let control_path = dir.join("ctl").to_string_lossy().into_owned();
        let s = seg.to_string_lossy().into_owned();
        let c = dir.join("candidates").to_string_lossy().into_owned();
        pitr_plan(&[&s, "5", "41", "45", &c, "6", &control_path]).unwrap();
        assert_eq!(fs::read(&control_path).unwrap(), control(1, 5, 5, &[(41, 5)]));
        pitr_plan(&["--describe", &control_path]).unwrap();
        // a transaction complete in the backup with changes after it
        let error = pitr_plan(&[&s, "5", "42", "45", &c, "6", &control_path]).unwrap_err();
        assert!(
            error.message.contains("transaction 41 is complete in the backup"),
            "{}",
            error.message
        );
        // segment 7 missing
        assert!(pitr_plan(&[&s, "5", "41", "45", &c, "7", &control_path])
            .unwrap_err()
            .message
            .contains("segment 7 is not available"));
        // without segment 5 the earlier segments are needed: the backup's (5) is not there
        fs::remove_file(seg.join("db.journal-000000005")).unwrap();
        let need = pitr_plan(&[&s, "5", "41", "45", &c, "6", &control_path]).unwrap_err();
        assert_eq!((need.code, need.message.as_str()), (3, ""));
        // from 6 on, transaction 41 began before: needed too, and said why
        let begun = pitr_plan(&[&s, "0", "41", "45", &c, "6", &control_path]).unwrap_err();
        assert_eq!(begun.code, 3);
        assert_eq!(begun.message, "transaction(s) 41 started before segment 6\n");
        fs::remove_dir_all(dir).unwrap();
    }
}
