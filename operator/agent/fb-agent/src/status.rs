//! The requests about an instance's state that are answered from files on its volume
//! (docs/rust-agent-segment-server.md, step 2), each with the reply segment-server.pl gives:
//! HEADER, ISOLATION, POSITION, PRIMARYSEEN, FOLLOW, SYNCTO, POINTS, LINEAGE, ARCHIVED, and the
//! two that record a number for the pruning, UPLOADED and RETAIN.

use crate::server::segment_name;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

/// The files of an instance, as segment-server.pl names them.
pub struct Files {
    pub database: PathBuf,
    pub archive: PathBuf,
    /// SOURCE_DIR: where a replica's segments and its control file are
    pub source: PathBuf,
    /// REPLICATION_DIR
    pub base: PathBuf,
    /// PRIMARY_FILE: the primary's name, from the ConfigMap
    pub primary_file: PathBuf,
    /// POD_NAME
    pub pod: String,
}

/// A file's content without the white space at its end; empty when it cannot be read.
fn slurp(path: &Path) -> String {
    fs::read_to_string(path).map(|text| text.trim_end().to_string()).unwrap_or_default()
}

fn digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

fn u16_at(page: &[u8], at: usize) -> Option<usize> {
    Some(u16::from_le_bytes(page.get(at..at + 2)?.try_into().ok()?) as usize)
}

/// The replication sequence (HDR_repl_seq clump, 0 without one) from the header page of a
/// database file; None if unreadable. u16 ODS version at 18, hdr_end and the clumps at 66 / 128
/// (ODS 13) or 36 / 148 (ODS 14).
pub fn header_sequence(database: &Path) -> Option<u64> {
    let mut page = Vec::new();
    File::open(database).ok()?.take(65536).read_to_end(&mut page).ok()?;
    if page.len() < 4096 || page[0] != 1 {
        return None;
    }
    let (end_at, mut at) = match u16_at(&page, 18)? & 0x7fff {
        13 => (66, 128),
        14 => (36, 148),
        _ => return None,
    };
    let end = u16_at(&page, end_at)?;
    if end > page.len() {
        return None;
    }
    while at < end {
        let (kind, length) = (*page.get(at)?, *page.get(at + 1)? as usize);
        if kind == 0 {
            break;
        }
        if kind == 11 && length == 8 {
            return Some(u64::from_le_bytes(page.get(at + 2..at + 10)?.try_into().ok()?));
        }
        at += 2 + length;
    }
    Some(0)
}

/// The sequence in a segment file's header; None without one.
pub fn segment_sequence(path: &Path) -> Option<u64> {
    let mut header = [0u8; 48];
    File::open(path).ok()?.read_exact(&mut header).ok()?;
    header
        .starts_with(b"FBCHANGELOG")
        .then(|| u64::from_le_bytes(header[32..40].try_into().expect("8 bytes")))
}

/// The segment files of a directory, sorted by name (complete or not, as the Perl server's).
fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else { return Vec::new() };
    let mut names: Vec<String> = entries
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| segment_name(name))
        .collect();
    names.sort();
    names.into_iter().map(|name| dir.join(name)).filter(|path| path.is_file()).collect()
}

/// A replica's control file ({char[10] "FBREPLCTL", u16 version, u32 txn_count, u64 sequence,
/// u32 offset, ...}): the only {GUID} file in the source directory. (sequence, offset)
fn control(source: &Path) -> Option<(u64, u32)> {
    let name = fs::read_dir(source)
        .ok()?
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .find(|name| {
            name.len() > 2
                && name.starts_with('{')
                && name.ends_with('}')
                && name[1..name.len() - 1].bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
        })?;
    control_file(&source.join(name))
}

/// A control file's (sequence, offset); None when it is not one.
pub fn control_file(path: &Path) -> Option<(u64, u32)> {
    let data = fs::read(path).ok()?;
    if data.len() < 40 || !data.starts_with(b"FBREPLCTL") {
        return None;
    }
    Some((
        u64::from_le_bytes(data[16..24].try_into().ok()?),
        u32::from_le_bytes(data[24..28].try_into().ok()?),
    ))
}

impl Files {
    pub fn is_primary(&self) -> bool {
        if self.base.join("promoted").exists() {
            return true;
        }
        let primary = slurp(&self.primary_file);
        primary.is_empty()
            || primary
                .strip_prefix(self.pod.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
    }

    pub fn header(&self) -> String {
        match header_sequence(&self.database) {
            Some(sequence) => format!("OK {sequence}\n"),
            None => format!("ERR cannot read the header of {}\n", self.database.display()),
        }
    }

    pub fn isolation(&self) -> String {
        let marker = self.base.join("self-fenced");
        if !marker.is_file() {
            return "OK online\n".into();
        }
        let since = slurp(&marker);
        format!("OK fenced {}\n", if since.is_empty() { "0" } else { &since })
    }

    /// "OK primary"; a synchronous standby: as far as the archive went; a replica: the control
    /// file's sequence and offset, and the segments fetched after it.
    pub fn position(&self) -> String {
        if self.is_primary() {
            return "OK primary\n".into();
        }
        let seen = slurp(&self.base.join("sync-seen"));
        if self.base.join("sync-standby").is_file() && digits(&seen) {
            return format!("OK {seen} 0 0\n");
        }
        let Some((sequence, offset)) = control(&self.source) else {
            return "ERR no replica control file\n".into();
        };
        let pending = segment_files(&self.source)
            .iter()
            .filter(|path| segment_sequence(path).is_some_and(|s| s > sequence))
            .count();
        format!("OK {sequence} {offset} {pending}\n")
    }

    /// Seconds since the segment puller last reached the primary, and which one.
    pub fn primary_seen(&self, now: u64) -> String {
        let seen = slurp(&self.base.join("primary-seen"));
        match seen.split_once(' ') {
            Some((at, host)) if digits(at) && !host.is_empty() && !host.contains(char::is_whitespace) => match at.parse::<i64>() {
                Ok(at) => format!("OK {} {host}\n", now as i64 - at),
                Err(_) => "OK never\n".into(),
            },
            _ => "OK never\n".into(),
        }
    }

    /// A replica cluster's archive follower: its last state.
    pub fn follow(&self) -> String {
        let state = slurp(&self.base.join("follow.state"));
        format!("OK {}\n", if state.is_empty() { "none" } else { &state })
    }

    /// The synchronous replicas named in the primary's sync.conf (`sync_replica = host:...`).
    pub fn sync_to(&self) -> String {
        let text = fs::read_to_string(self.base.join("sync.conf")).unwrap_or_default();
        let hosts: Vec<&str> = text
            .lines()
            .filter_map(|line| {
                let value = line.strip_prefix("sync_replica")?.trim_start().strip_prefix('=')?.trim_start();
                let (host, _) = value.split_once(':')?;
                (!host.is_empty() && !host.contains(char::is_whitespace)).then_some(host)
            })
            .collect();
        format!("OK {}\n", if hosts.is_empty() { "none".into() } else { hosts.join(",") })
    }

    /// The recovery points sampled for a segment: "<epoch> <offset>" lines, then ".".
    pub fn points(&self, segment: &str) -> String {
        let text = fs::read_to_string(self.base.join("points").join(segment)).unwrap_or_default();
        let mut reply = String::new();
        for line in text.split_inclusive('\n') {
            let pair = line.strip_suffix('\n').and_then(|line| line.split_once(' '));
            if pair.is_some_and(|(a, b)| digits(a) && digits(b)) {
                reply.push_str(line);
            }
        }
        reply + ".\n"
    }

    /// The lineage markers of the database's promotions, as file names, then ".".
    pub fn lineage(&self) -> String {
        let name = self
            .database
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let text = fs::read_to_string(self.base.join("lineage")).unwrap_or_default();
        let mut reply = String::new();
        for line in text.lines() {
            if let Some((a, b)) = line.trim_end().split_once(' ') {
                if digits(a) && digits(b) {
                    reply.push_str(&format!("{name}.lineage-{a}-{b}\n"));
                }
            }
        }
        reply + ".\n"
    }

    /// Each archived segment's sequence and age in seconds (by this server's clock), then ".".
    pub fn archived(&self, now: u64) -> String {
        let mut reply = String::new();
        for path in segment_files(&self.archive) {
            let modified = fs::metadata(&path).and_then(|m| m.modified()).ok();
            let modified = modified.and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok());
            if let (Some(sequence), Some(modified)) = (segment_sequence(&path), modified) {
                reply.push_str(&format!("{sequence} {}\n", now as i64 - modified.as_secs() as i64));
            }
        }
        reply + ".\n"
    }

    /// Records a number the pruning reads (uploaded-floor, retain-floor); None removes the file.
    pub fn record(&self, file: &str, value: Option<&str>) -> String {
        let path = self.base.join(file);
        match value {
            None => {
                let _ = fs::remove_file(&path);
            }
            Some(value) => {
                let temporary = self.base.join(format!("{file}.tmp"));
                if fs::write(&temporary, format!("{value}\n")).is_ok() {
                    let _ = fs::rename(&temporary, &path);
                }
            }
        }
        "OK\n".into()
    }
}

/// The engine version ("5.0.4") from the server's version string ("LI-V5.0.4.1812 Firebird 5.0",
/// "LI-T6.0.0.2455 Firebird 6.0 ..."): what `RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION')` says.
pub fn engine_version(server_version: &str) -> Option<String> {
    let build = server_version.split_whitespace().next()?.split_once('-')?.1;
    let numbers: Vec<&str> = build.get(1..)?.split('.').collect();
    (numbers.len() >= 3 && numbers.iter().all(|n| digits(n))).then(|| numbers[..3].join("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(root: &Path) -> Files {
        for dir in ["archive", "source", "repl"] {
            fs::create_dir_all(root.join(dir)).unwrap();
        }
        Files {
            database: root.join("mydb.fdb"),
            archive: root.join("archive"),
            source: root.join("source"),
            base: root.join("repl"),
            primary_file: root.join("primary"),
            pod: "db-1".into(),
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("fb-agent-status-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn segment(sequence: u64) -> Vec<u8> {
        let mut bytes = b"FBCHANGELOG\0".to_vec();
        bytes.extend([0u8; 20]);
        bytes.extend(sequence.to_le_bytes());
        bytes.extend(48u64.to_le_bytes());
        bytes
    }

    #[test]
    fn reads_the_engine_version() {
        assert_eq!(engine_version("LI-V5.0.4.1812 Firebird 5.0").as_deref(), Some("5.0.4"));
        assert_eq!(engine_version("LI-T6.0.0.2455 Firebird 6.0 67dbeb4").as_deref(), Some("6.0.0"));
        assert_eq!(engine_version("LI-V4.0.7.3271 Firebird 4.0").as_deref(), Some("4.0.7"));
        assert_eq!(engine_version("Firebird"), None);
        assert_eq!(engine_version(""), None);
    }

    #[test]
    fn reads_the_header_sequence() {
        let root = scratch("header");
        let f = files(&root);
        assert!(f.header().starts_with("ERR cannot read the header of "));
        // ODS 13: hdr_end at 66, clumps from 128; a sweep clump, then the replication sequence
        let mut page = vec![0u8; 8192];
        page[0] = 1;
        page[18..20].copy_from_slice(&(13u16 | 0x8000).to_le_bytes());
        page[128..132].copy_from_slice(&[5, 2, 9, 9]);
        page[132..134].copy_from_slice(&[11, 8]);
        page[134..142].copy_from_slice(&77u64.to_le_bytes());
        page[66..68].copy_from_slice(&143u16.to_le_bytes());
        fs::write(&f.database, &page).unwrap();
        assert_eq!(f.header(), "OK 77\n");
        // ODS 14, without the clump
        page[18..20].copy_from_slice(&14u16.to_le_bytes());
        page[36..38].copy_from_slice(&148u16.to_le_bytes());
        fs::write(&f.database, &page).unwrap();
        assert_eq!(f.header(), "OK 0\n");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tells_the_position() {
        let root = scratch("position");
        let f = files(&root);
        // no primary named: this one
        assert_eq!(f.position(), "OK primary\n");
        fs::write(&f.primary_file, "db-1.db-headless\n").unwrap();
        assert_eq!(f.position(), "OK primary\n");
        fs::write(&f.primary_file, "db-10.db-headless\n").unwrap();
        assert_eq!(f.position(), "ERR no replica control file\n");
        let mut control = b"FBREPLCTL\0".to_vec();
        control.extend(1u16.to_le_bytes());
        control.extend(0u32.to_le_bytes());
        control.extend(41u64.to_le_bytes());
        control.extend(1234u32.to_le_bytes());
        control.extend([0u8; 12]);
        fs::write(f.source.join("{6A0B3C1D-0000-4000-8000-000000000001}"), control).unwrap();
        fs::write(f.source.join("mydb.fdb.journal-000000041"), segment(41)).unwrap();
        fs::write(f.source.join("mydb.fdb.journal-000000042"), segment(42)).unwrap();
        fs::write(f.source.join("mydb.fdb.journal-000000043"), segment(43)).unwrap();
        assert_eq!(f.position(), "OK 41 1234 2\n");
        // a synchronous standby that has seen the archive's end
        fs::write(f.base.join("sync-standby"), "").unwrap();
        assert_eq!(f.position(), "OK 41 1234 2\n");
        fs::write(f.base.join("sync-seen"), "57\n").unwrap();
        assert_eq!(f.position(), "OK 57 0 0\n");
        // promoted in place: the primary whatever the file says
        fs::write(f.base.join("promoted"), "").unwrap();
        assert_eq!(f.position(), "OK primary\n");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn answers_from_the_small_files() {
        let root = scratch("small");
        let f = files(&root);
        assert_eq!(f.isolation(), "OK online\n");
        assert_eq!(f.primary_seen(1000), "OK never\n");
        assert_eq!(f.follow(), "OK none\n");
        assert_eq!(f.sync_to(), "OK none\n");
        assert_eq!(f.points("7"), ".\n");
        assert_eq!(f.lineage(), ".\n");
        assert_eq!(f.archived(1000), ".\n");
        fs::write(f.base.join("self-fenced"), "1700000000 lease\n").unwrap();
        assert_eq!(f.isolation(), "OK fenced 1700000000 lease\n");
        fs::write(f.base.join("self-fenced"), "").unwrap();
        assert_eq!(f.isolation(), "OK fenced 0\n");
        fs::write(f.base.join("primary-seen"), "940 db-0.db-headless\n").unwrap();
        assert_eq!(f.primary_seen(1000), "OK 60 db-0.db-headless\n");
        fs::write(f.base.join("follow.state"), "applied 12\n").unwrap();
        assert_eq!(f.follow(), "OK applied 12\n");
        fs::write(
            f.base.join("sync.conf"),
            "sync_replica = db-1.db-headless:3050/x\n# no\nsync_replica=db-2.h:1\n",
        )
        .unwrap();
        assert_eq!(f.sync_to(), "OK db-1.db-headless,db-2.h\n");
        fs::create_dir_all(f.base.join("points")).unwrap();
        fs::write(f.base.join("points/7"), "100 48\nbad\n101 96\n102 1").unwrap();
        assert_eq!(f.points("7"), "100 48\n101 96\n.\n");
        fs::write(f.base.join("lineage"), "3 1700000000\nx\n4 1700000100 \n").unwrap();
        assert_eq!(f.lineage(), "mydb.fdb.lineage-3-1700000000\nmydb.fdb.lineage-4-1700000100\n.\n");
        fs::write(f.archive.join("mydb.fdb.journal-000000005"), segment(5)).unwrap();
        fs::write(f.archive.join("mydb.fdb.journal-000000006"), "x").unwrap();
        let listed = f.archived(crate::client::now() + 30);
        let (sequence, age) = listed.lines().next().unwrap().split_once(' ').unwrap();
        assert_eq!(sequence, "5");
        assert!((29..=32).contains(&age.parse::<i64>().unwrap()), "{listed}");
        assert_eq!(listed.lines().count(), 2);
        assert_eq!(f.record("retain-floor", Some("12")), "OK\n");
        assert_eq!(fs::read_to_string(f.base.join("retain-floor")).unwrap(), "12\n");
        assert_eq!(f.record("retain-floor", None), "OK\n");
        assert!(!f.base.join("retain-floor").exists());
        fs::remove_dir_all(root).unwrap();
    }
}
