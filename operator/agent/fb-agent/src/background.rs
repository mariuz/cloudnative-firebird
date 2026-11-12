//! What the segment server does beside answering requests (docs/rust-agent-segment-server.md,
//! step 5): the pruning of the journal archive and the recovery-point sampler, as threads of the
//! agent, with the rules of segment-server.pl. The Perl server behind the front is started with
//! its own switched off (the environment it already reads), so each is done once.

use crate::server::segment_name;
use crate::status::{header_sequence, segment_sequence, slurp, Files};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The retention of the archive, from the environment segment-server.pl reads.
pub struct Retention {
    /// SEGMENT_RETENTION_SECONDS (a day): the age a segment is kept for at least
    pub keep: u64,
    /// SEGMENT_MAX_RETENTION_SECONDS (a week, never below `keep`): the age at which a segment
    /// goes even though a replica has not applied it
    pub keep_at_most: u64,
    /// PRUNE_APPLIED: delete what every replica applied before the retention age
    pub applied: bool,
    /// ARCHIVE_UPLOAD: only what the journal archive Job has uploaded too
    pub upload: bool,
}

/// What the Perl server behind the front is given, so that it neither prunes nor samples: ages
/// no file reaches, and the sampler's own switch.
pub const BACKEND_OFF: [(&str, &str); 4] = [
    ("SEGMENT_RETENTION_SECONDS", "4000000000"),
    ("SEGMENT_MAX_RETENTION_SECONDS", "4000000000"),
    ("PRUNE_APPLIED", "false"),
    ("RECOVERY_POINTS", "false"),
];

fn number(text: &str) -> Option<u64> {
    (!text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()))
        .then(|| text.parse().ok())
        .flatten()
}

fn age(path: &Path, now: u64) -> Option<i64> {
    let modified = fs::metadata(path).and_then(|m| m.modified()).ok()?;
    Some(now as i64 - modified.duration_since(UNIX_EPOCH).ok()?.as_secs() as i64)
}

/// Segments older than the retention age are deleted unless a replica has not applied them yet
/// (sequence above the floor); those are kept up to the maximum retention age, so a slow or
/// stopped replica can catch up without being re-seeded, and a replica that never returns cannot
/// fill the volume.
pub fn prune(files: &Files, retention: &Retention, now: u64) {
    // recovery points of segments long gone
    if let Ok(entries) = fs::read_dir(files.base.join("points")) {
        for path in entries.filter_map(|entry| Some(entry.ok()?.path())) {
            let named = path.file_name().and_then(|n| n.to_str()).is_some_and(|n| number(n).is_some());
            if named && age(&path, now).is_some_and(|age| age > retention.keep_at_most as i64) {
                let _ = fs::remove_file(path);
            }
        }
    }
    // the lowest segment a replica still needs, as the operator last measured it
    let measured = number(&slurp(&files.base.join("retain-floor")));
    let mut floor = measured;
    // No replica holds segments back (a single instance): keep those after the offline bootstrap
    // seed, so a replica added later can still be seeded from it without locking the primary.
    let bootstrap = files.base.join("bootstrap-seed.fdb");
    if floor.is_none() && bootstrap.is_file() {
        floor = number(&slurp(&files.base.join("bootstrap-seed.seq"))).or(Some(header_sequence(&bootstrap).unwrap_or(0)));
    }
    // Early: segments below the measured floor (every replica has pulled and applied them; the
    // floor segment itself may be partly applied) and, when the journal archive uploads them,
    // not above the last upload
    let mut applied: Option<i128> = None;
    if let (true, Some(measured)) = (retention.applied, measured) {
        applied = Some(measured as i128 - 1);
        if retention.upload {
            applied = number(&slurp(&files.base.join("uploaded-floor"))).map(|uploaded| (uploaded as i128).min(measured as i128 - 1));
        }
    }
    let Ok(entries) = fs::read_dir(&files.archive) else { return };
    for path in entries.filter_map(|entry| Some(entry.ok()?.path())) {
        let named = path.file_name().and_then(|n| n.to_str()).is_some_and(segment_name);
        if !named || !path.is_file() {
            continue;
        }
        let Some(age) = age(&path, now) else { continue };
        if let Some(applied) = applied {
            if segment_sequence(&path).is_some_and(|sequence| sequence as i128 <= applied) {
                let _ = fs::remove_file(&path);
                continue;
            }
        }
        if age < retention.keep as i64 {
            continue;
        }
        if age < retention.keep_at_most as i64 {
            if let Some(floor) = floor {
                if segment_sequence(&path).is_none_or(|sequence| sequence > floor) {
                    continue;
                }
            }
        }
        let _ = fs::remove_file(&path);
    }
}

/// One look at the primary's journal: each new length of a segment still being written is
/// appended to points/<sequence> as "<epoch seconds> <length>". The header length only grows by
/// whole writes (the blocks of a commit), so a segment cut at a recorded length is a valid
/// segment ending at that moment: point-in-time recovery cuts the segment after its target there.
pub fn sample(journal: &Path, points: &Path, last: &mut HashMap<u64, u64>, now: u64) {
    let Ok(entries) = fs::read_dir(journal) else { return };
    let names: Vec<String> = entries
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| segment_name(name))
        .collect();
    for name in &names {
        let mut header = [0u8; 48];
        let read = File::open(journal.join(name)).and_then(|mut file| file.read_exact(&mut header));
        if read.is_err() || !header.starts_with(b"FBCHANGELOG") {
            continue;
        }
        let sequence = u64::from_le_bytes(header[32..40].try_into().expect("8 bytes"));
        let length = u64::from_le_bytes(header[40..48].try_into().expect("8 bytes"));
        if sequence == 0 || length <= 48 || last.get(&sequence) == Some(&length) {
            continue;
        }
        last.insert(sequence, length);
        if let Ok(mut file) = OpenOptions::new().append(true).create(true).open(points.join(sequence.to_string())) {
            let _ = writeln!(file, "{now} {length}");
        }
    }
    // forget segments no longer written
    let live: Vec<u64> = names
        .iter()
        .filter_map(|name| name.rsplit_once(".journal-")?.1.parse().ok())
        .collect();
    last.retain(|sequence, _| live.contains(sequence));
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Starts the pruning (at once, then every minute) and, with a journal to sample, the sampler
/// (every second, while this instance is the primary).
pub fn start(files: Files, retention: Retention, journal: Option<PathBuf>) {
    let points = files.base.join("points");
    let sampled = Files {
        database: files.database.clone(),
        archive: files.archive.clone(),
        source: files.source.clone(),
        base: files.base.clone(),
        primary_file: files.primary_file.clone(),
        pod: files.pod.clone(),
    };
    thread::spawn(move || loop {
        prune(&files, &retention, now());
        thread::sleep(Duration::from_secs(60));
    });
    if let Some(journal) = journal {
        thread::spawn(move || {
            let _ = fs::create_dir(&points);
            let mut last = HashMap::new();
            loop {
                if sampled.is_primary() {
                    sample(&journal, &points, &mut last, now());
                }
                thread::sleep(Duration::from_secs(1));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> (PathBuf, Files) {
        let root = std::env::temp_dir().join(format!("fb-agent-background-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        for dir in ["archive", "source", "repl/points", "journal"] {
            fs::create_dir_all(root.join(dir)).unwrap();
        }
        let files = Files {
            database: root.join("mydb.fdb"),
            archive: root.join("archive"),
            source: root.join("source"),
            base: root.join("repl"),
            primary_file: root.join("primary"),
            pod: "db-0".into(),
        };
        (root, files)
    }

    fn segment(sequence: u64, length: u64) -> Vec<u8> {
        let mut bytes = b"FBCHANGELOG\0".to_vec();
        bytes.extend([0u8; 20]);
        bytes.extend(sequence.to_le_bytes());
        bytes.extend(length.to_le_bytes());
        bytes
    }

    /// segments 1..=6 in the archive, aged 10 days, 10 days, 2 days, 2 days, an hour, an hour
    fn archive(files: &Files, now: u64) {
        for (sequence, age) in [(1u64, 864_000u64), (2, 864_000), (3, 172_800), (4, 172_800), (5, 3600), (6, 3600)] {
            let path = files.archive.join(format!("mydb.fdb.journal-{sequence:09}"));
            fs::write(&path, segment(sequence, 48)).unwrap();
            let at = UNIX_EPOCH + Duration::from_secs(now - age);
            File::options().write(true).open(&path).unwrap().set_modified(at).unwrap();
        }
    }

    fn left(files: &Files) -> Vec<u64> {
        let mut sequences: Vec<u64> = fs::read_dir(&files.archive)
            .unwrap()
            .filter_map(|entry| segment_sequence(&entry.ok()?.path()))
            .collect();
        sequences.sort();
        sequences
    }

    #[test]
    fn prunes_by_age_floor_and_progress() {
        let (root, files) = scratch("prune");
        let now = now();
        let day = Retention {
            keep: 86_400,
            keep_at_most: 604_800,
            applied: false,
            upload: false,
        };
        // no floor: everything older than a day goes
        archive(&files, now);
        prune(&files, &day, now);
        assert_eq!(left(&files), [5, 6]);
        // a replica still needs segment 4 on: 4 is kept past the retention age, 3 (applied) is
        // not, and nothing is kept past the maximum age
        archive(&files, now);
        fs::write(files.base.join("retain-floor"), "3\n").unwrap();
        prune(&files, &day, now);
        assert_eq!(left(&files), [4, 5, 6]);
        // early: below the measured floor at once, whatever the age
        archive(&files, now);
        fs::write(files.base.join("retain-floor"), "6\n").unwrap();
        let early = Retention { applied: true, ..day };
        prune(&files, &early, now);
        assert_eq!(left(&files), [6]);
        // with an upload: not above what was uploaded, and nothing early before the first upload
        archive(&files, now);
        let uploading = Retention {
            applied: true,
            upload: true,
            ..day
        };
        prune(&files, &uploading, now);
        assert_eq!(left(&files), [5, 6], "nothing uploaded yet: only the ages and the floor, 5 stays");
        archive(&files, now);
        fs::write(files.base.join("uploaded-floor"), "4\n").unwrap();
        prune(&files, &uploading, now);
        assert_eq!(left(&files), [5, 6], "uploaded up to 4: 5 stays though every replica applied it");
        fs::write(files.base.join("uploaded-floor"), "9\n").unwrap();
        prune(&files, &uploading, now);
        assert_eq!(left(&files), [6], "uploaded further than applied: up to the floor less one");
        // no replica: the segments after the bootstrap seed are kept up to the maximum age
        fs::remove_file(files.base.join("retain-floor")).unwrap();
        archive(&files, now);
        fs::write(files.base.join("bootstrap-seed.fdb"), "x").unwrap();
        fs::write(files.base.join("bootstrap-seed.seq"), "2\n").unwrap();
        prune(&files, &day, now);
        assert_eq!(left(&files), [3, 4, 5, 6]);
        // recovery points of segments long gone
        for (name, age) in [("7", 864_000u64), ("8", 3600)] {
            let path = files.base.join("points").join(name);
            fs::write(&path, "1 100\n").unwrap();
            File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(UNIX_EPOCH + Duration::from_secs(now - age))
                .unwrap();
        }
        prune(&files, &day, now);
        assert!(!files.base.join("points/7").exists() && files.base.join("points/8").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn samples_each_new_length_once() {
        let (root, files) = scratch("sample");
        let (journal, points) = (root.join("journal"), files.base.join("points"));
        let mut last = HashMap::new();
        fs::write(journal.join("mydb.fdb.journal-000000009"), segment(9, 100)).unwrap();
        fs::write(journal.join("mydb.fdb.journal-000000010"), segment(10, 48)).unwrap();
        fs::write(journal.join("other"), segment(11, 500)).unwrap();
        sample(&journal, &points, &mut last, 1000);
        sample(&journal, &points, &mut last, 1001);
        fs::write(journal.join("mydb.fdb.journal-000000009"), segment(9, 260)).unwrap();
        sample(&journal, &points, &mut last, 1002);
        assert_eq!(fs::read_to_string(points.join("9")).unwrap(), "1000 100\n1002 260\n");
        // an empty segment has no point, and other files are not looked at
        assert!(!points.join("10").exists() && !points.join("11").exists());
        // a segment that is gone is forgotten
        fs::remove_file(journal.join("mydb.fdb.journal-000000009")).unwrap();
        sample(&journal, &points, &mut last, 1003);
        assert!(last.is_empty());
        fs::remove_dir_all(root).unwrap();
    }
}
