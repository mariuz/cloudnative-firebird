//! The requests that change what an instance does (docs/rust-agent-segment-server.md, step 5),
//! with the sequence and the replies of segment-server.pl: SYNC and STANDBY (synchronous
//! replication), FENCE and REJOIN (the Lease holder's fence), NBACKUP. The promotion (PROMOTE)
//! stays with the Perl server.
//!
//! The full shutdown, the online and the nbackup go to the local server's services over the
//! agent's own attachments. When one is refused at once the request is passed to the Perl
//! server, undone as far as it went: fbsvcmgr may get through where the agent did not. When the
//! server does not answer within the limit it answers nobody, and the failure is reported.

use crate::seed::{exclusive, pause_until_applied, wait_for};
use crate::sql::{Database, Failed};
use crate::status::{control_file, control_path, slurp, Files};
use std::fs;
use std::path::{Path, PathBuf};

pub enum Answer {
    Reply(String),
    /// Nothing is left changed: the Perl server answers
    Forward,
}

fn reply(text: impl Into<String>) -> Answer {
    Answer::Reply(text.into() + "\n")
}

/// Through a temporary name, so a reader never sees half a file.
fn write_file(path: &Path, content: &[u8]) -> bool {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    fs::write(&temporary, content).is_ok() && fs::rename(&temporary, path).is_ok()
}

/// `^[A-Za-z0-9][A-Za-z0-9.-]*$`
fn host_name(name: &str) -> bool {
    name.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric())
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
}

/// SYNC none|<host>[,<host>...]: the primary's synchronous replicas, read by the server when it
/// opens the database. None for an argument the Perl server refuses.
pub fn sync(files: &Files, database: &Database, argument: &str) -> Option<Answer> {
    if argument != "none" && !argument.split(',').all(host_name) {
        return None;
    }
    let content: String = match argument {
        "none" => String::new(),
        hosts => hosts
            .split(',')
            .map(|host| {
                format!(
                    "sync_replica = {host}:{}\n{{\n  username = {}\n  password_env = ISC_PASSWORD\n}}\n",
                    database.path, database.user
                )
            })
            .collect(),
    };
    if !write_file(&files.base.join("sync.conf"), content.as_bytes()) {
        return Some(Answer::Forward);
    }
    let to = if argument == "none" {
        "off".to_string()
    } else {
        format!("to {argument}")
    };
    println!("synchronous replication {to} (applied when the database is opened)");
    Some(reply("OK"))
}

/// STANDBY on: this replica receives the primary's changes synchronously from now on.
pub fn standby_on(files: &Files, now: u64) -> Answer {
    if files.is_primary() {
        return reply("ERR this instance is the primary");
    }
    if !write_file(&files.base.join("sync-standby"), format!("{now}\n").as_bytes()) {
        return Answer::Forward;
    }
    println!("synchronous standby: journal segments are no longer applied here");
    reply("OK")
}

/// STANDBY off <segment>: back to journal shipping after that segment. The control file is set
/// to it (no transaction in progress: the primary is in full shutdown), keeping the database's
/// own sequence, and the puller's position too.
pub fn standby_off(files: &Files, state_file: &Path, segment: &str) -> Answer {
    let Ok(sequence) = segment.parse::<u64>() else {
        return Answer::Forward;
    };
    if files.base.join("sync-standby").is_file() {
        let Some(path) = control_path(&files.source) else {
            return reply("ERR no replica control file");
        };
        let Some(data) = fs::read(&path).ok().filter(|_| control_file(&path).is_some()) else {
            return reply(format!("ERR cannot read {}", path.display()));
        };
        let mut control = b"FBREPLCTL\0".to_vec();
        control.extend(1u16.to_le_bytes());
        control.extend(0u32.to_le_bytes());
        control.extend(sequence.to_le_bytes());
        control.extend(0u32.to_le_bytes());
        control.extend([0u8; 4]);
        control.extend(&data[32..40]);
        if !write_file(&path, &control) {
            return Answer::Forward;
        }
        let last = slurp(state_file);
        if let Some((prefix, digits)) = last.rsplit_once(".journal-") {
            if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) && !last.contains('\n') {
                let position = format!("{prefix}.journal-{sequence:0width$}\n", width = digits.len());
                if !write_file(state_file, position.as_bytes()) {
                    return Answer::Forward;
                }
            }
        }
        let _ = fs::remove_file(files.base.join("sync-standby"));
        let _ = fs::remove_file(files.base.join("sync-seen"));
    }
    println!("asynchronous replica again: journal shipping continues after segment {segment}");
    reply("OK")
}

/// `^[\w .:,()/-]{1,200}$`
fn plain_reason(reason: &str) -> bool {
    (1..=200).contains(&reason.len())
        && reason
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b' ' | b'.' | b':' | b',' | b'(' | b')' | b'/' | b'-'))
}

/// FENCE [<reason>]: the Lease holder's fence. The marker first, so that a restarted sidecar
/// never takes the fenced database for an online one, then the full shutdown.
pub fn fence(files: &Files, database: &Database, reason: Option<&str>, now: u64) -> Answer {
    let marker = files.base.join("self-fenced");
    if marker.is_file() {
        return reply("OK fenced");
    }
    let reason = reason.filter(|r| plain_reason(r)).unwrap_or("requested");
    if !write_file(&marker, format!("{now} lease\n").as_bytes()) {
        return Answer::Forward;
    }
    println!("{reason}: fencing the primary (database in full shutdown)");
    match database.shut_down() {
        Ok(()) => reply("OK"),
        Err(failed) => {
            let _ = fs::remove_file(&marker);
            match failed {
                Failed::Refused(why) => {
                    println!("full shutdown refused ({why}): passed to fbsvcmgr");
                    Answer::Forward
                }
                Failed::TimedOut => {
                    println!("full shutdown failed");
                    reply(format!("ERR cannot shut {} down", database.path))
                }
            }
        }
    }
}

/// REJOIN: the fence is lifted, the database online again.
pub fn rejoin(files: &Files, database: &Database) -> Answer {
    let marker = files.base.join("self-fenced");
    if !marker.is_file() {
        return reply("OK");
    }
    match database.bring_online() {
        Ok(()) => {
            let _ = fs::remove_file(&marker);
            println!("isolation fence lifted by the operator: database online");
            reply("OK")
        }
        Err(Failed::Refused(why)) => {
            println!("online refused ({why}): passed to fbsvcmgr");
            Answer::Forward
        }
        Err(Failed::TimedOut) => reply(format!("ERR cannot bring {} online", database.path)),
    }
}

/// `^([0-2]) ([A-Za-z0-9][A-Za-z0-9._-]*\.nbk)$`
fn nbackup_argument(argument: &str) -> Option<(u32, &str)> {
    let (level, file) = argument.split_once(' ')?;
    let level = match level {
        "0" => 0,
        "1" => 1,
        "2" => 2,
        _ => return None,
    };
    (crate::backups::plain_name(file) && file.len() > 4 && file.ends_with(".nbk")).then_some((level, file))
}

/// NBACKUP <level> <file>: an nbackup of this instance's database into the data directory. On a
/// replica (not the synchronous standby) the puller is paused and everything received applied
/// first, and the control file is written next to the backup as <file>.ctl: the position the
/// copy was taken at. None for an argument the Perl server refuses.
pub fn nbackup(files: &Files, database: &Database, data: &Path, argument: &str) -> Option<Answer> {
    let (level, file) = nbackup_argument(argument)?;
    let lock = match exclusive(&files.base) {
        Ok(lock) => lock,
        Err(error) => return Some(reply(format!("ERR {error}"))),
    };
    let position_file = data.join(format!("{file}.ctl"));
    let _ = fs::remove_file(&position_file);
    let replica = !files.is_primary() && !files.base.join("sync-standby").exists();
    let pause = files.base.join(".pause-pull");
    let taken = (|| {
        let mut position: Option<(PathBuf, Vec<u8>, u64)> = None;
        if replica {
            // the flag first, as the Perl server: the puller pauses whatever comes next
            let Some(control) = control_path(&files.source) else {
                let _ = fs::remove_file(files.base.join(".pull-paused"));
                let _ = fs::File::create(&pause);
                let paused = wait_for(60, || files.base.join(".pull-paused").exists());
                return Err(Refused::Said(if paused {
                    "no replica control file"
                } else {
                    "segment puller did not pause"
                }));
            };
            pause_until_applied(files, &control).map_err(Refused::Said)?;
            let data = fs::read(&control).map_err(|_| Refused::Said("no replica control file"))?;
            let (sequence, _) = control_file(&control).ok_or(Refused::Said("no replica control file"))?;
            position = Some((control, data, sequence));
        }
        let target = data.join(file);
        database.nbackup(&target.to_string_lossy(), level).map_err(Refused::ByTheServer)?;
        Ok(position)
    })();
    if replica {
        let _ = fs::remove_file(&pause);
    }
    Some(match taken {
        Ok(Some((_, control, sequence))) => {
            if !write_file(&position_file, &control) {
                println!("nbackup {file} refused: write {file}.ctl");
                return Some(reply(format!("ERR write {file}.ctl")));
            }
            println!("nbackup {file} (level {level}) taken at replica position {sequence}");
            reply(format!("OK replica {sequence}"))
        }
        Ok(None) => {
            println!("nbackup {file} (level {level}) taken");
            reply("OK")
        }
        Err(Refused::Said(reason)) => {
            println!("nbackup {file} refused: {reason}");
            reply(format!("ERR {reason}"))
        }
        // the lock and the pause are released before the Perl server takes them
        Err(Refused::ByTheServer(why)) => {
            println!("nbackup {file}: {why}: passed to fbsvcmgr");
            drop(lock);
            Answer::Forward
        }
    })
}

enum Refused {
    Said(&'static str),
    ByTheServer(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn scratch(name: &str) -> (PathBuf, Files, Database) {
        let root = std::env::temp_dir().join(format!("fb-agent-role-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        for dir in ["archive", "source", "repl", "data"] {
            fs::create_dir_all(root.join(dir)).unwrap();
        }
        let files = Files {
            database: root.join("data/mydb.fdb"),
            archive: root.join("archive"),
            source: root.join("source"),
            base: root.join("repl"),
            primary_file: root.join("primary"),
            pod: "db-1".into(),
        };
        // nothing listens on port 1: the services refuse at once
        let database = Database {
            path: "/data/mydb.fdb".into(),
            port: 1,
            user: "SYSDBA".into(),
            password: "x".into(),
            query_limit: Duration::from_secs(5),
            command_limit: Duration::from_secs(5),
        };
        (root, files, database)
    }

    fn text(answer: Answer) -> String {
        match answer {
            Answer::Reply(text) => text,
            Answer::Forward => "(forward)".into(),
        }
    }

    #[test]
    fn writes_the_synchronous_replicas() {
        let (root, files, database) = scratch("sync");
        assert_eq!(text(sync(&files, &database, "db-0.h,db-2.h").unwrap()), "OK\n");
        assert_eq!(
            fs::read_to_string(files.base.join("sync.conf")).unwrap(),
            "sync_replica = db-0.h:/data/mydb.fdb\n{\n  username = SYSDBA\n  password_env = ISC_PASSWORD\n}\nsync_replica = db-2.h:/data/mydb.fdb\n{\n  username = SYSDBA\n  password_env = ISC_PASSWORD\n}\n"
        );
        assert_eq!(text(sync(&files, &database, "none").unwrap()), "OK\n");
        assert_eq!(fs::read_to_string(files.base.join("sync.conf")).unwrap(), "");
        for refused in ["", "a,,b", "-a", "a b", "a:1", "none,a."] {
            assert!(sync(&files, &database, refused).is_none() || refused == "none,a.", "{refused}");
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn switches_a_standby_on_and_off() {
        let (root, files, _) = scratch("standby");
        // no primary named: this instance is one
        assert_eq!(text(standby_on(&files, 100)), "ERR this instance is the primary\n");
        fs::write(&files.primary_file, "db-0.h\n").unwrap();
        assert_eq!(text(standby_on(&files, 100)), "OK\n");
        assert_eq!(fs::read_to_string(files.base.join("sync-standby")).unwrap(), "100\n");
        let state = files.base.join(".last-pulled");
        assert_eq!(text(standby_off(&files, &state, "57")), "ERR no replica control file\n");
        let control = files.source.join("{6A0B3C1D-0000-4000-8000-000000000001}");
        fs::write(&control, "short").unwrap();
        assert_eq!(
            text(standby_off(&files, &state, "57")),
            format!("ERR cannot read {}\n", control.display())
        );
        let mut old = b"FBREPLCTL\0".to_vec();
        old.extend(1u16.to_le_bytes());
        old.extend(2u32.to_le_bytes());
        old.extend(41u64.to_le_bytes());
        old.extend(1234u32.to_le_bytes());
        old.extend([0u8; 4]);
        old.extend(9u64.to_le_bytes());
        old.extend([7u8; 32]);
        fs::write(&control, old).unwrap();
        fs::write(&state, "mydb.fdb.journal-000000041\n").unwrap();
        fs::write(files.base.join("sync-seen"), "57\n").unwrap();
        assert_eq!(text(standby_off(&files, &state, "57")), "OK\n");
        let new = fs::read(&control).unwrap();
        assert_eq!(new.len(), 40);
        assert_eq!(control_file(&control), Some((57, 0)));
        assert_eq!(&new[12..16], &[0u8; 4]);
        assert_eq!(u64::from_le_bytes(new[32..40].try_into().unwrap()), 9);
        assert_eq!(fs::read_to_string(&state).unwrap(), "mydb.fdb.journal-000000057\n");
        assert!(!files.base.join("sync-standby").exists() && !files.base.join("sync-seen").exists());
        // not a standby: nothing to do
        assert_eq!(text(standby_off(&files, &state, "60")), "OK\n");
        assert_eq!(control_file(&control), Some((57, 0)));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn leaves_nothing_behind_when_the_server_refuses() {
        let (root, files, database) = scratch("fence");
        // refused at once: the marker is gone again and the Perl server is asked
        assert_eq!(text(fence(&files, &database, Some("lease lost"), 100)), "(forward)");
        assert!(!files.base.join("self-fenced").exists());
        assert_eq!(text(rejoin(&files, &database)), "OK\n");
        fs::write(files.base.join("self-fenced"), "100 lease\n").unwrap();
        assert_eq!(text(fence(&files, &database, None, 200)), "OK fenced\n");
        assert_eq!(text(rejoin(&files, &database)), "(forward)");
        assert!(files.base.join("self-fenced").exists());
        // an nbackup of a primary: the server refuses, the lock is free again
        assert_eq!(
            text(nbackup(&files, &database, &root.join("data"), "0 a.nbk").unwrap()),
            "(forward)"
        );
        drop(exclusive(&files.base).unwrap());
        for refused in ["", "3 a.nbk", "0 a.fbk", "0 ../a.nbk", "0", "0 .nbk", "00 a.nbk"] {
            assert!(nbackup(&files, &database, &root.join("data"), refused).is_none(), "{refused}");
        }
        assert!(plain_reason("lease lost (holder db-0): 3/4") && !plain_reason("") && !plain_reason("a\nb") && !plain_reason("x;y"));
        fs::remove_dir_all(root).unwrap();
    }
}
