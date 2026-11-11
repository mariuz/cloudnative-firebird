//! The backup files in the data directory (docs/rust-agent-segment-server.md, step 3): FILE,
//! STORE, REMOVE, FILES and EXISTS, through which backup and restore Jobs copy, list and delete
//! nbackup and gbak files next to the database, each with the reply segment-server.pl gives.
//! Where that reply would carry the system's error text, the caller passes the request to the
//! Perl server instead (None here), which then says it in its words.

use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// `^[A-Za-z0-9][A-Za-z0-9._-]*$`
pub fn plain_name(name: &str) -> bool {
    name.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric())
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// `^[A-Za-z0-9][A-Za-z0-9._-]*\.(?:nbk|fbk|nbk\.ctl)$`: backups, and the replica control file
/// written next to a replica's nbackup.
pub fn backup_name(name: &str) -> bool {
    plain_name(name)
        && [".nbk", ".fbk", ".nbk.ctl"]
            .iter()
            .any(|suffix| name.len() > suffix.len() && name.ends_with(suffix))
}

/// FILES: the backup files, sorted, then ".".
pub fn list(dir: &Path) -> String {
    let mut names: Vec<String> = fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
                .filter(|name| backup_name(name) && dir.join(name).is_file())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names.iter().map(|name| format!("{name}\n")).collect::<String>() + ".\n"
}

/// EXISTS <name>
pub fn exists(dir: &Path, name: &str) -> String {
    format!("OK {}\n", if dir.join(name).exists() { "yes" } else { "no" })
}

/// REMOVE <name>: "OK" when the file is gone; None when it could not be removed.
pub fn remove(dir: &Path, name: &str) -> Option<String> {
    let path = dir.join(name);
    (!path.exists() || fs::remove_file(&path).is_ok()).then(|| "OK\n".into())
}

/// FILE <name>: "OK <size>" and the bytes. None when the file cannot be opened (nothing was
/// written to the client).
pub fn send(dir: &Path, name: &str, client: &mut impl Write) -> Option<io::Result<()>> {
    let mut file = File::open(dir.join(name)).ok()?;
    let size = file.metadata().ok()?.len();
    Some(writeln!(client, "OK {size}").and_then(|()| io::copy(&mut file, client).map(|_| ())))
}

/// STORE <name> <size>: receives <size> bytes into the file, through a hidden partial file, so
/// nothing is left behind on failure. None when the partial file cannot be created (nothing was
/// read from the client).
pub fn store(dir: &Path, name: &str, size: u64, from: &mut impl BufRead) -> Option<String> {
    let part = dir.join(format!(".{name}.part"));
    let mut file = File::create(&part).ok()?;
    let copied = io::copy(&mut from.take(size), &mut file);
    if !matches!(copied, Ok(got) if got == size) {
        // what arrived before the client stopped, or before the disk refused
        let got = copied.unwrap_or_else(|_| file.metadata().map(|m| m.len()).unwrap_or(0));
        drop(file);
        let _ = fs::remove_file(&part);
        return Some(format!("ERR short write for {name} ({got} of {size} bytes)\n"));
    }
    drop(file);
    let _ = fs::set_permissions(&part, fs::Permissions::from_mode(0o644));
    Some(match fs::rename(&part, dir.join(name)) {
        Ok(()) => "OK\n".into(),
        Err(error) => {
            let _ = fs::remove_file(&part);
            format!("ERR rename {name}: {error}\n")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn knows_the_names() {
        for name in ["a.nbk", "nightly-2026.11.11_0.fbk", "db.nbk.ctl", "0.nbk"] {
            assert!(backup_name(name), "{name}");
        }
        for name in [
            "",
            ".nbk",
            "a.fdb",
            "../a.nbk",
            "a/b.nbk",
            "-a.nbk",
            "a.nbk.part",
            "a nbk",
            "a.nbk\n",
            "nbk",
        ] {
            assert!(!backup_name(name), "{name}");
        }
        assert!(plain_name("mydb.fdb") && !plain_name(".hidden") && !plain_name("a/b") && !plain_name(""));
    }

    #[test]
    fn stores_sends_lists_and_removes() {
        let dir = std::env::temp_dir().join(format!("fb-agent-backups-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        assert_eq!(list(&dir), ".\n");
        let body = vec![7u8; 200_000];
        // what follows the body on the connection is not read
        let mut from = Cursor::new([body.clone(), b"more".to_vec()].concat());
        assert_eq!(store(&dir, "a.nbk", 200_000, &mut from).as_deref(), Some("OK\n"));
        assert_eq!(fs::read(dir.join("a.nbk")).unwrap(), body);
        assert_eq!(fs::metadata(dir.join("a.nbk")).unwrap().permissions().mode() & 0o777, 0o644);
        assert_eq!(from.position(), 200_000);
        // a client that stops early leaves nothing
        let mut short = Cursor::new(vec![1u8; 10]);
        assert_eq!(
            store(&dir, "b.nbk", 50, &mut short).as_deref(),
            Some("ERR short write for b.nbk (10 of 50 bytes)\n")
        );
        assert_eq!(list(&dir), "a.nbk\n.\n");
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        // an empty file is a file
        assert_eq!(store(&dir, "0.fbk", 0, &mut Cursor::new(Vec::new())).as_deref(), Some("OK\n"));
        fs::write(dir.join("mydb.fdb"), "x").unwrap();
        fs::create_dir(dir.join("d.nbk")).unwrap();
        assert_eq!(list(&dir), "0.fbk\na.nbk\n.\n");
        let mut out = Vec::new();
        send(&dir, "a.nbk", &mut out).unwrap().unwrap();
        assert_eq!(out, [b"OK 200000\n".to_vec(), body].concat());
        assert!(send(&dir, "none.nbk", &mut Vec::new()).is_none());
        assert_eq!(exists(&dir, "mydb.fdb"), "OK yes\n");
        assert_eq!(exists(&dir, "other.fdb"), "OK no\n");
        assert_eq!(remove(&dir, "a.nbk").as_deref(), Some("OK\n"));
        assert_eq!(remove(&dir, "a.nbk").as_deref(), Some("OK\n"));
        // a directory cannot be removed as a file: the Perl server says why
        assert_eq!(remove(&dir, "d.nbk"), None);
        assert_eq!(list(&dir), "0.fbk\n.\n");
        fs::remove_dir_all(dir).unwrap();
    }
}
