//! The declarative databases an instance replicates beside the cluster database
//! (docs/multi-database-replication.md).
//!
//! REPLICATED_DATABASES_FILE names a file (the cluster ConfigMap's `replicated-databases` key)
//! with one database file name per line, e.g. `app.fdb`; the cluster database is not listed. Each
//! one lives next to the cluster database (DATABASE_PATH's directory) and has its own tree under
//! REPLICATION_DIR:
//!
//! ```text
//! <REPLICATION_DIR>/databases/<file>/journal
//! <REPLICATION_DIR>/databases/<file>/archive
//! <REPLICATION_DIR>/databases/<file>/source
//! <REPLICATION_DIR>/databases/<file>/.last-pulled
//! ```
//!
//! The list is read when a process starts: adding or removing a database rolls the instances
//! (its replication.conf changes too).
//!
//! In the segment protocol a request about one of them is `DB <file> <request>`; without the
//! selector a request is about the cluster database.

use std::fs;
use std::path::{Path, PathBuf};

/// How many databases one list may name (the rest are ignored, with a message)
pub const MAX_DATABASES: usize = 16;

/// One replicated database beside the cluster database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replicated {
    /// Its file name, as listed and as the `DB` selector names it
    pub file: String,
    /// The database file
    pub path: PathBuf,
    /// Its tree: `<REPLICATION_DIR>/databases/<file>`, where its flags and records are
    pub base: PathBuf,
    pub journal: PathBuf,
    pub archive: PathBuf,
    pub source: PathBuf,
    /// The puller's position
    pub state_file: PathBuf,
}

/// A database file name the list may hold: a plain name, nothing that leaves the directory.
pub fn database_file(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

impl Replicated {
    pub fn new(file: &str, data_dir: &Path, replication_dir: &Path) -> Replicated {
        let base = replication_dir.join("databases").join(file);
        Replicated {
            file: file.to_string(),
            path: data_dir.join(file),
            journal: base.join("journal"),
            archive: base.join("archive"),
            source: base.join("source"),
            state_file: base.join(".last-pulled"),
            base,
        }
    }
}

/// The databases a list names, in its order: blank lines and `#` comments skipped, a name that is
/// not a plain file name, the cluster database's own and repetitions left out (each with a
/// message), at most MAX_DATABASES.
pub fn parse(text: &str, cluster_database: &Path, replication_dir: &Path) -> Vec<Replicated> {
    let data_dir = cluster_database
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let own = cluster_database.file_name().and_then(|name| name.to_str()).unwrap_or("");
    let mut databases: Vec<Replicated> = Vec::new();
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if !database_file(line) {
            println!("replicated databases: {line:?} is not a database file name, left out");
        } else if line == own {
            println!("replicated databases: {line} is the cluster database, left out");
        } else if databases.iter().any(|database| database.file == line) {
            // listed twice: once is enough
        } else if databases.len() == MAX_DATABASES {
            println!("replicated databases: more than {MAX_DATABASES}, {line} left out");
        } else {
            databases.push(Replicated::new(line, data_dir, replication_dir));
        }
    }
    databases
}

/// The databases REPLICATED_DATABASES_FILE names; none without it, or when it cannot be read (an
/// optional ConfigMap key that is not there yet).
pub fn from_environment() -> Vec<Replicated> {
    let list = crate::client::var("REPLICATED_DATABASES_FILE");
    let (database, base) = (crate::client::var("DATABASE_PATH"), crate::client::var("REPLICATION_DIR"));
    if list.is_empty() || database.is_empty() || base.is_empty() {
        return Vec::new();
    }
    let text = fs::read_to_string(&list).unwrap_or_default();
    parse(&text, Path::new(&database), Path::new(&base))
}

/// A request with its database: `DB <file> <request>` gives Some(file) and the request after it,
/// anything else None and the request as it is. A selector without a request is not one.
pub fn selected(request: &str) -> (Option<&str>, &str) {
    match request.strip_prefix("DB ").and_then(|rest| rest.split_once(' ')) {
        Some((file, rest)) if !rest.is_empty() => (Some(file), rest),
        _ => (None, request),
    }
}

/// The request about the database SEGMENT_DATABASE names (the replication init of a declarative
/// database sets it for its helpers: fetch-seed, replica-control, request); unset, the cluster
/// database's request as it is.
pub fn request_for_environment(request: &str) -> String {
    let file = crate::client::var("SEGMENT_DATABASE");
    request_about(Some(file.as_str()).filter(|file| !file.is_empty()), request)
}

/// The request about `database` (None: the cluster database), as a client sends it.
pub fn request_about(database: Option<&str>, request: &str) -> String {
    match database {
        Some(file) => format!("DB {file} {request}"),
        None => request.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_the_declarative_databases_with_their_trees() {
        let text = "app.fdb\n\n# a comment\n  sales.fdb  \nmydb.fdb\napp.fdb\n../etc/passwd\n.hidden\n";
        let databases = parse(
            text,
            Path::new("/var/lib/firebird/data/mydb.fdb"),
            Path::new("/var/lib/firebird/data/replication"),
        );
        assert_eq!(
            databases.iter().map(|d| d.file.as_str()).collect::<Vec<_>>(),
            ["app.fdb", "sales.fdb"]
        );
        let app = &databases[0];
        assert_eq!(app.path, Path::new("/var/lib/firebird/data/app.fdb"));
        assert_eq!(app.base, Path::new("/var/lib/firebird/data/replication/databases/app.fdb"));
        assert_eq!(
            app.archive,
            Path::new("/var/lib/firebird/data/replication/databases/app.fdb/archive")
        );
        assert_eq!(
            app.journal,
            Path::new("/var/lib/firebird/data/replication/databases/app.fdb/journal")
        );
        assert_eq!(app.source, Path::new("/var/lib/firebird/data/replication/databases/app.fdb/source"));
        assert_eq!(
            app.state_file,
            Path::new("/var/lib/firebird/data/replication/databases/app.fdb/.last-pulled")
        );
    }

    #[test]
    fn keeps_at_most_the_limit() {
        let text: String = (0..MAX_DATABASES + 3).map(|i| format!("db{i}.fdb\n")).collect();
        assert_eq!(
            parse(&text, Path::new("/data/mydb.fdb"), Path::new("/data/replication")).len(),
            MAX_DATABASES
        );
    }

    #[test]
    fn reads_the_selector() {
        assert_eq!(selected("DB app.fdb LIST"), (Some("app.fdb"), "LIST"));
        assert_eq!(
            selected("DB app.fdb WAIT 5 app.fdb.journal-000000003"),
            (Some("app.fdb"), "WAIT 5 app.fdb.journal-000000003")
        );
        assert_eq!(selected("LIST"), (None, "LIST"));
        // no request after the database
        assert_eq!(selected("DB app.fdb"), (None, "DB app.fdb"));
        assert_eq!(selected("DB app.fdb "), (None, "DB app.fdb "));
        assert_eq!(request_about(Some("app.fdb"), "LIST"), "DB app.fdb LIST");
        assert_eq!(request_about(None, "LIST"), "LIST");
    }
}
