#!/bin/sh
# Replication init of the declarative databases (docs/multi-database-replication.md), after the
# cluster database's (init-instance.sh): init-instance.sh again for each database
# REPLICATED_DATABASES_FILE names, as `fb-agent databases` lists them, with that database's
# own paths and its tree as the work directory:
#
#   <REPLICATION_DIR>/databases/<file>/{journal,archive,source,.last-pulled}
#
# DECLARATIVE=true: the primary does not create one (its FirebirdDatabase does), and a replica
# that finds no seed of it yet starts without it. SEGMENT_DATABASE=<file>: the helpers ask the
# other instances about that database ("DB <file>"). A database whose init fails is reported and
# left for the next start: the instance starts with the others.
#
#   init-databases.sh [<file>]      only that database (the segment puller seeds one a replica
#                                   does not have yet, every minute, with the same environment)
#
# The seed sources are asked once (SEED_ATTEMPTS, 1): the puller tries again while the instance
# runs, so a replica restarted before its primary has a seed of the database does not wait.
set -u
[ -n "${REPLICATED_DATABASES_FILE:-}" ] || exit 0
only=${1:-}
export SEED_ATTEMPTS="${SEED_ATTEMPTS:-1}"
here=$(cd "$(dirname "$0")" && pwd)
data=$(dirname "$DATABASE_PATH")
base=$REPLICATION_DIR
own() { [ "$(id -u)" -ne 0 ] || chown "$@"; }
for file in $("$FB_AGENT" databases); do
  [ -z "$only" ] || [ "$file" = "$only" ] || continue
  tree="$base/databases/$file"
  mkdir -p "$tree"
  own firebird:firebird "$base/databases" "$tree"
  echo "replication init of $file"
  if (
    unset INIT_SQL_DIR
    export DATABASE_PATH="$data/$file" REPLICATION_DIR="$tree" DATA_DIR="$tree" \
      JOURNAL_DIR="$tree/journal" ARCHIVE_DIR="$tree/archive" SOURCE_DIR="$tree/source" \
      STATE_FILE="$tree/.last-pulled" SEGMENT_DATABASE="$file" DECLARATIVE=true
    sh "$here/init-instance.sh"
  ); then
    :
  else
    echo "the replication init of $file failed: the instance starts without it, it is tried again at the next start"
  fi
done
exit 0
