#!/bin/sh
# Shared helpers for the reproduction scripts: a Firebird 5 container with a replication
# journal configured, and concurrent writers. Requires Docker only.
IMAGE=${IMAGE:-firebirdsql/firebird:5}
DB=/var/lib/firebird/data/mydb.fdb

# start_server <name> <workdir> <publication: 1|0>
# (POSIX sh has no local variables, hence the prefixed names)
start_server() {
  _name=$1 _dir=$2 _publication=$3
  mkdir -p "$_dir/data/replication/journal" "$_dir/data/replication/archive"
  cat > "$_dir/replication.conf" <<CONF
database
{
}
database = $DB
{
    journal_directory = /var/lib/firebird/data/replication/journal
    journal_archive_directory = /var/lib/firebird/data/replication/archive
    journal_archive_command = "test ! -f \$(archivepathname) && cp \$(pathname) \$(archivepathname)"
    journal_archive_timeout = ${ARCHIVE_TIMEOUT:-3}
}
CONF
  docker rm -f "$_name" >/dev/null 2>&1
  docker run -d --name "$_name" -e FIREBIRD_ROOT_PASSWORD=pw -e ISC_USER=SYSDBA -e ISC_PASSWORD=pw \
    -e FIREBIRD_DATABASE=mydb.fdb -v "$_dir/data:/var/lib/firebird/data" \
    -v "$_dir/replication.conf:/opt/firebird/replication.conf:ro" "$IMAGE" >/dev/null
  sleep 12
  if [ "$_publication" = 1 ]; then
    sql "$_name" 'ALTER DATABASE ENABLE PUBLICATION; ALTER DATABASE INCLUDE ALL TO PUBLICATION; COMMIT;'
  fi
  sql "$_name" 'CREATE TABLE app (id INT PRIMARY KEY, note VARCHAR(10)); COMMIT;'
}

# sql <name> <statements>: run through the server
sql() { docker exec "$1" sh -c "echo \"$2\" | isql -q localhost:$DB"; }

# start_writers <name> <count> <rows-per-writer>: each commit is an insert+update.
# CONNECTIONS=per-commit (default) opens one connection per commit; CONNECTIONS=persistent
# keeps one connection per writer, like a connection pool.
start_writers() {
  for w in $(seq 1 "$2"); do
    if [ "${CONNECTIONS:-per-commit}" = persistent ]; then
      docker exec -d "$1" sh -c "for i in \$(seq ${w}00000 \$((${w}00000 + $3))); do
        echo \"insert into app values (\$i, 'w'); update app set note = 'u' where id = \$i - 1; commit;\"; done | isql -q localhost:$DB"
    else
      docker exec -d "$1" sh -c "for i in \$(seq ${w}00000 \$((${w}00000 + $3))); do
        echo \"insert into app values (\$i, 'w'); update app set note = 'u' where id = \$i - 1; commit;\" | isql -q localhost:$DB; done"
    fi
  done
}

# responsive <name>: the server answers a query within 15 seconds
responsive() { timeout 15 docker exec "$1" sh -c "echo 'select count(*) from rdb\$database;' | isql -q localhost:$DB" >/dev/null 2>&1; }
