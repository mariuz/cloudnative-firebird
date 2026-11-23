#!/bin/bash
# Checks `fb-agent objects` (docs/object-stores.md, phase 1) against real servers of each store, in
# Docker: S3 (rclone serve s3, plain and over TLS with a CA of its own; access keys of 3
# characters or more) and Azure Blob (Azurite), every operation:
#
#   - put of a small object and of one over the multipart threshold, get of it back, whole;
#   - list of the names directly under a prefix, past the first page of a listing (1100 objects);
#   - sync of what is missing only, exists (0, 1, and 2 for a missing bucket), rm (twice);
#   - a failed get leaves no file, a refused credential is an error;
#   - S3: SSE-C (the object unreadable without its key), TLS through OBJECT_CA_FILE only.
#
# GCS (fake-gcs-server, with a bearer token it does not check: objects.rs): reads only. object_store
# uploads through GCS's XML API (PUT of an object, POST ?uploads for a multipart one), which
# fake-gcs-server takes for signed URLs only, as Google's storage-testbench has no XML multipart
# upload or listing: the objects are put through fake-gcs-server's JSON API, and the agent lists,
# gets (70 MiB, whole), tests and removes them. Uploads to GCS are covered by no emulator.
#
#   hack/agent/objects.sh
#
# Needs Docker and the fb-agent binary: FB_AGENT_BIN, or a build in operator/agent/target.
# Exit status 1 when a check fails.
set -u
here=$(cd "$(dirname "$0")" && pwd)
agent=${FB_AGENT_BIN:-}
if [ -z "$agent" ]; then
  agent=$(ls -t "$here"/../../operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent \
    "$here"/../../operator/agent/target/release/fb-agent 2>/dev/null | head -1)
fi
[ -x "$agent" ] || { echo "no fb-agent binary (set FB_AGENT_BIN)"; exit 1; }
RCLONE=rclone/rclone:1.75.2
AZURITE=mcr.microsoft.com/azure-storage/azurite:3.34.0
FAKE_GCS=fsouza/fake-gcs-server:1.52.2
work=$(mktemp -d)
chmod 755 "$work"
names=(objects-s3 objects-s3-tls objects-azurite objects-gcs)
cleanup() {
  docker rm -f "${names[@]}" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "$RCLONE" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rm -rf "$work" 2>/dev/null || true
}
trap cleanup EXIT

# the servers: S3 (a bucket is a directory), S3 over TLS (a CA and a certificate for 127.0.0.1),
# Azurite (its well-known development account), fake-gcs-server
mkdir -p "$work/s3/fbk" "$work/certs"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=objects-ca -keyout "$work/certs/ca.key" -out "$work/certs/ca.pem" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=127.0.0.1 -keyout "$work/certs/key.pem" -out "$work/certs/req.pem" 2>/dev/null
printf 'subjectAltName=IP:127.0.0.1\n' > "$work/certs/san"
openssl x509 -req -in "$work/certs/req.pem" -CA "$work/certs/ca.pem" -CAkey "$work/certs/ca.key" -CAcreateserial -days 1 -extfile "$work/certs/san" -out "$work/certs/cert.pem" 2>/dev/null
chmod 644 "$work/certs/"*
docker run -d --name objects-s3 -p 127.0.0.1:18080:8080 -v "$work/s3:/data" "$RCLONE" serve s3 --auth-key ci-key,ci-secret-key --addr :8080 /data >/dev/null
docker run -d --name objects-s3-tls -p 127.0.0.1:18443:8443 -v "$work/s3:/data" -v "$work/certs:/certs:ro" "$RCLONE" \
  serve s3 --auth-key ci-key,ci-secret-key --addr :8443 --cert /certs/cert.pem --key /certs/key.pem /data >/dev/null
docker run -d --name objects-azurite -p 127.0.0.1:10000:10000 "$AZURITE" azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck --loose >/dev/null
docker run -d --name objects-gcs -p 127.0.0.1:4443:4443 "$FAKE_GCS" -scheme http -port 4443 -public-host 127.0.0.1:4443 >/dev/null
up() { for _ in $(seq 1 60); do curl -sk -o /dev/null "$1" && return 0; sleep 1; done; echo "$1 did not come up"; exit 1; }
up http://127.0.0.1:18080/
up https://127.0.0.1:18443/
up http://127.0.0.1:10000/
up http://127.0.0.1:4443/storage/v1/b
# the buckets: Azurite's container through rclone's azureblob backend, fake-gcs-server's JSON API
docker run --rm --network host "$RCLONE" mkdir ':azureblob,use_emulator=true:fbk' \
  || { echo "cannot create the Azurite container"; exit 1; }
curl -sf -X POST -H 'Content-Type: application/json' -d '{"name":"fbk"}' http://127.0.0.1:4443/storage/v1/b >/dev/null \
  || { echo "cannot create the fake-gcs-server bucket"; exit 1; }

failed=0
ok() { echo "ok: $1"; }
bad() { echo "FAILED: $1"; failed=1; }
# the files: a small one, one over the multipart threshold (64 MiB), segments, many small ones
head -c 1000 /dev/urandom > "$work/small"
head -c $((70 * 1024 * 1024)) /dev/urandom > "$work/big"
mkdir -p "$work/seg" "$work/many"
for i in 1 2 3; do echo "segment $i" > "$work/seg/db.fdb.journal-00000000$i"; done
for i in $(seq -w 1 1100); do echo "$i" > "$work/many/o$i"; done

# exercise <store> <env...>: every operation against the store the environment names
exercise() {
  local store=$1 out
  shift
  run() { env OBJECT_PREFIX="run-$store" "${cfg[@]}" "$agent" objects "$@"; }
  cfg=("$@")
  echo "### $store"
  rm -f "$work/seg/db.fdb.journal-000000004" "$work/back" "$work/x"
  run put "$work/small" backup.fbk && ok "$store: put" || bad "$store: put"
  if run put "$work/big" big.nbk && run get big.nbk "$work/back" && cmp -s "$work/big" "$work/back"; then
    ok "$store: a 70 MiB object (multipart) and back, whole"
  else
    bad "$store: the multipart object"
  fi
  [ ! -e "$work/back.tmp" ] && ok "$store: no partial file left" || bad "$store: $work/back.tmp left"
  out=$(run list "" | sort | tr '\n' ' ')
  [ "$out" = "backup.fbk big.nbk " ] && ok "$store: list: $out" || bad "$store: list: \"$out\""
  out=$(run sync "$work/seg" journals/ | tr '\n' ' ')
  [ "$out" = "db.fdb.journal-000000001 db.fdb.journal-000000002 db.fdb.journal-000000003 " ] && ok "$store: sync uploads every segment" || bad "$store: sync: \"$out\""
  echo "segment 4" > "$work/seg/db.fdb.journal-000000004"
  out=$(run sync "$work/seg" journals | tr '\n' ' ')
  [ "$out" = "db.fdb.journal-000000004 " ] && ok "$store: sync again uploads the new one only" || bad "$store: sync again: \"$out\""
  [ "$(run list journals/ | grep -c .)" = 4 ] && ok "$store: list of a prefix" || bad "$store: list journals/: $(run list journals/ | tr '\n' ' ')"
  # the root lists the objects directly under it, not the prefix's
  [ "$(run list "" | grep -c journal)" = 0 ] && ok "$store: list is not recursive" || bad "$store: list of the root shows the prefix's objects"
  run sync "$work/many" many >/dev/null && [ "$(run list many | grep -c .)" = 1100 ] \
    && ok "$store: a listing of 1100 objects, past the first page" || bad "$store: the listing of 1100: $(run list many | grep -c .)"
  run exists backup.fbk; [ $? = 0 ] && ok "$store: exists" || bad "$store: exists"
  run exists nothing.fbk; [ $? = 1 ] && ok "$store: not there: 1" || bad "$store: not there"
  run rm backup.fbk && run rm backup.fbk && ok "$store: rm, and rm of what is gone" || bad "$store: rm"
  run exists backup.fbk; [ $? = 1 ] && ok "$store: gone after rm" || bad "$store: still there after rm"
  if run get nothing.fbk "$work/x" 2>/dev/null; then bad "$store: get of a missing object succeeded"; else ok "$store: get of a missing object fails"; fi
  [ ! -e "$work/x" ] && [ ! -e "$work/x.tmp" ] && ok "$store: and leaves no file" || bad "$store: a file left by the failed get"
}

# exercise_reads <store> <put command> <env...>: the reads, against objects put by <put command> <file> <key>
gcs_put() { curl -sf -X POST -H 'Content-Type: application/octet-stream' --data-binary "@$1" \
  "http://127.0.0.1:4443/upload/storage/v1/b/fbk/o?uploadType=media&name=$(printf %s "$2" | sed 's|/|%2F|g')" >/dev/null; }
exercise_reads() {
  local store=$1 out
  shift
  cfg=("$@")
  run() { env OBJECT_PREFIX="run-$store" "${cfg[@]}" "$agent" objects "$@"; }
  echo "### $store (reads)"
  rm -f "$work/back" "$work/x"
  gcs_put "$work/small" "run-$store/backup.fbk" && gcs_put "$work/big" "run-$store/big.nbk" \
    && for f in "$work"/seg/*; do gcs_put "$f" "run-$store/journals/${f##*/}"; done \
    || { bad "$store: the objects could not be put through the emulator's own API"; return; }
  if run get big.nbk "$work/back" && cmp -s "$work/big" "$work/back"; then ok "$store: a 70 MiB object, whole"; else bad "$store: get of the 70 MiB object"; fi
  [ ! -e "$work/back.tmp" ] && ok "$store: no partial file left" || bad "$store: $work/back.tmp left"
  out=$(run list "" | sort | tr '\n' ' ')
  [ "$out" = "backup.fbk big.nbk " ] && ok "$store: list: $out" || bad "$store: list: \"$out\""
  [ "$(run list journals/ | grep -c .)" = "$(ls "$work/seg" | grep -c .)" ] && ok "$store: list of a prefix" || bad "$store: list journals/: $(run list journals/ | tr '\n' ' ')"
  run exists backup.fbk; [ $? = 0 ] && ok "$store: exists" || bad "$store: exists"
  run exists nothing.fbk; [ $? = 1 ] && ok "$store: not there: 1" || bad "$store: not there"
  run rm backup.fbk && run rm backup.fbk && ok "$store: rm, and rm of what is gone" || bad "$store: rm"
  run exists backup.fbk; [ $? = 1 ] && ok "$store: gone after rm" || bad "$store: still there after rm"
  if run get nothing.fbk "$work/x" 2>/dev/null; then bad "$store: get of a missing object succeeded"; else ok "$store: get of a missing object fails"; fi
  [ ! -e "$work/x" ] && [ ! -e "$work/x.tmp" ] && ok "$store: and leaves no file" || bad "$store: a file left by the failed get"
}

exercise s3 OBJECT_STORE=s3 AWS_ENDPOINT=http://127.0.0.1:18080 AWS_ACCESS_KEY_ID=ci-key AWS_SECRET_ACCESS_KEY=ci-secret-key AWS_DEFAULT_REGION=us-east-1 AWS_BUCKET=fbk
exercise azure OBJECT_STORE=azure AZURE_STORAGE_USE_EMULATOR=true AZURE_CONTAINER_NAME=fbk
exercise_reads gcs OBJECT_STORE=gcs GOOGLE_BASE_URL=http://127.0.0.1:4443 GOOGLE_BEARER_TOKEN=emulator GOOGLE_BUCKET=fbk

echo "### the stores' edges"
s3=(OBJECT_STORE=s3 AWS_ENDPOINT=http://127.0.0.1:18080 AWS_ACCESS_KEY_ID=ci-key AWS_SECRET_ACCESS_KEY=ci-secret-key AWS_DEFAULT_REGION=us-east-1 OBJECT_PREFIX=edges)
env "${s3[@]}" AWS_BUCKET=nosuch "$agent" objects exists x 2>/dev/null; [ $? = 2 ] && ok "s3: a missing bucket is an error (2), not a missing object" || bad "s3: exists in a missing bucket"
if env "${s3[@]}" AWS_BUCKET=fbk AWS_SECRET_ACCESS_KEY=wrong "$agent" objects list "" 2>/dev/null; then bad "s3: a wrong secret was accepted"; else ok "s3: a wrong secret is refused"; fi
if env OBJECT_STORE=azure AZURE_STORAGE_USE_EMULATOR=true AZURE_CONTAINER_NAME=nosuch "$agent" objects list "" 2>/dev/null; then bad "azure: a missing container listed"; else ok "azure: a missing container is an error"; fi
# SSE-C: written and read with the key; unreadable without it (rclone serve s3 does not
# implement SSE-C: the request is checked for the key's headers only)
head -c 32 /dev/urandom > "$work/ssec.key"
if env "${s3[@]}" AWS_BUCKET=fbk OBJECT_SSE_C_KEY_FILE="$work/ssec.key" "$agent" objects put "$work/small" enc.fbk \
  && env "${s3[@]}" AWS_BUCKET=fbk OBJECT_SSE_C_KEY_FILE="$work/ssec.key" "$agent" objects get enc.fbk "$work/enc" && cmp -s "$work/small" "$work/enc"; then
  ok "s3: SSE-C put and get with the key"
else
  bad "s3: SSE-C"
fi
head -c 31 /dev/urandom > "$work/short.key"
env "${s3[@]}" AWS_BUCKET=fbk OBJECT_SSE_C_KEY_FILE="$work/short.key" "$agent" objects put "$work/small" enc2.fbk 2>/dev/null
[ $? = 2 ] && ok "s3: an SSE-C key that is not 32 bytes is refused" || bad "s3: a short SSE-C key"
# TLS: the endpoint's CA through OBJECT_CA_FILE; without it, the certificate is not trusted
tls=(OBJECT_STORE=s3 AWS_ENDPOINT=https://127.0.0.1:18443 AWS_ACCESS_KEY_ID=ci-key AWS_SECRET_ACCESS_KEY=ci-secret-key AWS_DEFAULT_REGION=us-east-1 AWS_BUCKET=fbk OBJECT_PREFIX=run-s3)
[ "$(env "${tls[@]}" OBJECT_CA_FILE="$work/certs/ca.pem" "$agent" objects list journals | grep -c .)" = 4 ] \
  && ok "s3 over TLS with OBJECT_CA_FILE" || bad "s3 over TLS: $(env "${tls[@]}" OBJECT_CA_FILE="$work/certs/ca.pem" "$agent" objects list journals 2>&1 | head -2)"
if env "${tls[@]}" "$agent" objects list journals >/dev/null 2>&1; then bad "s3 over TLS: an untrusted certificate was accepted"; else ok "s3 over TLS: an untrusted certificate is refused"; fi
env OBJECT_STORE=nfs "$agent" objects list x 2>/dev/null; [ $? = 2 ] && ok "an unknown store is refused (2)" || bad "an unknown store"

# the time of a 70 MiB put and get, each store an emulator takes uploads for
for store in s3 azure; do
  case $store in
    s3) cfg=(OBJECT_STORE=s3 AWS_ENDPOINT=http://127.0.0.1:18080 AWS_ACCESS_KEY_ID=ci-key AWS_SECRET_ACCESS_KEY=ci-secret-key AWS_DEFAULT_REGION=us-east-1 AWS_BUCKET=fbk) ;;
    azure) cfg=(OBJECT_STORE=azure AZURE_STORAGE_USE_EMULATOR=true AZURE_CONTAINER_NAME=fbk) ;;
  esac
  s=$(date +%s%N)
  env "${cfg[@]}" OBJECT_PREFIX=timing "$agent" objects put "$work/big" big.nbk && env "${cfg[@]}" OBJECT_PREFIX=timing "$agent" objects get big.nbk "$work/back"
  echo "$store: 70 MiB put and get in $(( ($(date +%s%N) - s) / 1000000 )) ms"
done

if [ "$failed" = 1 ]; then
  for n in "${names[@]}"; do echo "--- $n"; docker logs --tail 20 "$n" 2>&1; done
  echo "### objects: FAILED"; exit 1
fi
echo "### objects: passed"
