#!/bin/sh
# Thread stacks of the Firebird server in a container (a hung one: publication-under-load.sh
# with KEEP=1), with the release's debug symbols. Runs gdb on the Docker host, which needs gdb,
# root (ptrace) and the server's processes visible (a local Docker daemon).
#
#   ./capture-stacks.sh fb-repro-load > stacks.txt
#
# Debug symbols are downloaded for the server version in the container from the Firebird
# release (e.g. Firebird-5.0.4.1812-0-linux-x64-debugSymbols.tar.gz); snapshot builds have
# none kept, so their engine frames stay unnamed.
set -eu
name=${1:?container name}
version=$(docker exec "$name" sh -c 'isql -z </dev/null 2>&1 | head -n 1' | sed -n 's/.*LI-V\([0-9.]*\).*/\1/p')
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
if [ -n "$version" ]; then
  tag=v$(echo "$version" | cut -d. -f1-3)
  url="https://github.com/FirebirdSQL/firebird/releases/download/$tag/Firebird-$version-0-linux-x64-debugSymbols.tar.gz"
  if curl -sSfL -o "$work/sym.tar.gz" "$url"; then
    engine=$(docker exec "$name" sh -c 'cd /opt/firebird/plugins && ls libEngine*.so').debug
    tar xzf "$work/sym.tar.gz" -C "$work" "./opt/firebird/plugins/.debug/$engine" ./opt/firebird/bin/.debug/firebird.debug
    # gdb finds them next to the binaries (.gnu_debuglink), seen through /proc/<pid>/root
    docker exec "$name" mkdir -p /opt/firebird/plugins/.debug /opt/firebird/bin/.debug
    docker cp "$work/opt/firebird/plugins/.debug/$engine" "$name:/opt/firebird/plugins/.debug/"
    docker cp "$work/opt/firebird/bin/.debug/firebird.debug" "$name:/opt/firebird/bin/.debug/"
  else
    echo "no debug symbols at $url: engine frames stay unnamed" >&2
  fi
else
  echo "not a release build: engine frames stay unnamed" >&2
fi
for pid in $(docker top "$name" -eo pid,args | awk '/\/firebird( |$)/ && !/awk/ { print $1 }'); do
  echo "=== firebird pid $pid (${version:-unknown version})"
  gdb -batch -p "$pid" -ex 'set pagination off' -ex "set sysroot /proc/$pid/root" \
    -ex 'info threads' -ex 'thread apply all bt' 2>&1
done
