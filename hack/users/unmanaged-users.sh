#!/bin/bash
# Lists the Firebird users of a cluster that no FirebirdUser manages, per instance, and with
# --yaml prints FirebirdUser resources to adopt them (docs: README "Users", upgrading).
#
#   hack/users/unmanaged-users.sh <cluster> [-n <namespace>] [--yaml]
#
# Each instance has its own security database, which replication does not ship: users created
# with plain SQL (CREATE USER) exist only on the instance they were created on, and before v0.12.0
# the security database was on the container filesystem, so they were lost on every restart.
# A FirebirdUser applies a user to every instance and keeps it there.
#
# Firebird stores password verifiers, not passwords, so the generated resources cannot carry the
# current ones: each refers to a Secret <name>-password that you create with the password the
# application uses. Applying a FirebirdUser sets the user's password to the Secret's value.
set -euo pipefail
usage() { echo "usage: $0 <cluster> [-n <namespace>] [--yaml]" >&2; exit 2; }
cluster="" namespace="" yaml=false
while [ $# -gt 0 ]; do
  case "$1" in
    -n|--namespace) namespace="${2:?}"; shift 2 ;;
    --yaml) yaml=true; shift ;;
    -h|--help) usage ;;
    *) [ -z "$cluster" ] || usage; cluster="$1"; shift ;;
  esac
done
[ -n "$cluster" ] || usage
ns=()
[ -z "$namespace" ] || ns=(-n "$namespace")

# users the cluster's FirebirdUser resources manage: spec.username, or the name with "-" as "_",
# upper-cased as Firebird stores it (utils/users.ts firebirdUsername)
managed=$(kubectl get firebirdusers "${ns[@]}" \
  -o jsonpath='{range .items[*]}{.spec.clusterName}{" "}{.metadata.name}{" "}{.spec.username}{"\n"}{end}' \
  | awk -v c="$cluster" '$1 == c { u = ($3 != "" ? $3 : $2); gsub(/-/, "_", u); print toupper(u) }' | sort -u)

pods=$(kubectl get pods "${ns[@]}" -l "firebird.cloudnative-firebird.io/cluster=$cluster,app.kubernetes.io/component=database" \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
  | awk '$2 == "True" { print $1 }' | sort)
[ -n "$pods" ] || { echo "no ready instance of cluster $cluster" >&2; exit 1; }

# "<user> <pod> <active> <admin>" for every non-SYSDBA user of every ready instance
listing=""
for pod in $pods; do
  rows=$(kubectl exec "${ns[@]}" "$pod" -c firebird -- bash -c \
    'printf "%s\n" "set list on;" "select trim(sec\$user_name) as u, iif(coalesce(sec\$active, true), 1, 0) as a, iif(sec\$admin, 1, 0) as d from sec\$users;" \
       | isql -q "localhost:$FIREBIRD_DATA/$FIREBIRD_DATABASE"' \
    | awk -v pod="$pod" '$1 == "U" { u = $2 } $1 == "A" { a = $2 } $1 == "D" { if (u != "SYSDBA") print u, pod, a, $2 }')
  listing+="$rows"$'\n'
done

unmanaged=$(printf '%s' "$listing" | awk 'NF' | while read -r user pod active admin; do
  grep -qx "$user" <<<"$managed" || echo "$user $pod $active $admin"
done)

if [ -z "$unmanaged" ]; then
  echo "every user of cluster $cluster is managed by a FirebirdUser" >&2
  exit 0
fi

if ! $yaml; then
  printf '%-32s %-8s %-6s %s\n' USER ACTIVE ADMIN INSTANCES
  printf '%s\n' "$unmanaged" | awk '
    { inst[$1] = inst[$1] (inst[$1] ? "," : "") $2; act[$1] = $3; adm[$1] = $4 }
    END { for (u in inst) printf "%-32s %-8s %-6s %s\n", u, (act[u] ? "yes" : "no"), (adm[u] ? "yes" : "no"), inst[u] }' | sort
  echo "$(printf '%s\n' "$unmanaged" | awk '{ print $1 }' | sort -u | wc -l) unmanaged user(s); --yaml prints FirebirdUser resources to adopt them" >&2
  exit 0
fi

# the users' role memberships, from the cluster database (it replicates: one instance has them
# all). A FirebirdUser revokes the roles it does not list, so the resources carry them.
first=$(printf '%s\n' $pods | head -1)
grants=$(kubectl exec "${ns[@]}" "$first" -c firebird -- bash -c \
  'printf "%s\n" "set list on;" "select trim(rdb\$user) as u, trim(rdb\$relation_name) as r from rdb\$user_privileges where rdb\$privilege = '"'"'M'"'"' and rdb\$user_type = 8;" \
     | isql -q "localhost:$FIREBIRD_DATA/$FIREBIRD_DATABASE"' \
  | awk '$1 == "U" { u = $2 } $1 == "R" { sub(/^R +/, ""); print u "\t" $0 }')
# a role as FirebirdUser.spec.roles takes it: plain names as they are, others double-quoted
role_item() {
  if [[ "$1" =~ ^[A-Z][A-Z0-9_\$]*$ ]]; then echo "$1"; else printf "'\"%s\"'\n" "${1//\"/\"\"}"; fi
}

printf '%s\n' "$unmanaged" | awk '{ act[$1] = $3; adm[$1] = $4 } END { for (u in act) print u, act[u], adm[u] }' | sort \
  | while read -r user active admin; do
    roles=$(printf '%s\n' "$grants" | awk -F'\t' -v u="$user" '$1 == u { print $2 }' | sort -u | while read -r r; do role_item "$r"; done | paste -sd, - | sed 's/,/, /g')
    name=$(echo "$user" | tr 'A-Z_$' 'a-z--')
    cat <<YAML
# Firebird user $user: create the Secret with the password the application uses first, e.g.
#   kubectl create secret generic $name-password --from-literal=password='...'${namespace:+ -n $namespace}
apiVersion: firebird.cloudnative-firebird.io/v1
kind: FirebirdUser
metadata:
  name: $name
${namespace:+  namespace: $namespace
}spec:
  clusterName: $cluster
  username: $user
  passwordSecret:
    name: $name-password
  # the roles it has now in the cluster database (a FirebirdUser revokes those it does not list)
  roles: [${roles}]
  active: $( [ "$active" = 1 ] && echo true || echo false )
  admin: $( [ "$admin" = 1 ] && echo true || echo false )
  reclaimPolicy: retain
---
YAML
  done
