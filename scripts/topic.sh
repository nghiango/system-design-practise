#!/usr/bin/env bash
# Operator helper for system-design topics on the local cluster.
#   scripts/topic.sh status
#   scripts/topic.sh port-forward <topic> <service> <local-port>
#   scripts/topic.sh logs <topic> [service]
#   scripts/topic.sh reset <topic>     # wipe DB, Kafka topics and Redis keys (topic must be offline)
set -euo pipefail

read -r -a K <<< "${KUBECTL:-kubectl --context kind-hyperscale-k8s}"
k() { "${K[@]}" "$@"; }

usage() {
  sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//' | sed '1s/^/usage:\n/' >&2
  exit 2
}

slug() {
  [[ "${1:-}" =~ ^[a-z][a-z0-9-]{1,30}$ ]] || { echo "invalid topic: ${1:-<missing>}" >&2; exit 2; }
}

cmd=${1:-}
shift || true
case "$cmd" in
  status)
    k -n argocd get applications -l system-design/topic
    k get ns -l system-design/topic
    ;;
  port-forward)
    slug "${1:-}"
    [[ $# -eq 3 ]] || usage
    k -n "topic-$1" port-forward --address 127.0.0.1 "svc/$2" "$3:8080"
    ;;
  logs)
    slug "${1:-}"
    if [[ -n "${2:-}" ]]; then
      k -n "topic-$1" logs -l "app.kubernetes.io/name=$2" --tail=200 -f
    else
      k -n "topic-$1" logs -l "system-design/topic=$1" --all-containers --prefix --tail=200 -f
    fi
    ;;
  reset)
    slug "${1:-}"
    topic=$1
    if k get ns "topic-$topic" >/dev/null 2>&1; then
      echo "topic $topic is online; deactivate it first (system-design/topic-switch)" >&2
      exit 1
    fi
    read -r -p "Type $topic to delete its database, Kafka topics and Redis keys: " answer
    [[ "$answer" == "$topic" ]] || { echo "aborted" >&2; exit 1; }
    db=${topic//-/_}
    primary=$(k -n hyperscale get pod -l app.kubernetes.io/component=postgres-ha,role=primary -o name | head -n1)
    [[ -n "$primary" ]] || { echo "no Postgres primary found" >&2; exit 1; }
    # Passwords are read from the containers' own environment, never passed on the command line.
    k -n hyperscale exec "$primary" -c postgres-patroni -- sh -c \
      'PGPASSWORD="$POSTGRES_PASSWORD" psql -h localhost -U hyperscale -d hyperscale -c "$1"' _ \
      "DROP DATABASE IF EXISTS \"$db\" WITH (FORCE)"
    k -n hyperscale exec kafka-0 -c kafka -- \
      kafka-topics --bootstrap-server localhost:9092 --delete --if-exists --topic "$topic\\..*"
    k -n hyperscale exec redis-0 -c redis -- sh -c \
      'export REDISCLI_AUTH="$REDIS_PASSWORD"; redis-cli --scan --pattern "$1" | xargs -r redis-cli DEL' _ \
      "$topic:*"
    echo "reset $topic: database $db, Kafka topics $topic.*, Redis keys $topic:*"
    ;;
  *)
    usage
    ;;
esac
