#!/usr/bin/env bash
# Compose chaos check: upload, kill a worker, measure failure detection and
# the time until every chunk has full RF again (self-healing), bring the
# worker back, and check the audit after each step. Run against a stack
# started with `docker compose up -d` from this repository. It only stops and
# starts one of this project's worker services and never deletes volumes.
#
#   CONTROLLER_URL   default http://127.0.0.1:${CONTROLLER_PORT:-8080}
#   CHAOS_WORKER     Compose service to stop (default worker2)
#   CHAOS_SIZE       upload size in bytes (default 5500000)
#   CHAOS_TIMEOUT    seconds to wait for each phase (default 180)
set -euo pipefail

controller_url=${CONTROLLER_URL:-http://127.0.0.1:${CONTROLLER_PORT:-8080}}
worker=${CHAOS_WORKER:-worker2}
size=${CHAOS_SIZE:-5500000}
timeout=${CHAOS_TIMEOUT:-180}
workdir=$(mktemp -d)
restart_worker=false
cleanup() {
  if [[ "${restart_worker}" == true ]]; then docker compose start "${worker}" >/dev/null 2>&1 || true; fi
  rm -rf "${workdir}"
}
trap cleanup EXIT

now_ms() { date +%s%3N; }
cluster() { curl -fsS "${controller_url}/cluster"; }
audit() { curl -fsS "${controller_url}/admin/audit"; }
# wait_until DESCRIPTION COMMAND...: retry COMMAND once per second.
wait_until() {
  local description=$1; shift
  for ((i = 0; i < timeout; i++)); do
    if "$@"; then return 0; fi
    sleep 1
  done
  echo "timed out after ${timeout}s waiting for ${description}" >&2
  cluster >&2 || true
  exit 1
}
# Responses are captured first: with pipefail, `curl | grep -q` can fail
# when grep exits before curl has written everything.
matches() { local body; body=$("$1") || return 1; grep -q -- "$2" <<<"${body}"; }
worker_state_is() { matches cluster "\"worker_id\":\"${worker}\"[^}]*\"state\":\"$1\""; }
all_alive() { matches cluster '"alive":4'; }
audit_safe() { matches audit '"ok":true'; }
audit_steady() { matches audit '"ok":true,"steady_state":true'; }

wait_until 'all workers ALIVE' all_alive
head -c "${size}" /dev/urandom >"${workdir}/chaos.bin"
response=$(curl --fail-with-body -sS -F "file=@${workdir}/chaos.bin" "${controller_url}/api/v1/objects")
object_id=$(sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p' <<<"${response}")
[[ -n "${object_id}" ]] || { echo "upload failed: ${response}" >&2; exit 1; }
echo "uploaded ${object_id} (${size} bytes)"
wait_until 'steady audit before the failure' audit_steady

echo "== stop ${worker} =="
restart_worker=true
t0=$(now_ms)
docker compose stop "${worker}" >/dev/null 2>&1
wait_until "${worker} DEAD" worker_state_is DEAD
t_dead=$(now_ms)
curl -fsS -o "${workdir}/degraded.bin" "${controller_url}/api/v1/objects/${object_id}"
cmp "${workdir}/chaos.bin" "${workdir}/degraded.bin"
echo "degraded download: IDENTICAL"
audit_safe || { echo "safety invariant violated while degraded" >&2; audit >&2; exit 1; }
wait_until 'full RF restored (self-healing)' audit_steady
t_healed=$(now_ms)

echo "== start ${worker} =="
docker compose start "${worker}" >/dev/null 2>&1
restart_worker=false
wait_until 'all workers ALIVE again' all_alive
wait_until 'steady audit after the return' audit_steady
curl -fsS -o "${workdir}/after.bin" "${controller_url}/api/v1/objects/${object_id}"
cmp "${workdir}/chaos.bin" "${workdir}/after.bin"
echo "download after return: IDENTICAL"

echo "== results =="
echo "failure detection (stop -> DEAD): $(( t_dead - t0 )) ms"
echo "full RF restored (stop -> steady): $(( t_healed - t0 )) ms"
cluster | sed -n 's/.*"repair":\({[^}]*}\).*/repair counters: \1/p'
