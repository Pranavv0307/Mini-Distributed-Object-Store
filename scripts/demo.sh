#!/usr/bin/env bash
set -euo pipefail

# Full, safe demo for a fresh or already-running Compose stack. It only uses
# this project's Compose services and never deletes volumes.
controller_url=${CONTROLLER_URL:-http://127.0.0.1:${CONTROLLER_PORT:-8080}}
workdir=$(mktemp -d)
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

wait_for() {
  local description=$1 pattern=$2 timeout=${3:-60} value
  for ((i=0; i<timeout; i++)); do
    value=$(curl -fsS "${controller_url}/cluster" 2>/dev/null || true)
    if grep -q "${pattern}" <<<"${value}"; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for ${description}" >&2
  curl -fsS "${controller_url}/cluster" || true
  exit 1
}

echo '== build and start =='
docker compose up -d --build
wait_for 'all workers ALIVE' '"alive":4' 90
curl -fsS "${controller_url}/healthz"
echo
curl -fsS "${controller_url}/cluster"
echo

echo '== upload, download, metadata, patch, list =='
head -c 5500000 /dev/urandom >"${workdir}/demo.bin"
response=$(curl -fsS -F 'metadata={"owner":"alice"}' -F "file=@${workdir}/demo.bin" "${controller_url}/api/v1/objects")
printf '%s\n' "${response}"
object_id=$(sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p' <<<"${response}")
if [[ -z "${object_id}" ]]; then echo 'upload response did not contain object_id' >&2; exit 1; fi
curl -fsS "${controller_url}/api/v1/objects/${object_id}/metadata"
echo
curl -fsS -X PATCH -H 'Content-Type: application/json' -d '{"owner":"bob","demo":"phase11"}' \
  "${controller_url}/api/v1/objects/${object_id}/metadata"
echo
curl -fsS "${controller_url}/api/v1/objects?limit=10"
echo
curl -fsS -o "${workdir}/download.bin" "${controller_url}/api/v1/objects/${object_id}"
cmp "${workdir}/demo.bin" "${workdir}/download.bin"
echo 'download comparison: IDENTICAL'

echo '== worker loss, degraded read, return =='
docker compose stop worker2
sleep 2
curl -fsS "${controller_url}/cluster"
echo
curl -fsS -o "${workdir}/degraded.bin" "${controller_url}/api/v1/objects/${object_id}"
cmp "${workdir}/demo.bin" "${workdir}/degraded.bin"
echo 'degraded download comparison: IDENTICAL'
docker compose start worker2
wait_for 'worker2 ALIVE again' '"alive":4' 90
curl -fsS "${controller_url}/cluster"
echo

echo '== audit and delete =='
curl -fsS "${controller_url}/admin/audit"
echo
curl -fsS -X DELETE "${controller_url}/api/v1/objects/${object_id}" -o /dev/null -w 'delete HTTP %{http_code}\n'
echo 'Demo complete. Leave the stack running for inspection, or run: docker compose down -v'
