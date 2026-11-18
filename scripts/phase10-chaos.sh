#!/usr/bin/env bash
set -euo pipefail

controller_url=${CONTROLLER_URL:-http://127.0.0.1:${CONTROLLER_PORT:-8080}}
worker=${CHAOS_WORKER:-worker2}
payload=${CHAOS_PAYLOAD:-phase10-chaos-payload}

echo "Uploading evaluation object"
response=$(curl --fail-with-body -sS -F "file=@<(printf '%s' "${payload}")" "${controller_url}/api/v1/objects")
printf '%s\n' "${response}"
object_id=$(printf '%s' "${response}" | sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p')
if [[ -z "${object_id}" ]]; then
  echo "could not extract object_id" >&2
  exit 1
fi

echo "Audit before failure"
curl --fail-with-body -sS "${controller_url}/admin/audit"
echo
echo "Stopping ${worker}"
docker compose stop "${worker}"
trap 'docker compose start "${worker}" >/dev/null' EXIT
echo "Cluster during failure"
curl --fail-with-body -sS "${controller_url}/cluster"
echo
echo "Start ${worker}; wait for repair according to the configured grace and intervals"
docker compose start "${worker}"
trap - EXIT
echo "Final audit"
curl --fail-with-body -sS "${controller_url}/admin/audit"
echo
echo "Final cluster"
curl --fail-with-body -sS "${controller_url}/cluster"
echo
