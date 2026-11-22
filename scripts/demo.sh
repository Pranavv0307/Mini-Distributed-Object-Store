#!/usr/bin/env bash
# One-command demonstration of every required behaviour of the specification
# (§10), against the real Docker Compose cluster (1 controller, 4 workers,
# the objctl client container):
#
#   1. Object storage       upload, download, byte-for-byte comparison
#   2. Chunking             a 5.5 MB object becomes 6 chunks spread over workers
#   3. Replication          two copies per chunk, and the files on the volumes
#   4. Metadata             custom metadata at upload; read and PATCH it
#   5. Worker failure       docker stop worker2 → still served → re-replicated
#                           → docker start worker2 → replication factor kept
#   6. Data integrity       corruption caught on read, by the scrubber (no
#                           read), and both copies bad → 503, never wrong bytes
#
# Usage:   ./scripts/demo.sh               run straight through (≈ 1–2 min)
#          DEMO_PAUSE=1 ./scripts/demo.sh  wait for Enter before each step
#
# The stack is (re)started with shortened timings so failures and repairs
# happen within seconds; any of them can be overridden from the environment.
# The script only touches this project's Compose services, deletes only the
# objects it uploaded, and never deletes volumes. It leaves the stack running.
set -euo pipefail

export HEARTBEAT_INTERVAL=${HEARTBEAT_INTERVAL:-1s}
export DEAD_AFTER=${DEAD_AFTER:-5s}
export REPAIR_GRACE=${REPAIR_GRACE:-15s}
export RECONCILER_INTERVAL=${RECONCILER_INTERVAL:-2s}
export JANITOR_INTERVAL=${JANITOR_INTERVAL:-2s}
export SCRUB_INTERVAL=${SCRUB_INTERVAL:-15s}
export INVENTORY_SWEEP_INTERVAL=${INVENTORY_SWEEP_INTERVAL:-15s}

url=${CONTROLLER_URL:-http://127.0.0.1:${CONTROLLER_PORT:-8080}}
prefix=${OBJSTORE_CONTAINER_PREFIX:-}
workdir=$(mktemp -d)
stopped=""
id=""
vid=""
cleanup() {
  if [[ -n "${stopped}" ]]; then docker compose start "${stopped}" >/dev/null 2>&1 || true; fi
  # Delete what this run uploaded, also when a step failed (idempotent).
  for o in "${id}" "${vid}"; do
    if [[ -n "${o}" ]]; then curl -s -o /dev/null -X DELETE "${url}/api/v1/objects/${o}" || true; fi
  done
  rm -rf "${workdir}"
}
trap cleanup EXIT

# ---------------------------------------------------------------- helpers
bold() { printf '\033[1m%s\033[0m\n' "$*"; }
ok() { printf '  \033[32m✔ %s\033[0m\n' "$*"; }
fail() { printf '  \033[31m✘ %s\033[0m\n' "$*" >&2; exit 1; }
say() { printf '  %s\n' "$*"; }
step() {
  echo
  bold "━━━ $* ━━━"
  if [[ "${DEMO_PAUSE:-}" == 1 ]]; then read -r -p "  (press Enter) " _ </dev/tty; fi
}
show() { printf '  \033[36m$ %s\033[0m\n' "$*"; }
# Commands inside containers: Git Bash must not rewrite /data/... paths.
wexec() { MSYS_NO_PATHCONV=1 docker compose exec -T "$@"; }
objctl() { MSYS_NO_PATHCONV=1 docker compose --profile tools run --rm --no-deps client "$@" 2>/dev/null; }
cluster() { curl -fsS "${url}/cluster"; }
audit_flags() { curl -fsS "${url}/admin/audit" | grep -o '"ok":[a-z]*,"steady_state":[a-z]*'; }
summary() { cluster | grep -o '"summary":{[^}]*}' | sed 's/"summary"://'; }
chunk_health() { cluster | grep -o '"chunks":{[^}]*}' | sed 's/"chunks"://'; }
repair_counts() { cluster | grep -o '"repair":{[^}]*}' | grep -o '"completed":[0-9]*'; }
metadata() { curl -fsS "${url}/api/v1/objects/$1/metadata"; }
# holders ID INDEX → the workers holding an OK copy of that chunk
holders() { metadata "$1" | grep -o "\"index\":$2,[^]]*]" | grep -o '"worker_id":"[^"]*","state":"OK"' | cut -d'"' -f4; }
state_on() { metadata "$1" | grep -o "\"index\":$2,[^]]*]" | grep -o "\"worker_id\":\"$3\",\"state\":\"[A-Z]*\"" | cut -d'"' -f8; }
wait_until() { # wait_until SECONDS DESCRIPTION COMMAND...
  local secs=$1 what=$2; shift 2
  for ((i = 0; i < secs; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  fail "timed out after ${secs}s waiting for ${what}"
}
download_matches() { # download_matches ID FILE
  curl -fsS -o "${workdir}/got.bin" "${url}/api/v1/objects/$1" && cmp -s "$2" "${workdir}/got.bin"
}
count_files() { wexec "$1" sh -c "ls /data/chunks | grep -c '^$2_' || true"; }

# ------------------------------------------------------------------ start
step "0. Start the cluster (1 controller + 4 workers, one volume each)"
show "docker compose up -d --build"
docker compose up -d --build 2>&1 | grep -E 'Started|Running|Healthy|Built' | sed 's/^/  /' || true
alive4() { cluster | grep -q '"alive":4'; }
wait_until 120 "4 workers ALIVE" alive4
show "curl ${url}/cluster   (summary)"
say "$(summary)"
ok "controller up, 4 workers registered and ALIVE (heartbeat ${HEARTBEAT_INTERVAL}, DEAD after ${DEAD_AFTER}, repair grace ${REPAIR_GRACE})"

# ---------------------------------------------------------------- 1. store
step "1. Object storage: upload, download, compare"
head -c 5500000 /dev/urandom >"${workdir}/photo.bin"
show "curl -F 'metadata={\"owner\":\"alice\",\"project\":\"demo\"}' -F file=@photo.bin ${url}/api/v1/objects"
# Relative file names (run from the work directory) keep curl's -F argument
# free of absolute paths, which Git Bash would otherwise rewrite on Windows.
resp=$(cd "${workdir}" && curl -fsS -F 'metadata={"owner":"alice","project":"demo"}' -F "file=@photo.bin" "${url}/api/v1/objects")
id=$(sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p' <<<"${resp}")
[[ -n "${id}" ]] || fail "upload failed: ${resp}"
say "${resp}"
show "curl -o downloaded.bin ${url}/api/v1/objects/${id} && cmp photo.bin downloaded.bin"
download_matches "${id}" "${workdir}/photo.bin" || fail "downloaded bytes differ"
ok "downloaded 5,500,000 bytes; identical to the original (SHA-256 also checked by the controller)"

# -------------------------------------------------------------- 2. chunking
step "2. Chunking and distribution: 5.5 MB → 1 MB chunks across workers"
show "objctl chunks ${id}      (the client container)"
objctl chunks "${id}" | sed 's/^/  /'
for w in worker1 worker2 worker3 worker4; do
  say "${prefix}${w}: $(count_files "${w}" "${id}") chunk file(s) of this object"
done
ok "6 chunks (5 × 1,000,000 bytes + 500,000), placed on different workers"

# ----------------------------------------------------------- 3. replication
step "3. Replication: every chunk has two copies, on two different volumes"
c0="${id}_000000"
mapfile -t h0 < <(holders "${id}" 0)
[[ ${#h0[@]} -eq 2 && "${h0[0]}" != "${h0[1]}" ]] || fail "chunk 0 does not have 2 copies on distinct workers"
for w in "${h0[@]}"; do
  show "docker exec ${prefix}${w} ls -l /data/chunks/${c0}"
  wexec "${w}" ls -l "/data/chunks/${c0}" | sed 's/^/  /'
done
total=0
for w in worker1 worker2 worker3 worker4; do total=$((total + $(count_files "${w}" "${id}"))); done
[[ ${total} -eq 12 ]] || fail "expected 12 chunk files, found ${total}"
ok "chunk 0 is stored on ${h0[0]} and ${h0[1]}; 12 files in total for 6 chunks (RF = 2)"

# -------------------------------------------------------------- 4. metadata
step "4. Metadata: read and modify without re-uploading"
show "curl ${url}/api/v1/objects/${id}/metadata   (custom metadata part)"
say "$(metadata "${id}" | grep -o '"metadata_version":[0-9]*,"custom_metadata":{[^}]*}')"
show "objctl patch ${id} owner=bob reviewed=yes project="
objctl patch "${id}" owner=bob reviewed=yes project= >/dev/null
after=$(metadata "${id}")
say "$(grep -o '"metadata_version":[0-9]*,"custom_metadata":{[^}]*}' <<<"${after}")"
grep -q '"owner":"bob"' <<<"${after}" && grep -q '"reviewed":"yes"' <<<"${after}" && ! grep -q '"project"' <<<"${after}" \
  || fail "PATCH did not apply"
[[ "$(holders "${id}" 0 | sort)" == "$(printf '%s\n' "${h0[@]}" | sort)" ]] || fail "chunk placement changed"
ok "owner updated, reviewed added, project removed; version 1 → 2; the stored chunks were not touched"

# -------------------------------------------------------- 5. worker failure
step "5. Worker failure: docker stop worker2"
victim=worker2
if [[ "$(count_files worker2 "${id}")" -eq 0 ]]; then
  for w in worker1 worker3 worker4; do [[ "$(count_files "${w}" "${id}")" -gt 0 ]] && victim=${w} && break; done
  say "(worker2 holds no copy of this object; using ${victim})"
fi
lost=$(count_files "${victim}" "${id}")
say "before: $(audit_flags)  ·  ${victim} holds ${lost} of the 12 copies"
show "docker stop ${prefix}${victim}"
docker compose stop "${victim}" >/dev/null 2>&1
stopped=${victim}
dead() { cluster | grep -q "\"worker_id\":\"${victim}\"[^}]*\"state\":\"DEAD\""; }
wait_until 60 "${victim} DEAD" dead
say "during: ${victim} is DEAD · chunks $(chunk_health)"
say "        audit $(audit_flags)   (safe, but not at full replication)"
download_matches "${id}" "${workdir}/photo.bin" || fail "download failed while ${victim} is down"
ok "object still downloads byte-identical from the surviving copies"
say "waiting for REPAIR_GRACE (${REPAIR_GRACE}) and re-replication onto the other workers…"
steady() { audit_flags | grep -q '"steady_state":true'; }
wait_until 120 "replication factor restored" steady
say "repaired: chunks $(chunk_health) · repairs since controller start: $(repair_counts | cut -d: -f2)"
download_matches "${id}" "${workdir}/photo.bin" || fail "download failed after repair"
ok "every chunk has 2 copies again on the 3 remaining workers, while ${victim} is still down"
show "docker start ${prefix}${victim}"
docker compose start "${victim}" >/dev/null 2>&1
stopped=""
wait_until 60 "4 workers ALIVE" alive4
clean() { cluster | grep -q '"over_replicated":0' && cluster | grep -q '"pending_deletes":0' && steady; }
wait_until 60 "superseded copies of the returning worker deleted" clean
say "after:  chunks $(chunk_health) · audit $(audit_flags)"
ok "${victim} is back; its now-redundant old copies were deleted: exactly 2 copies per chunk"

# --------------------------------------------------------- 6. data integrity
step "6a. Data integrity: corrupt a copy on disk, then download"
mapfile -t h1 < <(holders "${id}" 1)
c1="${id}_000001"
detections() { docker compose logs controller 2>/dev/null | grep '"corruption_detected"' | grep "$1" || true; }
repaired_before=$(repair_counts | cut -d: -f2)
show "docker exec ${prefix}${h1[0]} sh -c 'printf GARBAGE >> /data/chunks/${c1}'"
wexec "${h1[0]}" sh -c "printf GARBAGE >> /data/chunks/${c1}"
for _ in 1 2 3 4; do download_matches "${id}" "${workdir}/photo.bin" || fail "a download returned wrong bytes"; done
ok "4 downloads, all byte-identical: the bad copy is never returned"
# Each read picks a copy at random, so the reads may have used only the good
# one; then the scrubber (or a later read) finds the bad one.
detected() { [[ -n "$(detections "${c1}")" ]]; }
rewritten() { [[ "$(state_on "${id}" 1 "${h1[0]}")" == OK ]] && [[ "$(repair_counts | cut -d: -f2)" -gt "${repaired_before}" ]]; }
wait_until 90 "the corrupt copy to be detected" detected
wait_until 60 "the corrupt copy to be rewritten" rewritten
say "controller log: corruption_detected $(detections "${c1}" | head -1 | grep -o '"detected_by":"[a-z_]*"')"
ok "detected, marked CORRUPT, and rewritten in place from the good copy (state OK again)"

step "6b. Data integrity: silent corruption found by the scrubber, with no download"
mapfile -t h2 < <(holders "${id}" 2)
c2="${id}_000002"
show "docker exec ${prefix}${h2[0]} sh -c 'printf ZZZZ | dd of=/data/chunks/${c2} bs=1 seek=1000 conv=notrunc'"
wexec "${h2[0]}" sh -c "printf ZZZZ | dd of=/data/chunks/${c2} bs=1 seek=1000 conv=notrunc 2>/dev/null"
show "docker exec ${prefix}${h2[0]} worker inspect /data/chunks/${c2}"
# inspect exits non-zero for a damaged file; that is the point here.
{ wexec "${h2[0]}" worker inspect "/data/chunks/${c2}" || true; } | grep -E 'status' | sed 's/^/  /'
say "(same file size: only a checksum can tell; nobody downloads now; scrub pass every ${SCRUB_INTERVAL})"
scrubbed() { docker compose logs controller 2>/dev/null | grep '"corruption_detected"' | grep "${c2}" | grep -q '"detected_by":"scrubber"'; }
wait_until 120 "the scrubber to report the damage" scrubbed
healed() { [[ "$(state_on "${id}" 2 "${h2[0]}")" == OK ]] && wexec "${h2[0]}" worker inspect "/data/chunks/${c2}" | grep -q 'status: *VALID'; }
wait_until 60 "the scrubbed copy to be rewritten" healed
say "after repair: $(wexec "${h2[0]}" worker inspect "/data/chunks/${c2}" | grep -E 'status')"
say "controller log: corruption_detected $(detections "${c2}" | head -1 | grep -o '"reason":"[a-z_]*"') \"detected_by\":\"scrubber\""
ok "the worker's scrubber flagged it, the controller confirmed it against its own checksum, repair rewrote it"

step "6c. Data integrity: both copies corrupt → refused, never wrong data"
head -c 1500000 /dev/urandom >"${workdir}/victim.bin"
vid=$(cd "${workdir}" && curl -fsS -F "file=@victim.bin" "${url}/api/v1/objects" | sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p')
mapfile -t hv < <(holders "${vid}" 0)
for w in "${hv[@]}"; do wexec "${w}" sh -c "printf BAD >> /data/chunks/${vid}_000000"; done
say "corrupted both copies of chunk 0 of a second object (on ${hv[*]})"
show "curl ${url}/api/v1/objects/${vid}"
code=$(curl -s -o "${workdir}/err.json" -w '%{http_code}' "${url}/api/v1/objects/${vid}")
say "HTTP ${code} $(cat "${workdir}/err.json")"
[[ "${code}" == 503 ]] || fail "expected 503"
ok "clean 503 STORAGE_UNAVAILABLE: no valid copy left, so nothing is returned (an operator must re-upload)"
curl -fsS -X DELETE -o /dev/null "${url}/api/v1/objects/${vid}"

# ---------------------------------------------------------------- wrap-up
step "Wrap-up: invariant audit, delete, cluster state"
show "objctl audit"
report=$(objctl audit)
say "$(grep -E '^  "(ok|steady_state)"' <<<"${report}" | tr -d ' \n')"
grep -q '"ok": true' <<<"${report}" || fail "safety invariant violated"
show "curl -X DELETE ${url}/api/v1/objects/${id}"
say "HTTP $(curl -s -o /dev/null -w '%{http_code}' -X DELETE "${url}/api/v1/objects/${id}")  then GET → HTTP $(curl -s -o /dev/null -w '%{http_code}' "${url}/api/v1/objects/${id}")"
files_left() { local n=0; for w in worker1 worker2 worker3 worker4; do n=$((n + $(count_files "${w}" "${id}"))); done; echo "${n}"; }
gone() { cluster | grep -q '"pending_deletes":0' && [[ "$(files_left)" -eq 0 ]]; }
wait_until 60 "the deleted object's files to be removed" gone
say "$(summary)"
ok "all 12 chunk files removed from the volumes by the janitor"
echo
bold "Demo complete: every required demonstration passed."
say "The stack is still running: try   docker compose --profile tools run --rm client list"
say "Stop it with  docker compose down  (keeps data)  or  docker compose down -v  (deletes all data)."
