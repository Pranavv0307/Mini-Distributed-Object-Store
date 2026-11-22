#!/usr/bin/env bash
# Compose throughput/latency benchmark: uploads and downloads of several
# object sizes with 1 and 4 concurrent clients against a running stack, plus
# degraded downloads (one worker stopped). Prints one Markdown table row per
# scenario: median and p99 request time and median per-request throughput
# (with N clients, aggregate throughput is about N times that).
#
# Run against a stack started with `docker compose up -d` from this
# repository. Uploaded objects are deleted at the end. The degraded step stops
# and restarts one of this project's worker services; volumes are never
# deleted. Compare fsync on/off by restarting the stack with
# FSYNC_ON_WRITE=false and running the script again.
#
#   CONTROLLER_URL   default http://127.0.0.1:${CONTROLLER_PORT:-8080}
#   BENCH_SIZES      object sizes in bytes (default "1000000 10000000 100000000")
#   BENCH_REQUESTS   requests per scenario (default 10)
#   BENCH_CLIENTS    concurrency levels (default "1 4")
#   BENCH_WORKER     worker stopped for the degraded step (default worker2; "" skips it)
set -euo pipefail

controller_url=${CONTROLLER_URL:-http://127.0.0.1:${CONTROLLER_PORT:-8080}}
sizes=${BENCH_SIZES:-1000000 10000000 100000000}
requests=${BENCH_REQUESTS:-10}
clients_list=${BENCH_CLIENTS:-1 4}
degraded_worker=${BENCH_WORKER-worker2}
workdir=$(mktemp -d)
restart_worker=false
cleanup() {
  if [[ "${restart_worker}" == true ]]; then docker compose start "${degraded_worker}" >/dev/null 2>&1 || true; fi
  if [[ -f "${workdir}/ids" ]]; then
    while read -r id; do
      curl -fsS -X DELETE -o /dev/null "${controller_url}/api/v1/objects/${id}" || true
    done <"${workdir}/ids"
  fi
  rm -rf "${workdir}"
}
trap cleanup EXIT

# stats FILE BYTES: "median_s p99_s median_MBps" from a file of seconds.
stats() {
  sort -g "$1" | awk -v bytes="$2" '
    { t[NR] = $1 }
    END {
      med = (NR % 2) ? t[(NR + 1) / 2] : (t[NR / 2] + t[NR / 2 + 1]) / 2
      i = int(NR * 0.99 + 0.999999); if (i < 1) i = 1; if (i > NR) i = NR
      printf "%.3f %.3f %.1f\n", med, t[i], bytes / med / 1e6
    }'
}

upload_one() { # file -> prints "seconds object_id"
  curl -fsS -o "$2" -w '%{time_total}' -F "file=@$1" "${controller_url}/api/v1/objects"
}

# run_scenario NAME SIZE CLIENTS KIND(upload|download) FILE [OBJECT_ID]
run_scenario() {
  local name=$1 size=$2 clients=$3 kind=$4 file=$5 object=${6:-}
  local times="${workdir}/times.$$" per=$(( (requests + clients - 1) / clients ))
  : >"${times}"
  for ((c = 0; c < clients; c++)); do
    (
      for ((r = 0; r < per; r++)); do
        if [[ "${kind}" == upload ]]; then
          out="${workdir}/resp.${c}.${r}"
          t=$(upload_one "${file}" "${out}")
          sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p' "${out}" >>"${workdir}/ids"
        else
          t=$(curl -fsS -o /dev/null -w '%{time_total}' "${controller_url}/api/v1/objects/${object}")
        fi
        echo "${t}" >>"${times}"
      done
    ) &
  done
  wait
  read -r med p99 mbps < <(stats "${times}" "${size}")
  printf '| %s | %s | %d | %d | %s | %s | %s |\n' "${name}" "$(human "${size}")" "${clients}" "$(wc -l <"${times}")" "${med}" "${p99}" "${mbps}"
}

human() { awk -v b="$1" 'BEGIN { if (b >= 1e9) printf "%g GB", b/1e9; else if (b >= 1e6) printf "%g MB", b/1e6; else printf "%g kB", b/1e3 }'; }

curl -fsS "${controller_url}/healthz" >/dev/null
echo "| Scenario | Object | Clients | Requests | Median (s) | p99 (s) | Median MB/s per request |"
echo "| --- | --- | ---: | ---: | ---: | ---: | ---: |"
for size in ${sizes}; do
  file="${workdir}/obj.${size}"
  head -c "${size}" /dev/urandom >"${file}"
  for clients in ${clients_list}; do
    run_scenario upload "${size}" "${clients}" upload "${file}"
  done
  t=$(upload_one "${file}" "${workdir}/dl.json")
  object=$(sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p' "${workdir}/dl.json")
  echo "${object}" >>"${workdir}/ids"
  for clients in ${clients_list}; do
    run_scenario download "${size}" "${clients}" download "${file}" "${object}"
  done
  echo "${object} ${size}" >>"${workdir}/degraded"
done

if [[ -n "${degraded_worker}" ]]; then
  restart_worker=true
  docker compose stop "${degraded_worker}" >/dev/null 2>&1
  while read -r object size; do
    run_scenario "download, ${degraded_worker} stopped" "${size}" 1 download "${workdir}/obj.${size}" "${object}"
  done <"${workdir}/degraded"
  docker compose start "${degraded_worker}" >/dev/null 2>&1
  restart_worker=false
fi
