#!/usr/bin/env bash
set -euo pipefail

bench_args=${BENCH_ARGS:--benchtime=10x}
echo "Running upload window benchmark with: ${bench_args}"
# shellcheck disable=SC2086
go test -run '^$' -bench 'UploadWindow$' ${bench_args} ./test/integration/
