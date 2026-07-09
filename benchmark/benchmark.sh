#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCHMARK="${ROOT_DIR}/bin/benchmark"
CONFIG="${1:-${ROOT_DIR}/bin/test.conf}"
OUTPUT="${2:-${ROOT_DIR}/benchmark/results.csv}"

DURATION="${DURATION:-30}"
WARMUP="${WARMUP:-5}"
KEY_COUNT="${KEY_COUNT:-10000}"
VALUE_SIZE="${VALUE_SIZE:-128}"
CONCURRENCIES="${CONCURRENCIES:-1 2 4 8 16 32}"
WORKLOADS="${WORKLOADS:-write read mixed}"
READ_RATIO="${READ_RATIO:-50}"

if [[ ! -x "${BENCHMARK}" ]]; then
  echo "benchmark binary not found: ${BENCHMARK}" >&2
  echo "build it first with: cmake --build <build-dir> --target benchmark" >&2
  exit 1
fi

if [[ ! -f "${CONFIG}" ]]; then
  echo "config file not found: ${CONFIG}" >&2
  exit 1
fi

mkdir -p "$(dirname "${OUTPUT}")"
echo "workload,threads,read_ratio,key_count,value_size,duration_s,operations,qps,p50_us,p90_us,p99_us,p999_us" >"${OUTPUT}"

for workload in ${WORKLOADS}; do
  for concurrency in ${CONCURRENCIES}; do
    echo "running workload=${workload} threads=${concurrency}" >&2
    "${BENCHMARK}" \
      --config "${CONFIG}" \
      --workload "${workload}" \
      --threads "${concurrency}" \
      --duration "${DURATION}" \
      --warmup "${WARMUP}" \
      --read-ratio "${READ_RATIO}" \
      --key-count "${KEY_COUNT}" \
      --value-size "${VALUE_SIZE}" >>"${OUTPUT}"
  done
done

echo "results written to ${OUTPUT}" >&2
