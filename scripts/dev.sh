#!/usr/bin/env bash
# Usage: scripts/dev.sh [build|test|run|deps|itest]
#   deps   builds vendor/ (static librdkafka + OpenSSL) once; needed for the live Kafka client
#   itest  integration tests against a real broker (default localhost:9092, plaintext); KM_ITEST_BOOTSTRAP overrides
set -euo pipefail
cd "$(dirname "$0")/.."
# The manifest reads vendor/ to decide whether the Kafka targets exist, and SwiftPM's manifest cache does not notice
# that, so every command here bypasses the cache.
SWIFT=(swift)
FLAGS=(--manifest-cache none)
need_vendor() {
  [[ -f vendor/install/lib/librdkafka.a ]] || { echo "vendor/ is missing: run 'scripts/dev.sh deps' first" >&2; exit 1; }
}
note_vendor() {
  [[ -f vendor/install/lib/librdkafka.a ]] || echo "note: no vendor/, building WITHOUT the live Kafka client (demo only). Run 'scripts/dev.sh deps' to enable it." >&2
}
case "${1:-build}" in
  build) note_vendor; "${SWIFT[@]}" build "${FLAGS[@]}" ;;
  test)  note_vendor; "${SWIFT[@]}" test "${FLAGS[@]}" ;;
  run)   note_vendor; "${SWIFT[@]}" run "${FLAGS[@]}" KafkaManager ;;
  deps)  scripts/build-deps.sh ;;
  itest) need_vendor; KM_ITEST=1 "${SWIFT[@]}" test "${FLAGS[@]}" --filter KafkaManagerKafkaTests ;;
  *) echo "usage: $0 [build|test|run|deps|itest]" >&2; exit 1 ;;
esac
