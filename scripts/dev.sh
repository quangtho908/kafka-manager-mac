#!/usr/bin/env bash
# Usage: scripts/dev.sh [build|test|run]
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-build}" in
  build) swift build ;;
  test)  swift test ;;
  run)   swift run KafkaManager ;;
  *) echo "usage: $0 [build|test|run]" >&2; exit 1 ;;
esac
