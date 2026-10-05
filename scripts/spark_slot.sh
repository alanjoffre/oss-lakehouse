#!/usr/bin/env bash
# Roda um comando segurando uma das 2 "vagas" de Spark da máquina.
# Por quê: cada sessão Spark local reserva ~2-3 GB; mais de duas ao mesmo tempo esgota a RAM.
# Uso: scripts/spark_slot.sh uv run pytest tests/test_silver.py
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCK_DIR="$ROOT/data/.locks"
mkdir -p "$LOCK_DIR"

if [[ -z "${JAVA_HOME:-}" && -d "$HOME/.local/opt/jdk17" ]]; then
  export JAVA_HOME="$HOME/.local/opt/jdk17"
fi
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
unset VIRTUAL_ENV

while true; do
  for slot in 1 2; do
    exec {fd}>"$LOCK_DIR/spark.$slot.lock"
    if flock -n "$fd"; then
      "$@"
      exit $?
    fi
    exec {fd}>&-
  done
  sleep 3
done
