#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source "$HOME/bigdata_tarea2/env.sh"
export PYTHONUNBUFFERED=1
mkdir -p logs
"$SPARK_HOME/bin/spark-submit" --master spark://127.0.0.1:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g --total-executor-cores 2 \
  src/batch_taxis.py --pausa "${1:-600}" 2>&1 | tee "logs/batch_$(date +%Y%m%d_%H%M%S).log"
