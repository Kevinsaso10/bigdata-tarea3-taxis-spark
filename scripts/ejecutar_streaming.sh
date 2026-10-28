#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source "$HOME/bigdata_tarea2/env.sh"
export PYTHONUNBUFFERED=1
mkdir -p logs
"$SPARK_HOME/bin/spark-submit" --master spark://127.0.0.1:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g --total-executor-cores 2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  src/spark_streaming_consumer.py --sesion "${1:-sensores_demo1}" 2>&1 | tee "logs/stream_$(date +%Y%m%d_%H%M%S).log"
