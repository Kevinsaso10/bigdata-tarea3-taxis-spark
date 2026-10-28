#!/usr/bin/env bash
set -euo pipefail
source "$HOME/bigdata_tarea2/env.sh"
# La Tarea 3 no exige reiniciar HDFS ni YARN para este procesamiento de archivos locales.
if ! jps | awk '{print $2}' | grep -qx Master; then "$SPARK_HOME/sbin/start-master.sh"; fi
if ! jps | awk '{print $2}' | grep -qx Worker; then "$SPARK_HOME/sbin/start-worker.sh" spark://127.0.0.1:7077; fi
jps
