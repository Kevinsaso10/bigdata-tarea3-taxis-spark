#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source "$HOME/bigdata_tarea2/env.sh"
root="$(pwd)/runtime"
kafka="$root/kafka_2.13-3.9.2"
mkdir -p logs
check_port() {
  python3 - "$1" <<'PY'
import socket,sys
try:
    with socket.create_connection(('127.0.0.1',int(sys.argv[1])),timeout=1):pass
except OSError:sys.exit(1)
PY
}
wait_port(){ for _ in $(seq 1 40); do if check_port "$1"; then return; fi; sleep 1; done; echo "No inició el puerto $1. Revisar logs."; exit 1; }
case "${1:-}" in
 iniciar)
  test -x "$kafka/bin/kafka-server-start.sh"
  if ! check_port 2181; then
    KAFKA_HEAP_OPTS='-Xms128m -Xmx256m' nohup "$kafka/bin/zookeeper-server-start.sh" "$root/config/zookeeper.properties" >logs/zookeeper.log 2>&1 &
    echo $! > "$root/zookeeper.pid"
  fi
  wait_port 2181
  if ! check_port 9092; then
    KAFKA_HEAP_OPTS='-Xms256m -Xmx512m' nohup "$kafka/bin/kafka-server-start.sh" "$root/config/server.properties" >logs/kafka.log 2>&1 &
    echo $! > "$root/kafka.pid"
  fi
  wait_port 9092
  "$kafka/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic sensor_data_125 --partitions 1 --replication-factor 1
  "$kafka/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --describe --topic sensor_data_125
  ;;
 estado)
  "$kafka/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --describe --topic sensor_data_125
  jps
  ;;
 detener)
  for service in kafka zookeeper; do
    pid_file="$root/$service.pid"
    [[ -f "$pid_file" ]] || continue
    read -r service_pid < "$pid_file"
    [[ "$service_pid" =~ ^[0-9]+$ ]] || { echo 'PID inválido'; exit 1; }
    if kill -0 "$service_pid" 2>/dev/null; then
      config_name=server.properties
      [[ "$service" == zookeeper ]] && config_name=zookeeper.properties
      if ! tr '\0' ' ' < "/proc/$service_pid/cmdline" | grep -Fq "$root/config/$config_name"; then
        echo "El PID de $service no pertenece a esta instalación. No se detuvo."; exit 1
      fi
      kill "$service_pid"
      for _ in $(seq 1 15); do kill -0 "$service_pid" 2>/dev/null || break; sleep 1; done
      if kill -0 "$service_pid" 2>/dev/null; then echo "$service aún está cerrando. Revisar logs."; exit 1; fi
    fi
    rm -- "$pid_file"
  done
  ;;
 *) echo 'Uso: bash scripts/control_kafka.sh iniciar|estado|detener';exit 1;;
esac
