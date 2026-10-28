#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$(id -u)" != 0 ]] || { echo 'Ejecutar sin sudo bash.'; exit 1; }
source "$HOME/bigdata_tarea2/env.sh"
test -x "$JAVA_HOME/bin/java"
root="$(pwd)/runtime"
mkdir -p "$root/descargas" "$root/config" "$root/zookeeper_data" "$root/kafka_data" logs
archive=kafka_2.13-3.9.2.tgz
base_url=https://archive.apache.org/dist/kafka/3.9.2
if [[ ! -d "$root/kafka_2.13-3.9.2" ]]; then
  if [[ ! -f "$root/descargas/$archive" ]]; then
    curl --fail --location --retry 3 --continue-at - -o "$root/descargas/$archive.part" "$base_url/$archive"
    mv "$root/descargas/$archive.part" "$root/descargas/$archive"
  fi
  curl --fail --location --retry 3 -o "$root/descargas/$archive.sha512" "$base_url/$archive.sha512"
  python3 - "$root/descargas/$archive" <<'PY'
import hashlib,re,sys
from pathlib import Path
p=Path(sys.argv[1]); texto=Path(str(p)+'.sha512').read_text()
hashes=re.findall(r'\b[0-9a-fA-F]{128}\b',texto)
if not hashes:
    # Apache también utiliza grupos de ocho caracteres separados por espacios.
    candidatos=re.findall(r'(?i)(?:[0-9a-f]{8}[ \t\r\n]+){15}[0-9a-f]{8}',texto)
    hashes=[''.join(c.split()) for c in candidatos]
if len(hashes)!=1: raise SystemExit('No se pudo interpretar SHA-512 oficial')
h=hashlib.sha512()
with p.open('rb') as f:
    for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
if h.hexdigest()!=hashes[0].lower():raise SystemExit('SHA-512 no coincide. Conservar archivos para revisión.')
print('SHA-512 oficial de Kafka correcto')
PY
  tar -xzf "$root/descargas/$archive" -C "$root"
fi
if ! python3 -m venv --help >/dev/null 2>&1; then echo 'Se necesita python3-venv'; exit 1; fi
if [[ ! -x .venv/bin/python ]] || ! .venv/bin/python -m pip --version >/dev/null 2>&1; then
  if ! python3 -m venv .venv; then
    echo 'Instalar: sudo apt-get update && sudo apt-get install -y python3-venv; después repetir este script.'
    exit 1
  fi
fi
.venv/bin/python -m pip install -r requirements.txt
if [[ ! -f "$root/config/zookeeper.properties" ]]; then
cat > "$root/config/zookeeper.properties" <<CONF
dataDir=$root/zookeeper_data
clientPort=2181
clientPortAddress=127.0.0.1
maxClientCnxns=20
admin.enableServer=false
CONF
fi
if [[ ! -f "$root/config/server.properties" ]]; then
cat > "$root/config/server.properties" <<CONF
broker.id=125
listeners=PLAINTEXT://127.0.0.1:9092
advertised.listeners=PLAINTEXT://127.0.0.1:9092
log.dirs=$root/kafka_data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
zookeeper.connect=127.0.0.1:2181
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.retention.hours=24
CONF
fi
echo 'Kafka preparado. Iniciar con bash scripts/control_kafka.sh iniciar'
