"""Simulación de sensores según el Anexo 3, con confirmación de entrega."""
import argparse
import json
import random
import time
from eventos import generar_sensor


def ejecutar(args):
    from kafka import KafkaProducer
    rng = random.Random(args.semilla)
    productor = KafkaProducer(bootstrap_servers=args.servidor, acks="all", retries=5,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"), key_serializer=lambda x: str(x).encode("utf-8"))
    try:
        for _ in range(args.eventos):
            evento = generar_sensor(rng)
            entrega = productor.send(args.topic, key=evento["sensor_id"], value=evento).get(timeout=30)
            print(json.dumps({"evento": evento, "particion": entrega.partition, "offset": entrega.offset}, ensure_ascii=False), flush=True)
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("Productor detenido por el usuario", flush=True)
    finally:
        productor.flush(timeout=30)
        productor.close(timeout=30)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--servidor", default="127.0.0.1:9092")
    p.add_argument("--topic", default="sensor_data_125")
    p.add_argument("--eventos", type=int, default=300)
    p.add_argument("--intervalo", type=float, default=1.0)
    p.add_argument("--semilla", type=int, default=125)
    a = p.parse_args()
    if a.eventos <= 0 or a.intervalo < 0:
        p.error("eventos debe ser positivo e intervalo no negativo")
    ejecutar(a)
