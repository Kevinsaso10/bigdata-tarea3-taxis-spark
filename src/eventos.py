"""Generación pura de eventos para pruebas y productor Kafka."""
from datetime import datetime, timezone


def generar_sensor(rng, ahora=None):
    ahora = ahora or datetime.now(timezone.utc)
    if ahora.tzinfo is None:
        raise ValueError("La fecha del evento necesita zona horaria")
    return {"sensor_id": rng.randint(1, 10), "temperature": round(rng.uniform(20, 30), 2),
            "humidity": round(rng.uniform(30, 70), 2), "timestamp": ahora.astimezone(timezone.utc).isoformat()}
