"""Inspección independiente con Arrow/Pandas. No constituye una ejecución Spark.

Dependencias opcionales: pyarrow y pandas. Procesa lotes para limitar memoria.
"""
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

BASE = Path(__file__).resolve().parents[1]


def ejecutar():
    origen = BASE / "datos/originales/yellow_tripdata_2025-01.parquet"
    reglas = json.loads((BASE / "config/calidad.json").read_text(encoding="utf-8"))
    with (BASE / "datos/originales/taxi_zone_lookup.csv").open(encoding="utf-8-sig", newline="") as f:
        zonas = {int(r["LocationID"]) for r in csv.DictReader(f) if r["Borough"] not in ("Unknown", "N/A") and r["Zone"] not in ("Unknown", "N/A")}
    horas, dias, ubicaciones = Counter(), Counter(), Counter()
    calidad = Counter()
    archivo = pq.ParquetFile(origen)
    columnas = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount", "total_amount"]
    for b in archivo.iter_batches(batch_size=100000, columns=columnas):
        d = b.to_pandas()
        pickup, dropoff = d.tpep_pickup_datetime, d.tpep_dropoff_datetime
        minutos = (dropoff - pickup).dt.total_seconds() / 60
        reglas_fila = {
            "fechas_presentes": pickup.notna() & dropoff.notna(),
            "recogida_en_periodo": (pickup >= pd.Timestamp(reglas["periodo_inicio"])) & (pickup < pd.Timestamp(reglas["periodo_fin_exclusivo"])),
            "duracion_aceptada": (minutos > 0) & (minutos <= reglas["duracion_max_minutos"]),
            "distancia_aceptada": np.isfinite(d.trip_distance) & (d.trip_distance > 0) & (d.trip_distance <= reglas["distancia_max_millas"]),
            "importes_positivos": np.isfinite(d.fare_amount) & np.isfinite(d.total_amount) & (d.fare_amount > 0) & (d.total_amount > 0),
            "zonas_identificadas": d.PULocationID.isin(zonas) & d.DOLocationID.isin(zonas),
        }
        valida = np.ones(len(d), dtype=bool)
        calidad["filas_originales"] += len(d)
        for n, r in reglas_fila.items():
            mascara = r.fillna(False).to_numpy(dtype=bool)
            calidad["excluidas_por_" + n] += int((~mascara).sum())
            valida &= mascara
        calidad["filas_aceptadas"] += int(valida.sum())
        horas.update(d.loc[valida, "tpep_pickup_datetime"].dt.hour.value_counts().to_dict())
        dias.update(d.loc[valida, "tpep_pickup_datetime"].dt.strftime("%Y-%m-%d").value_counts().to_dict())
        ubicaciones.update(d.loc[valida, "PULocationID"].value_counts().to_dict())
    calidad["filas_excluidas"] = calidad["filas_originales"] - calidad["filas_aceptadas"]
    h = hashlib.sha256()
    with origen.open("rb") as f:
        for bloque in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloque)
    resultado = {"metodo": "Inspección local independiente con Arrow/Pandas; NO ejecución Spark", "verificado_utc": datetime.now(timezone.utc).isoformat(),
        "sha256": h.hexdigest(), "filas_metadatos": archivo.metadata.num_rows, "calidad": dict(calidad),
        "viajes_por_hora": dict(sorted(horas.items())), "viajes_por_dia": dict(sorted(dias.items())),
        "zonas_top10": [{"LocationID": int(k), "viajes": int(v)} for k, v in ubicaciones.most_common(10)]}
    assert sum(horas.values()) == sum(dias.values()) == calidad["filas_aceptadas"]
    (BASE / "datos/control_local.json").write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(resultado, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    ejecutar()
