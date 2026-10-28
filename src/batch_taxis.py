"""Análisis batch distribuido de los viajes TLC. Ejecutar con spark-submit."""
import argparse
import csv
import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from visualizaciones import crear_informe

BASE = Path(__file__).resolve().parents[1]


def guardar_tabla(destino, nombre, df, limite):
    """Solo se recogen agregados acotados; los viajes crudos permanecen en Spark."""
    filas = [r.asDict(recursive=True) for r in df.limit(limite + 1).collect()]
    if len(filas) > limite:
        raise RuntimeError(f"El agregado {nombre} excede su límite de {limite}")
    with (destino / f"{nombre}.csv").open("w", newline="", encoding="utf-8") as salida:
        escritor = csv.DictWriter(salida, fieldnames=df.columns)
        escritor.writeheader()
        escritor.writerows(filas)
    print(f"\n{nombre.upper()}", flush=True)
    df.show(limite, truncate=False)
    return filas


def ejecutar(args):
    entrada = Path(args.entrada).resolve()
    zonas = Path(args.zonas).resolve()
    reglas = json.loads(Path(args.reglas).read_text(encoding="utf-8"))
    if not entrada.is_file() or not zonas.is_file():
        raise SystemExit("Faltan los archivos oficiales. Ejecutar scripts/descargar_datos.py.")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]
    salida = Path(args.salida).resolve() / run_id
    salida.mkdir(parents=True, exist_ok=False)
    spark = (SparkSession.builder.appName("Tarea3_125_Kevin_Taxis")
             .config("spark.sql.session.timeZone", "America/New_York")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    limpio = None
    inicio = time.perf_counter()
    try:
        print("Kevin Obed Tique Villada | Grupo 202016911_125", flush=True)
        print("Spark", spark.version, "| Maestro", spark.sparkContext.master, flush=True)
        print("Interfaz", spark.sparkContext.uiWebUrl, flush=True)
        raw = spark.read.parquet(entrada.as_uri())
        necesarias = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount", "total_amount"]
        if not set(necesarias).issubset(raw.columns):
            raise RuntimeError("El esquema del archivo TLC cambió. Revisar antes de procesar.")
        raw.printSchema()
        with zonas.open(encoding="utf-8-sig", newline="") as f:
            validas = [int(r["LocationID"]) for r in csv.DictReader(f)
                       if r["Borough"] not in ("Unknown", "N/A") and r["Zone"] not in ("Unknown", "N/A")]
        df = (raw.select(*necesarias)
            .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
            .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))).withColumn(
            "duracion_min", (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0)
        def finita(nombre):
            c = F.col(nombre)
            return c.isNotNull() & ~F.isnan(c) & (F.abs(c) < F.lit(float("inf")))
        condiciones = {
            "fechas_presentes": F.col("tpep_pickup_datetime").isNotNull() & F.col("tpep_dropoff_datetime").isNotNull(),
            "recogida_en_periodo": (F.col("tpep_pickup_datetime") >= F.to_timestamp(F.lit(reglas["periodo_inicio"]))) & (F.col("tpep_pickup_datetime") < F.to_timestamp(F.lit(reglas["periodo_fin_exclusivo"]))),
            "duracion_aceptada": (F.col("duracion_min") > 0) & (F.col("duracion_min") <= reglas["duracion_max_minutos"]),
            "distancia_aceptada": finita("trip_distance") & (F.col("trip_distance") > 0) & (F.col("trip_distance") <= reglas["distancia_max_millas"]),
            "importes_positivos": finita("fare_amount") & finita("total_amount") & (F.col("fare_amount") > 0) & (F.col("total_amount") > 0),
            "zonas_identificadas": F.col("PULocationID").isin(validas) & F.col("DOLocationID").isin(validas),
        }
        aceptada = F.lit(True)
        for c in condiciones.values():
            aceptada = aceptada & F.coalesce(c, F.lit(False))
        medidas = [F.count("*").alias("filas_originales"), F.sum(F.when(aceptada, 1).otherwise(0)).alias("filas_aceptadas")]
        medidas += [F.sum(F.when(~F.coalesce(c, F.lit(False)), 1).otherwise(0)).alias("excluidas_por_" + n) for n, c in condiciones.items()]
        calidad = df.agg(*medidas).first().asDict()
        calidad["filas_excluidas"] = calidad["filas_originales"] - calidad["filas_aceptadas"]
        calidad["nota"] = "Los motivos se solapan: no deben sumarse. No se eliminaron posibles duplicados sin un identificador único de viaje."
        print("CALIDAD", json.dumps(calidad, ensure_ascii=False), flush=True)
        if not calidad["filas_aceptadas"]:
            raise RuntimeError("Ninguna fila cumple las reglas")
        limpio = df.where(aceptada).persist(StorageLevel.MEMORY_AND_DISK)
        resumen = limpio.agg(F.count("*").alias("viajes"), F.avg("trip_distance").alias("distancia_media_millas"),
            F.avg("duracion_min").alias("duracion_media_min"), F.avg("fare_amount").alias("tarifa_media_usd"),
            F.avg("total_amount").alias("total_medio_usd"), F.max("total_amount").alias("total_maximo_usd")).first().asDict()
        assert resumen["viajes"] == calidad["filas_aceptadas"]
        horas = limpio.groupBy(F.hour("tpep_pickup_datetime").alias("hora")).agg(F.count("*").alias("viajes"), F.round(F.avg("total_amount"), 2).alias("total_medio_usd")).orderBy("hora")
        dias = limpio.groupBy(F.date_format("tpep_pickup_datetime", "yyyy-MM-dd").alias("dia")).count().withColumnRenamed("count", "viajes").orderBy("dia")
        dim = (spark.read.option("header", True).csv(zonas.as_uri())
               .select(F.col("LocationID").cast("int").alias("PULocationID"), F.col("Borough").alias("distrito"), F.col("Zone").alias("zona")))
        top = (limpio.groupBy("PULocationID").agg(F.count("*").alias("viajes"), F.round(F.avg("total_amount"), 2).alias("total_medio_usd"))
               .join(F.broadcast(dim), "PULocationID", "left").orderBy(F.desc("viajes"), "PULocationID").limit(10))
        tablas = {"viajes_por_hora": guardar_tabla(salida, "viajes_por_hora", horas, 24),
                  "viajes_por_dia": guardar_tabla(salida, "viajes_por_dia", dias, 31),
                  "zonas_principales": guardar_tabla(salida, "zonas_principales", top, 10)}
        h = hashlib.sha256()
        with entrada.open("rb") as archivo:
            for bloque in iter(lambda: archivo.read(1024 * 1024), b""):
                h.update(bloque)
        huella = h.hexdigest()
        resultado = {"autor": "Kevin Obed Tique Villada", "grupo": "202016911_125", "ejecucion": run_id,
            "spark_version": spark.version, "master": spark.sparkContext.master,
            "fuente": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page", "archivo": entrada.name,
            "sha256": huella, "reglas": reglas, "calidad": calidad, "resumen": resumen,
            "segundos_procesamiento": round(time.perf_counter() - inicio, 3),
            "alcance": "Viajes observados de taxis amarillos en enero de 2025; no demanda insatisfecha. No es un benchmark distribuido."}
        (salida / "resumen.json").write_text(json.dumps(resultado, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        crear_informe(salida, tablas, resultado)
        print("\nFINALIZACIÓN CORRECTA", salida, flush=True)
        print(json.dumps(resumen, ensure_ascii=False), flush=True)
        if args.pausa:
            print(f"Panel disponible durante {args.pausa} segundos para capturas", flush=True)
            time.sleep(args.pausa)
    finally:
        if limpio is not None:
            limpio.unpersist()
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entrada", default=str(BASE / "datos/originales/yellow_tripdata_2025-01.parquet"))
    parser.add_argument("--zonas", default=str(BASE / "datos/originales/taxi_zone_lookup.csv"))
    parser.add_argument("--reglas", default=str(BASE / "config/calidad.json"))
    parser.add_argument("--salida", default=str(BASE / "resultados/batch"))
    parser.add_argument("--pausa", type=int, choices=range(0, 601), metavar="0..600", default=0)
    ejecutar(parser.parse_args())
