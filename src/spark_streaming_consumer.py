"""Ventanas de sensores Kafka con Structured Streaming y checkpoint persistente."""
import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T
from bloqueo_sesion import bloquear_sesion

BASE = Path(__file__).resolve().parents[1]

HTML = '''<!doctype html><html lang="es"><meta charset="utf-8"><title>Sensores Kafka y Spark</title>
<style>body{font:17px Arial;max-width:1100px;margin:35px auto;padding:0 20px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:10px;text-align:left}th{background:#e8f0f5}#estado{padding:15px;background:#eef4f7}</style>
<h1>Sensores simulados con Kafka y Spark</h1><p>Kevin Obed Tique Villada · Grupo 202016911_125</p>
<p>Últimas actualizaciones de ventanas de un minuto. No representa todas las ventanas históricas. Temperatura en °C; humedad relativa en %. Datos sintéticos.</p>
<div id="estado">Esperando eventos procesados...</div><table><thead><tr><th>Inicio UTC</th><th>Sensor</th><th>Eventos</th><th>Temperatura media °C</th><th>Humedad media %</th></tr></thead><tbody id="filas"></tbody></table>
<script>async function actualizar(){try{const r=await fetch('ultimo.json?t='+Date.now());if(!r.ok)return;const d=await r.json();document.getElementById('estado').textContent='Microbatch '+d.batch_id+' · '+d.actualizado_utc;const b=document.getElementById('filas');b.replaceChildren();for(const f of d.filas){const tr=document.createElement('tr');for(const k of ['inicio','sensor_id','eventos','temperatura_media','humedad_media']){const td=document.createElement('td');td.textContent=f[k];tr.appendChild(td)}b.appendChild(tr)}}catch(e){document.getElementById('estado').textContent='Sin acceso al JSON; abrir mediante el servidor HTTP del laboratorio.'}}actualizar();setInterval(actualizar,3000);</script></html>'''


def ejecutar(args):
    if not args.sesion.replace("_", "").replace("-", "").isalnum():
        raise SystemExit("Nombre de sesión inválido")
    carpeta = BASE / "resultados/stream" / args.sesion
    carpeta.mkdir(parents=True, exist_ok=True)
    checkpoint = BASE / "runtime/checkpoints" / args.sesion
    checkpoint.mkdir(parents=True, exist_ok=True)
    (carpeta / "monitor.html").write_text(HTML, encoding="utf-8")
    spark = (SparkSession.builder.appName("Tarea3_125_Kevin_Sensores")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    schema = T.StructType([T.StructField("sensor_id", T.IntegerType()), T.StructField("temperature", T.DoubleType()),
                          T.StructField("humidity", T.DoubleType()), T.StructField("timestamp", T.StringType())])
    consulta = None
    try:
        df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.servidor)
            .option("subscribe", args.topic).option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "1000").option("failOnDataLoss", "true").load())
        eventos = (df.select(F.from_json(F.col("value").cast("string"), schema).alias("evento"))
            .select("evento.*").withColumn("event_time", F.to_timestamp("timestamp")))
        validos = eventos.where(F.col("sensor_id").between(1, 10) & F.col("event_time").isNotNull()
            & F.col("temperature").between(-50, 80) & F.col("humidity").between(0, 100))
        ventanas = (validos.withWatermark("event_time", "2 minutes")
            .groupBy(F.window("event_time", "1 minute"), "sensor_id")
            .agg(F.count("*").alias("eventos"), F.round(F.avg("temperature"), 2).alias("temperatura_media"),
                 F.round(F.avg("humidity"), 2).alias("humedad_media")))

        def guardar(actualizaciones, batch_id):
            filas = [r.asDict() for r in (actualizaciones.select(F.col("window.start").cast("string").alias("inicio"),
                F.col("window.end").cast("string").alias("fin"), "sensor_id", "eventos", "temperatura_media", "humedad_media")
                .orderBy(F.desc("inicio"), "sensor_id").limit(101).collect())]
            if len(filas) > 100:
                raise RuntimeError("Más de 100 actualizaciones por microbatch; ampliar el sumidero antes de continuar")
            if not filas:
                return
            datos = {"batch_id": batch_id, "actualizado_utc": datetime.now(timezone.utc).isoformat(), "filas": filas,
                     "nota": "Actualizaciones de ventanas, no inventario completo del estado. Eventos inválidos se filtran antes de agregar."}
            texto = json.dumps(datos, ensure_ascii=False, indent=2)
            (carpeta / f"batch_{batch_id:06d}.json").write_text(texto, encoding="utf-8")
            temporal = carpeta / "ultimo.tmp"
            temporal.write_text(texto, encoding="utf-8")
            temporal.replace(carpeta / "ultimo.json")
            print(f"\nMICROBATCH {batch_id} | actualizaciones: {len(filas)}", flush=True)
            for fila in filas:
                print(fila, flush=True)

        consulta = (ventanas.writeStream.outputMode("update").foreachBatch(guardar)
            .option("checkpointLocation", checkpoint.resolve().as_uri()).trigger(processingTime="5 seconds").start())
        print("CONSUMIDOR INICIADO", spark.sparkContext.uiWebUrl, flush=True)
        print("Checkpoint", checkpoint, "| Resultados", carpeta, flush=True)
        print("Iniciar ahora kafka_producer.py en otra terminal. Ctrl+C detiene la consulta.", flush=True)
        if args.duracion:
            consulta.awaitTermination(args.duracion)
        else:
            consulta.awaitTermination()
    except KeyboardInterrupt:
        print("Consumidor detenido por el usuario", flush=True)
    finally:
        if consulta is not None:
            progreso = consulta.lastProgress
            if progreso:
                (carpeta / "ultimo_progreso.json").write_text(json.dumps(progreso, ensure_ascii=False, indent=2), encoding="utf-8")
            consulta.stop()
        spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--servidor", default="127.0.0.1:9092")
    p.add_argument("--topic", default="sensor_data_125")
    p.add_argument("--sesion", default="sensores_demo1")
    p.add_argument("--duracion", type=int, default=0, help="Segundos; 0 espera hasta Ctrl+C")
    args = p.parse_args()
    if args.duracion < 0:
        p.error("La duración no puede ser negativa")
    if not args.sesion.replace("_", "").replace("-", "").isalnum():
        p.error("Nombre de sesión inválido")
    try:
        with bloquear_sesion(BASE / "runtime/locks" / (args.sesion + ".lock")):
            ejecutar(args)
    except RuntimeError as error:
        raise SystemExit(str(error)) from error
