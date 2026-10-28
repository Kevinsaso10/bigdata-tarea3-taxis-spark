# Ejecución y capturas de la Tarea 3

## Primera sesión con batch

1. Encender en VirtualBox la máquina `BIGDATA_T2_125`. Mantener el usuario `kevinsaso` y la contraseña personal. No reinstalar Ubuntu.
2. En PowerShell de Windows, copiar el paquete:

```powershell
scp -P 2222 "C:\Users\gamer\Music\Semestre 9\Big Data\3\tarea3_taxis.zip" kevinsaso@127.0.0.1:~/
ssh -p 2222 kevinsaso@127.0.0.1
```

3. Ya en Ubuntu, comprobar que no exista una extracción anterior:

```bash
ls -ld ~/tarea3_taxis
```

Si no existe, extraer el paquete y entrar. Si existe, conservarla y revisar antes de reemplazar archivos.

```bash
python3 -m zipfile -e ~/tarea3_taxis.zip ~/
cd ~/tarea3_taxis
python3 scripts/descargar_datos.py
python3 -m unittest discover -s tests -v
bash scripts/iniciar_spark.sh
```

El descargador conserva los archivos incluidos, comprueba su SHA-256 frente al manifiesto y no vuelve a descargarlos si ya están presentes.

4. Captura `01_entorno_tarea3.png`: versiones y procesos.

```bash
source ~/bigdata_tarea2/env.sh
clear
python3 --version
java -version
"$SPARK_HOME/bin/spark-submit" --version
jps
```

5. Ejecutar el batch. No iniciar el streaming a la vez.

```bash
cd ~/tarea3_taxis
bash scripts/ejecutar_batch.sh 600
```

Capturas reales que deben conservarse:

- `02_esquema_taxis.png`: columnas del Parquet.
- `03_calidad_taxis.png`: conteos originales, aceptados y excluidos.
- `04_viajes_hora.png`: agregado por hora.
- `05_zonas_principales.png`: diez zonas con más recogidas.
- `06_finalizacion_batch.png`: resumen y mensaje de finalización correcta.

No confundir la pausa de diez minutos con bloqueo. No pulsar Ctrl+C antes de revisar el panel.

6. En otra pestaña PowerShell crear el túnel:

```powershell
ssh -p 2222 -N -L 4041:127.0.0.1:4040 -L 8765:127.0.0.1:8765 kevinsaso@127.0.0.1
```

Mientras el batch siga activo, abrir http://127.0.0.1:4041 y tomar `07_batch_jobs.png` y `08_batch_executors.png`.

7. En otra conexión SSH servir las visualizaciones:

```bash
cd ~/tarea3_taxis
python3 -m http.server 8765 --bind 127.0.0.1 --directory resultados
```

Abrir http://127.0.0.1:8765/batch/, entrar a la ejecución y a `informe_batch.html`. Captura `09_graficos_batch.png`.

8. Comparar el resultado real con el control local, sustituyendo EJECUCION por el nombre de la carpeta:

```bash
python3 scripts/verificar_resultado.py resultados/batch/EJECUCION/resumen.json
```

El control fue calculado con otra herramienta y sobre el mismo archivo. Una diferencia debe investigarse; no cambiar las cifras para forzar una coincidencia.

## Segunda sesión con Kafka y streaming

El anexo pide sensores simulados. Mantener ese ejemplo separado de los viajes históricos.

```bash
cd ~/tarea3_taxis
bash scripts/preparar_kafka.sh
bash scripts/control_kafka.sh iniciar
```

Si se solicita `python3-venv`, instalar ese paquete con apt y repetir la preparación. No cambiar Java ni volver a formatear Hadoop.

Captura `10_topic_kafka.png`: debe verse el topic `sensor_data_125`, una partición y replicación 1.

Iniciar primero el consumidor:

```bash
bash scripts/ejecutar_streaming.sh sensores_demo1
```

En otra terminal:

```bash
cd ~/tarea3_taxis
.venv/bin/python src/kafka_producer.py --eventos 300 --intervalo 1
```

Tomar `11_productor_kafka.png` con confirmaciones de offsets, y `12_ventanas_streaming.png` con conteos y promedios por sensor. Abrir el monitor en http://127.0.0.1:8765/stream/sensores_demo1/monitor.html y guardar `13_monitor_streaming.png`. Tomar también capturas del panel Spark mientras siga activa la consulta.

Si un topic conserva eventos de una sesión previa, `earliest` puede leerlos en una sesión nueva. El checkpoint conserva el progreso al reutilizar la misma sesión. Documentar cualquier repetición; no afirmar que todos los eventos observados acaban de producirse.

## Cierre

Detener productor y consumidor con Ctrl+C. Detener el servidor HTTP con Ctrl+C. Luego:

```bash
bash scripts/control_kafka.sh detener
```

Apagar Ubuntu normalmente cuando ya se hayan copiado los resultados y evidencias. No borrar los originales, checkpoints o logs para ocultar un fallo.

## Aún por completar

Publicación del repositorio; evidencias de ejecución real; presentación individual; video o videos según la aclaración del tutor; investigación y PDF grupal. El código preparado y los controles locales no sustituyen estas evidencias.
