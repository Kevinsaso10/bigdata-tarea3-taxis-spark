# Presentación individual de Kevin Obed Tique Villada

Grupo 202016911_125 · Big Data Tarea 3

Contenido listo para diagramar en una presentación online. Incorporar los enlaces reales de videos y repositorio antes de entregar. Las figuras se basan en los resultados guardados de la ejecución `20261027T154200Z_51508b`; las capturas del primer batch muestran los mismos conteos y agregados.

## Diapositiva 1 Viajes de taxi con Apache Spark

Análisis de viajes de Nueva York en enero de 2025 y práctica de sensores con Kafka.

Kevin Obed Tique Villada. Grupo 202016911_125. UNAD. Octubre de 2026.

## Diapositiva 2 Problema y objetivos

Pregunta: ¿En qué horarios y zonas se concentran los viajes registrados y cómo se caracterizan sus distancias, duraciones e importes?

Objetivo: describir patrones temporales y geográficos con Spark, aplicando reglas de calidad documentadas.

Alcance: taxis amarillos durante enero de 2025. Los registros no permiten medir demanda insatisfecha.

## Diapositiva 3 Fuente y conjunto de datos

NYC Taxi and Limousine Commission. Archivo `yellow_tripdata_2025-01.parquet`: 3.475.226 filas y 20 columnas. Catálogo oficial de zonas para identificar las ubicaciones.

Marcas de tiempo locales de Nueva York, distancias en millas e importes en USD. Conservar el original y su huella SHA-256 permite identificar la copia utilizada.

Fuente: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Diapositiva 4 Procesamiento y calidad

Spark 3.5.8 y DataFrames. Lectura Parquet, validación de fechas, filtros de duración, distancia e importes y zonas identificadas.

3.475.226 filas originales; 3.225.495 aceptadas; 249.731 excluidas (7,19 %).

Los motivos de exclusión se solapan. Las reglas delimitan el estudio y no constituyen umbrales oficiales de TLC. No se eliminaron filas solo por parecer duplicadas.

Visual: captura `03_calidad_taxis.png` o tabla editable con los conteos.

## Diapositiva 5 Actividad por hora

Visual: gráfico de `viajes_por_hora.csv` o captura `09a_viajes_hora.png`.

Mayor actividad entre 18:00 y 18:59: 234.630 viajes. Son viajes acumulados durante enero, no un promedio diario.

Hora mínima: 04:00 a 04:59, con 17.760 viajes. Estos patrones describen el subconjunto aceptado.

## Diapositiva 6 Actividad por día y por zona

Visuales: `09b_viajes_dia.png` y `09c_zonas.png`, o dos diapositivas separadas si el texto queda pequeño.

Día con más viajes: 25 de enero, 128.167. Zona con más recogidas: Midtown Center, 160.642. Upper East Side South y North le siguen con 157.417 y 149.183, respectivamente.

## Diapositiva 7 Caracterización y valores atípicos

Distancia media: 3,12 millas. Duración media: 14,61 minutos. Tarifa base media: 17,95 USD. Importe total medio: 26,77 USD.

Un viaje registra 863.380,37 USD de importe total y 1,6 millas de distancia. Es un dato del original que requiere revisión, no una tarifa representativa. Los promedios monetarios necesitan esa advertencia.

Fuente: `resumen.json` y revisión del registro en el archivo original. La mediana de 20,33 USD corresponde a la inspección local independiente, no a un cálculo incluido en la ejecución Spark.

## Diapositiva 8 Validación y entorno

Los conteos de ambas ejecuciones Spark coinciden con el control independiente sobre la misma copia del Parquet.

Ubuntu 22.04, Java 11, Spark Standalone, un trabajador con dos núcleos. Los tiempos observados fueron 42,935 y 36,274 segundos, excluyendo la pausa para capturas. No constituyen un benchmark comparativo ni prueban escalabilidad multinodo.

Visual: `09d_control_batch.png` y, si queda legible, `08_batch_executors.png`.

## Diapositiva 9 Sensores con Kafka y Structured Streaming

Componente separado que desarrolla el Anexo 3: productor Python con temperatura y humedad sintéticas; topic `sensor_data_125`; Kafka 3.9.2 con ZooKeeper; consumidor Spark 3.5.8.

Una partición y replicación 1. El productor espera confirmación de Kafka y muestra offsets.

Visual: `10_topic_kafka.png` o `11_productor_kafka.png`.

## Diapositiva 10 Ventanas y monitor

Ventanas de un minuto por sensor y marca de agua de dos minutos. Conteo, temperatura media y humedad media. Fechas UTC. Salida de actualizaciones con checkpoint persistente.

El monitor muestra las últimas ventanas modificadas y se actualiza cada tres segundos. No representa todo el historial.

Visual: `13_monitor_streaming.png`. Fuente técnica: https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html

## Diapositiva 11 Resultados de streaming y dificultades

Las últimas cuentas de cada ventana y sensor, reconstruidas desde los archivos guardados, representan 420 eventos. No se sumaron las actualizaciones repetidas.

Se observó un conflicto al lanzar un consumidor adicional sobre el mismo checkpoint. La consulta original siguió generando resultados. La corrección es mantener un solo consumidor activo por sesión; se añadió un bloqueo local como protección para futuras ejecuciones.

Visuales: `14_streaming_jobs.png` y `15_streaming_executors.png`. En la captura de Executors: driver y un ejecutor de dos núcleos, cero tareas fallidas.

## Diapositiva 12 Conclusión individual

La práctica permitió procesar un archivo de millones de viajes con DataFrames, verificar el efecto de las reglas de calidad y visualizar agregados acotados. Kafka y Structured Streaming permitieron observar el procesamiento incremental de sensores y la actualización de ventanas. Las evidencias muestran el funcionamiento en una máquina virtual, con límites de alcance y sin demostrar alta disponibilidad o demanda insatisfecha.

## Diapositiva 13 Código y videos

Incorporar el enlace real del repositorio, el video batch y el video de Kafka/streaming. Verificar acceso sin iniciar sesión antes de entregar. Si el tutor confirma un solo video, ajustar esta diapositiva.

## Diapositiva 14 Referencias

Apache Software Foundation. (s. f.). *Spark SQL, DataFrames and Datasets guide*. Apache Spark 3.5.8. https://spark.apache.org/docs/3.5.8/sql-programming-guide.html

Apache Software Foundation. (s. f.). *Structured Streaming programming guide*. Apache Spark 3.5.8. https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html

Apache Software Foundation. (s. f.). *Structured Streaming and Kafka integration guide*. Apache Spark 3.5.8. https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html

Apache Software Foundation. (s. f.). *Quick start*. Apache Kafka 3.9. https://kafka.apache.org/39/getting-started/quickstart/

New York City Taxi and Limousine Commission. (s. f.). *TLC trip record data* [Conjunto de datos]. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
