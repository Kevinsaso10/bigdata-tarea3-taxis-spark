# Análisis de viajes de taxi con Apache Spark

Kevin Obed Tique Villada · Grupo 202016911_125 · UNAD · Big Data Tarea 3

## Problema

Identificar qué horarios y zonas concentran los viajes observados de taxis amarillos de Nueva York en enero de 2025 y cómo varían la duración, distancia y los importes. Los resultados pueden orientar una descripción de la actividad registrada. No se mide demanda insatisfecha, rentabilidad neta del conductor ni causalidad.

## Datos y alcance

Fuente primaria: [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Archivo: `yellow_tripdata_2025-01.parquet`, con **3.475.226 filas** y 20 columnas en la copia verificada el 20 de octubre de 2026. Catálogo `taxi_zone_lookup.csv`. TLC advierte que no garantiza la exactitud de los registros proporcionados por los proveedores. El original se conserva sin modificar y se registran tamaño, origen y SHA-256 local en `datos/manifest.json`.

Spark ejecuta la limpieza y las agregaciones de todo el archivo. Solo se recogen en el controlador los agregados acotados: 24 horas, 31 días y diez zonas. No se utiliza `toPandas` ni `collect` sobre los millones de viajes.

Las marcas de recogida se interpretan como horas locales de Nueva York. Los importes están en USD y las distancias en millas. La tarifa base y el importe total son variables distintas. Los promedios son por viaje y no representan ingresos netos.

## Entorno

- Ubuntu 22.04 y usuario `kevinsaso` de la Tarea 2.
- Java 11, Spark 3.5.8, Python 3.10 de Ubuntu.
- Spark Standalone con un trabajador, dos CPU y 1 GB por ejecutor.
- Kafka 3.9.2 con ZooKeeper, para seguir el procedimiento del Anexo 3.
- Conector Spark Kafka `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`.
- Cliente Python fijado en `requirements.txt`, dentro de `.venv`.

El sufijo Scala 2.13 de la distribución del broker Kafka no determina el sufijo del conector de Spark. Este último debe coincidir con el Scala 2.12 de Spark instalado.

## Ejecución batch

Desde la raíz del proyecto en Ubuntu:

```bash
python3 scripts/descargar_datos.py
bash scripts/iniciar_spark.sh
bash scripts/ejecutar_batch.sh 600
```

El análisis guarda una carpeta nueva por ejecución en `resultados/batch`. Contiene `resumen.json`, tres tablas CSV, tres gráficos SVG y `informe_batch.html`. El tiempo registrado excluye la pausa para capturas. Si falla no debe presentarse como ejecución completada.

Las reglas se documentan en `config/calidad.json`: fechas presentes, recogida dentro de enero, duración entre más de cero y 180 minutos, distancia positiva y de hasta 100 millas, importes finitos y positivos y zonas identificadas. Son criterios operativos del estudio, no umbrales oficiales de TLC. Se cuenta cuántas filas incumplen cada regla, permitiendo solapamientos; la suma de los motivos no equivale al total de filas excluidas. No se eliminan posibles duplicados al no disponer de un identificador único de viaje.

## Streaming del Anexo 3

Este componente simula temperatura y humedad de diez sensores. Es independiente del conjunto histórico de taxis y se incluye para desarrollar el Anexo 3. Los eventos son sintéticos, con hora UTC en formato ISO 8601. El formato corrige la ambigüedad del ejemplo, cuyo productor entrega segundos Unix mientras el consumidor espera un timestamp.

```bash
bash scripts/preparar_kafka.sh
bash scripts/control_kafka.sh iniciar
bash scripts/ejecutar_streaming.sh sensores_demo1
```

En otra terminal conectada a Ubuntu:

```bash
cd ~/tarea3_taxis
.venv/bin/python src/kafka_producer.py --eventos 300 --intervalo 1
```

El consumidor mantiene ventanas de un minuto por sensor y marca de agua de dos minutos. Los eventos inválidos se filtran. La salida `update` contiene ventanas modificadas, no un listado de todas las ventanas históricas. El productor espera la confirmación de Kafka; con un broker y replicación 1 no hay tolerancia a la pérdida de la máquina. No se atribuyen garantías generales de exactly-once a todos los componentes.

Resultados en `resultados/stream/sensores_demo1`: un JSON por microbatch y `ultimo.json`, que alimenta `monitor.html`. El checkpoint en `runtime/checkpoints/sensores_demo1` permite retomar la sesión con su misma configuración. Cambiar la estructura de la consulta requiere otra sesión; no borrar checkpoints ni datos para resolver fallos sin revisar la causa.

## Abrir visualizaciones desde Windows

En una terminal adicional de Ubuntu:

```bash
cd ~/tarea3_taxis
python3 -m http.server 8765 --bind 127.0.0.1 --directory resultados
```

En una nueva pestaña de PowerShell de Windows:

```powershell
ssh -p 2222 -N -L 4041:127.0.0.1:4040 -L 8765:127.0.0.1:8765 kevinsaso@127.0.0.1
```

- Aplicación Spark activa: http://127.0.0.1:4041
- Resultados batch: http://127.0.0.1:8765/batch/ (elegir la ejecución y `informe_batch.html`).
- Sensores: http://127.0.0.1:8765/stream/sensores_demo1/monitor.html

El navegador no debe conectarse al puerto del broker. La interfaz 4040 existe solo mientras la aplicación Spark está activa. Batch y streaming se ejecutan por separado para no competir por el único trabajador.

## Pruebas y estado

```bash
python3 -m unittest discover -s tests -v
```

Las pruebas locales verifican el contrato de los eventos y los gráficos. La prueba del bloqueo flock solo se ejecuta en Linux. La inspección independiente del Parquet no equivale a ejecución de Spark. La integración batch y streaming fue ejecutada en Ubuntu el 27 de octubre de 2026. Las dos ejecuciones batch coinciden con el control local. La reconstrucción de las últimas cuentas por ventana y sensor representa 420 eventos. Se observó un conflicto al iniciar un consumidor adicional sobre el mismo checkpoint; la versión actual incorpora un bloqueo local para impedirlo, con una prueba específica para Ubuntu. Esta protección fue añadida después de esas ejecuciones y aún debe probarse en la VM. Consulte docs/RESULTADOS_VERIFICADOS.md.

## Repositorio

Se publica código, configuración, documentación y el manifiesto; se excluyen datos crudos, dependencias, credenciales, checkpoints y logs. Los archivos originales pueden descargarse con `scripts/descargar_datos.py`. Repositorio: https://github.com/Kevinsaso10/bigdata-tarea3-taxis-spark .

## Fuentes técnicas

- https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html
- https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html
- https://kafka.apache.org/39/getting-started/quickstart/
- https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

El informe grupal debe integrar la investigación sobre Hadoop/Spark, RDD, DataFrame y arquitectura de Kafka. Los enlaces individuales publicados figuran en la sección siguiente y en `docs/ENLACES_ENTREGA.md`.

## Evidencia verificable

En `evidencia_resultados/` se incluyen los agregados batch observados, el control de calidad y una reconstrucción resumida del streaming. Se preservan los logs originales en la copia local. El paquete para repositorio excluye los logs completos y los checkpoints.

## Entrega individual publicada

- [Presentación online de Kevin](https://docs.google.com/presentation/d/1VpXfudRAkVF2FW0mSTkmscIz-o7Vi9PbFloew8x_TrY/edit)
- [Video 1: análisis batch de taxis](https://youtu.be/Tc6o40wohoE)
- [Video 2: Kafka y streaming de sensores](https://youtu.be/5EfPmqjkKPQ)
- [Enlaces y pasos para la entrega](docs/ENLACES_ENTREGA.md)
- [Aporte grupal: RDD y DataFrames](docs/APORTE_RDD_DATAFRAMES.md)

El primer video tiene una mención a Streaming en su título. Kevin confirmó que su contenido corresponde al análisis batch. Las etiquetas de esta documentación identifican cada ejercicio correctamente.
