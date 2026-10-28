# Presentación individual de Kevin Obed Tique Villada

Grupo 202016911_125. Big Data, Tarea 3.

Presentación terminada y revisada: https://docs.google.com/presentation/d/1VpXfudRAkVF2FW0mSTkmscIz-o7Vi9PbFloew8x_TrY/edit

## Diapositiva 1

Procesamiento de datos
con Apache Spark

Taxis de Nueva York y sensores simulados con Kafka

Kevin Obed Tique Villada
Grupo 202016911_125 · UNAD · ECBTI
28 de octubre de 2026

## Diapositiva 2

PROBLEMA Y FUENTES

¿Cuándo y dónde se concentran las recogidas?

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

2

Estudiar los viajes observados de taxis amarillos de Nueva York.

Usar registros TLC de enero de 2025 y el catálogo oficial de zonas.

Identificar los horarios, días y zonas con más recogidas.

Procesar millones de filas con DataFrames y Spark SQL.

Desarrollar, por separado, el ejercicio de sensores del Anexo 3.

Fuente: NYC TLC, Trip Record Data. Alcance: viajes registrados, sin estimar demanda insatisfecha.

## Diapositiva 3

MÉTODO BATCH

Del archivo original a los agregados

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

3

Conservar el Parquet original y registrar su huella SHA-256.

Leer el esquema y aplicar reglas explícitas de calidad.

Filtrar enero de 2025, duraciones y distancias operativas.

Agrupar por hora, día y zona. Unir el catálogo de zonas.

Guardar CSV, resumen JSON y gráficos de agregados pequeños.

Entorno: Ubuntu 22.04, Java 11 y Spark 3.5.8. Un worker con dos núcleos.

## Diapositiva 4

CALIDAD DE LOS DATOS

El filtro conserva 3.225.495 viajes

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

4

3.475.226

Filas originales

Parquet TLC de enero de 2025

3.225.495

Filas aceptadas

Cumplen las reglas del estudio

249.731

Filas excluidas

Diferencia entre originales y aceptadas

7,19 %

Proporción excluida

Los motivos de exclusión se solapan

Criterios operativos propios. No son umbrales oficiales de TLC. Fuente: resumen.json y control local.

## Diapositiva 5

RESULTADOS POR HORA

Las 18:00 reúnen más recogidas

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

5

Enero de 2025. Suma mensual por hora local del archivo TLC. Fuente: viajes_por_hora.csv.

0 mil

100 mil

200 mil

0

3

6

9

12

15

18

21

23

Viajes aceptados

Hora de recogida

## Diapositiva 6

RESULTADOS POR DÍA

El 25 de enero registra 128.167 viajes

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

6

Viajes aceptados por fecha de recogida. Fuente: viajes_por_dia.csv.

0 mil

50 mil

100 mil

150 mil

1

5

10

15

20

25

30

31

Viajes aceptados

Día de enero

## Diapositiva 7

DIEZ ZONAS PRINCIPALES

Midtown Center lidera las recogidas

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

7

Número de viajes aceptados por zona de origen. Fuente: zonas_principales.csv.

Midtown Center

160.642

Upper East Side South

157.417

Upper East Side North

149.183

JFK Airport

128.996

Times Sq / Theatre District

117.643

Penn Station / Madison Sq W.

113.471

Midtown East

112.125

Lincoln Square East

105.621

Upper West Side South

91.572

Midtown North

91.205

## Diapositiva 8

INTERPRETACIÓN

Las medias monetarias requieren cautela

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

8

Promedios de los viajes aceptados

Distancia: 3,12 millas

Duración: 14,61 minutos

Importe total: USD 26,77

Un valor extremo permanece

Total máximo: USD 863.380,37

Supera lo esperable y afecta las medias.

La regla actual no impone un máximo monetario.

El dato requiere revisión. El importe total no equivale al ingreso neto del conductor.

## Diapositiva 9

Sensores simulados con Kafka y Spark

El productor genera temperatura y humedad para diez sensores.

Kafka conserva los eventos en el topic sensor_data_125.

Spark agrupa por sensor y ventana de un minuto. El watermark es de dos minutos y el disparador, de cinco segundos.

La consulta guarda actualizaciones JSON y conserva su checkpoint.

Kevin Obed Tique Villada · Grupo 202016911_125 · Tarea 3

## Diapositiva 10

EVIDENCIA DE STREAMING

El monitor muestra actualizaciones por ventana

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

10

Captura de la ejecución. Datos sintéticos. Temperatura en °C y humedad relativa en %.

## Diapositiva 11

Resultados e incidencias de la ejecución

Batch: los conteos de Spark coinciden con el control local independiente.

Streaming: 86 archivos con datos, 85 pares de ventana y sensor, y 420 eventos representados. Se toma la última actualización de cada par.

Un segundo consumidor sobre el mismo checkpoint produjo un conflicto. La consulta original continuó.

La ejecución usa una máquina virtual. Sus tiempos no constituyen una comparación de rendimiento distribuido.

Fuente: salidas guardadas, logs y capturas de Spark. Protección nueva de sesión: prueba en Ubuntu pendiente.

## Diapositiva 12

CIERRE INDIVIDUAL

Conclusiones de mi práctica

Kevin Obed Tique Villada · Big Data · Grupo 202016911_125 · Tarea 3

12

Los DataFrames permiten expresar filtros y agregaciones verificables.

La limpieza cambia el conjunto analizado y debe quedar documentada.

Los agregados identifican concentración de viajes observados.

Kafka y Spark actualizan los resultados de las ventanas de sensores.

Las evidencias y los límites del estudio sustentan las conclusiones.

## Diapositiva 13

Repositorio y videos

Código y resultados
https://github.com/Kevinsaso10/bigdata-tarea3-taxis-spark

Video 1: análisis batch de taxis
https://youtu.be/Tc6o40wohoE

Video 2: Kafka y streaming
https://youtu.be/5EfPmqjkKPQ

## Diapositiva 14

Referencias

Apache Software Foundation. (s. f.). Spark SQL, DataFrames and Datasets guide. Apache Spark 3.5.8.
https://spark.apache.org/docs/3.5.8/sql-programming-guide.html

Apache Software Foundation. (s. f.). Structured Streaming programming guide. Apache Spark 3.5.8.
https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html

Apache Software Foundation. (s. f.). Structured Streaming and Kafka integration guide. Apache Spark 3.5.8.
https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html

Apache Software Foundation. (s. f.). Quick start. Apache Kafka 3.9.
https://kafka.apache.org/39/getting-started/quickstart/

New York City Taxi and Limousine Commission. (s. f.). TLC trip record data [Conjunto de datos].
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Kevin Obed Tique Villada · Grupo 202016911_125 · Tarea 3

Los gráficos son objetos nativos editables y derivan de los CSV de evidencia_resultados. La diapositiva del monitor usa la captura propia 13_monitor_streaming.png con su proporción original. Las fuentes y los límites también están documentados en las notas de cada diapositiva.
