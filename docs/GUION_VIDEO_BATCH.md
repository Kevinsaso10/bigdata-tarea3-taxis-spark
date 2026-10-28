# Video individual de procesamiento batch

Kevin Obed Tique Villada · Grupo 202016911_125

Duración objetivo: 4 minutos y 30 segundos. Pantalla y voz, con código y resultados legibles. Realizar una prueba corta del micrófono antes de grabar. Este guion corresponde al análisis batch; el segundo video demuestra Kafka y streaming.

## Preparación

Dejar el código `src/batch_taxis.py` abierto en un editor. Si no hay editor disponible, usar `less -N src/batch_taxis.py` y salir con `q`. Preparar el gráfico HTML y una terminal conectada a Ubuntu. Mantener libre el trabajador de Spark: detener el consumidor de streaming con Ctrl+C en su propia terminal antes de ejecutar batch. No iniciar dos aplicaciones para competir por los mismos dos núcleos.

El túnel del panel, desde PowerShell de Windows, puede abrirse con:

```powershell
ssh -p 2222 -N -L 4042:127.0.0.1:4040 -L 8766:127.0.0.1:8765 kevinsaso@127.0.0.1
```

Usar el túnel que ya esté funcionando. Si indica puerto ocupado, localizar la pestaña anterior; no abrir copias innecesarias. Para servir gráficos, ejecutar en otra terminal de Ubuntu `python3 -m http.server 8765 --bind 127.0.0.1 --directory resultados` desde `~/tarea3_taxis`.

## 0:00 a 0:25 Presentación y problema

Mostrar la terminal o la portada de la presentación.

Decir: «Mi nombre es Kevin Obed Tique Villada, del grupo 202016911_125. Analicé viajes de taxis amarillos de Nueva York de enero de 2025 para identificar qué horarios y zonas concentran más recogidas y describir la duración, distancia e importes de los viajes.»

## 0:25 a 1:15 Fuente y código

Mostrar `docs/PROPUESTA.md`, la fuente oficial y las secciones de `batch_taxis.py` donde se leen el Parquet y las reglas.

Decir: «La fuente es la NYC Taxi and Limousine Commission. El archivo original contiene 3.475.226 filas. Utilicé Spark 3.5.8 con DataFrames, que permiten expresar filtros, agrupaciones y agregaciones mediante un esquema. Conservo el original y su SHA-256 para comprobar que el análisis se reproduce sobre el mismo archivo.»

Señalar en el código `spark.read.parquet`, `condiciones`, `where`, `groupBy`, `agg` y el `broadcast` del pequeño catálogo de zonas.

Decir: «Estas son mis reglas operativas: recogidas dentro de enero, fechas válidas, duración y distancia positivas dentro de límites documentados, importes positivos y zonas identificadas. Las causas de exclusión pueden solaparse. No elimino posibles duplicados sin un identificador único.»

## 1:15 a 2:20 Ejecución real

En la terminal:

```bash
cd ~/tarea3_taxis
bash scripts/ejecutar_batch.sh 600
```

Mientras trabaja, señalar las transformaciones en el editor. La ejecución previa tardó 36,274 segundos y la primera 42,935; una nueva puede variar. No presentar esos tiempos como benchmark entre herramientas.

Decir: «La lectura y las transformaciones se ejecutan sobre los viajes en Spark. Solo traigo al controlador los agregados pequeños: 24 horas, 31 días y diez zonas. Las tablas, gráficos y el resumen se guardan en una carpeta distinta por ejecución.»

## 2:20 a 3:35 Resultados y gráficos

Mostrar la finalización correcta y abrir el HTML de esa misma ejecución en el navegador. Si se muestran gráficos de otra ejecución, identificarla y aclarar que usa la misma fuente y las mismas reglas; lo preferible es mostrar la nueva.

Decir: «Se aceptaron 3.225.495 viajes y se excluyeron 249.731. La hora con más recogidas fue de las 18 a las 18:59, con 234.630 viajes. El día con más registros fue el 25 de enero, con 128.167, y Midtown Center encabezó las zonas, con 160.642 recogidas. Son totales del mes en este subconjunto, no promedios diarios.»

Mostrar los gráficos por hora, día y zona, sin leer todas las barras.

## 3:35 a 4:10 Calidad y verificación

Mostrar `09d_control_batch.png` o ejecutar el contraste en otra terminal, indicando la carpeta de la nueva ejecución:

```bash
ultima=$(ls -td resultados/batch/*/ | head -n 1)
python3 scripts/verificar_resultado.py "${ultima}resumen.json"
```

Decir: «Los conteos coinciden con una inspección independiente del archivo. Encontré además un importe máximo de 863.380,37 dólares en un viaje de 1,6 millas. Está en el original y merece revisión; no lo corregí arbitrariamente. Por esa razón, el promedio monetario debe interpretarse con cautela.»

## 4:10 a 4:40 Conclusión

Mostrar brevemente Jobs o Executors en http://127.0.0.1:4042 si está abierto el túnel.

Decir: «El análisis describe patrones de viajes observados y demuestra el uso de DataFrames para procesar un archivo de varios millones de registros. Sus límites son un solo mes, taxis amarillos y una sola máquina virtual. Los conteos no miden demanda insatisfecha y este ejercicio no prueba rendimiento de un clúster multinodo. El repositorio incluye el código y las instrucciones para reproducirlo.»

## Al terminar

Guardar como `Tarea3_Kevin_Batch.mp4`. Verificar audio, legibilidad y duración menor a cinco minutos. Publicar el video y comprobar el acceso sin sesión. Registrar la URL en `docs/ENLACES_ENTREGA.md` y en la presentación.
