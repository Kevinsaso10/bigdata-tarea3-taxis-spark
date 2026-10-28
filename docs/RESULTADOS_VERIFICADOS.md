# Resultados verificados de la Tarea 3

Kevin Obed Tique Villada · Grupo 202016911_125

## Batch

Se recibieron dos ejecuciones reales de Spark 3.5.8 en Ubuntu. Ambas finalizaron correctamente y sus conteos coinciden con el control independiente del mismo archivo.

| Ejecución | Segundos de procesamiento |
|---|---:|
| 20261027T152558Z_ca7c8b | 42.935 |
| 20261027T154200Z_51508b | 36.274 |

Los tiempos excluyen la pausa para capturas. No son un benchmark contra otro motor ni prueban escalabilidad multinodo.

- Filas originales: 3,475,226.
- Filas aceptadas: 3,225,495.
- Filas excluidas: 249,731 (7.19 %).
- Hora con más viajes: 18:00 a 18:59, 234.630 viajes acumulados durante enero.
- Día con más viajes: 25 de enero de 2025, 128.167 viajes.
- Zona con más recogidas: Midtown Center, 160.642 viajes.
- Distancia media: 3,12 millas; duración media: 14,61 minutos.
- Tarifa base media: 17,95 USD; importe total medio: 26,77 USD.

Las horas y las zonas describen viajes observados, no demanda insatisfecha. La tarifa base y el total no equivalen a ingresos netos del conductor.

## Valor atípico

El original contiene un viaje de 1,6 millas con tarifa base de 863.372,12 USD y total de 863.380,37 USD, recogido el 20 de enero a las 12:07:18 y terminado a las 12:12:42. Cumple las reglas programadas, que no imponen un tope monetario. Se conserva y se documenta como posible anomalía; no se corrige arbitrariamente.

La revisión local independiente de los importes aceptados obtuvo mediana de 20,33 USD y percentiles 95 y 99 de 73,55 y 100,31 USD. Estos percentiles no fueron calculados por el script batch Spark y no deben presentarse como salidas de esa ejecución.

## Streaming

Se recibieron 86 archivos con actualizaciones y se observaron 85 pares de ventana y sensor. Tomando únicamente la última cuenta de cada par, las salidas representan 420 eventos sintéticos. El último batch con datos guardado es 87.

No deben sumarse los conteos de cada microbatch, porque contienen actualizaciones de ventanas ya existentes. Las evidencias del panel muestran el driver y un ejecutor de dos núcleos, con cero tareas fallidas en esa captura.

## Incidente y corrección

Un segundo consumidor intentó usar el checkpoint `sensores_demo1` mientras la consulta original seguía activa. El log `stream_20261027_162957.log` conserva el error `SparkConcurrentModificationException`. La primera consulta siguió guardando resultados. No debe describirse este segundo intento como exitoso.

La versión preparada para publicar añade un bloqueo por sesión para evitar consumidores simultáneos. Debe copiarse a Ubuntu y ejecutar allí la prueba de exclusividad antes de considerarla validada. Las reglas analíticas y los agregados batch no cambian.

## Conclusión individual

La práctica me permitió relacionar la calidad del dato con la interpretación de los resultados. Con Spark procesé un histórico de varios millones de viajes y obtuve agregados temporales y geográficos acotados; con Kafka observé la llegada de eventos y el procesamiento incremental en ventanas. El valor atípico y el conflicto de checkpoint muestran la importancia de conservar evidencias, revisar las reglas y controlar las ejecuciones. Una máquina virtual permite demostrar el flujo, con un alcance distinto al de un despliegue de alta disponibilidad.
