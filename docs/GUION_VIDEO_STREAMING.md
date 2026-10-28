# Video individual de Kafka y Spark Structured Streaming

Kevin Obed Tique Villada · Grupo 202016911_125

Duración objetivo: 4 minutos y 30 segundos. Demostrar la ejecución real de productor, topic, consumidor y monitor. Este componente desarrolla los sensores simulados del Anexo 3 y es independiente del histórico de taxis.

## Preparación

Conservar una terminal por proceso: consumidor, productor y servidor HTTP. Cerrar cualquier consumidor previo desde su propia terminal antes de iniciar otro. Nunca abrir dos consumidores con `sensores_demo1` a la vez. Si no se sabe cuál sigue activo, revisar:

```bash
pgrep -af '[s]park_streaming_consumer.py'
```

No borrar el checkpoint para resolver un fallo. Kafka y ZooKeeper pueden permanecer activos. Verificar el topic con `bash scripts/control_kafka.sh estado`. El productor genera eventos sintéticos a la hora UTC actual.

Mantener un túnel a los puertos 4040 y 8765 de Ubuntu. Las direcciones de Windows que usamos son http://127.0.0.1:4042 y http://127.0.0.1:8766/stream/sensores_demo1/monitor.html.

## 0:00 a 0:25 Presentación

Decir: «Mi nombre es Kevin Obed Tique Villada, del grupo 202016911_125. En esta práctica del Anexo 3 simulo sensores de temperatura y humedad, envío sus eventos a Kafka y los proceso con Spark Structured Streaming.»

## 0:25 a 1:00 Topic e infraestructura

Mostrar:

```bash
cd ~/tarea3_taxis
bash scripts/control_kafka.sh estado
```

Decir: «Utilizo Kafka 3.9.2 con ZooKeeper, Spark 3.5.8 y el topic sensor_data_125. Hay una partición y replicación uno. Kafka conserva eventos con offsets, que permiten identificar su posición dentro de la partición. Este laboratorio de un broker no tolera la pérdida del equipo.»

## 1:00 a 1:45 Código

Mostrar `src/kafka_producer.py` y `src/spark_streaming_consumer.py` en el editor.

Decir: «El productor genera diez identificadores de sensor, temperatura y humedad en rangos definidos, y una fecha ISO con zona UTC. Espera la confirmación de Kafka antes de mostrar el offset. El consumidor interpreta el JSON y valida los campos. Agrupa por sensor en ventanas de un minuto, calcula el conteo y los promedios, y usa una marca de agua de dos minutos para gestionar eventos atrasados. El checkpoint guarda el avance de la consulta.»

## 1:45 a 3:10 Ejecución

Iniciar un solo consumidor, después de comprobar que no exista otro activo:

```bash
bash scripts/ejecutar_streaming.sh sensores_demo1
```

En otra terminal, cuando aparezca `CONSUMIDOR INICIADO`:

```bash
cd ~/tarea3_taxis
.venv/bin/python src/kafka_producer.py --eventos 120 --intervalo 1
```

Decir: «Aquí se ve el evento enviado con su partición y offset. En esta otra terminal aparecen los microbatches procesados por Spark y las ventanas que reciben actualizaciones. El número de eventos es el acumulado del sensor dentro de su ventana.»

Mostrar al menos un microbatch con datos nuevos. Si no aparecen datos, revisar el proceso y no presentar como actual una captura anterior.

## 3:10 a 4:00 Visualización

Abrir el monitor y mostrar cómo cambia el microbatch. Abrir después Jobs o Executors del panel Spark mientras el consumidor siga activo.

Decir: «El monitor lee el último archivo de resultados y se actualiza cada tres segundos. Muestra las ventanas modificadas en el último microbatch, no todo el historial. Las fechas son UTC, la temperatura se expresa en grados Celsius y la humedad en porcentaje. En el panel de Spark se observan los trabajos y un ejecutor de dos núcleos, además del driver.»

## 4:00 a 4:35 Conclusión

Decir: «La práctica me permitió comprobar el recorrido de un evento desde el productor, su almacenamiento en Kafka y el procesamiento incremental con Spark hasta su visualización. Aprendí también que un checkpoint pertenece a una sola consulta activa: ejecutar dos sobre el mismo directorio provoca conflicto. Los datos son simulados y una sola máquina no demuestra alta disponibilidad ni garantías generales de procesamiento exactamente una vez.»

## Al terminar

Guardar como `Tarea3_Kevin_Streaming.mp4`. Comprobar audio y duración menor a cinco minutos, publicar y verificar acceso sin sesión. Guardar la URL en `docs/ENLACES_ENTREGA.md`. Detener el consumidor desde su terminal con Ctrl+C cuando se hayan obtenido las evidencias.
