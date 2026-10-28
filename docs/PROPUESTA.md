# Propuesta del proyecto individual

Autor: Kevin Obed Tique Villada. Grupo 202016911_125.

## Pregunta

¿En qué horarios y zonas se concentran los viajes registrados de taxis amarillos de Nueva York durante enero de 2025 y cómo se caracterizan sus distancias, duraciones e importes?

## Objetivo general

Analizar los registros de viajes con Apache Spark para describir patrones temporales y geográficos, aplicando reglas de calidad explícitas y visualizando los agregados resultantes.

## Objetivos específicos

1. Obtener el archivo oficial y registrar fuente, tamaño, esquema y huella SHA-256.
2. Identificar filas fuera del periodo o de los criterios operativos definidos, sin modificar el original.
3. Calcular viajes por hora, por día y por zona de recogida, además de promedios de duración, distancia y valores cobrados.
4. Generar tablas y gráficos reproducibles y explicar su alcance.
5. Desarrollar el Anexo 3 mediante un productor de sensores, un topic Kafka y una aplicación de Structured Streaming que agregue eventos por ventanas temporales.

## Conjunto seleccionado

Fuente: NYC Taxi and Limousine Commission. Archivo `yellow_tripdata_2025-01.parquet`; 3.475.226 filas en la copia descargada el 20 de octubre de 2026. Se utiliza además el catálogo de zonas de la misma fuente. Enero permite trabajar con un mes completo sin mezclar cambios de esquema entre años. No se promete medir la operación de toda la ciudad ni incluir otros tipos de transporte.

## Calidad y decisiones

Las reglas operativas están en `config/calidad.json`. Excluir filas con distancia o importes no positivos, o duraciones fuera del umbral, delimita el subconjunto estudiado: no demuestra que todas las filas excluidas sean errores del proveedor. Algunos importes negativos pueden corresponder a correcciones. Se informa el efecto de cada regla y la unión de exclusiones, evitando sumar causas que se solapan. No se eliminan supuestos duplicados sin un identificador inequívoco.

## Productos

Código batch; archivos de calidad y resumen; tablas por hora, día y zona; gráficos SVG e informe HTML; productor y consumidor streaming; monitor HTML de ventanas de sensores; README y pruebas; repositorio público; video o videos según aclaración del tutor; presentación individual. Los enlaces y resultados de Spark se incorporan después de ejecutarlos y verificarlos.

## Estado inicial

Datos descargados y esquema verificado. Código preparado para el entorno existente. La inspección independiente local no acredita una ejecución de Spark. Kafka y Spark deben probarse dentro de Ubuntu y documentarse con capturas reales.

## Referencia

New York City Taxi and Limousine Commission. (s. f.). TLC trip record data [Conjunto de datos]. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
