# RDD y DataFrames en Apache Spark

Kevin Obed Tique Villada · Grupo 202016911_125 · Big Data Tarea 3

## RDD y sus propiedades

Un RDD es una colección distribuida y particionada de elementos que Spark puede procesar en paralelo. Es inmutable: una transformación produce otro RDD en vez de modificar el original. Las particiones distribuyen el trabajo entre tareas. Spark conserva un linaje de dependencias para reconstruir particiones perdidas cuando dispone de sus fuentes y recursos. La evaluación es diferida: las transformaciones construyen un plan que se materializa al solicitar una acción (Apache Software Foundation, s. f.-a).

Estas propiedades tienen consecuencias prácticas. La inmutabilidad facilita razonar sobre transformaciones, pero no evita errores en funciones que dependen de estado externo. Un particionamiento desequilibrado puede producir tareas lentas. La recuperación por linaje tampoco convierte una sola máquina en un entorno de alta disponibilidad.

## Transformaciones solicitadas

Las siguientes definiciones sintetizan las operaciones documentadas por Apache Software Foundation (s. f.-a).

| Operación | Función | Ejemplo de uso |
|---|---|---|
| `map` | Aplica una función a cada elemento y crea otro RDD. | Convertir unidades o construir pares clave y valor. |
| `filter` | Conserva elementos que cumplen una condición. | Seleccionar distancias positivas. |
| `flatMap` | Puede producir cero, uno o varios elementos por entrada. | Separar una línea en palabras. |
| `groupByKey` | Reúne los valores asociados a cada clave. | Examinar los elementos de un grupo cuando se requieren todos. |
| `reduceByKey` | Combina los valores de cada clave con una función asociativa y conmutativa. | Sumar viajes por zona o calcular conteos. |

Si solo se necesita una suma o un conteo, `reduceByKey` permite combinar datos antes del intercambio entre particiones. `groupByKey` puede trasladar más información y concentrar grupos grandes en una tarea. Su elección debe responder a la operación requerida y al tamaño de las claves.

## Acciones solicitadas

| Operación | Resultado | Precaución |
|---|---|---|
| `collect` | Devuelve todos los elementos al controlador. | Usar únicamente cuando el resultado cabe en la memoria del controlador. |
| `count` | Devuelve el número de elementos. | Requiere evaluar las particiones implicadas. |
| `take(n)` | Devuelve hasta `n` elementos. | No equivale a una muestra aleatoria representativa. |
| `reduce` | Combina los elementos mediante una función asociativa y conmutativa. | Debe definirse qué hacer si el RDD está vacío. |

Ejemplo didáctico con una colección pequeña, separado del análisis de millones de viajes:

```python
rdd = spark.sparkContext.parallelize([("zona_A", 1), ("zona_B", 1), ("zona_A", 1)], 2)
conteos = rdd.reduceByKey(lambda a, b: a + b)
print(conteos.collect())  # Resultado acotado a dos claves.

numeros = spark.sparkContext.parallelize([1, 2, 3, 4], 2)
print(numeros.map(lambda x: x * 2).filter(lambda x: x > 4).take(2))
print(numeros.count())
print(numeros.reduce(lambda a, b: a + b))
print(spark.sparkContext.parallelize(["datos en lote", "datos en flujo"])
      .flatMap(lambda linea: linea.split()).take(6))
```

Los ejemplos ilustran la API y no se presentan como evidencia de ejecución del proyecto individual.

## DataFrames y relación con los RDD

Un DataFrame organiza datos distribuidos en columnas con nombres y un esquema. Su API permite describir selecciones, filtros, uniones y agregaciones. La estructura de las columnas y de las operaciones proporciona a Spark SQL información que puede utilizar para optimizar el plan. En PySpark, el DataFrame es la interfaz estructurada disponible; la API tipada de Dataset corresponde a Scala y Java (Apache Software Foundation, s. f.-b).

| Aspecto | RDD | DataFrame |
|---|---|---|
| Organización | Colección distribuida de elementos. | Datos distribuidos con columnas y esquema. |
| Expresión del cálculo | Funciones sobre elementos o pares clave y valor. | Expresiones de columnas y operaciones relacionales. |
| Información para optimizar | La lógica de funciones generales puede ser opaca para el optimizador SQL. | Spark conoce estructura y operaciones, y puede optimizar el plan. |
| Control | Adecuado cuando se requiere lógica de bajo nivel. | Adecuado para filtrado, agrupaciones, uniones y SQL sobre datos estructurados. |
| Rendimiento | Depende del algoritmo, particionado e intercambios de datos. | Las expresiones nativas facilitan optimizaciones; no garantizan una mejora fija en cualquier caso. |

## Aplicación al proyecto de taxis

Elegí DataFrames para leer el Parquet, aplicar condiciones de calidad, agrupar por hora, día y zona y unir el catálogo pequeño de ubicaciones. El archivo contiene millones de filas, pero los resultados enviados al controlador están acotados a 24 horas, 31 días y diez zonas. La selección de esta API responde a la estructura tabular del problema; no se comparó su rendimiento contra una implementación equivalente con RDD.

```python
from pyspark.sql import functions as F

por_hora = viajes_validos.groupBy(
    F.hour("tpep_pickup_datetime").alias("hora")
).agg(F.count("*").alias("viajes")).orderBy("hora")
```

## Conclusión individual del aporte

La elección entre RDD y DataFrame debe considerar qué operación se necesita expresar y qué información puede aprovechar Spark para ejecutarla. En mi proyecto, las operaciones relacionales sobre un archivo Parquet hacen adecuado el uso de DataFrames. El control de calidad, el volumen que llega al controlador y la interpretación de los resultados son tan relevantes como la elección de la API.

## Referencias

Apache Software Foundation. (s. f.-a). *RDD programming guide*. Apache Spark 3.5.8. https://spark.apache.org/docs/3.5.8/rdd-programming-guide.html

Apache Software Foundation. (s. f.-b). *Spark SQL, DataFrames and Datasets guide*. Apache Spark 3.5.8. https://spark.apache.org/docs/3.5.8/sql-programming-guide.html
