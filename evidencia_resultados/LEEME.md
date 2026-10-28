# Evidencia de ejecución

Los agregados batch corresponden a 20261027T154200Z_51508b. El resumen de streaming se reconstruyó de las últimas cuentas por ventana y sensor. Los logs originales conservan un intento fallido de consumidor simultáneo; esta reconstrucción no convierte ese intento en una ejecución correcta.

El código actual añade una protección local de exclusividad posterior a las ejecuciones mostradas. La prueba específica se ejecuta en Ubuntu; no se ha ejecutado en Windows.
