"""Contrasta una ejecución Spark con el control local independiente del Parquet."""
import argparse
import json
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("resumen", help="Ruta al resumen.json de una ejecución Spark")
    args = p.parse_args()
    control = json.loads((BASE / "datos/control_local.json").read_text(encoding="utf-8"))
    resultado = json.loads(Path(args.resumen).read_text(encoding="utf-8"))
    if resultado["sha256"] != control["sha256"]:
        raise SystemExit("El archivo de entrada no corresponde a la copia del control")
    for clave in control["calidad"]:
        esperado, obtenido = control["calidad"][clave], resultado["calidad"][clave]
        if esperado != obtenido:
            raise SystemExit(f"Diferencia en {clave}: control={esperado}, Spark={obtenido}")
    print("CONTROL CORRECTO: las cuentas de Spark coinciden con la inspección local independiente.")
