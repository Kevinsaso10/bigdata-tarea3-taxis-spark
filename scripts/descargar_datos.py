"""Descarga reproducible de fuentes oficiales; no sobrescribe datos existentes."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

BASE = Path(__file__).resolve().parents[1]
FUENTES = {
    "yellow_tripdata_2025-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
    "taxi_zone_lookup.csv": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
}


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for bloque in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloque)
    return h.hexdigest()


if __name__ == "__main__":
    carpeta = BASE / "datos/originales"
    carpeta.mkdir(parents=True, exist_ok=True)
    previo_path = BASE / "datos/manifest.json"
    previo = json.loads(previo_path.read_text(encoding="utf-8")) if previo_path.exists() else {}
    documentos = []
    for nombre, url in FUENTES.items():
        destino = carpeta / nombre
        nuevo = not destino.exists()
        if nuevo:
            temporal = destino.with_suffix(destino.suffix + ".part")
            with urlopen(Request(url, headers={"User-Agent": "UNAD-BigData-Tarea3/1.0"}), timeout=60) as origen, temporal.open("wb") as salida:
                for bloque in iter(lambda: origen.read(1024 * 1024), b""):
                    salida.write(bloque)
            temporal.replace(destino)
        huella = sha256(destino)
        anterior = next((x for x in previo.get("archivos", []) if x["nombre"] == nombre), None)
        if anterior and anterior["sha256"] != huella:
            raise SystemExit(f"Cambió el archivo {nombre} respecto al manifiesto. Revisar antes de analizar.")
        documentos.append({"nombre": nombre, "url": url, "bytes": destino.stat().st_size, "sha256": huella,
            "descargado_utc": datetime.now(timezone.utc).isoformat() if nuevo else (anterior or {}).get("descargado_utc"),
            "verificado_utc": datetime.now(timezone.utc).isoformat()})
        print(nombre, destino.stat().st_size, huella)
    manifest = {"fuente": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page", "archivos": documentos,
        "nota": "SHA-256 calculado localmente para reproducibilidad; no constituye una firma oficial de TLC. La fuente puede actualizar archivos."}
    previo_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
