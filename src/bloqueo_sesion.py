"""Exclusión mutua local de consumidores por checkpoint (Ubuntu/Linux)."""
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def bloquear_sesion(ruta):
    import fcntl
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("a+", encoding="utf-8") as archivo:
        try:
            fcntl.flock(archivo.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Esta sesión de streaming ya tiene un consumidor activo. Usar su terminal o detenerlo antes de reiniciar.") from None
        try:
            yield
        finally:
            fcntl.flock(archivo.fileno(), fcntl.LOCK_UN)
