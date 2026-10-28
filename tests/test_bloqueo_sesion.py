import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bloqueo_sesion import bloquear_sesion


@unittest.skipUnless(sys.platform.startswith("linux"), "El bloqueo usa flock y se prueba en Ubuntu")
class ExclusividadCheckpoint(unittest.TestCase):
    def test_rechaza_segundo_consumidor_y_libera_al_salir(self):
        with tempfile.TemporaryDirectory() as d:
            ruta = Path(d) / "sesion.lock"
            with bloquear_sesion(ruta):
                with self.assertRaises(RuntimeError):
                    with bloquear_sesion(ruta):
                        self.fail("Se aceptaron dos consumidores sobre la misma sesión")
            with bloquear_sesion(ruta):
                pass


if __name__ == "__main__":
    unittest.main()
