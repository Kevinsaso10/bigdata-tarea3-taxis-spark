import random
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from eventos import generar_sensor
from visualizaciones import barras


class ContratoEventos(unittest.TestCase):
    def test_eventos_legibles_con_fecha_utc_y_rangos_del_anexo(self):
        rng = random.Random(125)
        fecha = datetime(2026, 10, 20, tzinfo=timezone.utc)
        for _ in range(100):
            e = generar_sensor(rng, fecha)
            self.assertEqual(datetime.fromisoformat(e["timestamp"]), fecha)
            self.assertTrue(1 <= e["sensor_id"] <= 10)
            self.assertTrue(20 <= e["temperature"] <= 30)
            self.assertTrue(30 <= e["humidity"] <= 70)

    def test_no_acepta_tiempo_ambiguo(self):
        with self.assertRaises(ValueError):
            generar_sensor(random.Random(1), datetime(2026, 10, 20))

    def test_grafico_escapa_etiquetas_y_admite_cero(self):
        svg = barras("Prueba <sensor>", ["<script>"], [0])
        self.assertIn("&lt;script&gt;", svg)
        self.assertNotIn("<script>", svg)


if __name__ == "__main__":
    unittest.main()
