"""Gráficos SVG autónomos para agregados pequeños calculados por Spark."""
import html
import json
from pathlib import Path


def barras(titulo, etiquetas, valores, unidad="Viajes"):
    ancho, alto = 1000, max(240, 70 + 30 * len(valores))
    maximo = max(valores, default=1) or 1
    partes = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {ancho} {alto}" role="img" aria-label="{html.escape(titulo)}">',
              '<rect width="100%" height="100%" fill="white"/>',
              f'<text x="15" y="28" font-size="21" font-family="Arial">{html.escape(titulo)}</text>']
    for i, (nombre, valor) in enumerate(zip(etiquetas, valores)):
        y = 50 + i * 30
        nombre = str(nombre)
        partes += [f'<text x="15" y="{y+16}" font-size="13" font-family="Arial">{html.escape(nombre[:38])}</text>',
                   f'<rect x="305" y="{y}" width="{580*valor/maximo:.2f}" height="21" fill="#226b9c"/>',
                   f'<text x="{315+580*valor/maximo:.2f}" y="{y+16}" font-size="12" font-family="Arial">{valor:,.0f}</text>']
    partes.append(f'<text x="15" y="{alto-8}" font-size="12" font-family="Arial">{html.escape(unidad)}</text></svg>')
    return "\n".join(partes)


def crear_informe(destino, tablas, resumen):
    destino = Path(destino)
    graficos = [
        ("viajes_por_hora", "Viajes por hora de recogida", "hora"),
        ("viajes_por_dia", "Viajes por día de enero de 2025", "dia"),
        ("zonas_principales", "Diez zonas con más recogidas", "zona"),
    ]
    bloques = []
    for nombre, titulo, etiqueta in graficos:
        datos = tablas[nombre]
        svg = barras(titulo, [r[etiqueta] for r in datos], [r["viajes"] for r in datos])
        (destino / f"{nombre}.svg").write_text(svg, encoding="utf-8")
        bloques.append(f"<section><h2>{html.escape(titulo)}</h2>{svg}</section>")
    contenido = f'''<!doctype html><html lang="es"><meta charset="utf-8">
<title>Análisis de viajes de taxi con Spark</title>
<style>body{{font:17px Arial;max-width:1100px;margin:35px auto;padding:0 20px;color:#182c3d}}svg{{width:100%;height:auto}}section{{margin:45px 0}}pre{{white-space:pre-wrap;background:#f3f6f8;padding:20px}}h1{{font-size:30px}}</style>
<h1>Viajes de taxi de Nueva York en enero de 2025</h1>
<p>Kevin Obed Tique Villada · Grupo 202016911_125 · Big Data Tarea 3</p>
<p>Agregados de viajes observados tras las reglas de calidad. Las horas corresponden a las marcas locales del archivo TLC. Los importes están en USD y las distancias en millas.</p>
{''.join(bloques)}
<h2>Control de calidad y trazabilidad</h2><pre>{html.escape(json.dumps(resumen,ensure_ascii=False,indent=2,default=str))}</pre>
<p>Alcance: taxis amarillos, un mes y una máquina virtual. Las exclusiones son decisiones analíticas documentadas, no una declaración de que todos esos registros sean erróneos. No se mide demanda insatisfecha ni se infiere causalidad.</p>
</html>'''
    (destino / "informe_batch.html").write_text(contenido, encoding="utf-8")
