"""
Página web de control y monitoreo.
- Controla el generador (Inicio, Pausa, Continuar, tasa, reenviar 100) por su Service interno.
- Consulta RabbitMQ (API de gestión): mensajes listos, sin confirmar y consumidores.
- Consulta PostgreSQL: ventas únicas procesadas y totales por sucursal/categoría.
"""
import json
import os
from pathlib import Path

import psycopg2
import requests
from flask import Flask, jsonify, request

GENERADOR = os.getenv("GENERADOR_URL", "http://generador:5000")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBIT_USER", "ventas_user")
RABBIT_PASS = os.getenv("RABBIT_PASS", "ventas_pass")
COLA = os.getenv("COLA", "ventas")
DB = dict(host=os.getenv("DB_HOST", "postgres"), port=int(os.getenv("DB_PORT", "5432")),
          dbname=os.getenv("DB_NAME", "ventasdb"), user=os.getenv("DB_USER", "ventas_user"),
          password=os.getenv("DB_PASS", "ventas_pass"), connect_timeout=3)

REF_PATH = Path(os.getenv("REF_PATH", "referencia.json"))
REFERENCIA = json.loads(REF_PATH.read_text(encoding="utf-8")) if REF_PATH.exists() else None

app = Flask(__name__, static_folder="static", static_url_path="")


@app.get("/")
def index():
    return app.send_static_file("index.html")


def consultar_generador():
    return requests.get(f"{GENERADOR}/estado", timeout=2).json()


def consultar_rabbit():
    url = f"http://{RABBIT_HOST}:15672/api/queues/%2F/{COLA}"
    q = requests.get(url, auth=(RABBIT_USER, RABBIT_PASS), timeout=2).json()
    return {"listos": q.get("messages_ready", 0),
            "sin_confirmar": q.get("messages_unacknowledged", 0),
            "consumidores": q.get("consumers", 0)}


def consultar_db():
    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), COALESCE(sum(importe_centimos), 0) FROM ventas")
            unicas, total = cur.fetchone()
            cur.execute("SELECT sucursal, sum(importe_centimos) FROM ventas GROUP BY 1 ORDER BY 1")
            sucursal = {k: int(v) for k, v in cur.fetchall()}
            cur.execute("SELECT categoria, sum(importe_centimos) FROM ventas GROUP BY 1 ORDER BY 1")
            categoria = {k: int(v) for k, v in cur.fetchall()}
            cur.execute("SELECT procesador, count(*) FROM ventas GROUP BY 1 ORDER BY 1")
            procesadores = {k: int(v) for k, v in cur.fetchall()}
        return {"unicas": int(unicas), "total": int(total), "sucursal": sucursal,
                "categoria": categoria, "procesadores": procesadores}
    finally:
        conn.close()


@app.get("/api/estado")
def estado():
    respuesta = {"referencia": REFERENCIA}
    for clave, funcion in (("generador", consultar_generador), ("rabbit", consultar_rabbit),
                           ("db", consultar_db)):
        try:
            respuesta[clave] = funcion()
        except Exception as e:                      # un servicio caído no tumba la página
            respuesta[clave] = {"error": repr(e)}
    return jsonify(respuesta)


@app.post("/api/accion/<nombre>")
def accion(nombre):
    if nombre not in {"iniciar", "pausa", "continuar", "tasa", "reiniciar", "reenviar"}:
        return jsonify(error="acción desconocida"), 404
    try:
        r = requests.post(f"{GENERADOR}/{nombre}", json=request.get_json(silent=True) or {}, timeout=3)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify(error=repr(e)), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
