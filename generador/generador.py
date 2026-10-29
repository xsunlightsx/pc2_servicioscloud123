"""
Generador de ventas + API de control.
- Lee data/ventas.csv (el MISMO archivo de siempre) y publica cada venta en RabbitMQ.
- Mensajes persistentes + confirmaciones del publicador (publisher confirms):
  una venta solo cuenta como "enviada confirmada" cuando RabbitMQ la aceptó.
- La web lo controla por HTTP: iniciar, pausa, continuar, cambiar tasa, reenviar 100.
- Pausar detiene SOLO el envío; los procesadores siguen consumiendo lo que ya está en la cola.
"""
import csv
import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path

import pika
from flask import Flask, jsonify, request

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "ventas_user")
RABBIT_PASS = os.getenv("RABBIT_PASS", "ventas_pass")
COLA = os.getenv("COLA", "ventas")
CSV_PATH = Path(os.getenv("CSV_PATH", "data/ventas.csv"))

# Perfil de carga del enunciado: (nombre, cantidad de ventas, ventas por segundo)
PERFIL = [("Carga baja", 300, 3), ("Carga alta", 3000, 15), ("Carga baja (retorno)", 6700, 3)]
TASA_REENVIO = 50          # ventas/seg al reenviar duplicados

lock = threading.Lock()
estado = {
    "estado": "detenido",      # detenido | enviando | pausado | finalizado
    "modo": "manual",          # manual (tasa elegida) | perfil (300@3 -> 3000@15 -> 6700@3)
    "tasa": 3,
    "indice": 0,               # próxima venta original a enviar
    "confirmadas": 0,          # ventas originales confirmadas por RabbitMQ
    "reenviadas": 0,           # duplicados confirmados (actividad G)
    "reenvio_pendiente": 0,
    "reenvio_pos": 0,
    "error": "",
}


def log(msg):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [generador] {msg}", flush=True)


def cargar_ventas():
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return [json.dumps(fila, ensure_ascii=False) for fila in csv.DictReader(f)]


VENTAS = cargar_ventas()


def perfil_actual(indice):
    acumulado = 0
    for nombre, cantidad, tasa in PERFIL:
        acumulado += cantidad
        if indice < acumulado:
            return nombre, tasa
    return PERFIL[-1][0], PERFIL[-1][2]


def tasa_actual(e):
    return perfil_actual(e["indice"])[1] if e["modo"] == "perfil" else e["tasa"]


def snapshot():
    with lock:
        e = dict(estado)
    e["tasa_actual"] = tasa_actual(e)
    e["fase"] = perfil_actual(e["indice"])[0] if e["modo"] == "perfil" else "Manual"
    e["total"] = len(VENTAS)
    return e


def leer_tasa(valor):
    try:
        v = float(valor)
    except (TypeError, ValueError):
        return None
    if not 0 < v <= 100:
        return None
    return int(v) if v.is_integer() else v


# ------------------------------------------------------------------ envío a RabbitMQ
def conectar():
    cred = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, credentials=cred, heartbeat=60))
    ch = conn.channel()
    ch.queue_declare(queue=COLA, durable=True)
    ch.confirm_delivery()          # basic_publish falla si RabbitMQ no confirma
    return conn, ch


def publicar(ch, cuerpo):
    ch.basic_publish(exchange="", routing_key=COLA, body=cuerpo, mandatory=True,
                     properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))


def emisor():
    conn = ch = None
    siguiente = time.monotonic()
    while True:
        try:
            with lock:
                st = estado["estado"]
                reenvio = estado["reenvio_pendiente"]
            if st != "enviando" and reenvio == 0:
                time.sleep(0.1)
                siguiente = time.monotonic()
                continue
            if ch is None or conn.is_closed:
                conn, ch = conectar()

            if st == "enviando":
                with lock:
                    i = estado["indice"]
                    tasa = tasa_actual(estado)
                publicar(ch, VENTAS[i])
                with lock:
                    estado["indice"] = i + 1
                    estado["confirmadas"] += 1
                    estado["error"] = ""
                    if estado["indice"] >= len(VENTAS):
                        estado["estado"] = "finalizado"
                        log("Envío completo: 10 000 ventas confirmadas")
                intervalo = 1.0 / tasa
            else:                                  # reenvío de duplicados
                with lock:
                    pos = estado["reenvio_pos"]
                publicar(ch, VENTAS[pos])
                with lock:
                    estado["reenvio_pos"] = pos + 1
                    estado["reenviadas"] += 1
                    estado["reenvio_pendiente"] -= 1
                    estado["error"] = ""
                intervalo = 1.0 / TASA_REENVIO

            siguiente += intervalo
            espera = siguiente - time.monotonic()
            if espera > 0:
                time.sleep(espera)
            else:
                siguiente = time.monotonic()       # no acumular atraso
        except Exception as e:
            log(f"Error publicando: {e!r}. Reintento en 2 s")
            with lock:
                estado["error"] = repr(e)
            try:
                conn.close()
            except Exception:
                pass
            conn = ch = None
            time.sleep(2)


# ------------------------------------------------------------------ API de control
app = Flask(__name__)


@app.get("/estado")
def r_estado():
    return jsonify(snapshot())


@app.post("/iniciar")
def r_iniciar():
    datos = request.get_json(silent=True) or {}
    modo = "perfil" if datos.get("modo") == "perfil" else "manual"
    tasa = leer_tasa(datos.get("tasa"))
    with lock:
        if estado["estado"] == "pausado":
            estado["estado"] = "enviando"          # Inicio sobre una pausa = continuar
        elif estado["estado"] != "enviando":
            estado.update(indice=0, confirmadas=0, estado="enviando", modo=modo, error="")
            if tasa:
                estado["tasa"] = tasa
    log(f"INICIO modo={modo} tasa={tasa}")
    return jsonify(snapshot())


@app.post("/pausa")
def r_pausa():
    with lock:
        if estado["estado"] == "enviando":
            estado["estado"] = "pausado"
    log("PAUSA (solo se detiene el envío)")
    return jsonify(snapshot())


@app.post("/continuar")
def r_continuar():
    with lock:
        if estado["estado"] == "pausado":
            estado["estado"] = "enviando"
    log("CONTINUAR")
    return jsonify(snapshot())


@app.post("/tasa")
def r_tasa():
    tasa = leer_tasa((request.get_json(silent=True) or {}).get("tasa"))
    if tasa is None:
        return jsonify(error="tasa debe estar entre 0 y 100"), 400
    with lock:
        estado["tasa"] = tasa
        estado["modo"] = "manual"
    log(f"TASA -> {tasa} ventas/s")
    return jsonify(snapshot())


@app.post("/reiniciar")
def r_reiniciar():
    with lock:
        estado.update(estado="detenido", indice=0, confirmadas=0, reenviadas=0,
                      reenvio_pendiente=0, reenvio_pos=0, error="")
    log("REINICIO del generador (no borra la base de datos ni la cola)")
    return jsonify(snapshot())


@app.post("/reenviar")
def r_reenviar():
    n = int((request.get_json(silent=True) or {}).get("n", 100))
    n = max(1, min(n, len(VENTAS)))
    with lock:
        if estado["estado"] == "enviando" or estado["reenvio_pendiente"] > 0:
            return jsonify(error="espera a que termine el envío actual"), 409
        estado.update(reenvio_pos=0, reenvio_pendiente=n)
    log(f"REENVIO de las primeras {n} ventas (duplicados)")
    return jsonify(snapshot())


if __name__ == "__main__":
    threading.Thread(target=emisor, daemon=True).start()
    log(f"Generador listo con {len(VENTAS)} ventas del CSV")
    app.run(host="0.0.0.0", port=5000, threaded=True)