"""
Procesador de ventas.
- Toma UNA tarea a la vez de RabbitMQ (prefetch = 1).
- Simula una validación externa de 0,2 s por venta.
- Guarda la venta en PostgreSQL (INSERT ... ON CONFLICT DO NOTHING => idempotente).
- Solo DESPUÉS de guardar (commit) confirma el mensaje (ack manual).
  Si el pod muere antes del ack, RabbitMQ reentrega la tarea; si ya estaba guardada,
  el ON CONFLICT evita duplicarla.
"""
import json
import os
import signal
import socket
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pika
import psycopg2

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "ventas_user")
RABBIT_PASS = os.getenv("RABBIT_PASS", "ventas_pass")
COLA = os.getenv("COLA", "ventas")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "ventasdb")
DB_USER = os.getenv("DB_USER", "ventas_user")
DB_PASS = os.getenv("DB_PASS", "ventas_pass")

VALIDACION_SEG = float(os.getenv("VALIDACION_SEG", "0.2"))
PROCESADOR = os.getenv("POD_NAME") or socket.gethostname()   # nombre del pod

SQL_INSERT = """
INSERT INTO ventas (id_venta, fecha_hora, sucursal, categoria, cantidad,
                    precio_unitario_centimos, importe_centimos, procesador)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id_venta) DO NOTHING
"""

detener = False


def log(msg):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{PROCESADOR}] {msg}", flush=True)


def _senal(signum, frame):
    global detener
    detener = True
    log("Señal de cierre recibida: termino la tarea actual y salgo")


def parsear(cuerpo):
    """Valida el mensaje y devuelve la tupla lista para el INSERT (importes en céntimos)."""
    d = json.loads(cuerpo)
    cantidad = int(d["cantidad"])
    precio = int(Decimal(str(d["precio_unitario"])) * 100)
    if cantidad <= 0 or precio <= 0:
        raise ValueError("cantidad o precio no válidos")
    return (d["id_venta"], d["fecha_hora"], d["sucursal"], d["categoria"],
            cantidad, precio, cantidad * precio, PROCESADOR)


def ejecutar():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)
    cred = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    mq = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, credentials=cred, heartbeat=60))
    ch = mq.channel()
    ch.queue_declare(queue=COLA, durable=True)   # la cola sobrevive a reinicios de RabbitMQ
    ch.basic_qos(prefetch_count=1)               # una tarea a la vez por pod
    log(f"Listo. Consumiendo la cola '{COLA}'")

    try:
        for metodo, props, cuerpo in ch.consume(COLA, auto_ack=False, inactivity_timeout=1):
            if metodo is None:                   # no llegó nada en 1 s
                if detener:
                    break
                continue

            try:
                fila = parsear(cuerpo)
            except (ValueError, KeyError, TypeError, InvalidOperation):
                log(f"INVALIDA descartada: {cuerpo[:80]!r}")
                ch.basic_ack(metodo.delivery_tag)   # no reintentar un mensaje roto
                continue

            time.sleep(VALIDACION_SEG)           # validación externa simulada

            with conn.cursor() as cur:
                cur.execute(SQL_INSERT, fila)
                nueva = cur.rowcount == 1
            conn.commit()                        # 1) primero guardar...
            ch.basic_ack(metodo.delivery_tag)    # 2) ...y solo entonces confirmar

            estado = "OK" if nueva else "DUPLICADA(ignorada)"
            log(f"{estado} {fila[0]} importe={fila[6]} reentrega={metodo.redelivered}")

            if detener:
                break
    finally:
        for cerrar in (ch.cancel, mq.close, conn.close):
            try:
                cerrar()
            except Exception:
                pass


def main():
    signal.signal(signal.SIGTERM, _senal)
    signal.signal(signal.SIGINT, _senal)
    log(f"Iniciando procesador (validación simulada: {VALIDACION_SEG}s por venta)")
    while not detener:
        try:
            ejecutar()
        except Exception as e:                   # RabbitMQ/Postgres aún no listos o se cayeron
            log(f"Error: {e!r}. Reintento en 3 s")
            time.sleep(3)
    log("Procesador detenido")


if __name__ == "__main__":
    main()