"""
Publica las primeras N ventas del CSV a la cola 'ventas' usando la API HTTP de RabbitMQ
(solo librería estándar). Sirve para probar el procesador antes de tener el generador.
Requiere el port-forward activo:  kubectl port-forward -n ventas svc/rabbitmq 15672:15672
Uso:  python scripts\\publicar_prueba.py 50
"""
import base64
import csv
import json
import sys
import urllib.request
from pathlib import Path

CSV = Path(__file__).resolve().parent.parent / "data" / "ventas.csv"
URL = "http://localhost:15672/api/exchanges/%2F/amq.default/publish"
AUTH = base64.b64encode(b"ventas_user:ventas_pass").decode()

def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    enviadas = 0
    with open(CSV, newline="", encoding="utf-8") as f:
        for fila in csv.DictReader(f):
            if enviadas >= n:
                break
            cuerpo = {
                "properties": {"delivery_mode": 2},       # mensaje persistente
                "routing_key": "ventas",
                "payload": json.dumps(fila, ensure_ascii=False),
                "payload_encoding": "string",
            }
            req = urllib.request.Request(URL, data=json.dumps(cuerpo).encode(), method="POST",
                headers={"Authorization": f"Basic {AUTH}", "Content-Type": "application/json"})
            with urllib.request.urlopen(req) as r:
                if not json.load(r).get("routed"):
                    sys.exit("La cola 'ventas' no existe: inicia primero el procesador.")
            enviadas += 1
    print(f"Publicadas {enviadas} ventas en la cola 'ventas'")

if __name__ == "__main__":
    main()