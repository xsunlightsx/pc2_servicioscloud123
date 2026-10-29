"""
CÁLCULO DE REFERENCIA INDEPENDIENTE.
Lee data/ventas.csv y calcula los resultados "correctos" SIN usar RabbitMQ, Postgres
ni Kubernetes. Después compararás lo que dé tu sistema contra este archivo.
Los importes se manejan en CÉNTIMOS ENTEROS (nada de float => nada de errores de redondeo).
Uso:  python scripts/referencia.py
"""
import csv
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
ENTRADA = BASE / "data" / "ventas.csv"
SALIDA = BASE / "data" / "referencia.json"

def a_centimos(texto: str) -> int:
    """'150.00' -> 15000 (usando Decimal, nunca float)."""
    return int(Decimal(texto) * 100)

def fmt(centimos: int) -> str:
    return f"{centimos // 100}.{centimos % 100:02d}"

def main():
    ids = set()
    por_sucursal = defaultdict(int)
    por_categoria = defaultdict(int)
    total = 0

    with open(ENTRADA, newline="", encoding="utf-8") as f:
        for fila in csv.DictReader(f):
            if fila["id_venta"] in ids:          # un id repetido no se cuenta dos veces
                continue
            ids.add(fila["id_venta"])
            importe = int(fila["cantidad"]) * a_centimos(fila["precio_unitario"])
            por_sucursal[fila["sucursal"]] += importe
            por_categoria[fila["categoria"]] += importe
            total += importe

    resultado = {
        "ventas_unicas": len(ids),
        "importe_general_centimos": total,
        "importe_general": fmt(total),
        "por_sucursal_centimos": dict(sorted(por_sucursal.items())),
        "por_categoria_centimos": dict(sorted(por_categoria.items())),
    }
    SALIDA.write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Ventas únicas : {len(ids)}")
    print(f"Importe total : S/ {fmt(total)}")
    print("\nPor sucursal:")
    for k, v in sorted(por_sucursal.items()):
        print(f"  {k:<12} S/ {fmt(v)}")
    print("\nPor categoría:")
    for k, v in sorted(por_categoria.items()):
        print(f"  {k:<12} S/ {fmt(v)}")
    print(f"\nGuardado en {SALIDA}")

if __name__ == "__main__":
    main()