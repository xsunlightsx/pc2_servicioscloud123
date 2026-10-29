"""
Genera data/ventas.csv con 10 000 ventas válidas y REPRODUCIBLES.
Reproducible = misma semilla (SEED) => siempre sale exactamente el mismo archivo.
Uso:  python scripts/generar_csv.py
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 2026            # no lo cambies: es lo que hace reproducible el archivo
N_VENTAS = 10_000
SALIDA = Path(__file__).resolve().parent.parent / "data" / "ventas.csv"

SUCURSALES = ["Lima Norte", "Lima Sur", "Lima Centro", "Arequipa", "Trujillo", "Cusco"]
# categoría -> (precio mínimo, precio máximo) en CÉNTIMOS enteros
CATEGORIAS = {
    "Electrónica": (5_000, 250_000),
    "Hogar":       (1_500, 80_000),
    "Ropa":        (2_000, 30_000),
    "Alimentos":   (300,   5_000),
    "Deportes":    (3_000, 60_000),
}

def main():
    rng = random.Random(SEED)                 # generador propio, aislado de otros usos de random
    inicio = datetime(2026, 10, 29, 8, 0, 0)  # primera venta
    SALIDA.parent.mkdir(parents=True, exist_ok=True)

    with open(SALIDA, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id_venta", "fecha_hora", "sucursal", "categoria", "cantidad", "precio_unitario"])
        fecha = inicio
        for i in range(1, N_VENTAS + 1):
            fecha += timedelta(seconds=rng.randint(1, 5))
            categoria = rng.choice(list(CATEGORIAS))
            pmin, pmax = CATEGORIAS[categoria]
            precio_centimos = rng.randint(pmin, pmax)
            w.writerow([
                f"V{i:06d}",                                   # V000001 ... V010000
                fecha.strftime("%Y-%m-%d %H:%M:%S"),
                rng.choice(SUCURSALES),
                categoria,
                rng.randint(1, 5),
                f"{precio_centimos // 100}.{precio_centimos % 100:02d}",  # "150.00" sin usar float
            ])
    print(f"OK -> {SALIDA}  ({N_VENTAS} ventas)")

if __name__ == "__main__":
    main()