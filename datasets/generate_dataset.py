import csv
import random
import sys
from datetime import datetime, timedelta

# Usa el valor pasado por consola (ej: 10, 20, 50...)
# Si no pasas nada, por defecto genera 10 MB
TARGET_MB = int(sys.argv[1]) if len(sys.argv) > 1 else 10
TARGET_BYTES = TARGET_MB * 1024 * 1024

filename = f"dataset_{TARGET_MB}mb.csv"

header = ["id", "name", "value", "category", "timestamp"]
names = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta"]
categories = ["A", "B", "C"]

def random_timestamp():
    start = datetime(2025, 1, 1)
    delta = timedelta(seconds=random.randint(0, 86400 * 30))
    return (start + delta).isoformat() + "Z"

row_id = 1
current_size = 0

with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)

with open(filename, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    while current_size < TARGET_BYTES:
        writer.writerow([
            row_id,
            f"Item {random.choice(names)}",
            round(random.uniform(100, 5000), 2),
            random.choice(categories),
            random_timestamp()
        ])
        row_id += 1

        # Revisar el tamaño cada 1000 filas (más rápido)
        if row_id % 1000 == 0:
            current_size = f.tell()

print(f"✅ Archivo creado: {filename}")
print(f"📦 Tamaño aproximado: {current_size / (1024 * 1024):.2f} MB")
