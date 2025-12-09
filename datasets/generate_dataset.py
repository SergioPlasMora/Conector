import csv
import random
import time
from datetime import datetime, timedelta

TARGET_SIZE_MB = 100
TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024

filename = "dataset_100mb.csv"

header = ["id", "name", "value", "category", "timestamp"]

names = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa"]
categories = ["A", "B", "C"]

def random_timestamp(start):
    delta = timedelta(seconds=random.randint(0, 86400 * 30))
    return (start + delta).isoformat() + "Z"

start_time = datetime(2025, 1, 1)

current_size = 0
row_id = 1

with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)

with open(filename, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    while current_size < TARGET_SIZE_BYTES:
        row = [
            row_id,
            f"Item {random.choice(names)}",
            round(random.uniform(100, 5000), 2),
            random.choice(categories),
            random_timestamp(start_time),
        ]

        writer.writerow(row)
        row_id += 1

        # Revisa tamaño cada 1000 filas (mejor rendimiento)
        if row_id % 1000 == 0:
            current_size = f.tell()

print(f"Archivo generado: {filename}")
print(f"Tamaño aproximado: {current_size / (1024 * 1024):.2f} MB")
