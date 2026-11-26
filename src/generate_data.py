import json
import random
import uuid

from datetime import datetime, timezone
from pathlib import Path


CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home",
    "Sports",
    "Books"
]

REGIONS = [
    "Europe",
    "Asia",
    "North America",
    "South America",
    "Middle East"
]


def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, 10000),
        "product_id": random.randint(1, 1000),
        "category": random.choice(CATEGORIES),
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(5, 500), 2),
        "region": random.choice(REGIONS),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def generate_batch(batch_id, record_count):
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    transactions = [
        generate_transaction()
        for _ in range(record_count)
    ]

    file_path = output_dir / f"batch_{batch_id:04d}.json"

    with file_path.open("w", encoding="utf-8") as file:
        json.dump(transactions, file)

    print(f"Generated {file_path}: {record_count} records")


if __name__ == "__main__":
    batch_count = 10
    records_per_batch = 100000

    for batch_id in range(1, batch_count + 1):
        generate_batch(batch_id, records_per_batch)
