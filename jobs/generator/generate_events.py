import os
import json
import random
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LANDING_DIR = os.path.join(BASE_DIR, "data", "landing")
os.makedirs(LANDING_DIR, exist_ok=True)


def generate_single_event(transaction_id=None, is_corrupted=False):
    """Generates a realistic e-commerce transaction with intentional anomalies."""
    if is_corrupted:
        # Malformed JSON payload to test Bronze quarantine (_corrupt_record)
        return "{\"transaction_id\": \"MALFORMED_LINE\", \"amount\": corrupted_value,,,\n"

    tx_id = transaction_id or str(uuid.uuid4())
    event_time = datetime.now(timezone.utc).isoformat()
    
    # 5% chance of missing customer struct (null handling test)
    has_null_customer = random.random() < 0.05
    customer_data = None if has_null_customer else {
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "city": fake.city(),
        "country": fake.country_code()
    }

    # Line items array (nested struct)
    num_items = random.randint(1, 4)
    items = []
    total_amount = 0.0
    for _ in range(num_items):
        price = round(random.uniform(5.0, 150.0), 2)
        qty = random.randint(1, 3)
        total_amount += (price * qty)
        items.append({
            "item_id": f"PROD-{random.randint(100, 500)}",
            "category": random.choice(["Electronics", "Home", "Fashion", "Books", "Beauty"]),
            "unit_price": price,
            "quantity": qty
        })

    # Injected edge case: 2% chance of negative/refund anomaly
    if random.random() < 0.02:
        total_amount = -abs(total_amount)

    payload = {
        "transaction_id": tx_id,
        "event_timestamp": event_time,
        "status": random.choice(["PENDING", "COMPLETED", "FAILED", "CANCELLED"]),
        "payment_method": random.choice(["CREDIT_CARD", "PAYPAL", "WIRE", "CRYPTO"]),
        "total_amount": round(total_amount, 2),
        "customer": customer_data,
        "items": items
    }
    return json.dumps(payload) + "\n"


def generate_batch(file_prefix="raw_transactions", num_records=250):
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(LANDING_DIR, f"{file_prefix}_{timestamp_str}.json")

    records = []
    cached_ids = []

    for _ in range(num_records):
        # 10% chance to duplicate an existing transaction ID with updated payload (CDC test case)
        if cached_ids and random.random() < 0.10:
            tx_id = random.choice(cached_ids)
            records.append(generate_single_event(transaction_id=tx_id))
        # 2% chance of a malformed unparseable row
        elif random.random() < 0.02:
            records.append(generate_single_event(is_corrupted=True))
        else:
            tx_id = str(uuid.uuid4())
            cached_ids.append(tx_id)
            records.append(generate_single_event(transaction_id=tx_id))

    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(records)

    print(f"[OK] Generated {num_records} events in landing zone: {file_path}")
    return file_path


if __name__ == "__main__":
    generate_batch(num_records=500)