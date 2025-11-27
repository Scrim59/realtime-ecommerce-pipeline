import random
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# --- KAFKA PRODUCER ---
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- CUSTOMER GENERATOR ---
def generate_customer(customer_id: int):
    first_names = ["Anna", "John", "Maria", "Tom", "Elena", "Omar", "Liu"]
    last_names = ["Nowak", "Smith", "Khan", "Müller", "Silva", "Ibrahim", "Chen"]

    return {
        "customer_id": f"C-{customer_id:05d}",
        "full_name": f"{random.choice(first_names)} {random.choice(last_names)}",
        "email": f"user{customer_id}@example.com",
        "country": random.choice(["PL", "DE", "UK", "ES", "FR", "US"]),
        "device": random.choice(["web", "mobile"]),
        "segment": random.choice(["new", "returning", "loyal", "premium"]),
        "age": random.randint(18, 75)
    }

# --- PRODUCT CATALOG ---
PRODUCTS = [
    {"product_id": "P-001", "product_name": "Smartphone", "category": "Electronics", "unit_price": 699.99},
    {"product_id": "P-002", "product_name": "Laptop", "category": "Electronics", "unit_price": 1299.99},
    {"product_id": "P-003", "product_name": "Headphones", "category": "Audio", "unit_price": 99.99},
    {"product_id": "P-004", "product_name": "Smartwatch", "category": "Electronics", "unit_price": 149.99},
    {"product_id": "P-005", "product_name": "Power Bank", "category": "Accessories", "unit_price": 24.99},
]

# --- ORDER GENERATOR ---
def generate_order(order_id: int) -> dict:
    customer = generate_customer(random.randint(1, 2000))

    n_items = random.randint(1, 4)
    items = []

    total = 0.0
    for _ in range(n_items):
        pr = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        line = pr["unit_price"] * qty
        total += line

        items.append({
            "product_id": pr["product_id"],
            "product_name": pr["product_name"],
            "category": pr["category"],
            "quantity": qty,
            "unit_price": pr["unit_price"],
            "line_total": round(line, 2)
        })

    return {
        "order_id": f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{order_id:06d}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "currency": "EUR",
        "status": random.choice(["placed", "paid", "cancelled"]),
        "channel": random.choice(["web", "mobile", "partner"]),
        "payment_method": random.choice(["card", "paypal", "cod"]),
        "customer": customer,
        "items": items,
        "total_amount": round(total, 2),
    }

# --- MAIN LOOP ---
def main():
    order_id = 1
    while True:
        order = generate_order(order_id)
        print(order)   # for debug
        producer.send("orders", order)
        producer.flush()
        order_id += 1
        time.sleep(1)

if __name__ == "__main__":
    main()