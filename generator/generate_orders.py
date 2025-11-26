#%%
import random
import json
import time
from datetime import datetime, timezone

#%%
COUNTRIES = ["PL", "DE", "SE", "US", "UK"]
CHANNELS = ["web", "mobile", "partner"]
PAYMENT_METHODS = ["card", "paypal", "cod"]

def generate_order(order_id: int) -> dict:
    return {
        "order_id": f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{order_id:06d}",
        "user_id": random.randint(1, 5000),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": random.choice(["placed", "paid", "cancelled"]),
        "items_count": random.randint(1, 5),
        "total_amount": round(random.uniform(10, 300), 2),
        "currency": "EUR",
        "country": random.choice(COUNTRIES),
        "channel": random.choice(CHANNELS),
        "payment_method": random.choice(PAYMENT_METHODS),
    }

def main():
    order_id = 1
    while True:
        order = generate_order(order_id)
        print(json.dumps(order))
        order_id += 1
        time.sleep(1)  # 1 event per second

if __name__ == "__main__":
    main()
# %%
