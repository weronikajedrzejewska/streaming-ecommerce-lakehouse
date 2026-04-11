import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from kafka import KafkaProducer


TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))

EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
CATEGORIES = ["books", "electronics", "home", "beauty", "toys"]
COUNTRIES = ["PL", "DE", "FR", "IT", "ES"]


def make_event() -> dict:
    now = datetime.now(timezone.utc)

    # 15% late events (up to 10 minutes in the past), plus slight out-of-order jitter.
    if random.random() < 0.15:
        event_ts = now - timedelta(minutes=random.randint(1, 10), seconds=random.randint(0, 59))
    else:
        event_ts = now

    event_ts = event_ts + timedelta(seconds=random.randint(-5, 5))

    event_type = random.choices(EVENT_TYPES, weights=[0.6, 0.25, 0.15])[0]
    price = round(random.uniform(10, 500), 2)

    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_timestamp": event_ts.isoformat(),
        "ingestion_timestamp": now.isoformat(),
        "user_id": f"user_{random.randint(1, 1000)}",
        "session_id": f"session_{random.randint(1, 2000)}",
        "country": random.choice(COUNTRIES),
        "payload": {
            "product_id": f"prod_{random.randint(100, 999)}",
            "category": random.choice(CATEGORIES),
            "price": price if event_type != "page_view" else None,
            "currency": "EUR" if event_type != "page_view" else None,
            "quantity": random.randint(1, 3) if event_type in {"add_to_cart", "purchase"} else None,
            "cart_value": price if event_type in {"add_to_cart", "purchase"} else None,
        },
    }


def main() -> None:
    random.seed(42)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="all",
    )

    sleep_seconds = 1.0 / max(EVENTS_PER_SECOND, 0.1)
    sent = 0

    print(f"Producing to topic={TOPIC} broker={BOOTSTRAP_SERVERS}")
    while True:
        event = make_event()
        producer.send(TOPIC, value=event)
        sent += 1

        if sent % 100 == 0:
            producer.flush()
            print(f"Sent {sent} events")

        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
