import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

STATUSES = ["approved", "declined", "reversed"]
CHANNELS = ["card_present", "card_not_present", "mobile_wallet", "ach"]
COUNTRIES = ["US", "ZA", "GB", "DE", "SG"]
CURRENCIES = {"US": "USD", "ZA": "ZAR", "GB": "GBP", "DE": "EUR", "SG": "SGD"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event(sequence: int, late_probability: float, invalid_probability: float) -> dict:
    country = random.choice(COUNTRIES)
    event_time = utc_now()
    if random.random() < late_probability:
        event_time -= timedelta(minutes=random.randint(12, 45))

    event = {
        "event_id": str(uuid4()),
        "event_type": "transaction_authorized",
        "event_time": isoformat(event_time),
        "ingested_at": isoformat(utc_now()),
        "account_id": f"acct-{random.randint(10000, 10150)}",
        "merchant_id": f"mrc-{random.randint(100, 999)}",
        "amount": round(random.uniform(3.0, 750.0), 2),
        "currency": CURRENCIES[country],
        "country": country,
        "channel": random.choice(CHANNELS),
        "status": random.choice(STATUSES),
        "sequence": sequence,
    }

    if random.random() < invalid_probability:
        field = random.choice(["event_id", "event_time", "amount", "account_id"])
        if field == "amount":
            event[field] = -1
        else:
            event[field] = None

    return event


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery_failed topic={msg.topic()} partition={msg.partition()} error={err}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate transaction events into Kafka.")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--rate-per-second", type=float, default=20)
    parser.add_argument("--duplicate-probability", type=float, default=0.03)
    parser.add_argument("--invalid-probability", type=float, default=0.02)
    parser.add_argument("--late-probability", type=float, default=0.05)
    return parser.parse_args()


def main() -> None:
    from confluent_kafka import Producer

    args = parse_args()
    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "linger.ms": 25,
        }
    )
    topic = os.getenv("KAFKA_TOPIC", "transactions.v1")
    sleep_seconds = 1 / args.rate_per_second if args.rate_per_second > 0 else 0
    previous_event = None

    for sequence in range(args.events):
        event = build_event(sequence, args.late_probability, args.invalid_probability)
        if previous_event and random.random() < args.duplicate_probability:
            event = dict(previous_event)
            event["ingested_at"] = isoformat(utc_now())

        key = str(event.get("account_id") or "invalid")
        producer.produce(topic, key=key, value=json.dumps(event), callback=delivery_report)
        producer.poll(0)
        previous_event = event

        if sequence % 100 == 0:
            print(f"produced={sequence} topic={topic}", flush=True)
        if sleep_seconds:
            time.sleep(sleep_seconds)

    producer.flush()
    print(f"completed produced={args.events} topic={topic}", flush=True)


if __name__ == "__main__":
    main()
