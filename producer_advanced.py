#!/usr/bin/env python3
"""
producer_advanced.py

Simple Kafka producer that sends JSON transaction records to `topic_raw`.
Messages with `location == "Cairo"` go to partition 0.
Messages with `location == "Alexandria"` go to partition 1.

Requires: confluent-kafka
"""
import argparse
import json
import random
import time
import uuid

from confluent_kafka import Producer


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")


def make_record(i, location=None):
    loc = location or random.choice(["Cairo", "Alexandria"])
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 10000),
        "amount": round(random.random() * 200000, 2),
        "location": loc,
    }


def partition_for_location(location: str) -> int:
    if location == "Cairo":
        return 0
    if location == "Alexandria":
        return 1
    # default partition if unknown
    return 0


def run(broker, topic, count, interval, fixed_location):
    p = Producer({"bootstrap.servers": broker})

    for i in range(count):
        rec = make_record(i, fixed_location)
        part = partition_for_location(rec["location"])
        val = json.dumps(rec).encode("utf-8")

        p.produce(topic, value=val, partition=part, callback=delivery_report)
        p.poll(0)
        time.sleep(interval)

    p.flush()


def main():
    parser = argparse.ArgumentParser(description="Partition-targeted Kafka producer")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap broker")
    parser.add_argument("--topic", default="topic_raw", help="Target topic")
    parser.add_argument("--count", type=int, default=50, help="Number of messages to send")
    parser.add_argument("--interval", type=float, default=0.2, help="Seconds between messages")
    parser.add_argument("--location", choices=["Cairo", "Alexandria"], help="If set, send all messages with this location")

    args = parser.parse_args()

    print(f"Broker: {args.broker}  Topic: {args.topic}  Count: {args.count}  Interval: {args.interval}")
    if args.location:
        print(f"Forcing all messages to location: {args.location}")

    run(args.broker, args.topic, args.count, args.interval, args.location)


if __name__ == "__main__":
    main()
