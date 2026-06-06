#!/usr/bin/env python3
"""
consumer_avro_local.py

Local Avro consumer that reads raw bytes from `sales_topic` and decodes them
using the same local Avro schema definition.
"""
import argparse
import io
import json
import sys

from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader


SALES_TOPIC_SCHEMA = {
    "name": "Sale",
    "type": "record",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "item_name", "type": "string"},
        {"name": "price", "type": "float"},
    ],
}


def decode_record(value: bytes, parsed_schema: dict) -> dict:
    stream = io.BytesIO(value)
    return schemaless_reader(stream, parsed_schema)


def run(broker: str, group: str, topic: str, start: str, timeout: int):
    parsed_schema = parse_schema(SALES_TOPIC_SCHEMA)
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": start,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print(f"Listening to {topic} with group {group}, start={start}")
    try:
        while True:
            msg = consumer.poll(timeout)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                record = decode_record(msg.value(), parsed_schema)
            except Exception as err:
                print(f"Failed to decode record: {err}")
                continue

            print(json.dumps(record, indent=2))
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("Shutting down")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Local Avro consumer with fastavro deserialization")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap broker")
    parser.add_argument("--group", default="consumer_avro_local_group", help="Consumer group id")
    parser.add_argument("--topic", default="sales_topic", help="Source topic")
    parser.add_argument("--start", choices=["earliest", "latest"], default="earliest", help="Where to start in the topic")
    parser.add_argument("--timeout", type=float, default=1.0, help="Poll timeout in seconds")

    args = parser.parse_args()
    run(args.broker, args.group, args.topic, args.start, args.timeout)


if __name__ == "__main__":
    main()
