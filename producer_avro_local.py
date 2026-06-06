#!/usr/bin/env python3
"""
producer_avro_local.py

Local Avro producer using fastavro and Kafka producer bytes payload.

This script validates input data against a local Avro schema before serializing it
into binary bytes and sending it to Kafka.
"""
import argparse
import io
import json
import sys

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer, validation


SALES_TOPIC_SCHEMA = {
    "name": "Sale",
    "type": "record",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "item_name", "type": "string"},
        {"name": "price", "type": "float"},
    ],
}


def serialize_record(record: dict, parsed_schema: dict) -> bytes:
    validation.validate(record, parsed_schema)
    buffer = io.BytesIO()
    schemaless_writer(buffer, parsed_schema, record)
    return buffer.getvalue()


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
        sys.exit(1)
    print(f"Delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")


def run(broker: str, topic: str, data: dict):
    parsed_schema = parse_schema(SALES_TOPIC_SCHEMA)
    payload = serialize_record(data, parsed_schema)

    producer = Producer({"bootstrap.servers": broker})
    producer.produce(topic, value=payload, callback=delivery_report)
    producer.flush()


def parse_data_arg(data_arg: str) -> dict:
    try:
        return json.loads(data_arg)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"Invalid JSON data: {exc}") from exc


def main():
    parser = argparse.ArgumentParser(description="Local Avro producer with fastavro validation")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap broker")
    parser.add_argument("--topic", default="sales_topic", help="Target sales topic")
    parser.add_argument(
        "--data",
        type=parse_data_arg,
        default=json.dumps({"order_id": 101, "item_name": "Laptop", "price": 1200.5}),
        help='JSON string for the record, e.g. "{\"order_id\": 101, \"item_name\": \"Laptop\", \"price\": 1200.5}"',
    )

    args = parser.parse_args()

    try:
        run(args.broker, args.topic, args.data)
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
