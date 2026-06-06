#!/usr/bin/env python3
"""
test_offsets.py

Simple script to observe consumer behavior with `auto.offset.reset` set to
`earliest` or `latest`. Run multiple times (with same or different `--group`) to
see how committed offsets interact with the setting.

Requires: confluent-kafka
"""
import argparse
import json
import time

from confluent_kafka import Consumer


def run(broker: str, topic: str, group: str, start: str, timeout: int, max_msgs: int):
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": start,
    }

    c = Consumer(conf)
    c.subscribe([topic])

    print(f"Consumer group='{group}' auto.offset.reset='{start}' subscribing to {topic}")
    received = 0
    start_time = time.time()

    try:
        while True:
            if max_msgs and received >= max_msgs:
                break
            if time.time() - start_time > timeout:
                break

            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            received += 1
            try:
                val = msg.value().decode("utf-8")
            except Exception:
                val = str(msg.value())

            print(f"[{msg.partition()}@{msg.offset()}] {val}")

    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        c.close()


def main():
    parser = argparse.ArgumentParser(description="Test earliest vs latest offsets")
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="topic_raw")
    parser.add_argument("--group", default="test_offsets_group")
    parser.add_argument("--start", choices=["earliest", "latest"], default="latest")
    parser.add_argument("--timeout", type=int, default=10, help="Seconds to wait for messages")
    parser.add_argument("--max-msgs", type=int, default=0, help="Stop after this many messages (0 = unlimited until timeout)")

    args = parser.parse_args()
    run(args.broker, args.topic, args.group, args.start, args.timeout, args.max_msgs)


if __name__ == "__main__":
    main()
