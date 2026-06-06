#!/usr/bin/env python3
"""
consumer_partition.py

Consumer pinned to partition 0 of `topic_raw`. Processes Cairo transactions only.
If `amount > 100000` the record is forwarded to `topic_fraud`.

Requires: confluent-kafka
"""
import argparse
import json
import sys
import time

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING, OFFSET_END


def delivery_report(err, msg):
    if err is not None:
        print(f"Forward delivery failed: {err}")
    else:
        print(f"Forwarded to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")


def run(broker: str, group: str, topic: str, start: str):
    consumer_conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "enable.auto.commit": False,
        # auto.offset.reset is ignored when using assign, but keep a sensible default
        "auto.offset.reset": "earliest",
    }

    c = Consumer(consumer_conf)
    p = Producer({"bootstrap.servers": broker})

    if start == "earliest":
        offset = OFFSET_BEGINNING
    else:
        offset = OFFSET_END

    # Assign to partition 0 only
    tp = TopicPartition(topic, 0, offset)
    c.assign([tp])

    print(f"Assigned to {topic} partition 0 starting at {start}")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"Failed to decode message: {e}")
                continue

            # Only process Cairo transactions
            if rec.get("location") != "Cairo":
                print(f"Skipping non-Cairo message: {rec.get('location')}")
                # commit offset for skipped messages as we've processed them
                c.commit(message=msg, asynchronous=False)
                continue

            amount = rec.get("amount", 0)
            print(f"Consumed Cairo txn id={rec.get('transaction_id')} amount={amount}")

            if amount and float(amount) > 100000:
                # forward to topic_fraud
                try:
                    p.produce("topic_fraud", value=json.dumps(rec).encode("utf-8"), callback=delivery_report)
                    p.poll(0)
                except Exception as e:
                    print(f"Failed to forward to topic_fraud: {e}")

            # commit offset after processing
            c.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("Stopping consumer")
    finally:
        # flush producer and close consumer
        try:
            p.flush(5)
        except Exception:
            pass
        c.close()


def main():
    parser = argparse.ArgumentParser(description="Partition-isolated consumer pinned to partition 0")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap broker")
    parser.add_argument("--group", default="consumer_partition_group", help="Consumer group id")
    parser.add_argument("--topic", default="topic_raw", help="Source topic")
    parser.add_argument("--start", choices=["earliest", "latest"], default="earliest", help="Where to start in the partition")

    args = parser.parse_args()

    run(args.broker, args.group, args.topic, args.start)


if __name__ == "__main__":
    main()
