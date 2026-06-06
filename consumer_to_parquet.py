#!/usr/bin/env python3
"""
consumer_to_parquet.py

Consumes messages from `topic_fraud`, buffers them in memory, and periodically
writes them to Parquet files under `data_lake/`.

Behavior:
- Buffer up to `--flush-size` messages or flush every `--flush-interval` seconds.
- After writing a Parquet file, commits the consumer offset for the last included message.

Requires: confluent-kafka, pandas, pyarrow
"""
import argparse
import json
import os
import time
from typing import List

import pandas as pd
from confluent_kafka import Consumer


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def write_parquet(records: List[dict], data_dir: str, seq: int) -> str:
    df = pd.DataFrame(records)
    ts = int(time.time())
    fname = os.path.join(data_dir, f"fraud_{ts}_{seq}.parquet")
    df.to_parquet(fname, index=False)
    return fname


def run(broker: str, topic: str, group: str, data_dir: str, flush_size: int, flush_interval: int, start: str):
    ensure_dir(data_dir)

    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": start,
    }

    c = Consumer(conf)
    c.subscribe([topic])

    buffer = []
    last_msg = None
    last_flush = time.time()
    seq = 0

    print(f"Listening to {topic} -> writing Parquet into {data_dir}")

    try:
        while True:
            msg = c.poll(1.0)
            now = time.time()

            if msg is None:
                # check time-based flush
                if buffer and (now - last_flush) >= flush_interval:
                    fname = write_parquet(buffer, data_dir, seq)
                    print(f"Flushed {len(buffer)} records to {fname}")
                    # commit last message included
                    if last_msg is not None:
                        try:
                            c.commit(message=last_msg, asynchronous=False)
                        except Exception as e:
                            print(f"Commit failed: {e}")
                    buffer = []
                    seq += 1
                    last_flush = now
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"Failed to decode message: {e}")
                # still commit this bad message to avoid re-processing loop
                try:
                    c.commit(message=msg, asynchronous=False)
                except Exception:
                    pass
                continue

            buffer.append(rec)
            last_msg = msg

            # size-based flush
            if len(buffer) >= flush_size:
                fname = write_parquet(buffer, data_dir, seq)
                print(f"Flushed {len(buffer)} records to {fname}")
                if last_msg is not None:
                    try:
                        c.commit(message=last_msg, asynchronous=False)
                    except Exception as e:
                        print(f"Commit failed: {e}")
                buffer = []
                seq += 1
                last_flush = now

    except KeyboardInterrupt:
        print("Shutting down, flushing remaining records...")
        if buffer:
            try:
                fname = write_parquet(buffer, data_dir, seq)
                print(f"Flushed {len(buffer)} records to {fname}")
                if last_msg is not None:
                    try:
                        c.commit(message=last_msg, asynchronous=False)
                    except Exception as e:
                        print(f"Commit failed: {e}")
            except Exception as e:
                print(f"Failed to flush on shutdown: {e}")
    finally:
        c.close()


def main():
    parser = argparse.ArgumentParser(description="Consume fraud topic and sink to Parquet files")
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="topic_fraud")
    parser.add_argument("--group", default="consumer_to_parquet_group")
    parser.add_argument("--data-dir", default="data_lake", help="Directory to write Parquet files")
    parser.add_argument("--flush-size", type=int, default=50, help="Number of records before flushing to Parquet")
    parser.add_argument("--flush-interval", type=int, default=10, help="Seconds between time-based flushes")
    parser.add_argument("--start", choices=["earliest", "latest"], default="earliest")

    args = parser.parse_args()
    run(args.broker, args.topic, args.group, args.data_dir, args.flush_size, args.flush_interval, args.start)


if __name__ == "__main__":
    main()
