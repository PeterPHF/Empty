#!/usr/bin/env python3
"""
analytics.py

Run serverless analytics against Parquet files in `data_lake/` using DuckDB.

Usage examples:
    python analytics.py --data-dir data_lake
    python analytics.py --data-dir data_lake --query "SELECT count(*) FROM parquet_scan('{files}')"

Requires: duckdb, pandas
"""
import argparse
import glob
import os
import sys

import duckdb


def find_parquet_files(data_dir):
    pattern = os.path.join(data_dir, "*.parquet")
    files = glob.glob(pattern)
    return files, pattern


def run_default_queries(con, pattern):
    queries = [
        ("Total fraud records", f"SELECT count(*) AS total FROM parquet_scan('{pattern}')"),
        ("Total fraud amount", f"SELECT sum(amount) AS total_amount FROM parquet_scan('{pattern}')"),
        ("Average fraud amount", f"SELECT avg(amount) AS avg_amount FROM parquet_scan('{pattern}')"),
        (
            "Top users by fraud amount",
            f"SELECT user_id, count(*) AS cnt, sum(amount) AS total FROM parquet_scan('{pattern}') GROUP BY user_id ORDER BY total DESC LIMIT 10",
        ),
        (
            "Location breakdown",
            f"SELECT location, count(*) AS cnt, sum(amount) AS total FROM parquet_scan('{pattern}') GROUP BY location ORDER BY total DESC",
        ),
    ]

    for title, q in queries:
        print(f"\n== {title} ==")
        try:
            df = con.execute(q).df()
            print(df.to_string(index=False))
        except Exception as e:
            print(f"Query failed: {e}")


def run_custom_query(con, pattern, query):
    # allow the user to use {files} placeholder for the parquet pattern
    q = query.replace("{files}", pattern)
    print(f"Running query: {q}")
    try:
        df = con.execute(q).df()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Query failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Run DuckDB analytics on Parquet files in a local data lake")
    parser.add_argument("--data-dir", default="data_lake", help="Directory containing Parquet files")
    parser.add_argument("--query", help="Custom SQL query. Use {files} to refer to the parquet glob pattern")

    args = parser.parse_args()

    files, pattern = find_parquet_files(args.data_dir)
    if not files:
        print(f"No Parquet files found in {args.data_dir}. Create Parquet files first and retry.")
        sys.exit(1)

    con = duckdb.connect(database=':memory:')

    if args.query:
        run_custom_query(con, pattern, args.query)
    else:
        run_default_queries(con, pattern)


if __name__ == "__main__":
    main()
