#!/usr/bin/env python3
import os
import sys
import time
import argparse
from datetime import datetime, timezone

import polars as pl
from influxdb_client_3 import InfluxDBClient3, Point


# -----------------------------
# Defaults (edit if you want)
# -----------------------------
HOST = "192.168.0.50"
PORT = 8181
URL = f"http://{HOST}:{PORT}"
TOKEN = os.getenv("INFLUXDB_TOKEN", "$TOKEN")
DATABASE = "bigdata"
CSV_FILE_PATH = "big.csv"
MEASUREMENT = "lab"
BATCH_SIZE = 10  # you can bump this to e.g. 5_000 or 20_000 for real throughput


# -----------------------------
# CSV generation (10 columns)
# -----------------------------
CSV_HEADER = [
    "ts", "host", "region", "sensor_id",
    "metric_a", "metric_b", "metric_c", "metric_d",
    "status", "message_len"
]

REGIONS = ["us-west", "us-east", "eu-west", "ap-south"]
STATUSES = ["ok", "warn", "err"]


def generate_csv(path: str, rows: int, seed: int = 42) -> None:
    """
    Generate a large CSV with the same 10-column schema as the bash/awk example.
    Writes incrementally in chunks so it works for millions of rows.
    """
    import random

    random.seed(seed)
    base_epoch = int(datetime.now(tz=timezone.utc).timestamp())

    # Tune this if you want larger write chunks
    chunk_size = 250_000

    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(CSV_HEADER) + "\n")

        written = 0
        while written < rows:
            n = min(chunk_size, rows - written)
            lines = []

            for i in range(1, n + 1):
                ts = base_epoch + written + i  # one row per second

                host = f"host{random.randint(1, 200):03d}"
                region = random.choice(REGIONS)
                sensor_id = random.randint(100_000, 999_999)

                metric_a = random.random() * 100.0
                metric_b = random.random() * 1000.0
                metric_c = -20.0 + random.random() * 80.0
                metric_d = random.random() * 1.0

                status = random.choice(STATUSES)
                message_len = random.randint(20, 219)

                lines.append(
                    f"{ts},{host},{region},{sensor_id},"
                    f"{metric_a:.3f},{metric_b:.3f},{metric_c:.3f},{metric_d:.6f},"
                    f"{status},{message_len}"
                )

            f.write("\n".join(lines) + "\n")
            written += n

            if written % (chunk_size * 2) == 0 or written == rows:
                print(f"[generate] wrote {written}/{rows} rows")

    print(f"[generate] done: {path} ({rows} rows)")


# -----------------------------
# InfluxDB ingestion
# -----------------------------
def epoch_seconds_to_datetime_utc(ts: int) -> datetime:
    # InfluxDB client Point likes datetime; keep it UTC.
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def ingest_csv(
    url: str,
    token: str,
    database: str,
    csv_path: str,
    measurement: str,
    batch_size: int,
) -> None:
    if not token or token == "$TOKEN":
        raise RuntimeError(
            "Token not set. Export INFLUXDB_TOKEN or edit TOKEN in the script."
        )

    client = InfluxDBClient3(host=url, token=token, database=database)

    # Polars lazy scan so we don't read the whole file at once
    lf = pl.scan_csv(
        csv_path,
        has_header=True,
        infer_schema_length=10_000,  # helps with large files
        try_parse_dates=False,
    )

    total_sent = 0
    t0 = time.time()

    # Stream in reasonably-sized chunks, then batch within those chunks.
    # You can tune chunk_rows for throughput vs memory.
    chunk_rows = 100_000

    for df in lf.collect(streaming=True).iter_slices(n_rows=chunk_rows):
        # Convert columns to Python lists for fast iteration
        ts_list = df["ts"].to_list()
        host_list = df["host"].to_list()
        region_list = df["region"].to_list()
        sensor_list = df["sensor_id"].to_list()
        a_list = df["metric_a"].to_list()
        b_list = df["metric_b"].to_list()
        c_list = df["metric_c"].to_list()
        d_list = df["metric_d"].to_list()
        status_list = df["status"].to_list()
        ml_list = df["message_len"].to_list()

        points = []
        for i in range(df.height):
            p = (
                Point(measurement)
                .tag("host", host_list[i])
                .tag("region", region_list[i])
                .tag("status", status_list[i])
                .tag("sensor_id", str(sensor_list[i]))  # tags are strings
                .field("metric_a", float(a_list[i]))
                .field("metric_b", float(b_list[i]))
                .field("metric_c", float(c_list[i]))
                .field("metric_d", float(d_list[i]))
                .field("message_len", int(ml_list[i]))
                .time(epoch_seconds_to_datetime_utc(int(ts_list[i])))
            )
            points.append(p)

            if len(points) >= batch_size:
                client.write(record=points)
                total_sent += len(points)
                points.clear()

                # lightweight progress
                if total_sent % (batch_size * 1000) == 0:
                    elapsed = time.time() - t0
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    print(f"[ingest] sent={total_sent:,} rate={rate:,.0f} pts/s")

        # flush remainder for this chunk
        if points:
            client.write(record=points)
            total_sent += len(points)

    elapsed = time.time() - t0
    rate = total_sent / elapsed if elapsed > 0 else 0
    print(f"[ingest] done: sent={total_sent:,} elapsed={elapsed:.2f}s rate={rate:,.0f} pts/s")


def main():
    ap = argparse.ArgumentParser(description="Generate and/or ingest a big CSV into InfluxDB 3.")
    ap.add_argument("--generate", action="store_true", help="Generate CSV file.")
    ap.add_argument("--ingest", action="store_true", help="Ingest CSV file into InfluxDB.")
    ap.add_argument("--rows", type=int, default=5_000_000, help="Rows to generate (if --generate).")
    ap.add_argument("--csv", type=str, default=CSV_FILE_PATH, help="CSV file path.")
    ap.add_argument("--url", type=str, default=URL, help="InfluxDB URL (e.g. http://host:port).")
    ap.add_argument("--token", type=str, default=TOKEN, help="InfluxDB token (or set INFLUXDB_TOKEN).")
    ap.add_argument("--db", type=str, default=DATABASE, help="Database name.")
    ap.add_argument("--measurement", type=str, default=MEASUREMENT, help="Measurement name.")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Points per write batch.")
    args = ap.parse_args()

    if not args.generate and not args.ingest:
        ap.error("Pick at least one: --generate and/or --ingest")

    if args.generate:
        generate_csv(args.csv, args.rows)

    if args.ingest:
        ingest_csv(
            url=args.url,
            token=args.token,
            database=args.db,
            csv_path=args.csv,
            measurement=args.measurement,
            batch_size=args.batch_size,
        )


if __name__ == "__main__":
    main()
