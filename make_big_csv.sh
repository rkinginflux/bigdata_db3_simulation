#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./make_big_csv.sh [rows] [output.csv]
# Example:
#   ./make_big_csv.sh 5000000 big.csv

ROWS="${1:-5000000}"
OUT="${2:-big.csv}"

# Base time (UTC) used to build timestamps (seconds since epoch)
BASE_EPOCH="$(date -u +%s)"

# 10 columns:
# ts,host,region,sensor_id,metric_a,metric_b,metric_c,metric_d,status,message_len
{
  echo "ts,host,region,sensor_id,metric_a,metric_b,metric_c,metric_d,status,message_len"

  awk -v n="$ROWS" -v base="$BASE_EPOCH" 'BEGIN{
    srand(42);

    regions[1]="us-west"; regions[2]="us-east"; regions[3]="eu-west"; regions[4]="ap-south";
    status[1]="ok"; status[2]="warn"; status[3]="err";

    for (i=1; i<=n; i++) {
      # One row per second
      ts = base + i;

      # Small-cardinality strings (useful for compression & grouping)
      host = "host" sprintf("%03d", 1 + int(rand()*200));
      reg  = regions[1 + int(rand()*4)];
      sid  = 100000 + int(rand()*900000);

      # Numeric columns
      a = rand()*100;
      b = rand()*1000;
      c = -20 + rand()*80;
      d = rand()*1.0;

      st = status[1 + int(rand()*3)];
      ml = 20 + int(rand()*200);

      # Print CSV (avoid scientific notation, keep it simple)
      printf "%d,%s,%s,%d,%.3f,%.3f,%.3f,%.6f,%s,%d\n",
             ts,host,reg,sid,a,b,c,d,st,ml;
    }
  }'
} > "$OUT"

echo "Wrote $ROWS rows to $OUT"
echo "Tip: gzip -f $OUT  # often shrinks dramatically"
