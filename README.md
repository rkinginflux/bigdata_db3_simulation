# bigdata_db3_simulation
Simulating DB3 ingest

## Commands
1. apt install python3.10-venv
2. python3 -m venv bigdata
3. source bigdata/bin/activate
4. cd bigdata/
5. pip install influxdb3-python
6. pip install polars
7. ./make_big_csv.sh
8. export INFLUXDB_TOKEN=apiv3_YOUR_TOKEN
9. python3 make_big_data.py --ingest --csv big.csv --db bigdata --measurement lab --batch-size 10

```
python3 make_big_data.py --help
usage: make_big_data.py [-h] [--generate] [--ingest] [--rows ROWS] [--csv CSV] [--url URL] [--token TOKEN] [--db DB] [--measurement MEASUREMENT]
                        [--batch-size BATCH_SIZE]

Generate and/or ingest a big CSV into InfluxDB 3.

options:
  -h, --help            show this help message and exit
  --generate            Generate CSV file.
  --ingest              Ingest CSV file into InfluxDB.
  --rows ROWS           Rows to generate (if --generate).
  --csv CSV             CSV file path.
  --url URL             InfluxDB URL (e.g. http://host:port).
  --token TOKEN         InfluxDB token (or set INFLUXDB_TOKEN).
  --db DB               Database name.
  --measurement MEASUREMENT
                        Measurement name.
  --batch-size BATCH_SIZE
                        Points per write batch.
```
