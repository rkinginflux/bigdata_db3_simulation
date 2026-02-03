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
9. python3 test.py --ingest --csv big.csv --db bigdata --measurement lab --batch-size 10
