"""The project's check: the whole pipeline again in DuckDB SQL, straight from the 24 raw files, compared with Spark.

    ./venv/bin/python check.py

1. The cleaning counts (every service, year and drop reason) match results/cleaning.csv exactly.
2. The congestion zone derived the same way is the same 37 zones.
3. Every monthly flow (service, year, month, within/into/out of/outside) has the same trip count, and the same total
   miles, hours, fare and CBD fee to a relative 1e-9, as results/flows_monthly.csv.
"""
import json
import time
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).parent
RAW = HERE / "data" / "raw"
R = HERE / "results"

TRIPS = f"""
WITH y AS (SELECT 'yellow' AS service, tpep_pickup_datetime AS pickup, tpep_dropoff_datetime AS dropoff, PULocationID::int AS pu,
                  DOLocationID::int AS "do", trip_distance AS miles, fare_amount AS fare, cbd_congestion_fee, filename
             FROM read_parquet('{RAW}/yellow_tripdata_*.parquet', union_by_name = true, filename = true)),
     h AS (SELECT 'fhvhv', pickup_datetime, dropoff_datetime, PULocationID::int, DOLocationID::int, trip_miles, base_passenger_fare,
                  cbd_congestion_fee, filename
             FROM read_parquet('{RAW}/fhvhv_tripdata_*.parquet', union_by_name = true, filename = true)),
     t AS (SELECT *, regexp_extract(filename, '_(\\d{{4}})-\\d{{2}}\\.parquet', 1)::int AS year,
                  regexp_extract(filename, '_\\d{{4}}-(\\d{{2}})\\.parquet', 1)::int AS month,
                  (floor(epoch(dropoff)) - floor(epoch(pickup))) / 60.0 AS minutes
             FROM (SELECT * FROM y UNION ALL SELECT * FROM h))
SELECT *, CASE
    WHEN NOT (year(pickup) = year AND month(pickup) = month) THEN 'pickup outside the file''s month'
    WHEN pu IS NULL OR "do" IS NULL OR pu IN (264, 265) OR "do" IN (264, 265) THEN 'zone unknown or outside NYC'
    WHEN minutes IS NULL OR minutes <= 0 OR minutes > 180 THEN 'no time or over 3 hours'
    WHEN miles IS NULL OR miles <= 0 OR miles > 100 THEN 'no distance or over 100 miles'
    WHEN miles / (minutes / 60) > 80 THEN 'faster than 80 mph'
    WHEN fare IS NULL OR fare < 0 THEN 'negative fare'
    ELSE 'kept' END AS dropped
  FROM t"""


def main():
    con = duckdb.connect()
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE trips AS {TRIPS}")
    clean = con.execute("SELECT service, year, dropped, count(*) AS n FROM trips GROUP BY ALL").df()
    spark_clean = pd.read_csv(R / "cleaning.csv")
    m = clean.merge(spark_clean, on=["service", "year", "dropped"], how="outer")
    assert len(m) == len(spark_clean) and (m["n"] == m["count"]).all(), m[m["n"] != m["count"]]

    zones = set(con.execute("""SELECT pu FROM trips WHERE dropped = 'kept' AND year = 2025 AND pu = "do" GROUP BY pu
                                HAVING avg((cbd_congestion_fee > 0)::int) >= 0.9 AND count(*) >= 1000""").df()["pu"])
    assert zones == set(pd.read_csv(R / "cbd_zones.csv")["zone"]), zones
    ids = ", ".join(map(str, sorted(zones)))
    flows = con.execute(f"""
        SELECT service, year, month,
               CASE WHEN pu IN ({ids}) AND "do" IN ({ids}) THEN 'within' WHEN "do" IN ({ids}) THEN 'into'
                    WHEN pu IN ({ids}) THEN 'out of' ELSE 'outside' END AS flow,
               count(*) AS trips, sum(miles) AS miles, sum(minutes / 60) AS hours, sum(fare) AS fare, sum(cbd_congestion_fee) AS cbd_fee
          FROM trips WHERE dropped = 'kept' GROUP BY ALL""").df()
    duck_s = time.perf_counter() - t0
    sp = pd.read_csv(R / "flows_monthly.csv")
    j = sp.merge(flows, on=["service", "year", "month", "flow"], suffixes=("", "_duck"))
    assert len(j) == len(sp) == len(flows) and (j["trips"] == j["trips_duck"]).all()
    for col in ["miles", "hours", "fare", "cbd_fee"]:
        a, b = j[col].fillna(0), j[col + "_duck"].fillna(0)
        assert ((a - b).abs() <= 1e-9 * b.abs().clip(lower=1)).all(), (col, j[(a - b).abs() > 1e-9 * b.abs().clip(lower=1)])
    (R / "duckdb_check.json").write_text(json.dumps({"duckdb_seconds_raw_to_flows": round(duck_s, 1)}, indent=2) + "\n")
    print(f"OK: {int(clean['n'].sum()):,} raw trips; cleaning counts, the {len(zones)} congestion-zone taxi zones and all "
          f"{len(sp)} monthly flows (trips, miles, hours, fare, fee) match Spark exactly; DuckDB took {duck_s:.0f} s from raw files")


if __name__ == "__main__":
    main()
