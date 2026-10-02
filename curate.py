"""Step 2: one clean trip table from 24 monthly files of two different services, written as partitioned Parquet.

    ./venv/bin/python curate.py    -> data/trips/service=.../year=.../month=.../*.parquet, results/cleaning.csv

Yellow taxis and high-volume for-hire vehicles (Uber, Lyft) have different columns; both become one schema. 2025 files
add cbd_congestion_fee (congestion pricing began 5 January 2025), so 2024 rows get NULL there. Trips that cannot be
real are dropped and counted by the first rule they break.
"""
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

HERE = Path(__file__).parent
RAW = HERE / "data" / "raw"
TRIPS = HERE / "data" / "trips"


def spark():
    return (SparkSession.builder.master("local[*]").appName("taxi-spark")
            .config("spark.driver.memory", "8g").config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.adaptive.enabled", "true").config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def unified(s):
    y = s.read.option("mergeSchema", "true").parquet(str(RAW / "yellow_tripdata_*.parquet")).select(
        F.lit("yellow").alias("service"),
        F.col("tpep_pickup_datetime").alias("pickup"), F.col("tpep_dropoff_datetime").alias("dropoff"),
        F.col("PULocationID").cast("int").alias("pu"), F.col("DOLocationID").cast("int").alias("do"),
        F.col("trip_distance").alias("miles"), F.col("fare_amount").alias("fare"),
        F.col("cbd_congestion_fee"), F.input_file_name().alias("file"))
    h = s.read.option("mergeSchema", "true").parquet(str(RAW / "fhvhv_tripdata_*.parquet")).select(
        F.lit("fhvhv").alias("service"),
        F.col("pickup_datetime").alias("pickup"), F.col("dropoff_datetime").alias("dropoff"),
        F.col("PULocationID").cast("int").alias("pu"), F.col("DOLocationID").cast("int").alias("do"),
        F.col("trip_miles").alias("miles"), F.col("base_passenger_fare").alias("fare"),
        F.col("cbd_congestion_fee"), F.input_file_name().alias("file"))
    t = y.unionByName(h)
    return (t.withColumn("year", F.regexp_extract("file", r"_(\d{4})-\d{2}\.parquet", 1).cast("int"))
             .withColumn("month", F.regexp_extract("file", r"_\d{4}-(\d{2})\.parquet", 1).cast("int"))
             .withColumn("minutes", (F.unix_timestamp("dropoff") - F.unix_timestamp("pickup")) / 60.0)
             .drop("file"))


def rules():
    """First rule broken wins; the order is the order a reviewer would check. (Built after the session starts.)"""
    return [("pickup outside the file's month", ~((F.year("pickup") == F.col("year")) & (F.month("pickup") == F.col("month")))),
        ("zone unknown or outside NYC", F.col("pu").isNull() | F.col("do").isNull() | F.col("pu").isin(264, 265) | F.col("do").isin(264, 265)),
        ("no time or over 3 hours", F.col("minutes").isNull() | (F.col("minutes") <= 0) | (F.col("minutes") > 180)),
        ("no distance or over 100 miles", F.col("miles").isNull() | (F.col("miles") <= 0) | (F.col("miles") > 100)),
        ("faster than 80 mph", F.col("miles") / (F.col("minutes") / 60) > 80),
        ("negative fare", F.col("fare").isNull() | (F.col("fare") < 0))]


def main():
    s = spark()
    t0 = time.perf_counter()
    t = unified(s)
    reason = F.lit(None).cast("string")
    for name, cond in reversed(rules()):
        reason = F.when(cond, F.lit(name)).otherwise(reason)
    t = t.withColumn("dropped", reason)
    # no cache(): two passes over columnar Parquet are cheaper than holding ~280 million rows in 8 GB
    counts = t.groupBy("service", "year", "dropped").count().toPandas()
    (HERE / "results").mkdir(exist_ok=True)
    counts.fillna({"dropped": "kept"}).sort_values(["service", "year", "count"], ascending=[True, True, False]) \
          .to_csv(HERE / "results" / "cleaning.csv", index=False)
    (t.where(F.col("dropped").isNull()).drop("dropped")
      .repartition("service", "year", "month")                      # one set of files per partition, not 200 tiny ones
      .write.mode("overwrite").partitionBy("service", "year", "month").parquet(str(TRIPS)))
    print(counts.to_string(index=False))
    secs = time.perf_counter() - t0
    (HERE / "results" / "timing_curate.json").write_text(json.dumps({"spark_seconds_raw_to_curated": round(secs, 1)}, indent=2) + "\n")
    print(f"curated in {secs:.0f} s")


if __name__ == "__main__":
    main()
