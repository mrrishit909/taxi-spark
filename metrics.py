"""Steps 3-4: find the congestion zone from the data, then measure trips, speed and fares before and after.

    ./venv/bin/python metrics.py   -> results/cbd_zones.csv, fee_check.csv, flows_monthly.csv, speed_by_hour.csv, plan.txt

The zone: a taxi zone is inside the Congestion Relief Zone if at least 90% of its 2025 trips that start and end in it
paid the new CBD fee (and it has 1,000 such trips). Every trip is then within, into, out of, or outside the zone. Trips
that only pass through count as outside, so the fee check below shows how well the zone definition matches the fee.
"""
import json
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

import curate as C

HERE = Path(__file__).parent
R = HERE / "results"


def main():
    s = C.spark()
    t0 = time.perf_counter()
    trips = s.read.parquet(str(C.TRIPS))
    zones = s.read.option("header", True).csv(str(C.RAW / "taxi_zone_lookup.csv")) \
             .select(F.col("LocationID").cast("int").alias("zone"), F.col("Borough").alias("borough"), F.col("Zone").alias("zone_name"))   # Spark names are case-insensitive

    same = (trips.where((F.col("year") == 2025) & (F.col("pu") == F.col("do")))      # partition pruning: 2025 only
                 .groupBy(F.col("pu").alias("zone"))
                 .agg(F.count("*").alias("intra_trips"), F.avg((F.col("cbd_congestion_fee") > 0).cast("int")).alias("share_paid")))
    cbd = same.where((F.col("share_paid") >= 0.9) & (F.col("intra_trips") >= 1000)).join(F.broadcast(zones), "zone")
    (R / "plan_zone_query.txt").write_text(cbd._jdf.queryExecution().executedPlan().toString())   # pruned to 2025, broadcast join
    cbd_pd = cbd.orderBy("zone").toPandas()
    cbd_pd.to_csv(R / "cbd_zones.csv", index=False)
    ids = set(cbd_pd["zone"])

    in_pu, in_do = F.col("pu").isin(*ids), F.col("do").isin(*ids)
    t = trips.withColumn("flow", F.when(in_pu & in_do, "within").when(in_do, "into").when(in_pu, "out of").otherwise("outside"))

    fee = (t.where(F.col("year") == 2025).groupBy("service", "flow")
            .agg(F.count("*").alias("trips"), F.round(100 * F.avg((F.col("cbd_congestion_fee") > 0).cast("int")), 2).alias("pct_paid_fee")))
    fee.orderBy("service", "flow").toPandas().to_csv(R / "fee_check.csv", index=False)

    monthly = (t.groupBy("service", "year", "month", "flow")
                .agg(F.count("*").alias("trips"), F.sum("miles").alias("miles"), F.sum(F.col("minutes") / 60).alias("hours"),
                     F.sum("fare").alias("fare"), F.sum("cbd_congestion_fee").alias("cbd_fee")))
    m = monthly.orderBy("service", "year", "month", "flow").toPandas()
    m["mph"] = (m["miles"] / m["hours"]).round(3)
    m.to_csv(R / "flows_monthly.csv", index=False)
    # before and after, January-June: change inside the zone set against the change outside it (difference in differences)
    g = m.groupby(["service", "flow", "year"])[["trips", "miles", "hours", "fare"]].sum().unstack("year")
    d = pd.DataFrame({"trips_2024": g[("trips", 2024)], "trips_2025": g[("trips", 2025)],
                      "trips_change_pct": 100 * (g[("trips", 2025)] / g[("trips", 2024)] - 1),
                      "mph_2024": g[("miles", 2024)] / g[("hours", 2024)], "mph_2025": g[("miles", 2025)] / g[("hours", 2025)],
                      "fare_per_trip_2024": g[("fare", 2024)] / g[("trips", 2024)], "fare_per_trip_2025": g[("fare", 2025)] / g[("trips", 2025)]})
    d["mph_change_pct"] = 100 * (d["mph_2025"] / d["mph_2024"] - 1)
    for col in ["trips_change_pct", "mph_change_pct"]:
        d[col.replace("_pct", "_vs_outside_pts")] = d[col] - d.groupby(level="service")[col].transform(lambda x: x.xs("outside", level="flow").iloc[0])
    d.round(3).reset_index().to_csv(R / "before_after.csv", index=False)

    weekday = F.dayofweek("pickup").between(2, 6)
    hourly = (t.where(F.col("flow").isin("within", "outside"))
               .groupBy("year", "flow", weekday.alias("weekday"), F.hour("pickup").alias("hour"))
               .agg(F.count("*").alias("trips"), (F.sum("miles") / F.sum(F.col("minutes") / 60)).alias("mph")))
    hourly.orderBy("year", "flow", "weekday", "hour").toPandas().round({"mph": 3}).to_csv(R / "speed_by_hour.csv", index=False)
    print(cbd_pd[["zone", "zone_name", "intra_trips", "share_paid"]].to_string(index=False))
    print(open(R / "fee_check.csv").read())
    secs = time.perf_counter() - t0
    (R / "timing_metrics.json").write_text(json.dumps({"spark_seconds_curated_to_metrics": round(secs, 1)}, indent=2) + "\n")
    print(f"metrics in {secs:.0f} s")


if __name__ == "__main__":
    main()
