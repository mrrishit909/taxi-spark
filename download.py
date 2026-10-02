"""Step 1: download NYC TLC trip records for January-June of 2024 (before congestion pricing) and 2025 (after).

    ./venv/bin/python download.py   -> data/raw/{yellow,fhvhv}_tripdata_YYYY-MM.parquet, taxi_zone_lookup.csv (not committed)

Yellow taxis and high-volume for-hire vehicles (Uber, Lyft, Via, Juno). Congestion pricing began on 5 January 2025.
"""
import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).parent
RAW = HERE / "data" / "raw"
BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
FILES = [f"{kind}_tripdata_{y}-{m:02d}.parquet" for kind in ("yellow", "fhvhv") for y in (2024, 2025) for m in range(1, 7)]
ZONES = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def fetch(name, url=None):
    out = RAW / name
    if not out.exists():
        tmp = RAW / (name + ".part")
        with urllib.request.urlopen(url or BASE + name, timeout=3600) as r, open(tmp, "wb") as f:
            while chunk := r.read(1 << 20):
                f.write(chunk)
        tmp.rename(out)
    print(name, out.stat().st_size // 1_000_000, "MB", flush=True)
    return out.stat().st_size


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    fetch("taxi_zone_lookup.csv", ZONES)
    with ThreadPoolExecutor(4) as pool:
        sizes = list(pool.map(fetch, FILES))
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "pull.json").write_text(json.dumps({"source": "NYC TLC trip record data", "files": FILES,
                                                            "total_mb": round(sum(sizes) / 1e6)}, indent=2) + "\n")


if __name__ == "__main__":
    main()
