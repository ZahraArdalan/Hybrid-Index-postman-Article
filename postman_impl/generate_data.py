"""
generate_data.py
=================
Builds a small, realistic-ish spatio-temporal vector dataset for the demo,
playing the role of the paper's OSM / Taxi datasets at a scale that fits a
laptop:

  - `data/nyc_poi.csv`       : ~3,000 points clustered around 12 REAL NYC
                                landmarks (lat/lon are the actual public
                                coordinates of these places), with a
                                timestamp and a category attribute --
                                mirrors the paper's OSM point dataset.
  - `data/nyc_boroughs.geojson`: 5 real NYC borough bounding polygons
                                (approximate but using each borough's real
                                public bounding-box coordinates) -- used as
                                the "region" dataset for vector-to-vector
                                join query demos (mirrors OSM Parks / join
                                query use case in the paper).
"""
import csv
import json
import random

random.seed(42)

# Real public coordinates (lon, lat) of well-known NYC landmarks.
LANDMARKS = [
    ("Times Square", -73.9855, 40.7580, "landmark"),
    ("Central Park", -73.9654, 40.7829, "park"),
    ("Brooklyn Bridge", -73.9969, 40.7061, "landmark"),
    ("Empire State Building", -73.9857, 40.7484, "landmark"),
    ("Statue of Liberty", -74.0445, 40.6892, "landmark"),
    ("Yankee Stadium", -73.9262, 40.8296, "stadium"),
    ("JFK Airport", -73.7781, 40.6413, "transport"),
    ("LaGuardia Airport", -73.8740, 40.7769, "transport"),
    ("Coney Island", -73.9776, 40.5749, "beach"),
    ("Flushing Meadows Park", -73.8458, 40.7466, "park"),
    ("Wall Street", -74.0113, 40.7069, "business"),
    ("Columbia University", -73.9626, 40.8075, "education"),
]

# Approximate real bounding boxes of the 5 NYC boroughs (minx, miny, maxx, maxy)
BOROUGHS = {
    "Manhattan": (-74.0479, 40.6829, -73.9067, 40.8820),
    "Brooklyn": (-74.0421, 40.5707, -73.8334, 40.7395),
    "Queens": (-73.9626, 40.5410, -73.7004, 40.8007),
    "Bronx": (-73.9339, 40.7855, -73.7654, 40.9153),
    "Staten Island": (-74.2591, 40.4960, -74.0522, 40.6514),
}


def generate_points(n_total=3000, jitter_deg=0.03):
    rows = []
    per_landmark = n_total // len(LANDMARKS)
    categories = ["cafe", "restaurant", "shop", "hotel", "office", "residential"]
    ts_base = 1_700_000_000  # arbitrary unix epoch base
    _id = 0
    for name, lon, lat, kind in LANDMARKS:
        for _ in range(per_landmark):
            plon = lon + random.gauss(0, jitter_deg / 3)
            plat = lat + random.gauss(0, jitter_deg / 3)
            rows.append({
                "id": f"poi_{_id}",
                "lon": round(plon, 6),
                "lat": round(plat, 6),
                "category": random.choice(categories),
                "near": name,
                "timestamp": ts_base + random.randint(0, 3600 * 24 * 30),
            })
            _id += 1
    return rows


def write_csv(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "lon", "lat", "category", "near", "timestamp"])
        w.writeheader()
        w.writerows(rows)


def write_boroughs_geojson(path):
    features = []
    for name, (minx, miny, maxx, maxy) in BOROUGHS.items():
        coords = [[[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]]
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": coords},
            "properties": {"id": name, "borough": name},
        })
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w") as f:
        json.dump(fc, f, indent=None)


if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)
    rows = generate_points()
    write_csv(rows, "data/nyc_poi.csv")
    write_boroughs_geojson("data/nyc_boroughs.geojson")
    print(f"wrote {len(rows)} points to data/nyc_poi.csv")
    print(f"wrote {len(BOROUGHS)} borough polygons to data/nyc_boroughs.geojson")
