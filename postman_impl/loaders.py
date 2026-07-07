"""
loaders.py
==========
Corresponds to Sect. 7.i "Load source data API": "Specify the file path
and file format and call different interfaces to read or generate
STDataset ... PostMan supports reading raw data from HDFS or OBS."

Here we just read from local disk, but the *shape* of the API
(`read_csv_points`, `read_geojson`) mirrors PostMan's format-specific
parsers mentioned in Sect. 3.2 Stage 1 ("PostMan provides specialized
parsers for different file formats").
"""
from __future__ import annotations

import csv
import json
from typing import List

from shapely.geometry import Point, shape

from st_dataset import Vector, VectorDataset


def read_csv_points(path: str, lon_col="lon", lat_col="lat", id_col="id",
                     ts_col="timestamp", n_partitions: int = 1) -> VectorDataset:
    records: List[Vector] = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            geom = Point(float(row[lon_col]), float(row[lat_col]))
            attrs = {k: v for k, v in row.items() if k not in (lon_col, lat_col, id_col, ts_col)}
            ts = float(row[ts_col]) if ts_col in row and row[ts_col] else None
            records.append(Vector(id=row.get(id_col, str(len(records))), geometry=geom,
                                   attributes=attrs, timestamp=ts))
    return VectorDataset.from_records(records, n_partitions=n_partitions)


def read_geojson(path: str, n_partitions: int = 1) -> VectorDataset:
    with open(path) as f:
        fc = json.load(f)
    records = [Vector.from_geojson_feature(feat) for feat in fc["features"]]
    return VectorDataset.from_records(records, n_partitions=n_partitions)
