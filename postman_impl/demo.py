"""
demo.py
=======
End-to-end demo of the reduced PostMan implementation, covering Sect. 4
(Unified Partition Management + Hybrid Index) plus the filter-refine query
strategies of Sect. 4.3, run on the small NYC POI / borough dataset.

Run with:  python3 demo.py
"""
import time

from loaders import read_csv_points, read_geojson
from partition_manager import PartitionManager
from hybrid_index import HybridIndex
from query_engine import QueryEngine
from st_dataset import Vector
from shapely.geometry import Point


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main():
    # ---------------------------------------------------------------
    section("Stage 1: Loading (Sect 3.2 / 7.i) -- raw CSV -> STDataset")
    # ---------------------------------------------------------------
    poi_raw = read_csv_points("data/nyc_poi.csv", n_partitions=1)
    print(f"Loaded raw dataset: {poi_raw}")

    # ---------------------------------------------------------------
    section("Sect 4.1.ii: Repartition (STR-style spatial partitioning)")
    # ---------------------------------------------------------------
    pm = PartitionManager()
    poi_ds = pm.repartition(poi_raw)
    print(f"Repartitioned into {poi_ds.num_partitions()} partitions, {poi_ds.count()} records total")
    for meta in poi_ds.partition_metadata[:5]:
        print(f"  {meta.partition_id}: pcount={meta.pcount:4d} psize={meta.psize:6d}B "
              f"mbr={tuple(round(x, 4) for x in meta.mbr)} types={meta.object_types}")
    if poi_ds.num_partitions() > 5:
        print(f"  ... ({poi_ds.num_partitions() - 5} more partitions)")

    # ---------------------------------------------------------------
    section("Sect 4.1.iii: Partition persistence & reloading (lazy load)")
    # ---------------------------------------------------------------
    out_dir = "data/_persisted_poi"
    pm.persist(poi_ds, out_dir)
    print(f"Persisted partitions + metadata JSON to {out_dir}/")
    reloaded = pm.reload(out_dir, lazy=True)
    print(f"Reloaded {reloaded.num_partitions()} partitions from metadata only "
          f"(no partition *data* file has been read yet)")

    # ---------------------------------------------------------------
    section("Sect 4.2: Hybrid Index (global STR-tree + lazy local indexes)")
    # ---------------------------------------------------------------
    index = HybridIndex(reloaded)
    print(f"Global index built over {len(index.global_index.metadata)} partition MBRs.")
    print(f"Local indexes built so far: {index.stats()['local_indexes_built']} "
          f"(built lazily, only when a partition becomes a query candidate)")

    engine = QueryEngine(index)

    # ---------------------------------------------------------------
    section("Sect 4.3.1.i: Range query (filter by global index, refine locally)")
    # ---------------------------------------------------------------
    # bbox roughly covering Times Square + Central Park area
    bbox = (-73.99, 40.75, -73.96, 40.79)
    t0 = time.time()
    results, stats = engine.range_query(bbox)
    dt = time.time() - t0
    print(f"query bbox={bbox}")
    print(stats)
    print(f"elapsed: {dt*1000:.2f} ms, sample result ids: {[r.id for r in results[:5]]}")

    # ---------------------------------------------------------------
    section("Sect 4.3.1.ii: kNN query (safe pruning boundary via maxdist)")
    # ---------------------------------------------------------------
    query_point = (-73.9855, 40.7580)  # Times Square
    t0 = time.time()
    results, stats = engine.knn_query(query_point, k=10)
    dt = time.time() - t0
    print(f"query point={query_point}, k=10")
    print(stats)
    print(f"elapsed: {dt*1000:.2f} ms, nearest ids: {[r.id for r in results]}")

    # ---------------------------------------------------------------
    section("Sect 4.3.1.iii: Vector-to-vector join query (distributed join)")
    # ---------------------------------------------------------------
    boroughs_raw = read_geojson("data/nyc_boroughs.geojson", n_partitions=1)
    boroughs_ds = pm.repartition(boroughs_raw, block_size_bytes=4096)
    boroughs_index = HybridIndex(boroughs_ds)
    t0 = time.time()
    join_results, stats = engine.join_query(boroughs_index, predicate="within")
    dt = time.time() - t0
    print(f"Join: nyc_poi WITHIN nyc_boroughs")
    print(stats)
    counts = {}
    for poi, borough in join_results:
        counts[borough.attributes.get("borough", borough.id)] = counts.get(
            borough.attributes.get("borough", borough.id), 0) + 1
    print("POI count per borough (join result):")
    for b, c in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {b:15s}: {c}")

    # ---------------------------------------------------------------
    section("Sect 4.1.iv: Incremental partition update (R*-tree insertion)")
    # ---------------------------------------------------------------
    before = poi_ds.num_partitions()
    new_pts = [
        Vector(id=f"new_{i}",
               geometry=Point(-73.9855 + 0.001 * i, 40.7580 + 0.001 * i),
               attributes={"category": "pop-up"}, timestamp=1_700_100_000)
        for i in range(50)
    ]
    pm.insert_records(poi_ds, new_pts)
    print(f"Inserted {len(new_pts)} new records near Times Square.")
    print(f"Partitions before={before}, after={poi_ds.num_partitions()} "
          f"(overflowing partitions get split, Sect 4.1.iv)")
    print(f"Total records now: {poi_ds.count()}")


if __name__ == "__main__":
    main()
