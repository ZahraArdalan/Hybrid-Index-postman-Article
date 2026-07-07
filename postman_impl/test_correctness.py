"""
test_correctness.py
====================
Sanity-checks that partition pruning + hybrid index querying does not
change *results*, only performance -- i.e. that global-index filtering and
local-index refining are safe optimizations, not approximations.
Run with: python3 test_correctness.py
"""
from shapely.geometry import Point, box

from loaders import read_csv_points, read_geojson
from partition_manager import PartitionManager
from hybrid_index import HybridIndex
from query_engine import QueryEngine


def brute_force_range(records, bbox):
    q = box(*bbox)
    return {r.id for r in records if r.geometry.intersects(q)}


def brute_force_knn(records, point_xy, k):
    q = Point(point_xy)
    ranked = sorted(records, key=lambda r: q.distance(r.geometry))
    return [r.id for r in ranked[:k]]


def main():
    raw = read_csv_points("data/nyc_poi.csv")
    all_records = raw.collect()

    pm = PartitionManager()
    ds = pm.repartition(raw)
    index = HybridIndex(ds)
    engine = QueryEngine(index)

    # --- range query correctness ---
    bbox = (-73.99, 40.75, -73.96, 40.79)
    results, stats = engine.range_query(bbox)
    got = {r.id for r in results}
    expected = brute_force_range(all_records, bbox)
    assert got == expected, f"range query mismatch: {len(got)} vs {len(expected)}"
    print(f"[OK] range_query matches brute force ({len(got)} results, "
          f"{stats.candidate_partitions}/{stats.total_partitions} partitions scanned)")

    # --- kNN correctness (ids, order-insensitive since ties are possible) ---
    point = (-73.9855, 40.7580)
    results, stats = engine.knn_query(point, k=10)
    got_ids = {r.id for r in results}
    expected_ids = set(brute_force_knn(all_records, point, 10))
    assert got_ids == expected_ids, f"knn mismatch: {got_ids} vs {expected_ids}"
    print(f"[OK] knn_query matches brute force (k=10, "
          f"{stats.candidate_partitions}/{stats.total_partitions} partitions scanned)")

    # --- join correctness ---
    boroughs_raw = read_geojson("data/nyc_boroughs.geojson")
    boroughs_ds = pm.repartition(boroughs_raw, block_size_bytes=4096)
    boroughs_index = HybridIndex(boroughs_ds)
    join_results, jstats = engine.join_query(boroughs_index, predicate="within")
    got_pairs = {(a.id, b.id) for a, b in join_results}
    all_boroughs = boroughs_raw.collect()
    expected_pairs = {(a.id, b.id) for a in all_records for b in all_boroughs if a.geometry.within(b.geometry)}
    assert got_pairs == expected_pairs, "join query mismatch"
    print(f"[OK] join_query matches brute force ({len(got_pairs)} pairs)")

    print("\nAll correctness tests passed.")


if __name__ == "__main__":
    main()
