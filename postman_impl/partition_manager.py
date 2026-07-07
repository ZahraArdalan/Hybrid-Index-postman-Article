"""
partition_manager.py
=====================
Corresponds to Sect. 4.1 "Unified Partition Management" of the paper.

Implements:
  i)   Partition metadata schema           -> PartitionMetadata / PartitionMetadataScheme
  ii)  Repartition method                  -> repartition() (STR-based spatial partitioning)
  iii) Partition persistence & reloading   -> persist() / reload()   (FilePartition)
  iv)  Incremental partition updates       -> insert_records() (R*-tree-style additional update)

Scaled-down choices (explicitly out of scope for this quick implementation):
  - No temporal partitioning phase (paper does time-then-space); we do
    space-only partitioning since our demo dataset is not heavily temporal.
  - TPSP's greedy *allocation to executors* (Sect. 5) is not implemented
    here since there are no real executors in a single-machine demo; only
    the *partition generation* half is used (an STR/R*-tree-like split),
    which is also the technique unified_partition_management relies on.
  - LSM-tree "merged updating" path (iv, second method) is skipped; only
    the R*-tree "additional update" path is implemented.
"""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import box

from st_dataset import Vector, VectorDataset, Partition

BLOCK_SIZE_BYTES = 128 * 1024 * 1024  # 128MB, matches HDFS block size (Sect. 4)
# Since our demo dataset is tiny, we use a much smaller "logical" block size
# so that repartitioning actually produces multiple partitions. This models
# the *same algorithm* at a scale that fits a laptop-sized demo.
DEMO_BLOCK_SIZE_BYTES = 20 * 1024  # 20KB "logical block" for demo purposes


def _mbr_union(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))


# ---------------------------------------------------------------------------
# i) Partition metadata schema
# ---------------------------------------------------------------------------
class PartitionMetadataScheme:
    """User-extensible interface (Sect 4.1.i): "Users can define their own
    partition metadata schema by implementing the PartitionMetadataScheme
    interface." Subclass and override `extra_stats()` to add custom fields
    (e.g. for cardinality estimation / execution-plan optimization)."""

    def extra_stats(self, records: List[Vector]) -> Dict[str, Any]:
        return {}


@dataclass
class PartitionMetadata:
    """Basic info + VectorDataset-specific stats (Sect 4.1.i)."""
    partition_id: str
    file_path: Optional[str] = None
    access_permissions: str = "rw"
    mbr: Optional[Tuple[float, float, float, float]] = None      # spatial MBR
    object_types: List[str] = field(default_factory=list)       # geometry types present
    temporal_range: Optional[Tuple[float, float]] = None
    attribute_ranges: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)
    pcount: int = 0
    psize: int = 0            # bytes
    pvertices: int = 0
    crs: str = "EPSG:4326"
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d)

    @staticmethod
    def from_json(s: str) -> "PartitionMetadata":
        d = json.loads(s)
        d["mbr"] = tuple(d["mbr"]) if d.get("mbr") else None
        if d.get("temporal_range"):
            d["temporal_range"] = tuple(d["temporal_range"])
        return PartitionMetadata(**d)

    @staticmethod
    def build(partition_id: str, records: List[Vector],
              scheme: Optional[PartitionMetadataScheme] = None,
              file_path: Optional[str] = None) -> "PartitionMetadata":
        mbr = None
        types = set()
        pvertices = 0
        psize = 0
        ts_min, ts_max = None, None
        attr_ranges: Dict[str, Tuple[Any, Any]] = {}
        for r in records:
            mbr = _mbr_union(mbr, r.mbr)
            types.add(r.geometry.geom_type)
            pvertices += r.vertex_count
            psize += r.size_bytes
            if r.timestamp is not None:
                ts_min = r.timestamp if ts_min is None else min(ts_min, r.timestamp)
                ts_max = r.timestamp if ts_max is None else max(ts_max, r.timestamp)
            for k, v in r.attributes.items():
                if isinstance(v, (int, float)):
                    lo, hi = attr_ranges.get(k, (v, v))
                    attr_ranges[k] = (min(lo, v), max(hi, v))
        pm = PartitionMetadata(
            partition_id=partition_id, file_path=file_path, mbr=mbr,
            object_types=sorted(types), temporal_range=(ts_min, ts_max) if ts_min is not None else None,
            attribute_ranges=attr_ranges, pcount=len(records), psize=psize, pvertices=pvertices,
        )
        if scheme:
            pm.extra = scheme.extra_stats(records)
        return pm


class FilePartition:
    """Sect 4.1.iii: "represents a file-based partition. It stores
    information such as the partition ID, file storage path, offset, and
    file size" -- used as the mapping between an STDataset partition and
    its on-disk file, without needing to read the data itself."""

    def __init__(self, partition_id: str, file_path: str, offset: int = 0, file_size: int = 0):
        self.partition_id = partition_id
        self.file_path = file_path
        self.offset = offset
        self.file_size = file_size


# ---------------------------------------------------------------------------
# ii) Repartition method: enhanced R*-Tree-ish / STR spatial partitioning
# ---------------------------------------------------------------------------
def _str_partition(records: List[Vector], target_count_per_partition: int) -> List[Partition]:
    """A Sort-Tile-Recursive (STR) style spatial partitioner.

    This is the same family of algorithm the paper cites for spatial
    partitioning (STR [16], and the improved R*-Tree used by TPSP, Sect 5.1)
    -- objects are sorted along one axis into vertical slices, then each
    slice is sorted along the other axis and cut into partitions. This
    keeps partitions spatially compact (low MBR overlap) which is the
    property PostMan's global/local index relies on.
    """
    n = len(records)
    if n == 0:
        return []
    n_partitions = max(1, round(n / max(1, target_count_per_partition)))
    slice_count = max(1, round(n_partitions ** 0.5))

    # sort by centroid x, split into vertical slices
    by_x = sorted(records, key=lambda r: r.geometry.centroid.x)
    slice_size = max(1, -(-n // slice_count))  # ceil
    slices = [by_x[i:i + slice_size] for i in range(0, n, slice_size)]

    partitions: List[Partition] = []
    per_slice_parts = max(1, round(n_partitions / max(1, len(slices))))
    for sl in slices:
        by_y = sorted(sl, key=lambda r: r.geometry.centroid.y)
        part_size = max(1, -(-len(sl) // per_slice_parts))
        for i in range(0, len(sl), part_size):
            partitions.append(by_y[i:i + part_size])
    return partitions


class PartitionManager:
    """Facade tying together metadata construction, repartitioning and
    persistence/reload/incremental-update for a VectorDataset."""

    def __init__(self, scheme: Optional[PartitionMetadataScheme] = None):
        self.scheme = scheme or PartitionMetadataScheme()

    # --- ii) repartition -------------------------------------------------
    def repartition(self, dataset: VectorDataset, block_size_bytes: int = DEMO_BLOCK_SIZE_BYTES) -> VectorDataset:
        records = dataset.collect()
        if not records:
            return VectorDataset([])
        avg_size = sum(r.size_bytes for r in records) / len(records)
        target_count = max(1, int(block_size_bytes / max(1, avg_size)))
        new_partitions = _str_partition(records, target_count)

        ds = VectorDataset(new_partitions)
        ds.partition_metadata = [
            PartitionMetadata.build(f"p{i}", part, self.scheme)
            for i, part in enumerate(new_partitions)
        ]
        return ds

    # --- iii) persistence & reloading -------------------------------------
    def persist(self, dataset: VectorDataset, out_dir: str) -> None:
        """Writes each partition to its own GeoJSON file + a metadata.json
        file in the same directory, per Sect. 4.1.iii."""
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        metas = []
        for i, part in enumerate(dataset.partitions):
            pid = f"p{i}"
            fp = os.path.join(out_dir, f"{pid}.geojson")
            fc = {"type": "FeatureCollection", "features": [r.to_geojson_feature() for r in part]}
            with open(fp, "w") as f:
                json.dump(fc, f)
            pm = PartitionMetadata.build(pid, part, self.scheme, file_path=fp)
            metas.append(pm)
        with open(os.path.join(out_dir, "_metadata.json"), "w") as f:
            json.dump([asdict(m) for m in metas], f, indent=None)

    def reload(self, out_dir: str, lazy: bool = True) -> VectorDataset:
        """Reads partition *metadata* first (cheap) without touching the
        actual partition data files, per Sect. 4.1.iii: "PostMan first
        reads the partition metadata ... and does not read the data.
        Instead, it uses FilePartition to establish a mapping". Data is
        only loaded lazily when a query actually needs that partition
        (see hybrid_index.HybridIndex.load_partition)."""
        with open(os.path.join(out_dir, "_metadata.json")) as f:
            raw_metas = json.load(f)
        metas = []
        for d in raw_metas:
            d["mbr"] = tuple(d["mbr"]) if d.get("mbr") else None
            if d.get("temporal_range"):
                d["temporal_range"] = tuple(d["temporal_range"])
            metas.append(PartitionMetadata(**d))

        ds = VectorDataset([[] if lazy else None for _ in metas])
        ds.partition_metadata = metas
        ds.extra = {"_reload_dir": out_dir, "_lazy_loaded": [not lazy] * len(metas)}
        return ds

    @staticmethod
    def load_partition_data(dataset: VectorDataset, idx: int) -> Partition:
        """Materializes partition `idx`'s actual records from disk on demand.

        If `dataset` was produced directly by `repartition()` (not via
        `reload()`), its partitions are already fully in memory -- there is
        no `extra`/lazy-loading bookkeeping, so we just return them as-is.
        """
        if not getattr(dataset, "extra", None):
            return dataset.partitions[idx]
        if dataset.extra["_lazy_loaded"][idx]:
            return dataset.partitions[idx]
        meta = dataset.partition_metadata[idx]
        with open(meta.file_path) as f:
            fc = json.load(f)
        records = [Vector.from_geojson_feature(feat) for feat in fc["features"]]
        dataset.partitions[idx] = records
        if getattr(dataset, "extra", None):
            dataset.extra["_lazy_loaded"][idx] = True
        return records

    # --- iv) incremental updates (R*-tree "additional update" path) -------
    def insert_records(self, dataset: VectorDataset, new_records: List[Vector],
                        block_size_bytes: int = DEMO_BLOCK_SIZE_BYTES) -> None:
        """(1) Data Refreshing: assign each new record to the partition
        whose MBR needs the least enlargement (R*-tree node-insertion
        heuristic), then append. (2) Partition Reorganization: split any
        partition that overflows the block-size threshold."""
        for rec in new_records:
            best_idx, best_enlargement = None, None
            for i, meta in enumerate(dataset.partition_metadata):
                if meta.mbr is None:
                    enlargement = 0.0
                else:
                    union = _mbr_union(meta.mbr, rec.mbr)
                    area_before = (meta.mbr[2] - meta.mbr[0]) * (meta.mbr[3] - meta.mbr[1])
                    area_after = (union[2] - union[0]) * (union[3] - union[1])
                    enlargement = area_after - area_before
                if best_enlargement is None or enlargement < best_enlargement:
                    best_idx, best_enlargement = i, enlargement
            if best_idx is None:
                dataset.partitions.append([])
                dataset.partition_metadata.append(PartitionMetadata.build(f"p{len(dataset.partitions)-1}", []))
                best_idx = len(dataset.partitions) - 1

            PartitionManager.load_partition_data(dataset, best_idx)
            dataset.partitions[best_idx].append(rec)
            dataset.partition_metadata[best_idx] = PartitionMetadata.build(
                dataset.partition_metadata[best_idx].partition_id,
                dataset.partitions[best_idx], self.scheme,
                file_path=dataset.partition_metadata[best_idx].file_path,
            )

        # (2) Partition Reorganization: split overflowing partitions
        i = 0
        while i < len(dataset.partitions):
            meta = dataset.partition_metadata[i]
            if meta.psize > block_size_bytes:
                records = dataset.partitions[i]
                sub_parts = _str_partition(records, max(1, len(records) // 2))
                dataset.partitions.pop(i)
                dataset.partition_metadata.pop(i)
                for j, sp in enumerate(sub_parts):
                    dataset.partitions.insert(i + j, sp)
                    dataset.partition_metadata.insert(
                        i + j, PartitionMetadata.build(f"p{i+j}_split", sp, self.scheme))
                i += len(sub_parts)
            else:
                i += 1
