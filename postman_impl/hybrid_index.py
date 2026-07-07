"""
hybrid_index.py
================
Corresponds to Sect. 4.2 "Hybrid Index" of the paper.

  i)  Global Index -> STR-tree built over *partition* MBRs, kept in memory
      on the "driver" (here: just the current process). Used to prune
      partitions before touching any partition data (partition filtering).
  ii) Local Index  -> one STR-tree per partition, built lazily over the
      *records* of that partition, used to avoid a full scan once a
      partition has been selected as a candidate.

Simplification: shapely's `STRtree` is used for BOTH global and local
indexes. STRtree is itself a *bulk-loaded R-tree* (Sort-Tile-Recursive
construction), so this is a faithful stand-in for the paper's R-tree /
R*-tree based local index without depending on `libspatialindex`
(the `rtree` package), which isn't preinstalled in this sandbox.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from shapely.geometry import box
from shapely.strtree import STRtree

from st_dataset import Vector, VectorDataset
from partition_manager import PartitionManager, PartitionMetadata


class GlobalIndex:
    """Sect 4.2.i. Prunes partitions unrelated to a query."""

    def __init__(self, metadata: List[PartitionMetadata]):
        self.metadata = metadata
        boxes = []
        self._idx_to_partition = []
        for i, m in enumerate(metadata):
            if m.mbr is None:
                continue
            boxes.append(box(*m.mbr))
            self._idx_to_partition.append(i)
        self._tree = STRtree(boxes) if boxes else None

    def candidate_partitions(self, query_geom) -> List[int]:
        """Returns indices of partitions whose MBR intersects `query_geom`."""
        if self._tree is None:
            return []
        hits = self._tree.query(query_geom, predicate="intersects")
        return sorted({self._idx_to_partition[h] for h in hits})

    def persist(self, path: str) -> None:
        """Sect 4.2.i: "PostMan supports two methods for maintaining the
        global index on disk ... 1) storing it as a persistent partition
        metadata table file". Here we just dump the metadata (the index
        itself is trivially rebuilt from the MBRs on load)."""
        import json
        from dataclasses import asdict
        with open(path, "w") as f:
            json.dump([asdict(m) for m in self.metadata], f)


class LocalIndex:
    """Sect 4.2.ii. One STR-tree per partition, built over that
    partition's records. Built lazily (only when the partition is
    actually a query candidate), mirroring "PostMan opts to build an
    index tree directly from all data in a single partition" combined
    with partition filtering."""

    def __init__(self):
        self._trees: Dict[int, Tuple[STRtree, List[Vector]]] = {}

    def build(self, partition_idx: int, records: List[Vector]) -> None:
        geoms = [r.geometry for r in records]
        tree = STRtree(geoms) if geoms else None
        self._trees[partition_idx] = (tree, records)

    def is_built(self, partition_idx: int) -> bool:
        return partition_idx in self._trees

    def query(self, partition_idx: int, query_geom) -> List[Vector]:
        tree, records = self._trees[partition_idx]
        if tree is None:
            return []
        hits = tree.query(query_geom, predicate="intersects")
        return [records[h] for h in hits]

    def nearest(self, partition_idx: int, point, k: int) -> List[Tuple[float, Vector]]:
        tree, records = self._trees[partition_idx]
        if tree is None:
            return []
        # shapely STRtree.nearest gives 1; do a manual top-k via distance sort
        # (fine at demo scale; a real impl would use tree.query + partial sort).
        dists = [(point.distance(r.geometry), r) for r in records]
        dists.sort(key=lambda t: t[0])
        return dists[:k]


class HybridIndex:
    """Ties GlobalIndex + LocalIndex + lazy partition loading together,
    i.e. the object query_engine.py actually talks to."""

    def __init__(self, dataset: VectorDataset):
        self.dataset = dataset
        self.global_index = GlobalIndex(dataset.partition_metadata)
        self.local_index = LocalIndex()
        dataset.global_index = self.global_index
        dataset.local_indexes = self.local_index._trees

    def ensure_local_index(self, partition_idx: int) -> None:
        if self.local_index.is_built(partition_idx):
            return
        records = PartitionManager.load_partition_data(self.dataset, partition_idx)
        self.local_index.build(partition_idx, records)

    def stats(self) -> dict:
        return {
            "num_partitions": len(self.dataset.partition_metadata),
            "local_indexes_built": len(self.local_index._trees),
        }
