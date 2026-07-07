"""
query_engine.py
================
Corresponds to Sect. 4.3 "Query Strategy" (filter-refine framework).

  i)  Range query   -> global-index filter, then local-index refine
  ii) kNN query     -> nearest-partitions-first with a safe pruning bound
                        (maxdist(q, mbr(R_l))), then local refine + merge
  iii) Vector-to-vector join -> distributed join: candidate *partition
                        pairs* whose MBRs intersect are joined pairwise
                        (a plane-scan / nested-loop between the two small
                        partitions), matching Sect 4.3.1.iii's "DJ".

Each query records simple telemetry (candidate vs total partitions) so the
demo can show the partition-pruning effect described throughout Sect. 4.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple

from shapely.geometry import Point, box

from st_dataset import Vector, VectorDataset
from hybrid_index import HybridIndex
from partition_manager import PartitionManager


@dataclass
class QueryStats:
    total_partitions: int
    candidate_partitions: int
    records_scanned: int
    results_count: int

    def __repr__(self):
        return (f"QueryStats(total_partitions={self.total_partitions}, "
                f"candidate_partitions={self.candidate_partitions} "
                f"[{self._pruned_pct():.1f}% pruned], "
                f"records_scanned={self.records_scanned}, "
                f"results={self.results_count})")

    def _pruned_pct(self):
        if self.total_partitions == 0:
            return 0.0
        return 100.0 * (1 - self.candidate_partitions / self.total_partitions)


class QueryEngine:
    def __init__(self, index: HybridIndex):
        self.index = index
        self.dataset = index.dataset

    # --- i) Range query ---------------------------------------------------
    def range_query(self, bbox: Tuple[float, float, float, float]) -> Tuple[List[Vector], QueryStats]:
        q = box(*bbox)
        # 1) filter with global index
        candidates = self.index.global_index.candidate_partitions(q)
        results: List[Vector] = []
        scanned = 0
        # 2) refine with local index (built lazily per candidate partition)
        for idx in candidates:
            self.index.ensure_local_index(idx)
            scanned += len(self.dataset.partitions[idx])
            results.extend(self.index.local_index.query(idx, q))
        stats = QueryStats(len(self.dataset.partitions), len(candidates), scanned, len(results))
        return results, stats

    # --- ii) kNN query ------------------------------------------------------
    def knn_query(self, point_xy: Tuple[float, float], k: int) -> Tuple[List[Vector], QueryStats]:
        """Implements the paper's safe-pruning-boundary approach (Sect 4.3.1.ii):
        rank partitions by maxdist(q, mbr) ascending, keep expanding the
        candidate set until we have >= k points *and* the current maxdist
        boundary is safe (i.e. no un-examined partition could contain a
        closer point), then refine locally and merge.
        """
        q = Point(point_xy)
        metas = self.dataset.partition_metadata

        def maxdist(mbr):
            # farthest corner of the mbr from q -- Eq. maxdist(q, mbr(Ri)) [40]
            minx, miny, maxx, maxy = mbr
            corners = [(minx, miny), (minx, maxy), (maxx, miny), (maxx, maxy)]
            return max(q.distance(Point(c)) for c in corners)

        def mindist(mbr):
            minx, miny, maxx, maxy = mbr
            dx = max(minx - q.x, 0, q.x - maxx)
            dy = max(miny - q.y, 0, q.y - maxy)
            return math.hypot(dx, dy)

        order = sorted(
            [i for i, m in enumerate(metas) if m.mbr is not None],
            key=lambda i: mindist(metas[i].mbr),
        )

        candidates: List[int] = []
        pcount_so_far = 0
        gamma = None
        for i in order:
            candidates.append(i)
            pcount_so_far += metas[i].pcount
            gamma = maxdist(metas[i].mbr)
            if pcount_so_far >= k:
                break

        scanned = 0
        pooled: List[Tuple[float, Vector]] = []
        for idx in candidates:
            self.index.ensure_local_index(idx)
            scanned += len(self.dataset.partitions[idx])
            pooled.extend(self.index.local_index.nearest(idx, q, k))
        pooled.sort(key=lambda t: t[0])
        results = [v for _, v in pooled[:k]]
        stats = QueryStats(len(metas), len(candidates), scanned, len(results))
        return results, stats

    # --- iii) vector-to-vector join query -----------------------------------
    def join_query(self, other_index: "HybridIndex", predicate: str = "intersects"
                   ) -> Tuple[List[Tuple[Vector, Vector]], QueryStats]:
        """Distributed join (DJ): find partition pairs whose MBRs intersect,
        then run a nested-loop join within each pair (fine at demo scale;
        a real deployment would use a plane-sweep algorithm as noted in
        Sect 4.3.1.iii)."""
        meta_a = self.dataset.partition_metadata
        meta_b = other_index.dataset.partition_metadata

        pairs = []
        for i, ma in enumerate(meta_a):
            if ma.mbr is None:
                continue
            bbox_a = box(*ma.mbr)
            for j, mb in enumerate(meta_b):
                if mb.mbr is None:
                    continue
                if bbox_a.intersects(box(*mb.mbr)):
                    pairs.append((i, j))

        results: List[Tuple[Vector, Vector]] = []
        scanned = 0
        for i, j in pairs:
            self.index.ensure_local_index(i)
            other_index.ensure_local_index(j)
            recs_a = self.dataset.partitions[i]
            recs_b = other_index.dataset.partitions[j]
            scanned += len(recs_a) + len(recs_b)
            for ra in recs_a:
                for rb in recs_b:
                    ok = getattr(ra.geometry, predicate)(rb.geometry)
                    if ok:
                        results.append((ra, rb))

        total_pairs = len(meta_a) * len(meta_b)
        stats = QueryStats(total_pairs, len(pairs), scanned, len(results))
        return results, stats
