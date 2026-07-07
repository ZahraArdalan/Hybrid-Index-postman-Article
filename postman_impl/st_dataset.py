"""
st_dataset.py
=============
Corresponds to Sect. 3.2 / 4 of the PostMan paper.

Since we don't have a real Spark cluster, an RDD is simulated as a plain
Python list of *partitions*, where each partition is itself a list of
`Element` objects. This preserves the important structural idea from the
paper: "the smallest granularity for data organization is a partition"
and STDataset = RDD[Element].

`STDataset` plays the role Spark RDD plays in the paper: it is a thin
"mixin" wrapper that adds partitioning / metadata / indexing behaviour on
top of a plain distributed (here: simulated) collection, instead of
requiring a new RDD subclass (à la Sedona's SpatialRDD) or patching Spark
itself (à la Simba). Concretely:

    RDDMixin        -> gives .partitions, .map(), .filter(), .collect()
    IndexableMixin  -> gives .partition_metadata, .global_index, .local_indexes
    STDataset(RDDMixin, IndexableMixin) -> the "code-mixing" STDataset (Sect 3.1)
    VectorDataset(STDataset) -> RDD[Vector]           (Sect 3.2)
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from shapely.geometry import shape, mapping
from shapely.geometry.base import BaseGeometry


# ---------------------------------------------------------------------------
# Element hierarchy  (Sect 3.2: "PostMan primarily supports two types of
# Elements: Vector and Raster.")  -- only Vector is implemented, Raster/Tile
# is out of scope for this reduced implementation.
# ---------------------------------------------------------------------------
@dataclass
class Vector:
    """A single spatio-temporal vector record.

    Mirrors the paper's description: "Vector Data consists of timestamps,
    spatial data (points, polylines, polygons), and additional attributes
    (IDs or version numbers)."
    """
    id: str
    geometry: BaseGeometry
    attributes: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[float] = None

    # --- helpers -----------------------------------------------------
    @property
    def mbr(self) -> Tuple[float, float, float, float]:
        return self.geometry.bounds  # (minx, miny, maxx, maxy)

    @property
    def vertex_count(self) -> int:
        """Approximates `vertices(r)` from Sect. 5 (pvertices)."""
        geom = self.geometry
        if geom.geom_type == "Point":
            return 1
        try:
            return len(list(geom.exterior.coords)) if geom.geom_type == "Polygon" else len(geom.coords)
        except Exception:
            return 1

    @property
    def size_bytes(self) -> int:
        """Rough estimate of `size(r)` (Sect. 5) used for psize accounting."""
        return 40 + 16 * self.vertex_count + len(json.dumps(self.attributes))

    def to_geojson_feature(self) -> dict:
        return {
            "type": "Feature",
            "geometry": mapping(self.geometry),
            "properties": {"id": self.id, "timestamp": self.timestamp, **self.attributes},
        }

    @staticmethod
    def from_geojson_feature(feat: dict) -> "Vector":
        props = dict(feat.get("properties", {}))
        _id = str(props.pop("id", uuid.uuid4()))
        ts = props.pop("timestamp", None)
        geom = shape(feat["geometry"])
        return Vector(id=_id, geometry=geom, attributes=props, timestamp=ts)


# ---------------------------------------------------------------------------
# RDD simulation layer
# ---------------------------------------------------------------------------
Partition = List[Vector]


class RDDMixin:
    """Simulates the subset of the Spark RDD API that PostMan relies on.

    Real PostMan extends the *actual* Spark RDD (`STDataset` "inherits from
    Spark RDD" per Sect. 2.2). Here, since there's no cluster, we simulate
    the partitioned-collection semantics directly in memory. Every method
    that would trigger a shuffle/stage in real Spark is written so that it
    is easy to see where a `.repartition()` / distributed shuffle would be
    required in a real deployment.
    """

    partitions: List[Partition]

    def collect(self) -> List[Vector]:
        return [e for part in self.partitions for e in part]

    def map(self, fn: Callable[[Vector], Vector]) -> "RDDMixin":
        new_parts = [[fn(e) for e in part] for part in self.partitions]
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__.update(self.__dict__)
        clone.partitions = new_parts
        return clone

    def filter(self, pred: Callable[[Vector], bool]) -> "RDDMixin":
        new_parts = [[e for e in part if pred(e)] for part in self.partitions]
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__.update(self.__dict__)
        clone.partitions = new_parts
        return clone

    def num_partitions(self) -> int:
        return len(self.partitions)

    def count(self) -> int:
        return sum(len(p) for p in self.partitions)


class STDataset(RDDMixin):
    """The core abstraction from Sect. 3.2.

    STDataset = RDD[Element] + partition metadata + hybrid index.
    Uses the "mixin" design pattern (composition over inheritance-of-Spark)
    so functional extensibility does not require modifying Spark's source
    (unlike Simba) nor introducing an incompatible RDD subtype (unlike
    Sedona's SpatialRDD).
    """

    def __init__(self, partitions: Optional[List[Partition]] = None):
        self.partitions: List[Partition] = partitions if partitions is not None else []
        # Populated by PartitionManager / HybridIndex -- see those modules.
        self.partition_metadata: List["PartitionMetadata"] = []  # noqa: F821 (fwd ref)
        self.global_index = None
        self.local_indexes: Dict[str, Any] = {}


class VectorDataset(STDataset):
    """STDataset implemented as RDD[Vector] (Sect. 3.2)."""

    def __repr__(self):
        return f"VectorDataset(partitions={len(self.partitions)}, records={self.count()})"

    @staticmethod
    def from_records(records: Iterable[Vector], n_partitions: int = 1) -> "VectorDataset":
        """Naive constructor: just chunk records evenly (hash-partition-like).
        Real spatial partitioning happens in `partition_manager.repartition`.
        """
        records = list(records)
        if n_partitions <= 1:
            return VectorDataset([records])
        chunk = max(1, len(records) // n_partitions)
        parts = [records[i:i + chunk] for i in range(0, len(records), chunk)]
        return VectorDataset(parts)
