"""票 06 · dp1 湖底座测试：InMemoryLake 语义（schema 四件齐/append-only/快照隔离/裁剪/写面审计）。"""
from __future__ import annotations

import pyarrow as pa
import pytest

from app.data_plane.lake_store import (BoundaryViolation, InMemoryLake, SchemaInfo,
                                       SnapshotGoneError, SnapshotInfo)


def rows_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows)


def sample(days=3):
    return rows_table([
        {"well_id": "N-01", "prod_date": f"2025-09-{d:02d}", "oil_output": 20.0 + d}
        for d in range(1, days + 1)
    ])


@pytest.fixture()
def lake():
    lake = InMemoryLake()
    cat = lake.load_catalog("jdbc")
    cat.create_table("oil_production_daily",
                     schema=sample(1).schema,
                     comment="日产计量",
                     partition_spec="day(prod_date)")
    return lake, cat


class TestSchema:
    def test_schema_info_four_pieces(self, lake):
        _, cat = lake
        t = cat.load_table("oil_production_daily")
        s: SchemaInfo = t.schema()
        assert [c[0] for c in s.columns] == ["well_id", "prod_date", "oil_output"]
        assert s.comment == "日产计量"
        assert s.partition_spec == "day(prod_date)"
        assert s.current_snapshot is not None  # DP-W-D1-1 四件齐

    def test_unknown_table_raises(self, lake):
        _, cat = lake
        with pytest.raises(KeyError):
            cat.load_table("no_such_table")


class TestAppendAndSnapshots:
    def test_append_isolation(self, lake):
        """DP-W-D1-2：两次 append，按旧快照读=旧视图（不脏读）。"""
        _, cat = lake
        cat.begin_caller("dp2.ingest")
        t = cat.load_table("oil_production_daily")
        s1 = cat.append("oil_production_daily", sample(2), {"provenance": "batch-1"})
        old_view = t.scan(snapshot_id=s1.snapshot_id)
        assert old_view.num_rows == 2
        s2 = cat.append("oil_production_daily", sample(1), {"provenance": "batch-2"})
        assert s2.snapshot_id != s1.snapshot_id
        assert t.scan(snapshot_id=s1.snapshot_id).num_rows == 2  # 旧快照仍是 2 行
        assert t.scan(snapshot_id=s2.snapshot_id).num_rows == 3  # 新快照 3 行
        assert t.scan().num_rows == 3                            # 当前视图

    def test_scan_predicate_and_projection(self, lake):
        """DP-W-D1-3：谓词 + 字段子集；stats 可解释。"""
        _, cat = lake
        cat.begin_caller("dp2.ingest")
        cat.append("oil_production_daily", sample(3), {})
        t = cat.load_table("oil_production_daily")
        out = t.scan(pred="prod_date >= '2025-09-02'", fields=("well_id", "oil_output"))
        assert out.num_rows == 2
        assert out.column_names == ["well_id", "oil_output"]
        stats = t.metrics()
        assert stats.min_max["oil_output"][0] == 21.0  # min
        assert stats.null_count["oil_output"] == 0

    def test_gone_snapshot_explicit(self, lake):
        _, cat = lake
        t = cat.load_table("oil_production_daily")
        with pytest.raises(SnapshotGoneError):
            t.scan(snapshot_id=99999)


class TestWriteSurface:
    def test_append_caller_allowlist(self, lake):
        """DP-W-D1-4：append 仅 dp2/dp3 可调，越界=BoundaryViolation。"""
        _, cat = lake
        cat.begin_caller("dp2.ingest")  # 白名单身份：放行
        cat.append("oil_production_daily", sample(1), {})
        with pytest.raises(BoundaryViolation):
            cat.begin_caller("dp8.tools")  # 越界身份：拒
            cat.append("oil_production_daily", sample(1), {})

    def test_no_delete_overwrite_surface(self):
        """append-only：门面不暴露 overwrite/delete/upsert。"""
        from app.data_plane.lake_store import InMemoryLake
        cat = InMemoryLake().load_catalog("jdbc")
        assert not hasattr(cat, "overwrite")
        assert not hasattr(cat, "delete_from")
