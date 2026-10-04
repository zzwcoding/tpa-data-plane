"""票 07 · dp2 进料与新鲜度测试：validate→append→register 链 / unknown / 多管道取最新。

DP-03：旧湖替身类已随真身化物理移除——本文件改用 tests/conftest 的
FakeSnapshotCatalog（seam 合法位，只活在测试侧）；湖真身语义测试在 tests/dp1（PG16+MinIO），
进料端到端在 tests/dp2/test_ingest_real.py。"""
from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pytest

from app.data_plane.freshness import FreshnessInfo
from app.data_plane.ingest import InMemoryFreshnessRegistry, Ingest, Provenance
from app.data_plane.transform import RULES_OIL_DAILY, validate_batch

from tests.conftest import FakeSnapshotCatalog


def dirty_batch(rows=6, dup_at_end=True):
    r = [{"well_id": "N-01", "prod_date": f"2025-09-{d:02d}", "oil_output": 25.0 + d,
          "liquid_output": round((25.0 + d) / 0.801, 2), "water_cut": 10.0, "open_days": 24.0}
         for d in range(1, rows + 1)]  # 折算密度 ≈ 0.89（带内）
    if dup_at_end:
        r.append(dict(r[0]))  # A/B 班重复行
    return pa.Table.from_pylist(r)


class MemFreshness(InMemoryFreshnessRegistry):
    def __init__(self):
        self._reg = {}

    def register(self, table, tier, as_of, pipeline):
        self._reg[table] = FreshnessInfo(tier=tier, as_of=as_of, pipeline=pipeline)


@pytest.fixture()
def ingest():
    cat = FakeSnapshotCatalog()
    cat.create_table("oil_production_daily", schema=dirty_batch(1).schema,
                     comment="日产计量", partition_spec="day(prod_date)")
    ing = Ingest(catalog=cat, freshness=MemFreshness(),
                 rules={"oil_production_daily": RULES_OIL_DAILY})
    return ing, cat


class TestBatchIngest:
    def test_dirty_batch_rejected_not_appended(self, ingest):
        """DP-W-D2-1 前半：质检不合格批被拦（闸在编排层，append 不发生）。"""
        ing, cat = ingest
        from app.data_plane.ingest import BatchRejected
        with pytest.raises(BatchRejected) as ei:
            ing.run_batch_load("oil_production_daily", dirty_batch(),
                               Provenance(quality_report_id="r0", operator="阿强"))
        assert not cat.committed  # 脏批未入湖
        assert ei.value.report.rule_ids  # 闸裁决随拒绝携带（DP-05：GateVerdict）

    def test_clean_batch_appends_with_provenance_and_freshness(self, ingest):
        """DP-W-D2-1 后半 + D2-3：净批入湖带 provenance；freshness=offline 可读。"""
        ing, cat = ingest
        table = dirty_batch(dup_at_end=False)  # 净批（无重复行）
        verdict, cleaned = validate_batch(table, RULES_OIL_DAILY)
        assert verdict.passed
        snap = ing.run_batch_load("oil_production_daily", cleaned,
                                  Provenance(quality_report_id="QT-20260928-01", operator="阿强"))
        assert cat.committed[0]["provenance"]["quality_report_id"] == "QT-20260928-01"
        info = ing.freshness("oil_production_daily")
        assert info.tier == "offline" and info.pipeline == "batch-shift-report"

    def test_unknown_table_freshness(self, ingest):
        """DP-W-D2-3 后半：未登记表 freshness=unknown。"""
        ing, _ = ingest
        info = ing.freshness("no_such_table")
        assert info.tier == "unknown" and info.as_of is None

    def test_multi_pipeline_keeps_latest(self, ingest):
        """同表多管道登记 → 返回 as_of 最新一条（DP2 底稿 L52-64）。"""
        ing, _ = ingest
        ing.register_freshness("oil_production_daily", "offline", datetime(2026, 9, 26), "batch")
        ing.register_freshness("oil_production_daily", "realtime", datetime(2026, 9, 27, 10), "flink")
        assert ing.freshness("oil_production_daily").tier == "realtime"

    def test_cdc_spec_shape(self, ingest):
        """DP-W-D2-2：spec 形状（S1 零实装）。"""
        spec = ingest[0].cdc_pipeline_spec("oltp", ["oil_production_daily"])
        assert spec.sink_semantics.startswith("at-least-once") or "exactly-once" in spec.sink_semantics


class TestCsvIngestLineage:
    """DP-03：CSV 收编为进料输入——manifest 血缘（dp3 格式）随 commit 登记。"""

    def test_run_csv_builds_lineage_manifest(self, ingest, tmp_path):
        ing, cat = ingest
        run_dir = tmp_path / "run-0926"
        run_dir.mkdir()
        csv = run_dir / "oil_production_daily.csv"
        pa.csv.write_csv(dirty_batch(dup_at_end=False), csv)
        ing.run_csv_load("oil_production_daily", csv,
                         Provenance(quality_report_id="r1", operator="阿强"))
        meta = cat.committed[0]
        m = meta["manifest"]
        model = "model.petro.oil_production_daily"
        src = "source.petro.run-0926/oil_production_daily.csv"
        assert meta["source"] == "run-0926/oil_production_daily.csv"  # 缺省 source=父目录/文件名
        assert meta["batch_id"]  # 批次号缺省生成
        assert m["parent_map"][model] == [src]           # 湖表 ← 进料输入
        assert m["child_map"][src] == [model]            # 互逆
        assert m["nodes"][model]["resource_type"] == "model"
        assert m["nodes"][src]["resource_type"] == "source"

    def test_batch_id_passthrough(self, ingest):
        ing, cat = ingest
        ing.run_batch_load("oil_production_daily", dirty_batch(dup_at_end=False),
                           Provenance(quality_report_id="r2", operator="阿强"),
                           batch_id="b-fixed", source="fieldforge/x.csv")
        meta = cat.committed[0]
        assert meta["batch_id"] == "b-fixed"
        assert meta["manifest"]["metadata"]["batch_id"] == "b-fixed"


class TestFailClosedGate:
    """DP-01 收口（P2-01 盘点 A2-1/A2-6）：替身不许顶正式名；无规则=拒绝入湖。"""

    def test_no_alias_under_historical_name(self):
        """别名删除咬合：InMemory 替身不许顶在 FreshnessRegistry 正式名下（A2-1）。"""
        import app.data_plane.ingest as ingest_mod
        assert not hasattr(ingest_mod, "FreshnessRegistry"), \
            "替身兼容别名 FreshnessRegistry 已删除（DP-01），不许复活"

    def test_unruled_table_rejected_not_appended(self):
        """fail-closed：表不在规则清单 → 显式拒绝入湖，append 不发生（A2-6，DP-W-D2-1）。"""
        from app.data_plane.ingest import UnruledTableRejected
        cat = FakeSnapshotCatalog()
        cat.create_table("oil_production_daily", schema=dirty_batch(1).schema)
        ing = Ingest(catalog=cat, freshness=MemFreshness())  # 未配任何规则集
        with pytest.raises(UnruledTableRejected) as ei:
            ing.run_batch_load("oil_production_daily", dirty_batch(dup_at_end=False),
                               Provenance(quality_report_id="r0", operator="阿强"))
        assert not cat.committed  # 免检批未入湖
        assert ei.value.table == "oil_production_daily"

    def test_empty_rules_list_rejected_too(self):
        """fail-closed 边界：规则清单为空列表 = 同样无闸可用 → 拒绝。"""
        from app.data_plane.ingest import UnruledTableRejected
        cat = FakeSnapshotCatalog()
        cat.create_table("oil_production_daily", schema=dirty_batch(1).schema)
        ing = Ingest(catalog=cat, freshness=MemFreshness(), rules={"oil_production_daily": []})
        with pytest.raises(UnruledTableRejected):
            ing.run_batch_load("oil_production_daily", dirty_batch(dup_at_end=False),
                               Provenance(quality_report_id="r0", operator="阿强"))
        assert not cat.committed


class TestFreshnessAsOfFromSnapshot:
    """DP-04 修 P6（as_of 虚标——盘点 A 组点名 ingest now()）：freshness 登记的 as_of
    必须读**进料快照账的 committed_at**（manifest 随行落账时刻＝数据真实生效时刻），
    不是登记动作的墙钟 now()——登记晚于提交时不再虚标新鲜度。"""

    def test_as_of_equals_snapshot_committed_at(self):
        from datetime import datetime as dt, timezone

        fixed = dt(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc)

        class FixedClockCatalog(FakeSnapshotCatalog):
            def append(self, table, arrow, commit_meta):
                snap = super().append(table, arrow, commit_meta)
                from app.data_plane.lake_store import SnapshotInfo
                return SnapshotInfo(snapshot_id=snap.snapshot_id,
                                    committed_at=fixed.isoformat(), provenance=snap.provenance)

        cat = FixedClockCatalog()
        cat.create_table("oil_production_daily", schema=dirty_batch(1).schema)
        ing = Ingest(catalog=cat, freshness=MemFreshness(),
                     rules={"oil_production_daily": RULES_OIL_DAILY})
        snap = ing.run_batch_load("oil_production_daily", dirty_batch(dup_at_end=False),
                                  Provenance(quality_report_id="r6", operator="阿强"))
        info = ing.freshness("oil_production_daily")
        assert info.as_of == dt.fromisoformat(snap.committed_at) == fixed  # 快照账时刻
        assert info.as_of != dt.now().replace(microsecond=0)               # 不再虚标墙钟

