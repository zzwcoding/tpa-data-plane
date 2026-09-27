"""票 07 · dp2 进料与新鲜度测试：validate→append→register 链 / unknown / 多管道取最新。"""
from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pytest

from app.data_plane.freshness import FreshnessInfo
from app.data_plane.ingest import FreshnessRegistry, Ingest, Provenance
from app.data_plane.transform import RULES_OIL_DAILY, validate_batch


def dirty_batch(rows=6, dup_at_end=True):
    r = [{"well_id": "N-01", "prod_date": f"2025-09-{d:02d}", "oil_output": 25.0 + d,
          "liquid_output": round((25.0 + d) / 0.801, 2), "water_cut": 10.0, "open_days": 24.0}
         for d in range(1, rows + 1)]  # 折算密度 ≈ 0.89（带内）
    if dup_at_end:
        r.append(dict(r[0]))  # A/B 班重复行
    return pa.Table.from_pylist(r)


class MemCatalog:
    """dp1 门面替身：记账 committed（无 .cat 属性，防 Ingest 解包绕过记账）。"""

    def __init__(self):
        self.committed = []
        from app.data_plane.lake_store import InMemoryLake
        self._lake = InMemoryLake()
        self._cat = self._lake.load_catalog("jdbc")
        self._cat.create_table("oil_production_daily", schema=dirty_batch(1).schema,
                               comment="日产计量", partition_spec="day(prod_date)")

    def begin_caller(self, caller):
        self._cat.begin_caller(caller)

    def load_table(self, name):
        return self._cat.load_table(name)

    def append(self, table, arrow, commit_meta):
        self.committed.append(commit_meta)
        return self._cat.append(table, arrow, commit_meta)


class MemFreshness(FreshnessRegistry):
    def __init__(self):
        self._reg = {}

    def register(self, table, tier, as_of, pipeline):
        self._reg[table] = FreshnessInfo(tier=tier, as_of=as_of, pipeline=pipeline)


@pytest.fixture()
def ingest():
    cat = MemCatalog()
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
        assert ei.value.report.report_id  # 质检报告随拒绝携带

    def test_clean_batch_appends_with_provenance_and_freshness(self, ingest):
        """DP-W-D2-1 后半 + D2-3：净批入湖带 provenance；freshness=offline 可读。"""
        ing, cat = ingest
        table = dirty_batch(dup_at_end=False)  # 净批（无重复行）
        report, cleaned = validate_batch(table, RULES_OIL_DAILY)
        assert report.passed
        snap = ing.run_batch_load("oil_production_daily", cleaned,
                                  Provenance(quality_report_id=report.report_id, operator="阿强"))
        assert cat.committed[0]["provenance"]["quality_report_id"] == report.report_id
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
