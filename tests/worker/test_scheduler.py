"""DP-04 · 周期管道 worker 测试（APScheduler——ADR-0003 裁决 12；hermetic：替身装配）。"""
from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.csv  # noqa: F401  (pyarrow 命名空间懒加载：pa.csv.* 需显式导入)
import pytest

apscheduler = pytest.importorskip("apscheduler")

from app.data_plane.ingest import InMemoryFreshnessRegistry
from app.data_plane.transform import RULES_OIL_DAILY, RULES_WELL_INFO
from tests.conftest import FakeSnapshotCatalog
from worker.scheduler import (PipelineConfig, build_scheduler, build_trigger,
                              load_config, run_pipeline_once)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"
TABLES = ("oil_production_daily", "well_info")


class FakeWriter:
    """AggStarWriter 测试替身：记账 write 调用（duck-typed）。"""

    def __init__(self):
        self.calls = []

    def write(self, agg_rows):
        self.calls.append(agg_rows)
        return {k: len(v) for k, v in agg_rows.items()}


@pytest.fixture()
def run_dir(tmp_path):
    d = tmp_path / "run-0926"
    d.mkdir()
    for f in ("oil_production_daily.csv", "well_info.csv"):
        shutil.copy(FIXTURES / f, d / f)
    return d


@pytest.fixture()
def catalog():
    import io
    cat = FakeSnapshotCatalog()
    for t in TABLES:
        with open(FIXTURES / f"{t}.csv", "rb") as f:  # 表头行嗅探（同 ingest.run_csv_load 读法）
            head = f.readline()
        names = pa.csv.read_csv(io.BytesIO(head)).column_names
        cat.create_table(t, schema=pa.schema([(n, pa.string()) for n in names]))
    return cat


def cfg(catalog, run_dir, writer=None, quality_store=None):
    return PipelineConfig(run_dir=run_dir, agg_writer=writer, quality_store=quality_store,
                          freshness=InMemoryFreshnessRegistry())


class FakeQualityStore:
    """QualityStore 测试替身（DP-05）：记账质检落库与报水位调用（duck-typed 同形）。

    启用集语义照 seed（1-12 启用、2 号隔离档必挂 fixture 空油量行……由 run_quality_task
    真编排消费——替身只承载 PG 面，编排纯逻辑另有 tests/dp3/test_quality_run.py 直测）。"""

    def __init__(self):
        self.quarantine: list[dict] = []
        self.water: list[tuple] = []
        self.finished: list[dict] = []
        self._n = 0

    def enabled_rules(self):
        return [{"no": n, "kind": "k", "rule": "r", "crit": "c",
                 "action": "隔离" if n == 2 else "告警", "autofix": False}
                for n in range(1, 13)]

    def create_task(self, rules_count, *, now=None):
        self._n += 1
        return f"QT-20260928-{self._n:02d}"

    def finish_task(self, task_id, *, hits, report, step=2):
        self.finished.append({"id": task_id, "hits": hits, "report": report})

    def append_quarantine(self, rows):
        self.quarantine.extend(rows)
        return len(rows)

    def raise_water_line(self, layer, data_date):
        self.water.append((layer, str(data_date)))
        return data_date is not None


class TestRunPipelineOnce:
    def test_full_cycle_ingest_quality_agg_freshness(self, catalog, run_dir):
        """一周期：日度进料（拉新批次）→清洗→质检跑批→agg 刷新→水位登记（freshness 更新）。"""
        writer = FakeWriter()
        summary = run_pipeline_once(cfg(catalog, run_dir, writer),
                                    catalog_factory=lambda: catalog)
        assert set(summary["ingested"]) == set(TABLES)
        assert summary["quality"].startswith("skipped")       # 未配 QUALITY_DATABASE_URL＝显式跳过
        assert writer.calls and set(writer.calls[0]) == {
            "dim_well", "dim_date", "fct_daily_metering"}         # agg 直写恰三表
        fct = writer.calls[0]["fct_daily_metering"]
        keys = [(r["well_id"], r["prod_date"]) for r in fct]
        assert len(keys) == len(set(keys))                        # grain=一井×一天
        reg = summary["freshness"]
        for t in ("agg.fct_daily_metering", "agg.dim_well", "agg.dim_date"):
            assert reg.freshness(t).tier == "offline" and reg.freshness(t).as_of is not None
        assert reg.freshness("agg.fct_daily_metering").pipeline == "dp3-agg-refresh"
        # 进料侧水位：as_of=快照账 committed_at（P6 修复随管道生效）
        assert reg.freshness("oil_production_daily").as_of is not None

    def test_quality_store_wiring_task_waterline(self, catalog, run_dir):
        """DP-05 接线：质检跑批落库（QT 任务号随 provenance 入湖）＋报水位三 tier
        （原始/清洗层=进料水位、汇总层=agg 刷新水位——E2 行为约定 8）。"""
        store = FakeQualityStore()
        writer = FakeWriter()
        summary = run_pipeline_once(cfg(catalog, run_dir, writer, store),
                                    catalog_factory=lambda: catalog)
        task_id = summary["quality"]["task_id"]
        assert task_id.startswith("QT-")
        assert len(store.finished) == 1 and store.finished[0]["id"] == task_id
        assert store.finished[0]["report"]["quarantined_rows"] \
            == summary["quality"]["quarantined_rows"]
        # 报水位：原始/清洗层（进料批业务日期）＋汇总层（agg fct 最大业务日期）
        assert [w[0] for w in store.water] == ["原始层", "清洗层", "原始层", "清洗层", "汇总层"]
        assert store.water[-1][1] == max(r["prod_date"] for r in writer.calls[0]["fct_daily_metering"])

    def test_quality_skipped_without_store_records_honestly(self, catalog, run_dir):
        summary = run_pipeline_once(cfg(catalog, run_dir, FakeWriter(), None),
                                    catalog_factory=lambda: catalog)
        assert summary["quality"].startswith("skipped（未配 QUALITY_DATABASE_URL")

    def test_agg_skipped_without_writer(self, catalog, run_dir):
        """未配 AGG_DATABASE_URL：agg 步显式跳过（摘要记 skipped，不静默装成功）。"""
        summary = run_pipeline_once(cfg(catalog, run_dir, None),
                                    catalog_factory=lambda: catalog)
        assert summary["agg"].startswith("skipped")

    def test_dirty_batch_not_landed(self, catalog, tmp_path):
        """进料闸照常：脏批（A/B 重复行）被拦，摘要记 rejected、agg 不发生。"""
        d = tmp_path / "run-dirty"
        d.mkdir()
        rows = FIXTURES.joinpath("oil_production_daily.csv").read_text(encoding="utf-8") \
            .splitlines()
        d.joinpath("oil_production_daily.csv").write_text(
            "\n".join(rows[:3] + [rows[2].replace(",A", ",B")]) + "\n", encoding="utf-8")
        summary = run_pipeline_once(cfg(catalog, d, FakeWriter()),
                                    catalog_factory=lambda: catalog)
        assert "oil_production_daily" in summary["rejected"]
        assert summary["agg"].startswith("skipped")               # 无净数据不硬刷


class TestScheduleConfig:
    def test_default_config_daily_cron(self, monkeypatch):
        for k in ("DP_PIPELINE_RUN_DIR", "DP_SCHED_TRIGGER", "DP_SCHED_CRON",
                  "DP_SCHED_INTERVAL_MINUTES"):
            monkeypatch.delenv(k, raising=False)
        loaded = load_config()
        assert loaded.trigger == ("cron", {"hour": 5, "minute": 30})   # 缺省日度（05:30）
        assert str(loaded.run_dir) == "/data/runs"

    def test_interval_env_override(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DP_SCHED_TRIGGER", "interval")
        monkeypatch.setenv("DP_SCHED_INTERVAL_MINUTES", "30")
        monkeypatch.setenv("DP_PIPELINE_RUN_DIR", str(tmp_path))
        loaded = load_config()
        assert loaded.trigger == ("interval", {"minutes": 30})
        from apscheduler.triggers.interval import IntervalTrigger
        assert isinstance(build_trigger(loaded), IntervalTrigger)

    def test_cron_env_override(self, monkeypatch):
        monkeypatch.setenv("DP_SCHED_TRIGGER", "cron")
        monkeypatch.setenv("DP_SCHED_CRON", "hour=6,minute=15")
        from apscheduler.triggers.cron import CronTrigger
        assert isinstance(build_trigger(load_config()), CronTrigger)

    def test_build_scheduler_has_one_job(self, catalog):
        sched = build_scheduler(cfg(catalog, Path("/data/runs")),
                                catalog_factory=lambda: catalog)
        assert len(sched.get_jobs()) == 1
        assert sched.get_jobs()[0].id == "dp-pipeline"
