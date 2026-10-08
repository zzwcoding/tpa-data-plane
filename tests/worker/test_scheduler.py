"""DP-04 · 周期管道 worker 测试（APScheduler——ADR-0003 裁决 12；hermetic：替身装配）。"""
from __future__ import annotations

import io
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.csv  # noqa: F401  (pyarrow 命名空间懒加载：pa.csv.* 需显式导入)
import pytest

apscheduler = pytest.importorskip("apscheduler")

from app.data_plane.ingest import InMemoryFreshnessRegistry, Ingest
from app.data_plane.transform import RULES_OIL_DAILY, RULES_WELL_INFO
from tests.conftest import FakeSnapshotCatalog
from app.data_plane.quality_store import SCAN_TABLES
from worker.scheduler import (PipelineConfig, build_scheduler, build_trigger,
                              load_config, run_pipeline_once)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"
TABLES = ("oil_production_daily", "well_info")  # agg 两源（多测试共用的进料面简写）


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
    for t in SCAN_TABLES:
        if not (FIXTURES / f"{t}.csv").is_file():
            continue  # 缺 fixture 的表（如 water_injection_daily）由 run 目录合成例试
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


# ---------- V2-11：进料幂等闸（内容未变零追加；旧脏批覆写后整批重灌） ------------


class TestIngestIdempotency:
    """V2-11（Q11 P0 数据基座）管道幂等闸两半：

    病灶（Q11 问题 1 旁证：血缘④双批次号 b-670d3a6a0d59/b-c3852fb36d75 同根）——
    v1 残留 run 目录（28 行拟造值）被常驻 worker 每日 cron 当「新批次」反复追加，
    湖 as-of 视图同键新批覆写真值，agg 刷新把 v1 值冲回 fct（28 行脏格＋批次号混源）。
    修复＝①内容未变＝零追加（不建快照、不重刷 agg、不报水位、不重盖 freshness）；
    ②视图被旧值覆写（≠源 CSV）＝整批重灌重建真值（append-only 纪律不动——
    清历史归 dp7.expire，消费面正确性由 as-of 覆写承载）。"""

    def test_second_run_zero_append(self, catalog, run_dir):
        """同内容 CSV 连跑两遍：第二遍零变更——零新快照、agg 不重刷、零水位主张。"""
        first = run_pipeline_once(cfg(catalog, run_dir, FakeWriter()),
                                  catalog_factory=lambda: catalog)
        assert set(first["ingested"]) == set(TABLES)
        committed_after_first = list(catalog.committed)
        w_second, store = FakeWriter(), FakeQualityStore()
        second = run_pipeline_once(cfg(catalog, run_dir, w_second, store),
                                   catalog_factory=lambda: catalog)
        assert second["ingested"] == []
        assert set(second["unchanged"]) == set(TABLES)      # 内容未变＝逐表跳过并如实记账
        assert catalog.committed == committed_after_first   # 零新快照（湖不翻倍）
        assert w_second.calls == []                         # 视图未动＝agg 不重刷
        assert second["agg"].startswith("skipped")
        assert store.water == []                            # 无落位＝无水位主张

    def test_second_run_freshness_not_restamped(self, catalog, run_dir):
        """幂等第二遍不触 freshness 登记（数据生效时刻未变＝零登记调用——
        记账替身点数，不靠时钟分辨率）。"""
        from app.data_plane.ingest import InMemoryFreshnessRegistry

        class RecordingFreshness(InMemoryFreshnessRegistry):
            def __init__(self):
                super().__init__()
                self.calls = []

            def register(self, table, tier, as_of, pipeline):
                self.calls.append((table, tier, pipeline))
                super().register(table, tier, as_of, pipeline)

        fresh = RecordingFreshness()
        run_pipeline_once(PipelineConfig(run_dir=run_dir, freshness=fresh,
                                         agg_writer=FakeWriter()),
                          catalog_factory=lambda: catalog)
        assert len(fresh.calls) == 3 + 2                           # agg 三表＋进料两表
        fresh.calls.clear()
        run_pipeline_once(PipelineConfig(run_dir=run_dir, freshness=fresh,
                                         agg_writer=FakeWriter()),
                          catalog_factory=lambda: catalog)
        assert fresh.calls == []                                   # 零落位＝零登记

    def test_stale_override_reingests_and_repairs(self, catalog, run_dir):
        """污染修复路径：视图被旧脏批覆写（v1 残留形态）后，下一周期视图≠源 CSV
        →整批重灌，agg 拿回真值；修完再跑恢复零变更。"""
        run_pipeline_once(cfg(catalog, run_dir, FakeWriter()),
                          catalog_factory=lambda: catalog)
        rows = FIXTURES.joinpath("oil_production_daily.csv").read_text(encoding="utf-8") \
            .splitlines()
        header, first_data = rows[0], rows[1].split(",")
        stale = first_data.copy()
        stale[2] = "99.99"                                       # oil_output 换 v1 拟造值
        stale_csv = "\n".join([header, ",".join(stale)]) + "\n"
        names = pa.csv.read_csv(io.BytesIO(header.encode("utf-8") + b"\n")).column_names
        catalog.begin_caller("dp2.ingest")
        catalog.append("oil_production_daily",
                       pa.csv.read_csv(io.BytesIO(stale_csv.encode("utf-8")),
                                       convert_options=pa.csv.ConvertOptions(
                                           column_types={n: pa.string() for n in names})),
                       commit_meta={"batch_id": "b-v1residue"})
        w_repair = FakeWriter()
        summary = run_pipeline_once(cfg(catalog, run_dir, w_repair),
                                    catalog_factory=lambda: catalog)
        assert "oil_production_daily" in summary["ingested"]     # 视图≠源 → 整批重灌
        fct = w_repair.calls[0]["fct_daily_metering"]
        row = next(r for r in fct if r["well_id"] == first_data[0]
                   and r["prod_date"] == first_data[1])
        assert row["oil_output"] == float(first_data[2])         # 真值回位
        assert row["oil_output"] != 99.99
        again = run_pipeline_once(cfg(catalog, run_dir, FakeWriter()),
                                  catalog_factory=lambda: catalog)
        assert again["ingested"] == []                           # 修复后恢复幂等
        assert set(again["unchanged"]) == set(TABLES)

    def test_numeric_scale_quantization_matches_pg_landing(self):
        """比对口径与 PG 落库定标对齐：raw 列 NUMERIC(5,1) 把 CSV 13.47 存成 13.5——
        比对必须按列标度先定型再比（否则真湖幂等闸永不满足＝假阴性重灌）。"""
        from worker.scheduler import _canonical_cell
        # 同标度：CSV 文本与视图存储值（湖 _normalize 后的 float）归一到同一形态
        assert _canonical_cell("13.47", "numeric", 1) == _canonical_cell(13.5, "numeric", 1)
        assert _canonical_cell("21.16", "numeric", 2) == _canonical_cell(21.16, "numeric", 2)
        # 不同标度不混比（5,1 的 13.5 ≠ 5,2 的 13.5? 同值不同形仍按数值相等——标度只服务定型）
        assert _canonical_cell("13.47", "numeric", 1) != _canonical_cell("13.47", "numeric", 2)
        # 无标度信息（替身湖全文本列）＝不定量，走数值等值
        assert _canonical_cell("13.47", None, None) == _canonical_cell(13.47, None, None)
        # 非数值列照常字符串比
        assert _canonical_cell("B", "character varying", None) == _canonical_cell("B", None, None)


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


# ---------- DP-07 F-01：扫描面解耦 -----------------------------------------------


class TestScanSurfaceDecoupled:
    """F-01（DP-07 全量审查 R-DP F-01）：管道质检的扫描面与进料面解耦——
    扫描面缺省＝全部受检 8 表（DP_SCAN_TABLES），进料面保持 DP_PIPELINE_TABLES
    agg 两源原语义；扩进料表清单不再 UnruledTableRejected 崩周期。"""

    @pytest.fixture()
    def run_dir8(self, tmp_path):
        """全 8 受检表 run 目录：fixture 拷贝＋water_injection_daily 合成＋定向坏行注入
        （rule-6 检修批 -T3／rule-8 停机码 X9／rule-11 传感 8 连零／rule-12/15 注水失衡
        ／rule-19 月结算钩稽失配）。"""
        d = tmp_path / "run-full8"
        d.mkdir()
        for f in FIXTURES.glob("*.csv"):
            shutil.copy(f, d / f.name)
        d.joinpath("water_injection_daily.csv").write_text(
            "well_id,prod_date,inj_volume\n"
            "N-01,2025-09-06,100\n"
            "N-01,2025-09-07,99999\n", encoding="utf-8")
        d.joinpath("equip_status.csv").write_text(
            FIXTURES.joinpath("equip_status.csv").read_text(encoding="utf-8")
            + "EQ-Z-09,螺杆泵,60.0,24.0,X9\n", encoding="utf-8")
        d.joinpath("well_overhaul.csv").write_text(
            FIXTURES.joinpath("well_overhaul.csv").read_text(encoding="utf-8")
            + "N-09,2026-05-10,2026-05-12,检泵,10.0,20260510-T3\n", encoding="utf-8")
        d.joinpath("monthly_settlement.csv").write_text(
            FIXTURES.joinpath("monthly_settlement.csv").read_text(encoding="utf-8")
            + "渤中,2025-09-30,0.01\n", encoding="utf-8")
        zeros = "".join(f"N-01,N-01-TEMP,2025-09-02T0{i}:00:00,0.0\n" for i in range(8))
        d.joinpath("sensor_reading_subdaily.csv").write_text(
            FIXTURES.joinpath("sensor_reading_subdaily.csv").read_text(encoding="utf-8")
            + zeros, encoding="utf-8")
        return d

    def test_pipeline_quality_scans_all_checked_tables(self, catalog, run_dir8):
        """管道质检扫描面＝全 8 受检表：报告 tables 八键、range「生产库 8 表」、
        规则 6/8/11/12 真跑（fixture＋定向坏行，非结构性空跑恒绿）。"""
        store = FakeQualityStore()
        writer = FakeWriter()
        summary = run_pipeline_once(cfg(catalog, run_dir8, writer, store),
                                    catalog_factory=lambda: catalog)
        report = store.finished[0]["report"]
        assert set(report["tables"]) == {
            "oil_production_daily", "well_info", "well_overhaul", "equip_status",
            "sensor_reading_subdaily", "water_injection_daily", "monthly_settlement",
            "fluid_test"}
        assert report["range"] == "生产库 8 表"                    # 实扫表数动态生成
        assert {h["rule_no"] for h in report["hits"]} >= {6, 8, 11, 12}
        assert set(summary["ingested"]) == {"oil_production_daily", "well_info"}  # 进料面原语义

    def test_expanded_pipeline_tables_do_not_crash_cycle(self, catalog, run_dir8):
        """扩进料表清单（纳入无进料规则集的表）不再崩周期：UnruledTableRejected 诚实
        跳过记摘要，闸 fail-closed 语义不变（表不入湖），其余表照常进料、agg 照常刷新。"""
        writer = FakeWriter()
        cfgx = PipelineConfig(run_dir=run_dir8, freshness=InMemoryFreshnessRegistry(),
                              agg_writer=writer, quality_store=None,
                              tables=("oil_production_daily", "well_info", "equip_status",
                                      "sensor_reading_subdaily"))
        summary = run_pipeline_once(cfgx, catalog_factory=lambda: catalog)
        assert {"oil_production_daily", "well_info", "equip_status"} <= set(summary["ingested"])
        # equip_status 规则注册后干净进料（2026-10-06 扩表）；fail-closed 覆盖保留：
        assert summary["unruled_skipped"] == ["sensor_reading_subdaily"]  # 无规则表仍诚实跳过
        assert writer.calls                                        # 周期照常走完（agg 发生）


class TestScanTablesConfig:
    def test_scan_tables_env_default_and_override(self, monkeypatch):
        """扫描面独立环境变量 DP_SCAN_TABLES（缺省 8 受检表）；进料面 DP_PIPELINE_TABLES
        原语义不动（缺省 agg 两源）——两面貌合变量解耦。"""
        from worker.scheduler import _SCAN_TABLES
        monkeypatch.delenv("DP_SCAN_TABLES", raising=False)
        loaded = load_config()
        assert loaded.scan_tables == _SCAN_TABLES
        assert set(loaded.scan_tables) >= set(loaded.tables)       # 缺省进料面 ⊆ 扫描面
        monkeypatch.setenv("DP_SCAN_TABLES", "oil_production_daily,equip_status")
        assert load_config().scan_tables == ("oil_production_daily", "equip_status")
        assert load_config().tables == ("oil_production_daily", "well_info")  # 进料面不受染


# ---------- DP-07 F-03：报水位取闸后净批 -----------------------------------------


class TestWaterLineNetBatch:
    def test_water_line_follows_landed_net_batch(self, catalog, run_dir, monkeypatch):
        """F-03（DP-07）：原始/清洗层水位取**闸后净批**（质检通过后落位批）——
        最新业务日期行被闸剔除（脏）时水位不得虚标到被剔日期（单调不回退会把虚标
        锁死）。以闸后净批裁剪模拟清洗语义（S1 设计：cleaned 剔除坏行），原始 CSV
        行仍见被剔日期——水位必须跟落位批走。"""
        src = FIXTURES.joinpath("oil_production_daily.csv").read_text(encoding="utf-8")
        run_dir.joinpath("oil_production_daily.csv").write_text(
            src + "N-01,2025-12-31,99.0,120.0,10.0,24.0,24.0,7.5,A\n", encoding="utf-8")
        real_load = Ingest._load

        def net_load(self_ing, table, records, provenance, *, source=None, batch_id=None):
            rows = [r for r in records.to_pylist() if r.get("prod_date") != "2025-12-31"]
            return real_load(self_ing, table,
                             pa.Table.from_pylist(rows, schema=records.schema),
                             provenance, source=source, batch_id=batch_id)

        monkeypatch.setattr(Ingest, "_load", net_load)
        store = FakeQualityStore()
        run_pipeline_once(cfg(catalog, run_dir, FakeWriter(), store),
                          catalog_factory=lambda: catalog)
        raw_days = [d for layer, d in store.water if layer == "原始层"]
        assert "2025-09-07" in raw_days                            # 落位净批日期在水位
        assert "2025-12-31" not in raw_days                        # 被剔日期不虚标（F-03）

    def test_rejected_batch_writes_no_water_claim(self, catalog, tmp_path):
        """F-03 另一半：脏批（进料闸拒）不入水位——落位不发生即无水位主张。"""
        d = tmp_path / "run-dirty"
        d.mkdir()
        shutil.copy(FIXTURES / "well_info.csv", d / "well_info.csv")
        rows = FIXTURES.joinpath("oil_production_daily.csv").read_text(
            encoding="utf-8").splitlines()
        d.joinpath("oil_production_daily.csv").write_text(
            "\n".join(rows[:3] + [rows[2].replace(",A", ",B")]) + "\n", encoding="utf-8")
        store = FakeQualityStore()
        run_pipeline_once(cfg(catalog, d, FakeWriter(), store),
                          catalog_factory=lambda: catalog)
        assert [w for w in store.water if w[1] != "None"] == []    # 无任何有日期的水位主张
