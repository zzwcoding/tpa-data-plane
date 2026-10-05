"""DP-06 · 质检任务拾取器测试（hermetic：行形替身装配；PG 真 DDL 对账在 tests/pg）。

行形合同冻结源＝主仓 tests/domains/dataops/quality/test_quality_task_channel.py 的
行形断言（M-05b 集成替身）——零共享库纪律：本文件只照抄**形状字面**（报告键集/hits
条目键集/status×step 值域），不 import 主仓任何代码。
- (a) 拾取→执行→step=2 全链（done/step=2/报告 1:1/hits 单据数/隔离行 batch_id=任务号）；
- (b) 残行自愈（running/step=1 超阈无更新→重置 step=0 可重拾——形态说明见
  quality_store.heal_stale_tasks docstring：status CHECK('running','done') 冻结，
  failed 不可落，故取重置形）；
- (c) 防重复拾取（条件 UPDATE 认领闸——M-05b claim_task_followup 原子认领先例同构）；
- (d) 既有管道作业不回归（tests/worker/test_scheduler.py 原样保留＋装配断言）。
"""
from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.csv  # noqa: F401  (pyarrow 命名空间懒加载：pa.csv.* 需显式导入)
import pytest

apscheduler = pytest.importorskip("apscheduler")

from tests.conftest import FakeSnapshotCatalog
from worker.scheduler import (PipelineConfig, build_scheduler, load_config,
                              run_pickup_once)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"

# —— 行形合同冻结字面（照抄主仓 M-05b 集成替身 _report()/_hit() 键集——零 import） ——
STUB_REPORT_KEYS = {"range", "tables", "scanned_rows", "rules_run",
                    "quarantined_rows", "rejected_batches", "hits", "verdict"}
STUB_HIT_KEYS = {"rule_no", "kind", "action", "hit_count", "sample"}
STUB_SAMPLE_CAP = 5  # 命中 sample 每规则截前 5 条（R2 观察项的截断语义）


class FakePickupStore:
    """quality_task 拾取面替身（hermetic）：0015 列形（id/status/step/rules_count/
    hits/report/time）×拾取四操作（pending/claim/heal/finish）的内存同构镜像——
    真 DDL 语义（条件 UPDATE 认领闸/RETURNING 自愈）由 tests/pg 复证。"""

    def __init__(self, *, fail_on: tuple = (), enabled_nos: tuple | None = None):
        self.rows: dict[str, dict] = {}
        self.quarantine: list[dict] = []
        self._fail_on = set(fail_on)
        # 启用集可注入（F-01 扫描面测试需含 13-20 段的 15/19；缺省照种子 1-12）
        self._enabled = tuple(enabled_nos) if enabled_nos is not None else tuple(range(1, 13))

    def add_task(self, task_id: str, *, status: str = "running", step: int = 0,
                 rules_count: int | None = None, time: datetime | None = None) -> str:
        self.rows[task_id] = {"id": task_id, "status": status, "step": step,
                              "rules_count": rules_count, "hits": None, "report": None,
                              "time": time or datetime.now().replace(microsecond=0)}
        return task_id

    # ---- 拾取面（与 QualityStore PG 实现同形） -------------------------------

    def enabled_rules(self) -> list[dict]:
        return [{"no": n, "kind": "k", "rule": "r", "crit": "c",
                 "action": "隔离" if n in (2, 8) else "告警", "autofix": False}
                for n in self._enabled]

    def pending_tasks(self) -> list[str]:
        return sorted((r["id"] for r in self.rows.values()
                       if r["status"] == "running" and r["step"] == 0),
                      key=lambda i: (self.rows[i]["time"], i))

    def claim_task(self, task_id: str) -> bool:
        """条件 UPDATE 认领闸同构：running∧step=0 → step=1，单语句原子（PG=UPDATE
        ... WHERE rowcount；替身=条件判断）。False＝已被认领/已完——防重复拾取。"""
        row = self.rows.get(task_id)
        if row is None or row["status"] != "running" or row["step"] != 0:
            return False
        row["step"] = 1
        return True

    def heal_stale_tasks(self, *, older_than_seconds: int) -> list[str]:
        cutoff = datetime.now() - timedelta(seconds=older_than_seconds)
        healed = []
        for row in self.rows.values():
            if (row["status"] == "running" and row["step"] == 1
                    and row["time"] < cutoff):
                row["step"] = 0
                healed.append(row["id"])
        return sorted(healed)

    def finish_task(self, task_id: str, *, hits: int, report: dict, step: int = 2) -> None:
        if task_id in self._fail_on:  # 毒任务：执行中断（残行形态的制造器）
            raise RuntimeError(f"执行中断（替身注入）：{task_id}")
        row = self.rows[task_id]
        row.update(status="done", step=step, hits=hits, report=report)

    def append_quarantine(self, rows: list[dict]) -> int:
        """隔离行幂等落表替身（与 QualityStore.append_quarantine 同构——F1）：按寻址键
        (batch_id, rule_no, action, loc, payload) 去重——同键重放不重复落行（自愈重拾
        的 at-least-once 执行不重复落区）；异键照追加（append-only 对真坏行不变）。"""
        inserted = 0
        for r in rows:
            if any(q["batch_id"] == r["batch_id"] and q["rule_no"] == r["rule_no"]
                   and q["action"] == r["action"] and q.get("loc") == r.get("loc")
                   and q["payload"] == r["payload"] for q in self.quarantine):
                continue
            self.quarantine.append(r)
            inserted += 1
        return inserted


def bundle(dirty: bool = True) -> dict:
    """fixture 全表 bundle；dirty 追加空油量坏行（rule-2 隔离档必挂——与
    tests/pg/test_quality_store._bundle 同款注入，独立井/日不与 fixture 混键）。"""
    b = {f.stem: list(csv.DictReader(open(f, encoding="utf-8")))
         for f in sorted(FIXTURES.glob("*.csv"))}
    if dirty:
        rows = b["oil_production_daily"]
        bad = dict(rows[0])
        bad.update({"oil_output": None, "well_id": "Z-98", "prod_date": "2025-09-30"})
        b["oil_production_daily"] = rows + [bad]
    return b


def cfg(store, *, stale_seconds: int = 3600, catalog_factory=None) -> PipelineConfig:
    return PipelineConfig(run_dir=Path("/nonexistent"), freshness=None,
                          quality_store=store, stale_seconds=stale_seconds,
                          catalog_factory=catalog_factory)


HOUR_AGO = datetime.now().replace(microsecond=0) - timedelta(hours=1)

# F-01 扫描面（DP-07）：拾取/质检受检表全集＝001 八张 as-is 源表（99 册 §2.0）
SCAN_TABLES_EIGHT = ("oil_production_daily", "well_info", "well_overhaul", "equip_status",
                     "sensor_reading_subdaily", "water_injection_daily",
                     "monthly_settlement", "fluid_test")


def _lake_with_scan_surface() -> FakeSnapshotCatalog:
    """八表全量湖（fixture 数据＋六条规则定向坏行注入）——F-01「规则在扫描面真跑」载体：
    rule-6 检修批 -T3／rule-8 未知停机码 X9／rule-11 传感 8 连零／rule-12 注入失衡／
    rule-15 注水骤变／rule-19 月结算钩稽失配。全列文本（与进料 as-is 口径一致）。"""
    cat = FakeSnapshotCatalog()
    cat.begin_caller("dp2.ingest")

    def rows_of(name):
        p = FIXTURES / f"{name}.csv"
        if not p.is_file():  # water_injection_daily 无 fixture——湖侧走注入行
            return []
        with open(p, encoding="utf-8") as f:
            return list(csv.DictReader(f))

    injected = {
        "well_overhaul": [{"well_id": "N-09", "overhaul_start": "2026-05-10",
                           "overhaul_end": "2026-05-12", "overhaul_type": "检泵",
                           "affected_output": "10.0", "batch_id": "20260510-T3"}],
        "equip_status": [{"equip_id": "EQ-Z-09", "equip_type": "螺杆泵", "load": "60.0",
                          "current": "24.0", "stop_reason_code": "X9"}],
        "sensor_reading_subdaily": [{"well_id": "N-01", "channel_id": "N-01-TEMP",
                                     "ts": f"2025-09-01T00:{m:02d}:00", "value": "0.0"}
                                    for m in (0, 15, 30, 45, 60, 75, 90, 105)],
        "water_injection_daily": [
            {"well_id": "N-01", "prod_date": "2025-09-05", "inj_volume": "100"},
            {"well_id": "N-01", "prod_date": "2025-09-06", "inj_volume": "500"},
            {"well_id": "N-01", "prod_date": "2025-09-07", "inj_volume": "99999"}],
        "monthly_settlement": [{"block_name": "渤中", "settle_date": "2025-09-30",
                                "net_weight": "0.01"}],
    }
    for name in SCAN_TABLES_EIGHT:
        data = rows_of(name) + injected.get(name, [])
        schema = pa.schema([(k, pa.string()) for k in data[0]])
        cat.create_table(name, schema=schema)
        cat.append(name, pa.Table.from_pylist(data, schema=schema), commit_meta={})
    return cat


# ---------- (a) 拾取→执行→step=2 全链（M-05b 行形合同对账） --------------------


class TestPickupFullChain:
    def test_picked_row_lands_done_step2_report_one_to_one(self):
        store = FakePickupStore()
        tid = store.add_task("QT-20261004-01", rules_count=12, time=HOUR_AGO)
        summary = run_pickup_once(cfg(store), bundle=bundle())
        assert summary["picked"] and summary["picked"][0]["task_id"] == tid
        row = store.rows[tid]
        assert (row["status"], row["step"]) == ("done", 2)     # 行形：拾取完＝done/step=2
        assert row["rules_count"] == 12                        # 受理行字段不被改写
        assert row["hits"] == summary["picked"][0]["hits"] >= 1  # hits=单据数（替身同语义）
        # 隔离行 batch_id=任务号（三落地同源——M-05b 验收 3 的数据面半边）
        assert store.quarantine and {q["batch_id"] for q in store.quarantine} == {tid}
        assert report_of(store, tid)["quarantined_rows"] == len(store.quarantine)

    def test_report_shape_matches_main_stub_field_by_field(self):
        """逐字段对账：报告键集／hits 条目键集／sample 截 5——与主仓集成替身冻结形零偏差。"""
        store = FakePickupStore()
        store.add_task("QT-20261004-02", time=HOUR_AGO)
        run_pickup_once(cfg(store), bundle=bundle())
        report = report_of(store, "QT-20261004-02")
        assert set(report) == STUB_REPORT_KEYS                 # 恰八键——零形状偏差
        hits = report["hits"]
        assert hits and all(set(h) == STUB_HIT_KEYS for h in hits)
        assert all(len(h["sample"]) <= STUB_SAMPLE_CAP for h in hits)
        assert {q["rule_no"] for q in store.quarantine} == {2}  # 恰隔离档落行（告警档零行）
        assert all(q["action"] == "隔离" for q in store.quarantine)

    def test_picked_verdict_carries_r2_note(self):
        """R2 观察项登记（M-05b 关票裁决）：拾取产报告头注记——实质修复归报告合同化批。"""
        store = FakePickupStore()
        store.add_task("QT-20261004-03", time=HOUR_AGO)
        run_pickup_once(cfg(store), bundle=bundle())
        assert "R2" in report_of(store, "QT-20261004-03")["verdict"]
        assert "sample" in report_of(store, "QT-20261004-03")["verdict"]

    def test_bundle_loaded_from_lake_catalog(self):
        """拾取数据面＝湖 as-of 当前视图（修正批次随进料入湖后可复扫）；缺表诚实跳过。"""
        cat = FakeSnapshotCatalog()
        cat.begin_caller("dp2.ingest")
        rows = bundle(dirty=False)["oil_production_daily"]
        bad = [dict(r) for r in rows]
        bad[0] = dict(bad[0], oil_output=None, well_id="Z-98", prod_date="2025-09-30")
        cat.create_table("oil_production_daily",
                         schema=pa.schema([(n, pa.string()) for n in rows[0]]))
        cat.append("oil_production_daily",
                   pa.Table.from_pylist(rows + bad, schema=cat.load_table(
                       "oil_production_daily").scan().schema),
                   commit_meta={"provenance": {}})
        store = FakePickupStore()
        store.add_task("QT-20261004-04", time=HOUR_AGO)
        summary = run_pickup_once(cfg(store, catalog_factory=lambda: cat))
        assert summary["bundle_tables"] == ["oil_production_daily"]  # well_info 缺表——跳过不炸
        assert store.rows["QT-20261004-04"]["status"] == "done"

    def test_empty_lake_no_pick_no_false_green(self):
        """假绿防线：湖视图空→不产空扫描报告（空扫描＝复扫全绿假结论）——本周期不拾取。"""
        store = FakePickupStore()
        tid = store.add_task("QT-20261004-05", time=HOUR_AGO)
        summary = run_pickup_once(cfg(store), bundle={})
        assert summary["picked"] == [] and summary["healed"] == []
        row = store.rows[tid]
        assert (row["status"], row["step"]) == ("running", 0)   # 原样待拾取（诚实 501 半边不变）

    def test_unconfigured_store_honest_skip(self):
        summary = run_pickup_once(cfg(None), bundle=bundle())
        assert summary["picked"] == [] and "skipped" in summary["note"]


# ---------- (a2) 扫描面解耦（DP-07 F-01） ----------------------------------------


class TestScanSurface:
    """F-01（DP-07 全量审查 R-DP F-01）：拾取复扫的扫描面缺省＝全部受检 8 表——
    与进料面（DP_PIPELINE_TABLES，agg 两源）解耦；规则 6/8/11/12/15/19 在扫描面
    真跑（fixture＋定向坏行注入），报告 range 按实扫表数动态生成。"""

    def test_scan_surface_defaults_to_all_checked_tables_rules_really_run(self):
        """扫描面全 8 表入湖：六条原「结构性空跑恒绿」规则真跑出命中——M-05b 复扫
        对规则 8 设备码单的假绿面（扫描面无 equip_status → 永不进 hits）随之封死。"""
        cat = _lake_with_scan_surface()
        store = FakePickupStore(enabled_nos=(2, 6, 8, 11, 12, 15, 19))
        tid = "QT-20261004-16"
        store.add_task(tid, time=HOUR_AGO)
        summary = run_pickup_once(cfg(store, catalog_factory=lambda: cat))
        assert summary["bundle_tables"] == sorted(SCAN_TABLES_EIGHT)   # 扫描面全 8 表
        report = report_of(store, tid)
        assert set(report["tables"]) == set(SCAN_TABLES_EIGHT)
        assert report["range"] == "生产库 8 表"                         # 实扫表数动态生成
        assert {h["rule_no"] for h in report["hits"]} == {6, 8, 11, 12, 15, 19}
        assert all(h["hit_count"] >= 1 for h in report["hits"])        # 真命中非空跑
        assert {q["rule_no"] for q in store.quarantine} == {8}         # 隔离档（rule-8）
        assert report["quarantined_rows"] == len(store.quarantine) == 1

    def test_report_range_dynamic_not_static_eight(self):
        """range 串断言：bundle 两表时如实「生产库 2 表」——不再静态宣称「生产库 8 表」。"""
        store = FakePickupStore()
        tid = "QT-20261004-17"
        store.add_task(tid, time=HOUR_AGO)
        b = bundle(dirty=True)
        run_pickup_once(cfg(store), bundle={k: b[k] for k in ("oil_production_daily",
                                                              "well_info")})
        report = report_of(store, tid)
        assert report["range"] == "生产库 2 表"
        assert set(report["tables"]) == {"oil_production_daily", "well_info"}

    def test_scan_tables_literal_matches_quality_store_domain(self):
        """worker 扫描面字面与 quality_store.SCAN_TABLES／_RULE_TABLE 主表值域同域
        （worker 模块保持零顶层 app 依赖——一致性由本钉子测试看守）。"""
        from app.data_plane.quality_store import SCAN_TABLES, _RULE_TABLE
        from worker.scheduler import _SCAN_TABLES
        assert set(_SCAN_TABLES) == set(SCAN_TABLES)            # worker 字面与 quality 真源同域
        assert set(_RULE_TABLE.values()) <= set(_SCAN_TABLES)   # 扫描面盖住全部规则主表
        assert len(_SCAN_TABLES) == 8                           # 001 八张 as-is 源表全量


def report_of(store: FakePickupStore, task_id: str) -> dict:
    return store.rows[task_id]["report"]


# ---------- (b) 残行自愈 -------------------------------------------------------


class TestStaleRowHealing:
    def test_stale_step1_row_resets_and_gets_repicked(self):
        """crash 残行（step=1 超阈无更新）→自愈重置 step=0→同周期重拾→done/step=2。"""
        store = FakePickupStore()
        tid = store.add_task("QT-20261004-06", step=1, time=HOUR_AGO)  # 认领后中断
        summary = run_pickup_once(cfg(store, stale_seconds=60), bundle=bundle())
        assert summary["healed"] == [tid]
        assert (store.rows[tid]["status"], store.rows[tid]["step"]) == ("done", 2)

    def test_fresh_step1_row_not_healed(self):
        store = FakePickupStore()
        tid = store.add_task("QT-20261004-07", step=1)  # 刚认领（执行中）
        summary = run_pickup_once(cfg(store, stale_seconds=3600), bundle=bundle())
        assert summary["healed"] == []
        assert store.rows[tid]["step"] == 1                    # 在途行不动

    def test_done_rows_never_healed(self):
        store = FakePickupStore()
        tid = store.add_task("QT-20261004-08", status="done", step=2, time=HOUR_AGO)
        summary = run_pickup_once(cfg(store, stale_seconds=60), bundle=bundle())
        assert summary["healed"] == [] and store.rows[tid]["step"] == 2

    def test_poison_task_crash_keeps_step1_then_heals_next_cycle(self):
        """执行中断（规则炸）→行停 step=1（不造假完成）→下周期自愈重拾——重试回路闭合。"""
        store = FakePickupStore(fail_on=("QT-20261004-09",))
        tid = store.add_task("QT-20261004-09", time=HOUR_AGO)
        s1 = run_pickup_once(cfg(store, stale_seconds=3600), bundle=bundle())
        assert s1["failed"] == [tid] and store.rows[tid]["step"] == 1   # 中断残行如实
        s2 = run_pickup_once(cfg(store, stale_seconds=0), bundle=bundle())  # 阈值过尽
        assert s2["healed"] == [tid] and s2["failed"] == [tid]          # 自愈重拾再炸——诚实循环
        assert store.rows[tid]["status"] == "running"                    # 不落假 done


# ---------- (b2) 隔离行幂等（DP-06.fix F1——rev 复现形的回归钉） ------------------


class TestQuarantineIdempotency:
    def test_heal_repick_does_not_double_quarantine_rows(self):
        """F1（DP-06.rev）：自愈重拾＝at-least-once 执行——append 后 finish 前中断，
        次周期重执行同任务不得重复落同寻址键隔离行；报告计数×物理行数对账保持。"""
        store = FakePickupStore(fail_on=("QT-20261004-12",))
        tid = "QT-20261004-12"
        store.add_task(tid, time=HOUR_AGO)
        s1 = run_pickup_once(cfg(store, stale_seconds=3600), bundle=bundle())
        assert s1["failed"] == [tid]
        assert len(store.quarantine) == 1            # 隔离行已落、finish 未竟（部分写现场）
        store._fail_on.clear()                       # 闪断修复
        s2 = run_pickup_once(cfg(store, stale_seconds=0), bundle=bundle())  # 自愈重拾重执行
        assert s2["healed"] == [tid] and s2["picked"]
        assert len(store.quarantine) == 1            # F1：同键不重复落行（两周期 1→1 不翻倍）
        assert report_of(store, tid)["quarantined_rows"] == len(store.quarantine) == 1

    def test_different_addressing_rows_still_append(self):
        """F1 边界：幂等只吞同寻址键——异键坏行照常落行（append-only 对真坏行不变）。"""
        store = FakePickupStore()
        tid = "QT-20261004-13"
        store.add_task(tid, time=HOUR_AGO)
        b = bundle(dirty=False)
        rows = b["oil_production_daily"]
        b["oil_production_daily"] = rows + [
            dict(rows[0], oil_output=None, well_id="Z-97", prod_date="2025-09-29"),
            dict(rows[0], oil_output=None, well_id="Z-98", prod_date="2025-09-30")]
        run_pickup_once(cfg(store), bundle=b)
        assert len(store.quarantine) == 2            # 两坏行两键＝两行（不吞）
        assert len({(q["loc"], str(q["payload"].get("well_id"))) for q in store.quarantine}) == 2

    def test_same_key_bad_rows_report_matches_physical(self):
        """F-04（DP-07）：同批完全同值 twin 坏行（同键同值本就同问题）——报告计数按
        寻址键去重口径，报告=物理（幂等落表物理 1 行，报告不再虚计 2——对账边缘不破）。"""
        store = FakePickupStore()
        tid = "QT-20261004-14"
        store.add_task(tid, time=HOUR_AGO)
        b = bundle(dirty=False)
        rows = b["oil_production_daily"]
        twin = dict(rows[0], oil_output=None, well_id="Z-96", prod_date="2025-09-28")
        b["oil_production_daily"] = rows + [twin, dict(twin)]   # 同批同键坏行×2
        run_pickup_once(cfg(store), bundle=b)
        assert len(store.quarantine) == 1                        # 物理 1 行（幂等寻址）
        assert report_of(store, tid)["quarantined_rows"] == 1    # 报告=物理（F-04 对账）


def test_pending_tasks_has_no_dead_now_param():
    """F3（DP-06.rev）：pending_tasks 死参 now 清除——与替身两形对称
    （签名误导面清除：拾取候选无 staleness 过滤）。"""
    import inspect
    from app.data_plane.quality_store import QualityStore
    assert "now" not in inspect.signature(QualityStore.pending_tasks).parameters


# ---------- (c) 防重复拾取 -----------------------------------------------------


class TestClaimGuard:
    def test_claim_is_one_shot(self):
        store = FakePickupStore()
        tid = store.add_task("QT-20261004-10")
        assert store.claim_task(tid) is True                   # 首认领赢
        assert store.claim_task(tid) is False                  # 二次认领＝False（原子闸）

    def test_claimed_row_not_double_executed(self):
        """他者已认领（step=1）的行不在拾取候选——双 worker 竞态下单执行。"""
        store = FakePickupStore()
        tid = store.add_task("QT-20261004-11", step=1)  # 已被别家认领（刚认领——未超阈）
        summary = run_pickup_once(cfg(store, stale_seconds=3600), bundle=bundle())
        assert summary["picked"] == [] and summary["healed"] == []
        assert store.rows[tid]["status"] == "running"          # 不被重复执行完


# ---------- (d) 装配与既有管道作业不回归 ---------------------------------------


class TestSchedulerWiring:
    def test_two_jobs_when_quality_configured(self):
        sched = build_scheduler(cfg(FakePickupStore()),
                                catalog_factory=lambda: FakeSnapshotCatalog())
        assert {j.id for j in sched.get_jobs()} == {"dp-pipeline", "dp-task-pickup"}

    def test_pickup_job_absent_without_quality_store(self):
        """未配 QUALITY_DATABASE_URL＝拾取作业不装配（不静默装无表可轮询的空作业）。"""
        sched = build_scheduler(cfg(None), catalog_factory=lambda: FakeSnapshotCatalog())
        assert [j.id for j in sched.get_jobs()] == ["dp-pipeline"]  # 既有管道作业原样

    def test_pickup_trigger_env_configurable(self, monkeypatch):
        for k in ("DP_PICKUP_TRIGGER", "DP_PICKUP_INTERVAL_SECONDS", "DP_PICKUP_CRON",
                  "DP_PICKUP_STALE_SECONDS"):
            monkeypatch.delenv(k, raising=False)
        loaded = load_config()
        assert loaded.pickup_trigger == ("interval", {"seconds": 60})  # 缺省 60s
        assert loaded.stale_seconds == 3600                            # 自愈阈值缺省 1h
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger
        monkeypatch.setenv("DP_PICKUP_TRIGGER", "interval")
        monkeypatch.setenv("DP_PICKUP_INTERVAL_SECONDS", "30")
        assert isinstance(build_trigger_of(load_config()), IntervalTrigger)
        monkeypatch.setenv("DP_PICKUP_TRIGGER", "cron")
        monkeypatch.setenv("DP_PICKUP_CRON", "minute=*/5")
        monkeypatch.setenv("DP_PICKUP_STALE_SECONDS", "600")
        loaded = load_config()
        assert loaded.pickup_trigger == ("cron", {"minute": "*/5"})
        assert isinstance(build_trigger_of(loaded), CronTrigger)
        assert loaded.stale_seconds == 600


def build_trigger_of(cfgx):
    from worker.scheduler import build_pickup_trigger
    return build_pickup_trigger(cfgx)
