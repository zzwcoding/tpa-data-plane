"""DP-05 · 质检跑批落库编排 hermetic 测试（无 PG）：run_quality_task 纯编排＋helper 契约。

- 落库真身 PG 实跑见 tests/pg/test_quality_store.py（(a)-(d) 四验收）；
- 本文件用 FakeStore 承载编排语义：启用集驱动/隔离行只出自「隔离」档规则（告警类零
  隔离行——E2 端点 9）/单据粒度 hits 去重/报告合同键（quarantined_rows/rejected_batches）；
- helper：data_date_of（水位业务日期提取——无日期键＝None 不虚标）/quarantine_loc/
  payload 行匹配。
"""
from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from app.data_plane.quality_store import (data_date_of, quarantine_loc, run_quality_task,
                                          _issue_key, _payload_row)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"


def _fixture_bundle() -> dict:
    return {f.stem: list(csv.DictReader(open(f, encoding="utf-8")))
            for f in sorted(FIXTURES.glob("*.csv"))}


def _dirty_bundle() -> dict:
    """fixture 批＋一行空油量（rule-2 非空率越界挂——隔离档，产隔离行）。"""
    bundle = _fixture_bundle()
    rows = bundle["oil_production_daily"]
    bad = dict(rows[0])
    bad.update({"oil_output": None})
    bundle["oil_production_daily"] = rows + [bad]
    return bundle


def _specs(*nos: int) -> list[dict]:
    """启用规则 spec（形状＝QualityStore.enabled_rules 输出；种子值取 0015 冻结值）。"""
    catalog = {
        2: ("缺失", "日产油量不许空着", "非空率 ≥ 99.5%", "隔离"),
        5: ("超范围", "含水率 0–100、开井小时数 0–24，越界即不合格", "越界即 fail；矛盾行清单制", "告警"),
        12: ("账对不上", "物质平衡账", "偏差 ≤ 3%", "告警"),
    }
    return [{"no": n, "kind": catalog[n][0], "rule": catalog[n][1], "crit": catalog[n][2],
             "action": catalog[n][3], "autofix": False} for n in nos]


class FakeStore:
    """QualityStore seam 测试替身：记账调用（duck-typed 同形）。"""

    def __init__(self, specs: list[dict]):
        self.specs = specs
        self.tasks: list[dict] = []
        self.finished: list[dict] = []
        self.quarantine: list[dict] = []
        self.water: list[tuple] = []

    def enabled_rules(self):
        return list(self.specs)

    def create_task(self, rules_count, *, now=None):
        task_id = f"QT-20260928-{len(self.tasks) + 1:02d}"
        self.tasks.append({"id": task_id, "rules_count": rules_count})
        return task_id

    def finish_task(self, task_id, *, hits, report, step=2):
        self.finished.append({"id": task_id, "hits": hits, "report": report, "step": step})

    def append_quarantine(self, rows):
        self.quarantine.extend(rows)
        return len(rows)

    def raise_water_line(self, layer, data_date):
        self.water.append((layer, data_date))
        return True


class TestRunQualityTask:
    def test_persists_task_report_and_quarantine(self):
        """跑批落库编排：建任务→隔离行只出自隔离档→报告 1:1 完任务（合同键必带）。"""
        store = FakeStore(_specs(2, 5, 12))
        bundle = _dirty_bundle()
        summary = run_quality_task(store, bundle)
        assert summary["task_id"] == "QT-20260928-01" and summary["rules_run"] == 3
        assert summary["failed_rules"] == [2, 12] and summary["hits"] >= 1  # rule-12 挂＝fixture 注入/产液失衡如实质检（DP-04 干跑同款）
        assert [t["rules_count"] for t in store.tasks] == [3]
        # 隔离行：rule-2 隔离档命中；rule-5/12 告警档零隔离行（E2 端点 9）
        assert len(store.quarantine) == 1
        row = store.quarantine[0]
        assert (row["batch_id"], row["rule_no"], row["action"]) == ("QT-20260928-01", 2, "隔离")
        assert row["loc"].startswith("生产日报 · ")
        assert row["payload"]["well_id"] == bundle["oil_production_daily"][0]["well_id"]
        # 报告 1:1：quarantined_rows/rejected_batches 合同键＋hits 明细＋完任务
        assert len(store.finished) == 1
        fin = store.finished[0]
        assert fin["id"] == "QT-20260928-01" and fin["step"] == 2
        assert fin["report"]["quarantined_rows"] == 1
        assert fin["report"]["rejected_batches"] == 0
        assert fin["hits"] == summary["hits"]
        assert {"rule_no": 2, "kind": "缺失", "action": "隔离"}.items() \
            <= fin["report"]["hits"][0].items()

    def test_hits_dedupe_issue_granularity(self):
        """单据粒度 hits＝（规则＋对象＋数据日期）去重——同对象多定位器不翻倍；
        隔离行数据粒度按寻址键去重（F-04/DP-07：同批完全同值 twin 行只计一）。"""
        store = FakeStore(_specs(2))
        bundle = _dirty_bundle()
        rows = bundle["oil_production_daily"]
        bad = dict(rows[0])
        bad.update({"oil_output": None, "prod_date": "2099-01-01"})  # 另一数据日期＝另一单
        bundle["oil_production_daily"] = rows + [bad, dict(bad)]
        summary = run_quality_task(store, bundle)
        assert summary["hits"] == 2          # 两个（规则2＋对象＋数据日期）组合
        assert len(store.quarantine) == 2    # bad 与其完全同值 twin 寻址键同→1＋另一单 1（F-04）

    def test_no_findings_clean_task(self):
        store = FakeStore(_specs(5))
        summary = run_quality_task(store, _fixture_bundle())
        assert summary["failed_rules"] == [] and summary["quarantined_rows"] == 0
        assert store.quarantine == []
        assert store.finished[0]["hits"] == 0

    def test_unknown_rule_no_skipped_honestly(self):
        """表有而执行体无的规则号＝如实跳过（rules_run 不含，不静默装跑过）。"""
        store = FakeStore([{"no": 99, "kind": "钩稽", "rule": "?", "crit": "?",
                            "action": "告警", "autofix": False}])
        summary = run_quality_task(store, _fixture_bundle())
        assert summary["rules_run"] == 0 and summary["failed_rules"] == []


class TestHelpers:
    def test_data_date_of_by_table(self):
        rows = _fixture_bundle()["oil_production_daily"]
        assert data_date_of("oil_production_daily", rows) == date(2025, 9, 7)
        assert data_date_of("well_info", rows) is None          # 无日期键＝不产出水位主张
        assert data_date_of("oil_production_daily", [{"prod_date": ""}]) is None

    def test_quarantine_loc_shapes(self):
        assert quarantine_loc("oil_production_daily",
                              {"well_id": "N-02", "prod_date": "2026-09-14"}) \
            == "生产日报 · N-02 · 2026-09-14"                    # E2 端点 9 loc 口径
        assert quarantine_loc("well_overhaul", {"batch_id": "20260911-T3"}) \
            == "检修台账 · 20260911-T3"
        assert quarantine_loc("water_injection_daily",
                              {"block_name": "渤南", "month": "2025-09"}) == "注水日报 · 渤南 · 2025-09"

    def test_payload_row_matches_overlap_keys(self):
        rows = _fixture_bundle()["oil_production_daily"]
        loc = {"well_id": rows[0]["well_id"], "prod_date": rows[0]["prod_date"],
               "dev_pp": 6.0}                                    # 派生量不参与匹配
        assert _payload_row("oil_production_daily", loc, rows) == rows[0]
        assert _payload_row("oil_production_daily", {"well_id": "ZZ-99"}, rows) == {"well_id": "ZZ-99"}

    def test_issue_key_ignores_derived(self):
        a = _issue_key(3, {"well_id": "N-02", "prod_date": "2026-09-14", "density": 1.0})
        b = _issue_key(3, {"well_id": "N-02", "prod_date": "2026-09-14", "density": 0.9})
        assert a == b                                            # 同单：派生量差异不拆单
