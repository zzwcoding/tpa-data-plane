"""票 10 · 老王问数 tracer bullet（端到端）：fixture 产物 → 进料 → 问数 → AnswerBundle。

数据源：tests/fixtures/fieldforge 仓内合成最小集（DP-01 fixture 化，形状同 fieldforge
合同产物；不再依赖 ~/Downloads/fieldforge 本机外部路径，CI hermetic 全量跑）。
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.data_plane.audit import Audit, InMemoryAudit
from app.data_plane.freshness import FreshnessRouter, FreshnessInfo
from app.data_plane.governance import Governance, StaticPolicy, UserContext
from app.data_plane.ingest import InMemoryFreshnessRegistry, Ingest, Provenance
from app.data_plane.transform import RULES_OIL_DAILY
from app.data_plane.lake_store import InMemoryLake
from app.data_plane.query_runner import QueryRunner
from app.data_plane.semantic import Semantic, StaticYaml
from app.data_plane.tools_mcp import DataTools

RUN = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"
TODAY = date(2026, 9, 27)


class Clock:
    def today(self):
        return TODAY


class RealRunner:
    """dp5 真实引擎：DuckDB 直读 fixture CSV（tpa S1 档查询形态）。"""
    engine = "duckdb"

    def execute(self, query, snapshot_id=None, guard=None):
        import duckdb
        run_dir = str(RUN)
        sql = f"""
        SELECT w.block_name AS block_name,
               round(sum(p.oil_output) / NULLIF(sum(p.plan_output), 0) * 100, 1) AS completion_rate
        FROM read_csv_auto('{run_dir}/oil_production_daily.csv') p
        JOIN read_csv_auto('{run_dir}/well_info.csv') w USING (well_id)
        WHERE p.prod_date >= '2025-09-01' AND p.prod_date <= '2025-09-30'
        GROUP BY w.block_name ORDER BY w.block_name
        """
        rows = duckdb.sql(sql).fetchall()
        arrow = [{"block_name": r[0], "完成率": float(r[1])} for r in rows]
        return arrow, (snapshot_id or 1), {"rows_scanned": len(rows), "engine": "duckdb"}


@pytest.fixture(scope="module")
def tools():
    lake = InMemoryLake()
    cat = lake.load_catalog("jdbc")
    policy = {"join_whitelist": [
        {"left": "oil_production_daily", "right": "well_info", "status": "allowed"}],
        "row_scope": {"column": "block_name", "orgs": ["渤南", "渤中", "渤西"]},
        "users": [{"user_id": "laowang", "org": "渤中", "role": "engineer"}]}
    gov = Governance(StaticPolicy(policy))
    sem = Semantic(StaticYaml.default())
    query_runner = QueryRunner(catalog=cat, engines=[RealRunner()], governance=gov)
    audit = Audit(store=InMemoryAudit(), runner=query_runner, clock=Clock())
    reg = InMemoryFreshnessRegistry()
    ing = Ingest(catalog=cat, freshness=reg,
                 rules={"oil_production_daily": RULES_OIL_DAILY})  # fail-closed：入湖必配规则集
    # 进料：fixture 产物油表带质检闸入湖（DP-01 fail-closed 后无规则=拒绝）
    import csv

    def load_csv(name):
        with (RUN / f"{name}.csv").open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    import pyarrow as pa
    prod = load_csv("oil_production_daily")
    cat.create_table("oil_production_daily", schema=pa.Table.from_pylist(prod).schema,
                     comment="日产计量", partition_spec="day(prod_date)")
    report, cleaned = validate_and_clean(prod)
    ing.run_batch_load("oil_production_daily", cleaned,
                       Provenance(quality_report_id=report.report_id, operator="laowang"))
    for t in ("well_info", "monthly_settlement", "injection_connection", "fluid_test"):
        reg.register(t, "offline", date(2026, 9, 26), f"fixture-{t}")
    router = FreshnessRouter(semantic=sem, freshness=reg, clock=Clock())
    runner = query_runner
    tables = {t: {"tier": "offline", "freshness": "2026-09-26", "metrics": []}
              for t in ("oil_production_daily", "well_info", "monthly_settlement")}
    tables["oil_production_daily"]["metrics"] = ["产量完成率", "商品量"]
    return DataTools(governance=gov, semantic=sem, runner=runner, router=router,
                     audit=audit, catalog_tables=tables,
                     default_ctx=UserContext("laowang", "渤中", "engineer"))


def validate_and_clean(prod_rows):
    from app.data_plane.transform import validate_batch
    import pyarrow as pa
    table = pa.Table.from_pylist(prod_rows)
    return validate_batch(table, RULES_OIL_DAILY)


def test_laowang_asks_completion_rate(tools):
    """老王问：渤中区块 2025-09 产量完成率 → 全链路 → AnswerBundle 四件齐 + 数字可对源。"""
    b = tools.query("产量完成率", dims=["block_name"],
                    window={"start": "2025-09-01", "end": "2025-09-30",
                            "granularity": "month"},
                    filters={"block_name": "渤中"})
    assert "refused" not in b
    assert {"arrow_summary", "snapshot_id", "quality_status", "as_of"} <= set(b)
    rows = b["arrow_summary"]
    assert rows and rows[0]["block_name"] == "渤中"
    # 数字可对源：完成率 = 油量合计/配产合计×100（独立重算）
    import csv
    prod = list(csv.DictReader(open(RUN / "oil_production_daily.csv", encoding="utf-8")))
    wells = {w["well_id"]: w for w in csv.DictReader(open(RUN / "well_info.csv", encoding="utf-8"))}
    oil = sum(float(r["oil_output"]) for r in prod
              if wells[r["well_id"]]["block_name"] == "渤中"
              and r["prod_date"].startswith("2025-09"))
    plan = sum(float(r["plan_output"]) for r in prod
               if wells[r["well_id"]]["block_name"] == "渤中"
               and r["prod_date"].startswith("2025-09"))
    expect = round(oil / plan * 100, 1)
    assert rows[0]["完成率"] == expect

    # 回放闭环（同快照 match）
    rr = tools.replay(b["audit_ref"])
    assert rr["match"] is True


def test_laowang_cannot_ask_future(tools):
    """问下个月 → 路由诚实拒答（未来窗）。"""
    b = tools.query("产量完成率", dims=["block_name"],
                    window={"start": "2026-10-01", "end": "2026-11-30",
                            "granularity": "month"},
                    filters={})
    assert b["refused"] is True and b["gate"] == "dp6.route"
