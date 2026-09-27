"""票 08 · dp5 执行适配测试：四拍主干 / 快照透传 / 成本护栏 / DuckDB 沙箱。"""
from __future__ import annotations

import pytest

from app.data_plane.governance import Governance, StaticPolicy, UserContext
from app.data_plane.query_runner import QueryRunner
from app.data_plane.semantic import StaticYaml, StructuredQuery


POLICY = {"join_whitelist": [
    {"left": "oil_production_daily", "right": "well_info", "status": "allowed"},
    {"left": "oil_production_daily", "right": "fluid_test", "status": "blocked",
     "note": "须井号+取样日期双键+时间容差，经 intermediate 视图"}],
    "row_scope": {"column": "block_name", "orgs": ["渤南", "渤中", "渤西"]},
    "users": [{"user_id": "u", "org": "渤中", "role": "admin"}]}


def query(base="oil_production_daily", tables=("oil_production_daily", "well_info"), limit=None):
    return StructuredQuery(metric="产量完成率", base_table=base, tables=tables,
                           measures=("oil_output",), dims=(("block_name", "well_info"),),
                           time_dimensions=(), filters={}, limit=limit)


class DuckAdapter:
    """S1 引擎替身→真 DuckDB：execute 在内存 Arrow 上跑 SQL 形状。"""
    engine = "duckdb"

    def execute(self, query, snapshot_id=None, guard=None):
        # S1 教学替身：返回确定性结果集（真实 SQL 编译随 e2e 接 read_parquet）
        rows = [{"block_name": "渤南", "oil_output_sum": 23576.3}]
        return rows, snapshot_id or 7, {"rows_scanned": 1, "bytes": 128, "engine": "duckdb"}


@pytest.fixture()
def runner():
    gov = Governance(StaticPolicy(POLICY))
    return QueryRunner(catalog=None, engines=[DuckAdapter()], governance=gov,
                       limit_guard=100_000)


class TestRun:
    def test_happy_path_snapshot_passthrough(self, runner):
        """DP-W-D5-4：snapshot_id 透传回填。"""
        r = runner.run(query(), snapshot_id=42)
        assert r.snapshot_id == 42 and r.stats["engine"] == "duckdb"

    def test_join_gate_rejects_before_engine(self, runner):
        """闸①前置：✗ 边整体拒绝，引擎不被调用。"""
        r = runner.run(query(tables=("oil_production_daily", "fluid_test")))
        assert r.refused is True and "intermediate" in r.reason
        assert r.stats is None

    def test_cost_guard_rejects(self, runner):
        """DP-W-D5-3：超护栏 → 拒绝（报表/导出类），不静默全表扫。"""
        r = runner.run(query(limit=200_000))
        assert r.refused is True and "成本护栏" in r.reason

    def test_cost_guard_samples_with_note(self, runner):
        """DP-W-D5-3 另一出口：探索类 → 降级采样 + 显式声明。"""
        r = runner.run(query(limit=200_000), exploratory=True)
        assert r.refused is False and "降级采样" in (r.guard_note or "")
