"""票 09 · dp8 五只读工具测试：JSON 合同 / 四道闸顺序 / 只读红线 / lineage。"""
from __future__ import annotations

import pytest

from app.data_plane.governance import Governance, StaticPolicy, UserContext
from app.data_plane.semantic import Semantic, StaticYaml
from app.data_plane.tools_mcp import DataTools, audit_write_surface

POLICY = {"join_whitelist": [
    {"left": "oil_production_daily", "right": "well_info", "status": "allowed"},
    {"left": "oil_production_daily", "right": "fluid_test", "status": "blocked",
     "note": "须经 intermediate 视图"}],
    "row_scope": {"column": "block_name", "orgs": ["渤南", "渤中", "渤西"]},
    "users": [{"user_id": "laowang", "org": "渤中", "role": "engineer"},
              {"user_id": "boss", "org": "总部", "role": "admin"}]}

WINDOW = {"start": "2025-09-01", "end": "2025-09-30", "granularity": "month"}


@pytest.fixture()
def tools(monkeypatch):
    """S1 组装：真实 dp 模块 + 测试性 stub 边（dp5 用 RecordedEngine 替身）。"""
    from app.data_plane.audit import Audit, InMemoryAudit
    from app.data_plane.freshness import FreshnessInfo, FreshnessRouter
    from app.data_plane.governance import Governance, StaticPolicy
    from app.data_plane.semantic import Semantic, StaticYaml

    class _Clock:
        def today(self):
            from datetime import date
            return date(2026, 9, 27)

    class _Reg:
        def freshness(self, table):
            if table in ("oil_production_daily", "well_info"):
                from datetime import date
                return FreshnessInfo("offline", date(2026, 9, 26), "batch")
            from app.data_plane.freshness import FreshnessInfo as FI
            return FI("unknown", None, None)

    class _Runner:
        engine = "duckdb"

        def run(self, query, snapshot_id=None, **kw):
            rows = [{"block_name": "渤中", "完成率": 96.1}]
            return type("R", (), {"arrow": rows, "snapshot_id": snapshot_id or 12})

    gov = Governance(StaticPolicy(POLICY))
    sem = Semantic(StaticYaml.default())
    audit = Audit(store=InMemoryAudit(), runner=_Runner(), clock=_Clock())
    router = FreshnessRouter(semantic=sem, freshness=_Reg(), clock=_Clock())
    return DataTools(governance=gov, semantic=sem, runner=_Runner(), router=router,
                     audit=audit, default_ctx=UserContext("boss", "总部", "admin"),
                     catalog_tables={
                         "oil_production_daily": {"tier": "offline",
                                                  "freshness": "2026-09-26",
                                                  "metrics": ["产量完成率", "商品量"]},
                         "well_info": {"tier": "offline", "freshness": "2026-09-26",
                                       "metrics": []}})


class TestFiveTools:
    def test_discover_tables(self, tools):
        """DP-W-D8-1：TableCard 含 name/tier/freshness/metrics。"""
        cards = tools.discover_tables()
        names = [c["name"] for c in cards]
        assert "oil_production_daily" in names
        card = next(c for c in cards if c["name"] == "oil_production_daily")
        assert card["tier"] == "offline" and "产量完成率" in card["metrics"]

    def test_discover_skips_unauthorized(self, tools):
        """越权表不进清单（不报错）。"""
        tools.governance = _DenyAll()
        cards = tools.discover_tables(ctx=UserContext("laowang", "渤中", "engineer"))
        assert cards == []

    def test_describe_table(self, tools):
        s = tools.describe_table("oil_production_daily")
        # S1 静态注入：四字段在场（current_snapshot 由 e2e 接真实 catalog 后非空）
        assert {"columns", "partition_spec", "current_snapshot", "metrics_linked"} <= set(s)

    def test_describe_unknown(self, tools):
        r = tools.describe_table("no_such")
        assert r["refused"] is True and r["reason"] == "unknown_table"

    def test_query_happy_path_four_fields(self, tools):
        """DP-W-D8-2：AnswerBundle 四件齐。"""
        b = tools.query("产量完成率", dims=["block_name"], window=WINDOW,
                        filters={}, ctx=UserContext("laowang", "渤中", "engineer"))
        assert set(b) >= {"arrow_summary", "snapshot_id", "quality_status", "as_of"}
        assert b["snapshot_id"] == 12 and b["quality_status"] == "pass"

    def test_query_join_gate_refuses_whole(self, tools):
        """DP-W-D8-2 拒绝路径：闸①（join）拒 → 整体拒 + 无审计记录。"""
        from app.data_plane.governance import Decision

        class DenyJoin:
            def check_join(self, q):
                return Decision(allowed=False, violated_edges=(
                    {"edge": "oil_production_daily⋈fluid_test",
                     "note": "须井号+取样日期双键+时间容差，经 intermediate 视图"},))

            def authorize(self, ctx, table, action, requested_scope=None):
                return Decision(allowed=True)

        tools.governance = DenyJoin()
        before = len(tools.audit.store._records)
        b = tools.query("产量完成率", dims=["block_name"], window=WINDOW,
                        filters={}, ctx=UserContext("laowang", "渤中", "engineer"))
        assert b["refused"] is True and "intermediate" in b["reason"]
        assert len(tools.audit.store._records) == before  # 被拒=无审计

    def test_write_intent_refused(self, tools):
        """DP-W-D8-3：写意图拒绝并指引走管道。"""
        r = tools.write_intent("删库")
        assert r["refused"] is True and "dp2" in r["fix"]

    def test_write_surface_zero(self):
        """只读红线机查：五工具清单无写动词、附录面=0（DP-W-D8-3）。"""
        report = audit_write_surface(DataTools)
        assert report["tools"] == 5 and report["write_surface"] == 0

    def test_replay_and_lineage(self, tools):
        b = tools.query("产量完成率", dims=["block_name"], window=WINDOW,
                        filters={}, ctx=UserContext("laowang", "渤中", "engineer"))
        rr = tools.replay(b["audit_ref"])
        assert rr["match"] is True  # DP-W-D8-4 / D7-2
        ln = tools.lineage("产量完成率")
        assert ln["source_table"] == "oil_production_daily"


class _DenyAll:
    def authorize(self, ctx, table, action, requested_scope=None):
        from app.data_plane.governance import Decision
        return Decision(allowed=False, reason="deny-all")
