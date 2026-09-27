"""dp8 数据工具面（S1 切片）：问数 Agent 与数据面的唯一接缝——五个只读工具。

- 四道闸顺序即合同（DP-W-D8-2）：build_query（术语归一）→ check_join+authorize（dp9）
  → route（dp6，拒答走话术合同）→ run（dp5，含成本护栏）→ record（dp7）→ AnswerBundle 四件；
- 只读红线：工具清单零写动词（audit_write_surface 机查）；写意图拒绝并指引走 dp2/dp3 管道；
- 被拒=整体拒（无部分结果、无审计记录）；S1=in_process 直调（MCP server 留位）。
"""
from __future__ import annotations

import inspect

from app.data_plane.freshness import RouteDecision
from app.data_plane.semantic import Clarify, StructuredQuery

_TOOL_NAMES = ("discover_tables", "describe_table", "query", "replay", "lineage")
_WRITE_VERBS = {"insert", "update", "delete", "create", "drop", "append", "write", "overwrite"}


_REFUSAL_SINKS = {"write_intent"}  # 拒绝 sink：名字含写动词但语义=拒绝写意图（只读红线的执行者）


def audit_write_surface(tools_cls=object) -> dict:
    """只读红线机查（DP-W-D8-3）：五工具、零写动词、附录面收敛。

    名字含写动词的方法只允许是"拒绝 sink"（write_intent），其余即违规。"""
    names = set(_TOOL_NAMES)
    if tools_cls is not object:
        names = {n for n, _ in inspect.getmembers(tools_cls, predicate=inspect.isfunction)
                 if not n.startswith("_")}
    write_hits = {n for n in names
                  if any(v in n.lower() for v in _WRITE_VERBS) and n not in _REFUSAL_SINKS}
    return {"tools": len(_TOOL_NAMES), "write_surface": len(write_hits),
            "violations": sorted(write_hits),
            "refusal_sinks": sorted(set(names) & _REFUSAL_SINKS),
            "readonly": not write_hits}


class DataTools:
    def __init__(self, *, governance, semantic, runner, router, audit,
                 catalog_tables: dict, default_ctx=None):
        self.governance = governance
        self.default_ctx = default_ctx  # S1 进程内直调=服务账号身份；显式 ctx 覆盖
        self.semantic = semantic
        self.runner = runner
        self.router = router
        self.audit = audit
        self.catalog_tables = catalog_tables  # 湖表清单（S1=静态注入）

    # ---- 1. 发现 -----------------------------------------------------------

    def discover_tables(self, prefix: str | None = None,
                        ctx: UserContext | None = None) -> list[dict]:
        ctx = ctx or self.default_ctx
        cards = []
        for name, meta in sorted(self.catalog_tables.items()):
            d = self.governance.authorize(ctx, name, "read",
                                          requested_scope=meta.get("scope"))
            if not d.allowed:
                continue  # 越权表不进清单，不报错
            if prefix and not name.startswith(prefix):
                continue
            cards.append({"name": name, "tier": meta["tier"],
                          "freshness": meta["freshness"], "metrics": meta["metrics"]})
        return cards

    # ---- 2. 描述 -----------------------------------------------------------

    def describe_table(self, name: str, ctx: UserContext | None = None) -> dict:
        ctx = ctx or self.default_ctx
        if name not in self.catalog_tables:
            return {"refused": True, "reason": "unknown_table"}
        d = self.governance.authorize(ctx, name, "read")
        if not d.allowed:
            return {"refused": True, "gate": "dp9.authorize", "reason": d.reason}
        meta = self.catalog_tables[name]
        return {"columns": [], "partition_spec": "", "current_snapshot": None,
                "metrics_linked": meta["metrics"], "tier": meta["tier"]}

    # ---- 3. 查询（四道闸顺序即合同） --------------------------------------

    def query(self, metric: str, dims: list[str], window: dict, filters: dict,
              ctx: UserContext | None = None) -> dict:
        ctx = ctx or self.default_ctx
        built = self.semantic.build_query(metric, dims, window, filters)
        if isinstance(built, Clarify):
            return {"refused": True, "gate": "dp4.clarify",
                    "reason": "口径歧义需消歧", "options": built.options, "note": built.note}
        q: StructuredQuery = built
        # 闸① dp9：白名单 + 行级
        jd = self.governance.check_join(q)
        if not jd.allowed:
            return {"refused": True, "gate": "dp9.check_join",
                    "reason": str(jd.violated_edges)}
        scope = q.filters.get("block_name")
        ad = self.governance.authorize(ctx, q.base_table, "read", requested_scope=scope)
        if not ad.allowed:
            return {"refused": True, "gate": "dp9.authorize", "reason": ad.reason}
        # 闸② dp6：路由（拒答走话术合同）
        rd = self.router.route(window, metric)
        if not rd.answerable:
            return {"refused": True, "gate": "dp6.route",
                    "reason": self.router.refusal_text(rd)}
        # 闸③ dp5：执行（成本护栏在 run 内）
        result = self.runner.run(q)
        if getattr(result, "refused", False):
            return {"refused": True, "gate": "dp5.run", "reason": result.reason}
        # 闸④ dp7：记录（AuditRef 是 replay 钥匙）
        audit_ref = self.audit.record(
            question_id=f"{metric}:{scope or '*'}:{window['start']}..{window['end']}",
            query=q, snapshot_id=result.snapshot_id,
            semantic_version=self.semantic.metric_def(metric).version,
            quality_status="pass", original=result.arrow)
        bundle = {"arrow_summary": result.arrow, "snapshot_id": result.snapshot_id,
                  "quality_status": "pass",
                  "as_of": str(rd.declared_as_of), "audit_ref": audit_ref.audit_id}
        if rd.note:  # realtime 口径差异声明（事故 13）
            bundle["caliber_note"] = rd.note
        return bundle

    # ---- 4. 回放 -----------------------------------------------------------

    def replay(self, audit_ref: str) -> dict:
        rr = self.audit.replay(audit_ref)
        return {"match": rr.match, "original": rr.original, "current": rr.current}

    # ---- 5. 溯源 -----------------------------------------------------------

    def lineage(self, table_or_metric: str) -> dict:
        md = self.semantic.metric_def(table_or_metric)
        if md is None:
            return {"refused": True, "reason": "口径唯一真源=dp3 manifest（未注册）"}
        return {"source_table": md.sql_table, "upstreams": [md.sql_table],
                "downstreams": [], "columns": []}

    # ---- 只读红线 -----------------------------------------------------------

    def write_intent(self, verb: str) -> dict:
        return {"refused": True,
                "fix": "写路径走 dp2 进料/dp3 加工管道——入湖必过质检",
                "reason": "工具面只读"}

    def serve_mcp(self) -> dict:
        return {"server_name": "petro-data-plane", "tools": list(_TOOL_NAMES),
                "readonly": True, "transport": "in_process（S1；MCP server S2+ 留位）"}
