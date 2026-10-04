"""tpa-data-plane API（S3）：dp8 五只读工具 HTTP 化 + 老王问数查询面。

【DEPRECATED · DP-02 退役标注（2026-10-04）】裁决 15 归位：问答面已退役，功能将由主平台
问数链重建承接；本文件保留为参考标本，勿再演进。（ADR-0003 裁决 15：dp4 语义层/dp5 查询
执行/dp8 对外工具面职责归主平台——语义=C1/semantics、查询=chat LangGraph 链＋安检、对外
工具=F1 expose。）全部问答路由（tables/describe/query/replay/lineage）恒返回 410 Gone
（带归位说明文案）；退役前实现（查询模板 v0/DuckRunner/build_tools 装配）留档对照、
不再可执行——其依赖模块 query_runner.py/semantic.py/tools_mcp.py 已 git rm（git 历史备查）。
线上 8.163.33.135/dp/ 停服＝Owner 手工项，DP-02 票面挂账。

- 组装=真实 dp 模块（治理闸/语义/路由/审计/执行），数据=fieldforge 云端 run 目录；
- 查询模板 v0：产量完成率 / 商品量（S1 形状），其余指标回"未支持"明示；
- 每个响应带 X-Synthetic-Data 头；只读红线：无任何写端点；
- 持久化装配（P0 热修）：有 DATABASE_URL → freshness/audit 落 PG 真身（pg_store），
  无 → 内存替身（本地开发不炸）。启用 PG 后 freshness 需先登记，空库=全线 unknown 拒答。
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.data_plane.audit import Audit, InMemoryAudit
from app.data_plane.freshness import FreshnessInfo, FreshnessRouter
from app.data_plane.governance import Governance, StaticPolicy, UserContext
from app.data_plane.pg_store import PgAuditStore, PgFreshnessRegistry, database_url
# DP-02（裁决 15 归位）：原下行三只 import 已随模块删除失效——
#   from app.data_plane.query_runner import QueryRunner   （dp5 → 主平台问数链）
#   from app.data_plane.semantic import Semantic, StaticYaml（dp4 → 主平台 C1/semantics）
#   from app.data_plane.tools_mcp import DataTools        （dp8 → 主平台 F1 expose）
# 下方退役前装配代码仅存档，不再可执行。

RUN_DIR = Path(os.environ.get("DP_RUN_DIR", "/data/runs"))
TABLES_ENV = os.environ.get("DP_TABLES", "oil_production_daily,well_info,monthly_settlement")

POLICY = {
    "join_whitelist": [
        {"left": "oil_production_daily", "right": "well_info", "status": "allowed"}],
    "row_scope": {"column": "block_name", "orgs": ["渤南", "渤中", "渤西"]},
    "users": [{"user_id": "service", "org": "总部", "role": "admin"}],
}

# 查询模板 v0（S1 形状）：metric → SQL（日期窗经参数注入）
_SQL_TEMPLATES = {
    "产量完成率": """
        SELECT w.block_name AS block_name,
               strftime(p.prod_date, '%Y-%m') AS month,
               round(sum(p.oil_output) / NULLIF(sum(p.plan_output), 0) * 100, 1) AS value
        FROM read_csv_auto('{run}/oil_production_daily.csv') p
        JOIN read_csv_auto('{run}/well_info.csv') w USING (well_id)
        WHERE p.prod_date >= ? AND p.prod_date <= ?
        GROUP BY 1, 2 ORDER BY 1, 2
    """,
    "商品量": """
        SELECT block_name AS block_name,
               strftime(settle_date, '%Y-%m') AS month,
               round(sum(net_weight), 2) AS value
        FROM read_csv_auto('{run}/monthly_settlement.csv')
        WHERE settle_date >= ? AND settle_date <= ?
        GROUP BY 1, 2 ORDER BY 1, 2
    """,
}


class DuckRunner:
    """S3 执行适配：DuckDB 直读 run 目录 CSV（模板 v0）。"""
    engine = "duckdb"
    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)


    def execute(self, query, snapshot_id=None, guard=None):
        import duckdb
        tpl = _SQL_TEMPLATES.get(query.metric)
        if tpl is None:
            raise NotImplementedError(f"查询模板 v0 未支持指标：{query.metric}")
        td = query.time_dimensions[0]
        start, end = td["dateRange"]
        # 内联日期（ISO 格式校验防注入；窗口来自结构化查询非自由文本）
        for d in (start, end):
            import re as _re
            if not _re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(d)):
                raise ValueError(f"非法日期：{d}")
        sql = tpl.format(run=str(self.run_dir)).replace("?", f"'{start}'").replace("?", f"'{end}'")
        rows = duckdb.sql(sql).fetchall()
        cols = ["block_name", "month", "value"]
        arrow = [{cols[0]: r[0], cols[1]: r[1], "value": float(r[2])} for r in rows]
        # dp5 合同：execute 返回 (rows, snapshot_id, stats) 三元组
        return arrow, (snapshot_id or 1), {"rows_scanned": len(rows), "engine": "duckdb"}


class _Clock:
    def today(self):
        return date.today()


class _Reg:
    def __init__(self, tables):
        from datetime import datetime
        self._r = {t: FreshnessInfo("offline", datetime(2026, 9, 26), "fieldforge-batch")
                   for t in tables}
        sub = RUN_DIR / "sensor_reading_subdaily.csv"
        if sub.is_file():
            self._r["sensor_reading_subdaily"] = FreshnessInfo(
                "realtime", datetime.now().replace(microsecond=0), "fieldforge-rt")

    def freshness(self, table):
        return self._r.get(table, FreshnessInfo("unknown", None, None))


def _latest_run() -> Path | None:
    """兼容两种目录形状：run 本体（含 manifest.json）或 runs 父目录（子目录各一个 run）。"""
    if (RUN_DIR / "manifest.json").is_file():
        return RUN_DIR
    if RUN_DIR.is_dir():
        runs = sorted((d for d in RUN_DIR.iterdir()
                       if d.is_dir() and (d / "manifest.json").is_file()), key=lambda d: d.name)
        return runs[-1] if runs else None
    return None


def build_tools() -> DataTools:
    run = _latest_run()
    if run is None:
        raise RuntimeError(f"{RUN_DIR} 下没有可用 run（先进料 fieldforge 产物）")
    engine = DuckRunner(run_dir=run)
    tables = {t: {"tier": "offline", "freshness": "2026-09-26", "metrics": []}
              for t in TABLES_ENV.split(",")}
    tables["oil_production_daily"]["metrics"] = ["产量完成率", "商品量"]
    gov = Governance(StaticPolicy(POLICY))
    sem = Semantic(StaticYaml.default())
    dsn = database_url()
    if dsn:  # P0 持久化热修：真身落 PG；未配置回落内存替身（本地开发不炸）
        audit = Audit(store=PgAuditStore(dsn), runner=engine, clock=_Clock())
        fresh = PgFreshnessRegistry(dsn)
    else:
        audit = Audit(store=InMemoryAudit(), runner=engine, clock=_Clock())
        fresh = _Reg(tables)
    router = FreshnessRouter(semantic=sem, freshness=fresh, clock=_Clock())
    runner = QueryRunner(catalog=None, engines=[engine], governance=gov)
    return DataTools(governance=gov, semantic=sem, runner=runner, router=router,
                     audit=audit, catalog_tables=tables,
                     default_ctx=UserContext("service", "总部", "admin"))


app = FastAPI(title="tpa-data-plane-api", version="0.1.0",
              description="Petro-Agent 数据面只读查询面。所有数据为合成数据，禁止用于储量申报、生产决策等真实业务。")


@app.middleware("http")
async def _synthetic_header(request, call_next):
    resp = await call_next(request)
    resp.headers["X-Synthetic-Data"] = "synthetic data; not for reserve filing or real decisions"
    return resp


_tools: DataTools | None = None


def tools() -> DataTools:
    global _tools
    if _tools is None:
        _tools = build_tools()
    return _tools


@app.get("/healthz")
def healthz():
    run = _latest_run()
    return {"status": "ok", "run": run.name if run else None, "deprecated": True}


# ---- 问答面退役（DP-02，2026-10-04）：六端点恒 410 Gone --------------------
# 裁决 15 归位：问答面已退役，功能将由主平台问数链重建承接（ADR-0003 裁决 15）。
_GONE = {
    "status": "410 Gone",
    "deprecated": True,
    "reason": "裁决 15 归位：问答面已退役（dp4 语义层/dp5 查询执行/dp8 对外工具面职责"
              "归主平台——语义=C1/semantics、查询=chat 问数链＋安检、对外工具=F1 expose），"
              "功能将由主平台问数链重建承接；本服务不再提供问答端点，本文件仅为参考标本。",
}


@app.get("/tables")
def tables():
    return JSONResponse(status_code=410, content=_GONE)


@app.get("/tables/{name}")
def describe(name: str):
    return JSONResponse(status_code=410, content=_GONE)


@app.post("/query")
def query(body: dict):
    return JSONResponse(status_code=410, content=_GONE)


@app.post("/replay")
def replay(body: dict):
    return JSONResponse(status_code=410, content=_GONE)


@app.get("/lineage/{metric}")
def lineage(metric: str):
    return JSONResponse(status_code=410, content=_GONE)


# ---- 以下为退役前实现留档（参考标本，不可执行） ----------------------------
# 依赖的 dp4/dp5/dp8 模块已 git rm（裁决 15 归位，git 历史备查）；路由恒 410，
# 本节代码永不触达，仅供主平台问数链重建时对照。勿再演进。
