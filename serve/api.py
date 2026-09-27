"""tpa-data-plane API（S3）：dp8 五只读工具 HTTP 化 + 老王问数查询面。

- 组装=真实 dp 模块（治理闸/语义/路由/审计/执行），数据=fieldforge 云端 run 目录；
- 查询模板 v0：产量完成率 / 商品量（S1 形状），其余指标回"未支持"明示；
- 每个响应带 X-Synthetic-Data 头；只读红线：无任何写端点。
"""
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from app.data_plane.audit import Audit, InMemoryAudit
from app.data_plane.freshness import FreshnessInfo, FreshnessRouter
from app.data_plane.governance import Governance, StaticPolicy, UserContext
from app.data_plane.query_runner import QueryRunner
from app.data_plane.semantic import Semantic, StaticYaml
from app.data_plane.tools_mcp import DataTools

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
        sql = tpl.format(run=str(RUN_DIR)).replace("?", f"'{start}'").replace("?", f"'{end}'")
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
    tables = {t: {"tier": "offline", "freshness": "2026-09-26", "metrics": []}
              for t in TABLES_ENV.split(",")}
    tables["oil_production_daily"]["metrics"] = ["产量完成率", "商品量"]
    gov = Governance(StaticPolicy(POLICY))
    sem = Semantic(StaticYaml.default())
    audit = Audit(store=InMemoryAudit(), runner=DuckRunner(), clock=_Clock())
    router = FreshnessRouter(semantic=sem, freshness=_Reg(tables), clock=_Clock())
    runner = QueryRunner(catalog=None, engines=[DuckRunner()], governance=gov)
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
    return {"status": "ok", "run": run.name if run else None}


@app.get("/tables")
def tables():
    return {"tables": tools().discover_tables()}


@app.get("/tables/{name}")
def describe(name: str):
    return tools().describe_table(name)


@app.post("/query")
def query(body: dict):
    try:
        out = tools().query(body.get("metric"), body.get("dims", []),
                            body.get("window"), body.get("filters", {}))
    except NotImplementedError as e:
        raise HTTPException(400, str(e)) from e
    if isinstance(out, dict) and out.get("refused"):
        return JSONResponse(status_code=422, content=out)
    return out


@app.post("/replay")
def replay(body: dict):
    ref = body.get("audit_ref")
    if not ref:
        raise HTTPException(400, "body 需要 {audit_ref}")
    return tools().replay(ref)


@app.get("/lineage/{metric}")
def lineage(metric: str):
    return tools().lineage(metric)
