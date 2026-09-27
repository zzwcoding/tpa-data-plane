"""dp5 执行适配 QueryRunner（S1 切片）：四拍主干 = check_join→成本护栏→路由→执行。

- EngineAdapter seam：S1=DuckDB（e2e 接 read_parquet），trino/starrocks 留位；
- 闸前置在每条进引擎的链上同源判定（dp7.replay 亦经此路）；
- 成本护栏：超限两出口=拒绝（报表/导出类）/降级采样+显式声明（探索类），零静默全表扫。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class QueryResult:
    arrow: list | None = None
    snapshot_id: int | None = None
    stats: dict | None = None
    refused: bool = False
    reason: str = ""
    guard_note: str = ""


class QueryRunner:
    LIMIT_GUARD_DEFAULT = 100_000

    def __init__(self, *, catalog, engines: list, governance, limit_guard: int | None = None):
        self.catalog = catalog
        self.engines = {e.engine: e for e in engines}
        self.governance = governance
        self.limit_guard = limit_guard or self.LIMIT_GUARD_DEFAULT

    def _engine(self):
        return next(iter(self.engines.values()))  # S1：单引擎直选

    def run(self, query, snapshot_id: int | None = None, exploratory: bool = False) -> QueryResult:
        # 闸①：join 白名单（同源判定，不为 dp8 独有）
        d = self.governance.check_join(query)
        if not d.allowed:
            return QueryResult(refused=True, reason=f"Join 白名单拒绝：{d.violated_edges}")
        # 闸②：成本护栏
        if query.limit is not None and query.limit > self.limit_guard:
            if exploratory:
                note = f"已降级采样 {self.limit_guard} 行，非全量"
                rows, snap, stats = self._engine().execute(query, snapshot_id, guard=self.limit_guard)
                return QueryResult(arrow=rows, snapshot_id=snap, stats=stats, guard_note=note)
            return QueryResult(refused=True,
                               reason=f"limit {query.limit} 超成本护栏 {self.limit_guard}，拒绝执行")
        rows, snap, stats = self._engine().execute(query, snapshot_id)
        return QueryResult(arrow=rows, snapshot_id=snap, stats=stats)

    def sandbox_query(self, table: str, pred: str | None = None,
                      fields: tuple[str, ...] = (), limit: int | None = None):
        return self.catalog.load_table(table).scan(pred=pred, fields=fields, limit=limit)

    def engines(self):
        return [type(e).__name__ for e in self.engines.values()]
