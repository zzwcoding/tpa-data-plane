"""dp2 进料与新鲜度（S1 切片）：validate→append→register 三步契约。

- 闸在编排层：validate_batch 先行（不合格 raise BatchRejected，append 不发生——DP-W-D2-1）；
  表未配规则集=同样拒绝（UnruledTableRejected，fail-closed：无规则不许免检入湖，DP-01 收口）；
- run_batch_load 以 dp2.ingest 身份调 dp1.append（写面白名单），provenance 随 commit 落元数据；
- FreshnessRegistry seam：内存版（InMemoryFreshnessRegistry）=测试替身（PRD 裁决 #1），
  真身=pg_store.PgFreshnessRegistry（P0 持久化热修，docs/adr/0002）；同表多管道取 as_of 最新；
- cdc_pipeline_spec 只产形状（S2 零实装）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.data_plane.freshness import FreshnessInfo


class BatchRejected(Exception):
    def __init__(self, report):
        self.report = report
        super().__init__(f"质检不合格（rule_ids={report.rule_ids}），批被拒绝入湖")


class UnruledTableRejected(Exception):
    """入湖闸 fail-closed（DP-W-D2-1，DP-01 收口 A2-6）：表未登记质检规则集=无闸可用，
    显式拒绝入湖，不许免检 append。"""

    def __init__(self, table: str):
        self.table = table
        super().__init__(f"表 {table} 未登记质检规则集：入湖必过质检，无规则=拒绝（fail-closed）")


@dataclass(frozen=True)
class Provenance:
    quality_report_id: str
    operator: str


@dataclass(frozen=True)
class FlinkJobSpec:
    variant: str
    source_db: str
    tables: tuple
    sink_semantics: str
    config_ref: str


class InMemoryFreshnessRegistry:
    """测试替身（PRD 裁决 #1）：内存 {table: FreshnessInfo}，进程重启即失——仅供测试/无 PG 本地兜底；
    真身=pg_store.PgFreshnessRegistry。同表多管道取 as_of 最新（DP2 底稿 L52-64）。"""

    def __init__(self):
        self._reg: dict[str, FreshnessInfo] = {}

    def register(self, table: str, tier: str, as_of: datetime, pipeline: str) -> None:
        cur = self._reg.get(table)
        if cur is None or (cur.as_of is None) or (as_of and _as_dt(as_of) > _as_dt(cur.as_of)):
            self._reg[table] = FreshnessInfo(tier=tier, as_of=as_of, pipeline=pipeline)

    def freshness(self, table: str) -> FreshnessInfo:
        return self._reg.get(table, FreshnessInfo(tier="unknown", as_of=None, pipeline=None))


def _as_dt(v):
    if isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v))


class Ingest:
    def __init__(self, *, catalog, freshness, rules: dict | None = None):
        self.catalog = catalog          # dp1 门面（append 身份由本模块声明）
        self._registry = freshness      # FreshnessRegistry seam（替身|PG 真身）；名字避开 freshness() 方法（属性冲突教训）
        self._rules = rules or {}       # 表 → 质量规则清单（入湖必过质检）

    def run_batch_load(self, table: str, records, provenance: Provenance):
        rules = self._rules.get(table)
        if not rules:  # fail-closed（DP-W-D2-1）：表不在规则清单/规则清单为空=无闸可用，拒绝入湖
            raise UnruledTableRejected(table)
        from app.data_plane.transform import validate_batch
        report, cleaned = validate_batch(records, rules)
        if not report.passed:
            raise BatchRejected(report)
        records = cleaned
        cat = self.catalog.cat if hasattr(self.catalog, "cat") else self.catalog
        cat.begin_caller("dp2.ingest")  # 写面白名单声明（DP-W-D1-4）
        snap = cat.append(table, records, commit_meta={"provenance": {
            "quality_report_id": provenance.quality_report_id,
            "operator": provenance.operator}})
        self._registry.register(table, tier="offline",
                                as_of=_committed_as_of(), pipeline="batch-shift-report")
        return snap

    def register_freshness(self, table: str, tier: str, as_of: datetime, pipeline: str) -> None:
        self._registry.register(table, tier=tier, as_of=as_of, pipeline=pipeline)

    def freshness(self, table: str) -> FreshnessInfo:
        return self._registry.freshness(table)

    def cdc_pipeline_spec(self, source_db: str, tables: list[str]) -> FlinkJobSpec:
        return FlinkJobSpec(variant="pipeline", source_db=source_db, tables=tuple(tables),
                            sink_semantics="at-least-once + 主键幂等（源表必须有主键）",
                            config_ref="deploy/flink/cdc.pipeline.yaml（S2 形状，零实装）")


def _committed_as_of() -> datetime:
    return datetime.now().replace(microsecond=0)
