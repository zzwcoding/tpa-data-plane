"""dp2 进料与新鲜度（DP-03 真链路）：validate→append→register 三步契约接湖真身。

- 闸在编排层：validate_batch 先行（不合格 raise BatchRejected，append 不发生——DP-W-D2-1）；
  表未配规则集=同样拒绝（UnruledTableRejected，fail-closed：无规则不许免检入湖，DP-01 收口）；
- run_batch_load 以 dp2.ingest 身份调 dp1.append（写面白名单），provenance 随 commit 落元数据；
  真身=LakeCatalog（PG raw schema＋MinIO 原始文件——DP-03 方案 C1，替身湖已物理移除）；
- **CSV 绕全链退役（DP-03）**：run_csv_load 把 run 目录 CSV 的读取路径收编为「进料输入」
  （fieldforge 产物 CSV→进料管道→raw schema），消费面不经 CSV 直查——湖 scan/SQL 只读 raw 层；
  manifest 血缘随 commit 登记（dp3 transform.py 的 manifest 格式沿用：metadata/nodes/
  parent_map/child_map，落 data_plane.lake_snapshot.manifest）；
- FreshnessRegistry seam：内存版（InMemoryFreshnessRegistry）=测试替身（PRD 裁决 #1），
  真身=pg_store.PgFreshnessRegistry（P0 持久化热修，docs/adr/0002；DP-03 归位 data_plane schema）；
  同表多管道取 as_of 最新；
- cdc_pipeline_spec 只产形状（S2 零实装）。
"""
from __future__ import annotations

import io
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa

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

    def run_batch_load(self, table: str, records, provenance: Provenance,
                       *, source: str | None = None, batch_id: str | None = None):
        """批量进料（内存/Arrow 记录形态）；batch_id/source 透传快照账（血缘与幂等寻址用）。"""
        return self._load(table, records, provenance, source=source, batch_id=batch_id)

    def run_csv_load(self, table: str, csv_path, provenance: Provenance,
                     *, source: str | None = None, batch_id: str | None = None):
        """CSV 进料（DP-03：run 目录 CSV 的读取路径收编为「进料输入」）。

        - CSV 全列按文本读入（as-is：CSV 本是文本，定型发生在 raw 结构化落库的显式转换）；
        - source 缺省取 `<父目录名>/<文件名>`（fieldforge 产物 run 目录的自然标识），
          manifest 血缘 source 节点随之登记——数据只经本管道进湖，不经 CSV 直查。"""
        path = Path(csv_path)
        with open(path, "rb") as f:  # 表头行嗅探定列名（DP-03.fix F4-③：全文只读一遍）
            header = f.readline()
        names = pa.csv.read_csv(io.BytesIO(header)).column_names
        as_text = pa.csv.read_csv(path, convert_options=pa.csv.ConvertOptions(
            column_types={n: pa.string() for n in names}))  # as-is：CSV 全列文本读入
        src = source or f"{path.parent.name}/{path.name}"
        return self._load(table, as_text, provenance, source=src, batch_id=batch_id)

    def _load(self, table: str, records, provenance: Provenance,
              *, source: str | None, batch_id: str | None):
        rules = self._rules.get(table)
        if not rules:  # fail-closed（DP-W-D2-1）：表不在规则清单/规则清单为空=无闸可用，拒绝入湖
            raise UnruledTableRejected(table)
        from app.data_plane.transform import validate_batch
        report, cleaned = validate_batch(records, rules)
        if not report.passed:
            raise BatchRejected(report)
        records = cleaned
        cat = self.catalog  # DP-03.fix F4-①：旧 MemCatalog 适配死路（hasattr .cat）清除
        cat.begin_caller("dp2.ingest")  # 写面白名单声明（DP-W-D1-4）
        # 批次号缺省与 lake_store 同形 uuid（DP-03.fix F4-②：秒级时间戳同秒碰撞隐患清除）
        batch_id = batch_id or f"b-{uuid.uuid4().hex[:12]}"
        snap = cat.append(table, records, commit_meta={
            "provenance": {"quality_report_id": provenance.quality_report_id,
                           "operator": provenance.operator},
            "source": source,
            "batch_id": batch_id,
            "manifest": _lineage_manifest(table, batch_id, source, records.column_names)})
        # P6 修复（DP-04，盘点 A 组点名 as_of 虚标）：登记 as_of 读**快照账 committed_at**
        # （manifest 随行落账时刻＝数据真实生效时刻），不再取登记动作的墙钟 now()——
        # 登记晚于提交时（重试/积压/重启追赶）不再把新鲜度虚标到登记瞬间。
        self._registry.register(table, tier="offline",
                                as_of=_snapshot_as_of(snap), pipeline="batch-shift-report")
        return snap

    def register_freshness(self, table: str, tier: str, as_of: datetime, pipeline: str) -> None:
        self._registry.register(table, tier=tier, as_of=as_of, pipeline=pipeline)

    def freshness(self, table: str) -> FreshnessInfo:
        return self._registry.freshness(table)

    def cdc_pipeline_spec(self, source_db: str, tables: list[str]) -> FlinkJobSpec:
        return FlinkJobSpec(variant="pipeline", source_db=source_db, tables=tuple(tables),
                            sink_semantics="at-least-once + 主键幂等（源表必须有主键）",
                            config_ref="deploy/flink/cdc.pipeline.yaml（S2 形状，零实装）")


def _lineage_manifest(table: str, batch_id: str, source: str | None,
                      columns) -> dict:
    """进料血缘 manifest（dp3 transform.py 的 manifest 格式沿用）：
    湖表节点 model.petro.<table> ← 进料输入 source.petro.<source>，parent_map/child_map 互逆。"""
    model = f"model.petro.{table}"
    src = f"source.petro.{source or 'manual-batch'}"
    return {
        "metadata": {"tool": "tpa-dp2-ingest", "version": "0.1.0",
                     "table": table, "batch_id": batch_id},
        "nodes": {model: {"columns": list(columns), "resource_type": "model"},
                  src: {"resource_type": "source"}},
        "parent_map": {model: [src]},
        "child_map": {src: [model]},
    }


def _snapshot_as_of(snap) -> datetime:
    """快照账 committed_at → freshness as_of（P6：数据生效时刻，非登记墙钟；
    快照无时刻时显式报错——宁缺不虚标）。"""
    committed = getattr(snap, "committed_at", None)
    if not committed:
        raise ValueError(f"快照缺 committed_at，freshness 拒绝虚标（snapshot={snap!r}）")
    return _as_dt(committed)
