"""dp7 快照审计与回放（S1 切片）：结论绑定快照四元组，绝不静默换快照。

【DP-02 归位标注（2026-10-04，ADR-0003 裁决 15）】dp7 快照审计回放语义归主平台问数链，
本文件保留至问数链重建时移植思想、不搬代码（本票不删）。
取舍定案（按 import 实况）：整文件保留不拆——AuditStore/AuditRecord/InMemoryAudit 同时是
PG 审计真身 pg_store.PgAuditStore 的数据契约（DP-05 质检实体仍要用 audit 的 PG 真身部分，
拆掉回放段会同时拆断该契约与 tests/dp7 本票未授权删除的测试）；Audit.replay 段保留作
dp7 回放语义标本。StructuredQuery 原定义在 dp4 semantic.py（已随裁决 15 归位删除），
现逐字内联落位于本文件。

- AuditStore seam：内存版（InMemoryAudit）=测试替身（PRD 裁决 #1），真身=pg_store.PgAuditStore
  （P0 持久化热修，docs/adr/0002）；S2 对账作业/S3 集团审计面留位；
- record 在作答成功后调用（记录的是"已经给出的答案"），original 随记录钉死；
- replay 按**记录快照**重跑（边界规则第 6 行），快照被 expire 清理 → SnapshotGoneError
  沿调用链浮出，绝不静默用当前快照替代（DP-W-D7-3）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class StructuredQuery:
    """四件套 + 显式槽位（裁决 #10：base_table/limit）。

    原 dp4（semantic.py）钦定类型，逐字保留；dp4 已随 ADR-0003 裁决 15 归位主平台
    （DP-02 删除），dp7 审计契约仍以本形状落库/回放，主平台问数链重建时以主平台
    自有 StructuredQuery 为准。
    """
    metric: str
    base_table: str
    tables: tuple
    measures: tuple
    dims: tuple
    time_dimensions: tuple
    filters: dict
    limit: int | None = None


@dataclass(frozen=True)
class AuditRef:
    audit_id: str


@dataclass(frozen=True)
class RetentionPolicy:
    rolling: int  # 滚动保留窗口（快照数；教学演绎值，可配）
    hard_cap: int
    audit_rule: str = "审计记录留存随快照窗口"


@dataclass(frozen=True)
class AuditRecord:
    question_id: str
    asked_at: str
    query: StructuredQuery
    snapshot_id: int
    semantic_version: str
    quality_status: str
    original: list


class AuditStore(Protocol):
    """AuditStore seam 合同：put/get；InMemoryAudit=测试替身，PgAuditStore=PG 真身。"""

    def put(self, record: AuditRecord) -> None: ...
    def get(self, audit_id: str) -> AuditRecord: ...


class InMemoryAudit:
    """测试替身（PRD 裁决 #1）：内存 dict，进程重启即失——仅供测试/无 PG 本地兜底；
    真身=pg_store.PgAuditStore。"""

    def __init__(self):
        self._records: dict[str, AuditRecord] = {}

    def put(self, record: AuditRecord) -> None:
        self._records[record.question_id] = record

    def get(self, audit_id: str) -> AuditRecord:
        if audit_id not in self._records:
            raise KeyError(f"审计记录不存在：{audit_id}")
        return self._records[audit_id]


class Audit:
    def __init__(self, *, store: AuditStore, runner, clock, retention: RetentionPolicy | None = None):
        self.store = store
        self.runner = runner
        self.clock = clock
        self._retention = retention or RetentionPolicy(rolling=50, hard_cap=200)

    def enforce_retention(self, catalog, table: str) -> list[int]:
        """保留策略执行入口（RF2-05）：以 dp7.retention 身份驱动 dp1.expire。"""
        cat = catalog.cat if hasattr(catalog, "cat") else catalog
        cat.begin_caller("dp7.retention")
        return cat.expire(table, keep=self._retention.rolling)

    def retention_policy(self) -> RetentionPolicy:
        return self._retention

    def record(self, question_id: str, query: StructuredQuery, snapshot_id: int,
               semantic_version: str, quality_status: str, original: list | None = None) -> AuditRef:
        rec = AuditRecord(question_id=question_id, asked_at=str(self.clock.today()),
                          query=query, snapshot_id=snapshot_id,
                          semantic_version=semantic_version, quality_status=quality_status,
                          original=original if original is not None else [])
        self.store.put(rec)
        return AuditRef(audit_id=question_id)

    def replay(self, ref: AuditRef | str) -> "ReplayResult":
        ref_id = ref.audit_id if isinstance(ref, AuditRef) else ref
        rec = self.store.get(ref_id)  # 未知 ref → KeyError 显式失败
        result = self.runner.run(rec.query, snapshot_id=rec.snapshot_id)  # 按记录快照重跑
        # RF-05 等值归一：dp5 合同 arrow=ArrowTable（或 list），与记录侧 to_pylist 比较
        current = result.arrow
        if hasattr(current, "to_pylist"):
            current = current.to_pylist()
        match = current == rec.original
        return ReplayResult(match=match, original=rec.original, current=current)


@dataclass(frozen=True)
class ReplayResult:
    match: bool
    original: list
    current: list
