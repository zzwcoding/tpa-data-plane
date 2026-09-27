"""dp1 湖底座 LakeStore（S1 切片）。

- InMemoryLake：Seam=Catalog/TableRef 的测试替身（dp 卡 L46-47），Arrow 表内存实现，
  语义与 PyIceberg 对齐（append-only/快照隔离/谓词列裁剪/ColumnStats）；
- 真实链路（SqlCatalog jdbc→postgres + MinIO + PyIceberg）为同一接口的第二实现，
  在 e2e（票 10）接入——切 catalog 不换调用方（DP-W-D1-1）；
- 写面收敛：Catalog.append 仅 dp2/dp3 可调（begin_caller 声明身份，默认越界），
  门面**故意不暴露** overwrite/delete/upsert（DP1 底稿 L131-132）。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa


class SnapshotGoneError(Exception):
    """快照不存在或已过保留窗口——绝不静默回退当前快照（DP-W-D7-3）。"""


class BoundaryViolation(Exception):
    """dp1.append 被白名单外模块调用（DP-W-D1-4 写面审计）。"""


@dataclass(frozen=True)
class SchemaInfo:
    columns: tuple
    comment: str
    partition_spec: str
    current_snapshot: int | None


@dataclass(frozen=True)
class SnapshotInfo:
    snapshot_id: int
    committed_at: str
    provenance: dict


@dataclass(frozen=True)
class ColumnStats:
    min_max: dict
    null_count: dict


_WRITE_ALLOWED = ("dp2.ingest", "dp3.transform")


class _MemTableRef:
    def __init__(self, state: "_TableState"):
        self._state = state

    def schema(self) -> SchemaInfo:
        s = self._state
        return SchemaInfo(columns=tuple((f.name, str(f.type)) for f in s.schema),
                          comment=s.comment, partition_spec=s.partition_spec,
                          current_snapshot=s.snapshots[-1].snapshot_id if s.snapshots else None)

    def scan(self, pred: str | None = None, fields: tuple[str, ...] = (),
             snapshot_id: int | None = None, limit: int | None = None) -> pa.Table:
        s = self._state
        if snapshot_id is None:
            table = s.current
        else:
            if snapshot_id not in s.by_snapshot:
                raise SnapshotGoneError("快照不存在或已过保留窗口")
            table = s.by_snapshot[snapshot_id]
        rows = table.to_pylist()
        if pred:  # S1 谓词：安全的等值/比较过滤（教育口径：只支持 col op value）
            rows = [r for r in rows if _eval_pred(pred, r)]
        if fields:
            rows = [{k: r.get(k) for k in fields} for r in rows]
        if limit is not None:
            rows = rows[:limit]
        return pa.Table.from_pylist(rows, schema=_project(table.schema, fields))

    def metrics(self) -> ColumnStats:
        table = self._state.current
        min_max, nulls = {}, {}
        for name in table.column_names:
            col = table[name].to_pylist()
            nums = [v for v in col if isinstance(v, (int, float)) and v is not None]
            if nums:
                min_max[name] = (min(nums), max(nums))
            nulls[name] = sum(1 for v in col if v is None)
        return ColumnStats(min_max=min_max, null_count=nulls)


def _project(schema: pa.Schema, fields: tuple[str, ...]) -> pa.Schema:
    if not fields:
        return schema
    return pa.schema([schema.field(f) for f in fields])


def _eval_pred(pred: str, row: dict) -> bool:
    """S1 谓词求值：`col >= value` / `col == value` / `col <= value`（值带引号则按字符串）。"""
    for op in (">=", "<=", "==", ">", "<"):
        if op in pred:
            col, _, raw = pred.partition(op)
            col, raw = col.strip(), raw.strip().strip("'\"")
            v = row.get(col.strip())
            if v is None:
                return False
            cmp_raw = type(v)(raw) if isinstance(v, (int, float)) else raw
            return {"==": v == cmp_raw, ">=": v >= cmp_raw, "<=": v <= cmp_raw,
                    ">": v > cmp_raw, "<": v < cmp_raw}[op]
    raise ValueError(f"不支持的谓词形状：{pred}")


@dataclass
class _TableState:
    schema: pa.Schema
    comment: str
    partition_spec: str
    current: pa.Table
    snapshots: list = field(default_factory=list)
    by_snapshot: dict = field(default_factory=dict)
    next_id: int = 1


class InMemoryCatalog:
    """S1 测试替身 Catalog：语义与 PyIceberg 对齐。"""

    def __init__(self, lake: "InMemoryLake", backend: str):
        self._tables: dict[str, _TableState] = {}
        self._lake = lake
        self.backend = backend
        self._caller = None  # 当前写方身份（默认未知=越界）

    def begin_caller(self, caller: str) -> None:
        """声明写方身份（dp2/dp3 进出场时调用）；身份与写面白名单比对。"""
        self._caller = caller

    def create_table(self, name: str, schema: pa.Schema, comment: str = "",
                     partition_spec: str = "") -> None:
        state = _TableState(schema=schema, comment=comment, partition_spec=partition_spec,
                            current=pa.Table.from_pylist([], schema=schema))
        seed = SnapshotInfo(snapshot_id=0, committed_at="created", provenance={})
        state.snapshots.append(seed)
        state.by_snapshot[0] = state.current
        self._tables[name] = state

    def load_table(self, name: str) -> _MemTableRef:
        if name not in self._tables:
            raise KeyError(f"表不存在：{name}")
        return _MemTableRef(self._tables[name])

    def expire(self, table: str, keep: int) -> list[int]:
        """滚动保留窗口：保留最近 keep 个快照，其余 expire（存储边界不是篡改口）。"""
        state = self._tables[table]
        keep_ids = {s.snapshot_id for s in state.snapshots[-keep:]}
        gone = [s.snapshot_id for s in state.snapshots if s.snapshot_id not in keep_ids]
        state.by_snapshot = {k: v for k, v in state.by_snapshot.items() if k in keep_ids}
        return gone

    def append(self, table: str, arrow: pa.Table, commit_meta: dict) -> SnapshotInfo:
        if self._caller not in _WRITE_ALLOWED:
            raise BoundaryViolation(
                f"dp1.append 调用方 {self._caller!r} 不在白名单 {_WRITE_ALLOWED}（写面收敛）")
        state = self._tables[table]
        state.current = pa.concat_tables([state.current, arrow])
        info = SnapshotInfo(snapshot_id=state.next_id, committed_at="now",
                            provenance=commit_meta)
        state.next_id += 1
        state.snapshots.append(info)
        state.by_snapshot[info.snapshot_id] = state.current
        return info


class InMemoryLake:
    """S1 湖底座替身：load_catalog 形状与真实实现一致（换 backend 不换调用方）。"""

    def load_catalog(self, backend: str, **cfg):
        if backend not in ("jdbc", "rest", "federated"):
            raise ValueError(f"未知 catalog backend：{backend}")
        return InMemoryCatalog(self, backend)
