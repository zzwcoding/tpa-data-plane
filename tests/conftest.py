"""tests 共享件：dp1 门面的最小测试替身（seam 合法位）。

DP-03 后湖真身=LakeCatalog（PG raw schema＋MinIO，tests/dp1 实跑）；旧湖替身类已随
「抛弃不修补」方针物理移除——编排层测试（dp2 三步契约／dp7 保留链）仍需要一个不依赖外部
服务的 Catalog 替身承载 append/快照语义，本替身**只活在测试侧**（conftest），实现侧零替身。
语义对齐真身：写面白名单／as-of 时点视图快照隔离（DP-03.fix F1：每业务键取最新批生效版，
未登记业务键的表=纯时点累积；键口径共用 lake_store._BUSINESS_KEYS——替身/真身一致断言在
tests/dp1 TestAsOfTimepointView）／expire 只清时间旅行可见性／committed_at=真时钟。
"""
from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from app.data_plane.lake_store import (BoundaryViolation, SnapshotGoneError, SnapshotInfo,
                                       _BUSINESS_KEYS)

_WRITE_ALLOWED = ("dp2.ingest", "dp3.transform")


def pytest_collection_modifyitems(items):
    """迁移双向测试先行：湖/进料真身测试（dp1/dp2）与 PG 集成（pg）依赖迁移态——
    down→up 复原在先，套件在全新库上也一次跑绿。"""
    first = [i for i in items if "tests/migrations" in str(i.fspath)]
    if first:
        items[:] = first + [i for i in items if i not in first]


class _FakeTableRef:
    def __init__(self, state: dict):
        self._state = state

    def scan(self, pred: str | None = None, fields: tuple[str, ...] = (),
             snapshot_id: int | None = None, limit: int | None = None) -> pa.Table:
        snaps = self._state["snaps"]
        if snapshot_id is None:
            table = snaps[max(snaps)]
        else:
            if snapshot_id not in snaps:
                raise SnapshotGoneError("快照不存在或已过保留窗口")
            table = snaps[snapshot_id]
        rows = table.to_pylist()
        if limit is not None:
            rows = rows[:limit]
        return pa.Table.from_pylist(rows, schema=table.schema)


class FakeSnapshotCatalog:
    """dp1 Catalog seam 的测试替身：记账 committed＋按快照存表（快照隔离语义最小集）。"""

    def __init__(self):
        self.committed: list[dict] = []
        self._caller: str | None = None
        self._tables: dict[str, dict] = {}
        self._next = 1

    def begin_caller(self, caller: str) -> None:
        self._caller = caller

    def create_table(self, name: str, schema: pa.Schema, comment: str = "",
                     partition_spec: str = "") -> None:
        empty = pa.Table.from_pylist([], schema=schema)
        self._tables[name] = {"schema": schema, "comment": comment,
                              "partition_spec": partition_spec, "snaps": {0: empty}}

    def load_table(self, name: str) -> _FakeTableRef:
        if name not in self._tables:
            raise KeyError(f"表不存在：{name}")
        return _FakeTableRef(self._tables[name])

    def append(self, table: str, arrow: pa.Table, commit_meta: dict) -> SnapshotInfo:
        if self._caller not in _WRITE_ALLOWED:
            raise BoundaryViolation(
                f"dp1.append 调用方 {self._caller!r} 不在白名单 {_WRITE_ALLOWED}（写面收敛）")
        state = self._tables[table]
        snaps = state["snaps"]
        current = snaps[max(snaps)]
        rows = arrow.to_pylist()
        key_cols = [c for c in _BUSINESS_KEYS.get(table, ()) if c in state["schema"].names]
        if current.num_rows == 0:
            merged = arrow
        else:
            if key_cols:
                # as-of 对齐真身（DP-03.fix F1）：同键新批覆盖旧批（每键取最新批生效版，
                # 不混用）；同批并列行全保留——A/B 班重复行是原始层合法内容
                incoming = {tuple(r[k] for k in key_cols) for r in rows}
                kept = pa.Table.from_pylist(
                    [r for r in current.to_pylist()
                     if tuple(r[k] for k in key_cols) not in incoming],
                    schema=current.schema)
            else:
                kept = current
            merged = pa.concat_tables([kept, arrow], promote_options="default")
        info = SnapshotInfo(snapshot_id=self._next,
                            committed_at=datetime.now().astimezone()
                            .replace(microsecond=0).isoformat(),
                            provenance=commit_meta)  # 真时钟（替身 "now" 字面的缺口不在复刻）
        self._next += 1
        snaps[info.snapshot_id] = merged
        self.committed.append(commit_meta)
        return info

    def expire(self, table: str, keep: int) -> list[int]:
        if self._caller != "dp7.retention":
            raise BoundaryViolation(
                f"dp1.expire 调用方 {self._caller!r} 不在白名单 ('dp7.retention',)（保留策略执行面收敛）")
        keep = max(int(keep), 1)
        snaps = self._tables[table]["snaps"]
        keep_ids = set(sorted(snaps)[-keep:])
        gone = [k for k in sorted(snaps) if k not in keep_ids]
        for k in gone:
            del snaps[k]
        return gone
