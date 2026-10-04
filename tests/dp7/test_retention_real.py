"""S2-03 · dp7 快照保留实跑：expire 真链路（非 mock raise）→ replay SnapshotGoneError。

DP-03：旧湖替身类已物理移除——本链改用 tests/conftest 的 FakeSnapshotCatalog
（seam 合法位，按快照存表＋expire 清可见性）；expire 的湖真身语义（PG 快照账＋BoundaryViolation
白名单）在 tests/dp1/test_lake_store.py 实跑机判。Runner 走 dp1 按快照 scan 的链路照旧非 mock。"""
from __future__ import annotations

import pyarrow as pa
import pytest

from app.data_plane.audit import Audit, InMemoryAudit, RetentionPolicy, StructuredQuery
from app.data_plane.lake_store import SnapshotGoneError

from tests.conftest import FakeSnapshotCatalog


def q():
    return StructuredQuery(metric="产量完成率", base_table="oil_production_daily",
                           tables=("oil_production_daily",), measures=("oil_output",),
                           dims=(), time_dimensions=(), filters={})


class Runner:
    """dp5 真链路替身：走 dp1 按快照 scan（不 mock SnapshotGoneError）。"""
    engine = "duckdb"

    def __init__(self, catalog):
        self.catalog = catalog

    def run(self, query, snapshot_id=None, **kw):
        arrow = self.catalog.load_table("t").scan(snapshot_id=snapshot_id).to_pylist()
        return type("R", (), {"arrow": arrow, "snapshot_id": snapshot_id})


class Clock:
    def today(self):
        return __import__("datetime").date(2026, 9, 27)


@pytest.fixture()
def env():
    cat = FakeSnapshotCatalog()
    cat.begin_caller("dp2.ingest")
    cat.create_table("t", schema=pa.Table.from_pylist([{"v": 0}]).schema)
    audit = Audit(store=InMemoryAudit(), runner=Runner(cat), clock=Clock(),
                  retention=RetentionPolicy(rolling=2, hard_cap=200))
    return cat, audit


def test_retention_real_chain(env):
    cat, audit = env
    # 5 次进料，每次记录一个绑定当期快照的结论
    refs = []
    for i in range(5):
        cat.begin_caller("dp2.ingest")
        snap = cat.append("t", pa.Table.from_pylist([{"v": i}]), {})
        view = cat.load_table("t").scan(snapshot_id=snap.snapshot_id).to_pylist()
        refs.append(audit.record(f"q-{i}", q(), snap.snapshot_id, "0.1.0", "pass",
                                 original=view))  # original=当期快照全视图
        # 每轮执行保留策略（rolling=2）
        audit.enforce_retention(cat, "t")

    # 快照 1–3 已被 expire：replay 显式报错（真链路，DP-W-D7-3）
    with pytest.raises(SnapshotGoneError):
        audit.replay(refs[0])
    with pytest.raises(SnapshotGoneError):
        audit.replay(refs[2])
    # 快照 4、5 仍在窗口内：replay match
    r4 = audit.replay(refs[3])
    assert r4.match is True
    r5 = audit.replay(refs[4])
    assert r5.match is True
