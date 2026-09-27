"""票 05 · dp7 快照审计测试：四元组记录 / 同快照回放 / 过期显式报错 / 保留窗口。"""
from __future__ import annotations

import pytest

from app.data_plane.audit import Audit, InMemoryAudit, RetentionPolicy
from app.data_plane.lake_store import SnapshotGoneError
from app.data_plane.semantic import StructuredQuery


def query():
    return StructuredQuery(metric="产量完成率", base_table="oil_production_daily",
                           tables=("oil_production_daily", "well_info"),
                           measures=("completion_rate",), dims=(("block_name", "well_info"),),
                           time_dimensions=(), filters={})


ROWS = [{"block_name": "渤南", "完成率": 93.4}]


class FakeRunner:
    def __init__(self, rows=ROWS, gone=False):
        self.rows, self.gone = rows, gone

    def run(self, query, snapshot_id=None):
        if self.gone:
            raise SnapshotGoneError("快照不存在或已过保留窗口")
        return type("R", (), {"arrow": self.rows, "snapshot_id": snapshot_id})


class Clock:
    def today(self):
        return __import__("datetime").date(2026, 9, 27)


def make_audit(runner=None, retention=None):
    store = InMemoryAudit()
    return Audit(store=store, runner=runner or FakeRunner(), clock=Clock(),
                 retention=retention)


class TestAudit:
    def test_record_binds_four_tuple(self):
        a = make_audit()
        ref = a.record("q-1", query(), snapshot_id=7,
                       semantic_version="0.1.0", quality_status="pass", original=ROWS)
        rec = a.store.get(ref.audit_id)
        assert rec.question_id == "q-1" and rec.snapshot_id == 7
        assert rec.semantic_version == "0.1.0" and rec.quality_status == "pass"
        assert rec.original == ROWS and rec.asked_at == "2026-09-27"  # DP-W-D7-1

    def test_replay_same_snapshot_matches(self):
        a = make_audit()
        ref = a.record("q-1", query(), 7, "0.1.0", "pass", original=ROWS)
        result = a.replay(ref)  # DP-W-D7-2
        assert result.match is True and result.original == ROWS

    def test_replay_detects_drift(self):
        a = make_audit(runner=FakeRunner(rows=[{"block_name": "渤南", "完成率": 99.9}]))
        ref = a.record("q-1", query(), 7, "0.1.0", "pass", original=ROWS)
        assert a.replay(ref).match is False

    def test_replay_snapshot_gone_raises_explicitly(self):
        """DP-W-D7-3：绝不静默用当前快照替代。"""
        a = make_audit(runner=FakeRunner(gone=True))
        ref = a.record("q-1", query(), 7, "0.1.0", "pass", original=ROWS)
        with pytest.raises(SnapshotGoneError):
            a.replay(ref)

    def test_replay_unknown_ref(self):
        with pytest.raises(KeyError):
            make_audit().replay("no-such-ref")

    def test_retention_policy(self):
        rp = make_audit(retention=RetentionPolicy(rolling=50, hard_cap=200)).retention_policy()
        assert (rp.rolling, rp.hard_cap) == (50, 200)
