"""P0 持久化热修集成测试：PG 真身（PgFreshnessRegistry / PgAuditStore）。

- 无 TEST_DATABASE_URL 或未安装 psycopg 时整文件 skip——CI/本地默认全绿；
- 用表即生产表名（freshness_registry / audit_record），每用例先 DROP 重建，需独占测试库。
"""
from __future__ import annotations

import os
from datetime import datetime

import pytest

psycopg = pytest.importorskip("psycopg")

TEST_DSN = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="需要 TEST_DATABASE_URL 指向可写 PG（PG 集成测试）")


@pytest.fixture()
def clean_db():
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS freshness_registry")
        cur.execute("DROP TABLE IF EXISTS audit_record")
    yield TEST_DSN


def test_freshness_roundtrip_survives_restart(clean_db):
    from app.data_plane.pg_store import PgFreshnessRegistry

    reg = PgFreshnessRegistry(clean_db)  # 首次连接自动建表
    reg.register("oil_production_daily", "offline", datetime(2026, 9, 26), "fieldforge-batch")
    reg.register("sensor_reading_subdaily", "realtime",
                 datetime(2026, 9, 28, 10, 0, 0), "fieldforge-rt")

    restarted = PgFreshnessRegistry(clean_db)  # 进程重启 = 全新实例
    info = restarted.freshness("oil_production_daily")
    assert info.tier == "offline" and info.pipeline == "fieldforge-batch"
    assert info.as_of == datetime(2026, 9, 26)
    rt = restarted.freshness("sensor_reading_subdaily")
    assert rt.tier == "realtime" and rt.as_of == datetime(2026, 9, 28, 10, 0, 0)
    # 未登记 = unknown（DP6 查二）
    unknown = restarted.freshness("no_such_table")
    assert unknown.tier == "unknown" and unknown.as_of is None and unknown.pipeline is None


def test_freshness_newest_as_of_wins(clean_db):
    from app.data_plane.pg_store import PgFreshnessRegistry

    reg = PgFreshnessRegistry(clean_db)
    reg.register("t", "offline", datetime(2026, 9, 25), "p1")
    reg.register("t", "offline", datetime(2026, 9, 26), "p2")
    assert reg.freshness("t").pipeline == "p2"  # 同表多管道取 as_of 最新
    reg.register("t", "offline", datetime(2026, 9, 20), "p3")  # 旧 as_of 不回退
    assert reg.freshness("t").pipeline == "p2"
    # 空 as_of 占位可被后续登记替代（对齐内存替身语义）
    reg.register("u", "offline", None, "p0")
    assert reg.freshness("u").pipeline == "p0"
    reg.register("u", "realtime", datetime(2026, 9, 28, 9, 0), "p1")
    assert reg.freshness("u").tier == "realtime" and reg.freshness("u").pipeline == "p1"


def test_audit_roundtrip_survives_restart_and_missing_ref(clean_db):
    from app.data_plane.audit import AuditRecord
    from app.data_plane.pg_store import PgAuditStore
    from app.data_plane.semantic import StructuredQuery

    q = StructuredQuery(
        metric="产量完成率", base_table="oil_production_daily",
        tables=("oil_production_daily", "well_info"), measures=("completion_rate",),
        dims=(("block_name", "well_info"),),
        time_dimensions=({"start": "2026-08-01", "end": "2026-08-31", "granularity": "month"},),
        filters={"block_name": "渤南"}, limit=10)
    original = [{"block_name": "渤南", "month": "2026-08", "value": 94.2}]
    store = PgAuditStore(clean_db)
    store.put(AuditRecord(question_id="q-1", asked_at="2026-09-28", query=q,
                          snapshot_id=7, semantic_version="0.1.0", quality_status="pass",
                          original=original))

    restarted = PgAuditStore(clean_db)  # 进程重启 = 全新实例，四元组不丢
    got = restarted.get("q-1")
    assert got.question_id == "q-1" and got.asked_at == "2026-09-28"
    assert got.snapshot_id == 7 and got.semantic_version == "0.1.0"
    assert got.quality_status == "pass" and got.original == original
    assert got.query.metric == q.metric and got.query.base_table == q.base_table
    assert got.query.tables == q.tables and got.query.dims == q.dims  # tuple 还原
    assert got.query.time_dimensions == q.time_dimensions
    assert got.query.filters == q.filters and got.query.limit == 10

    with pytest.raises(KeyError):  # 未知 ref 显式失败（Audit.replay 依赖该合同）
        restarted.get("no_such_ref")


def test_audit_put_overwrites_same_question(clean_db):
    from app.data_plane.audit import AuditRecord
    from app.data_plane.pg_store import PgAuditStore
    from app.data_plane.semantic import StructuredQuery

    q = StructuredQuery(metric="商品量", base_table="monthly_settlement", tables=(),
                        measures=(), dims=(), time_dimensions=(), filters={})
    store = PgAuditStore(clean_db)
    store.put(AuditRecord(question_id="q-1", asked_at="2026-09-28", query=q,
                          snapshot_id=1, semantic_version="0.1.0", quality_status="pass",
                          original=[]))
    store.put(AuditRecord(question_id="q-1", asked_at="2026-09-28", query=q,
                          snapshot_id=2, semantic_version="0.1.0", quality_status="pass",
                          original=[{"v": 1}]))  # 同 question_id 覆盖写（对齐内存替身）
    assert store.get("q-1").snapshot_id == 2
    assert store.get("q-1").original == [{"v": 1}]


def test_explicit_construction_without_dsn_fails():
    import app.data_plane.pg_store as pg

    saved = os.environ.pop("DATABASE_URL", None)
    try:
        with pytest.raises(ValueError):
            pg.PgFreshnessRegistry(None)
        with pytest.raises(ValueError):
            pg.PgAuditStore(None)
    finally:
        if saved is not None:
            os.environ["DATABASE_URL"] = saved


def test_database_url_env_contract(monkeypatch):
    import app.data_plane.pg_store as pg

    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert pg.database_url() is None  # 未配置 = 装配点回落内存替身
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:y@localhost/dp")
    assert pg.database_url() == "postgresql://x:y@localhost/dp"
