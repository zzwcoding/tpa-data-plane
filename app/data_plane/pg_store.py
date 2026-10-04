"""PG 持久化真身（P0 持久化热修）：FreshnessRegistry / AuditStore 的 PostgreSQL 实现。

- 选址裁决（云上 PG 一库三用：Iceberg JDBC catalog + 新鲜度 + 审计）见 docs/adr/0002；
- 公开签名与内存替身一致：register/freshness（dp2）、put/get（dp7）——构造注入即可互换；
- 连接走 DATABASE_URL 环境变量；未设置时装配点回落内存替身（本地开发不炸），
  已设但未装 psycopg = 显式报错（不静默降级丢持久化）；
- 建表幂等（CREATE TABLE IF NOT EXISTS），实例构造时自动 ensure_schema；
- 【DP-03 归位】两表落**数据面自有 schema `data_plane`**（Owner 拍板 2：中台库归数据面
  全权；三层 schema=raw/clean/data_plane，规范结构=migrations/001，此处幂等 ensure 同形兜底）。
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import date, datetime

from app.data_plane.audit import AuditRecord, StructuredQuery  # StructuredQuery 原 dp4 定义，裁决 15 归位后落位 audit.py（DP-02）
from app.data_plane.freshness import FreshnessInfo

_SCHEMA = "data_plane"

_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {_SCHEMA};
CREATE TABLE IF NOT EXISTS {_SCHEMA}.freshness_registry (
    table_name TEXT PRIMARY KEY,
    tier       TEXT NOT NULL,
    as_of      TIMESTAMP,
    pipeline   TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {_SCHEMA}.audit_record (
    audit_id         TEXT PRIMARY KEY,
    asked_at         TEXT NOT NULL,
    query            JSONB NOT NULL,
    snapshot_id      BIGINT NOT NULL,
    semantic_version TEXT NOT NULL,
    quality_status   TEXT NOT NULL,
    original         JSONB NOT NULL DEFAULT '[]'::jsonb
);
"""


def database_url() -> str | None:
    """PG 连接串；未配置 = 无 PG，装配点应回落内存替身。"""
    return os.environ.get("DATABASE_URL") or None


def _connect(dsn: str):
    try:
        import psycopg
    except ImportError as e:  # 已要 PG 却没装驱动：显式失败，不静默丢持久化
        raise RuntimeError("已配置 DATABASE_URL 但未安装 psycopg（pip install psycopg）") from e
    return psycopg.connect(dsn)


def _as_dt(v) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    return datetime.fromisoformat(str(v))


class PgFreshnessRegistry:
    """dp2 FreshnessRegistry 真身：{table: FreshnessInfo} 落 PG，进程重启不丢。

    语义对齐内存替身：同表多管道取 as_of 最新（旧 as_of 不回退、空 as_of 可被替代）；
    未登记=unknown。as_of 为 date 入参时落库存为当日 0 点 timestamp（读出为 datetime）。
    """

    def __init__(self, dsn: str | None = None):
        self._dsn = dsn or database_url()
        if not self._dsn:
            raise ValueError("PgFreshnessRegistry 需要 DATABASE_URL（或显式传 dsn）")
        with _connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(_DDL)

    def register(self, table: str, tier: str, as_of, pipeline: str) -> None:
        with _connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_SCHEMA}.freshness_registry AS f (table_name, tier, as_of, pipeline)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (table_name) DO UPDATE
                    SET tier = EXCLUDED.tier, as_of = EXCLUDED.as_of,
                        pipeline = EXCLUDED.pipeline, updated_at = now()
                    WHERE f.as_of IS NULL
                       OR (EXCLUDED.as_of IS NOT NULL AND EXCLUDED.as_of > f.as_of)
                """,
                (table, tier, _as_dt(as_of), pipeline))

    def freshness(self, table: str) -> FreshnessInfo:
        with _connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT tier, as_of, pipeline FROM {_SCHEMA}.freshness_registry "
                        "WHERE table_name = %s",
                        (table,))
            row = cur.fetchone()
        if row is None:
            return FreshnessInfo(tier="unknown", as_of=None, pipeline=None)
        return FreshnessInfo(tier=row[0], as_of=row[1], pipeline=row[2])


class PgAuditStore:
    """dp7 AuditStore 真身：审计四元组落 PG（audit_record 表），重启不丢、可跨进程回放。

    语义对齐内存替身：同 audit_id（=question_id）覆盖写；未知 ref=KeyError 显式失败。
    query 以 JSONB 落库，get 时还原为 StructuredQuery（tuple 字段复原）。
    """

    def __init__(self, dsn: str | None = None):
        self._dsn = dsn or database_url()
        if not self._dsn:
            raise ValueError("PgAuditStore 需要 DATABASE_URL（或显式传 dsn）")
        with _connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(_DDL)

    def put(self, record: AuditRecord) -> None:
        with _connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_SCHEMA}.audit_record
                    (audit_id, asked_at, query, snapshot_id, semantic_version,
                     quality_status, original)
                VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::jsonb)
                ON CONFLICT (audit_id) DO UPDATE
                    SET asked_at = EXCLUDED.asked_at, query = EXCLUDED.query,
                        snapshot_id = EXCLUDED.snapshot_id,
                        semantic_version = EXCLUDED.semantic_version,
                        quality_status = EXCLUDED.quality_status,
                        original = EXCLUDED.original
                """,
                (record.question_id, str(record.asked_at),
                 json_dumps(asdict(record.query)), record.snapshot_id,
                 record.semantic_version, record.quality_status,
                 json_dumps(record.original)))

    def get(self, audit_id: str) -> AuditRecord:
        with _connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""SELECT asked_at, query, snapshot_id, semantic_version, quality_status,
                          original
                   FROM {_SCHEMA}.audit_record WHERE audit_id = %s""", (audit_id,))
            row = cur.fetchone()
        if row is None:
            raise KeyError(f"审计记录不存在：{audit_id}")
        return AuditRecord(question_id=audit_id, asked_at=row[0],
                           query=_query_from_json(row[1]), snapshot_id=row[2],
                           semantic_version=row[3], quality_status=row[4],
                           original=row[5])


def json_dumps(v) -> str:
    return json.dumps(v, ensure_ascii=False, default=str)


def _query_from_json(d: dict) -> StructuredQuery:
    d = dict(d)
    for key in ("tables", "measures", "dims", "time_dimensions"):
        if key in d and d[key] is not None:
            d[key] = _to_tuples(d[key])
    return StructuredQuery(**d)


def _to_tuples(v):
    if isinstance(v, list):
        return tuple(_to_tuples(x) for x in v)
    return v
