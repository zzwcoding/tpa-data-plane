#!/usr/bin/env python3
"""data-plane 中台库迁移 runner（DP-03）：SQL 迁移文件＋最小实现。

选型说明：99 册 §4.4 冻结的迁移形态是 Alembic，但本仓现状（S1 以来）无 Alembic 依赖与
迁移链——「迁移工具随仓现状定」（DP-03 票 1）故取 SQL 文件＋最小 runner；目录形状沿用
`migrations/` 线性序列、禁手工改库、upgrade/downgrade 成对的 99 册纪律。

用法（--dsn 缺省依次回落 DATABASE_URL / TEST_DATABASE_URL）：
  python3 tools/migrate.py up            # 按线性序应用全部未应用迁移
  python3 tools/migrate.py down          # 回滚最近一个已应用迁移
  python3 tools/migrate.py down --all    # 回滚全部

文件契约：migrations/<version>_<name>.up.sql 与 .down.sql 成对（version=三位序号，字典序即应用序）。
已应用账：data_plane.schema_migrations(version TEXT PRIMARY KEY, name TEXT, applied_at TIMESTAMPTZ)。
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"
UP_DOWN = re.compile(r"^(\d{3})_[a-z0-9_]+\.(up|down)\.sql$")


def _dsn(cli: str | None) -> str:
    dsn = cli or os.environ.get("DATABASE_URL") or os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        sys.exit("migrate: 未提供 --dsn，且 DATABASE_URL/TEST_DATABASE_URL 均未设置")
    return dsn


def _connect(dsn: str):
    try:
        import psycopg
    except ImportError as e:
        raise RuntimeError("migrate 需要 psycopg（.venv 内已装；pip install \"psycopg[binary]\"）") from e
    return psycopg.connect(dsn)


def _pairs() -> list[tuple[str, Path, Path]]:
    ups = {}
    downs = {}
    for f in sorted(MIGRATIONS.iterdir()):
        m = UP_DOWN.match(f.name)
        if not m:
            continue
        (ups if m.group(2) == "up" else downs)[m.group(1)] = f
    missing = sorted(set(ups) ^ set(downs))
    if missing:
        sys.exit(f"migrate: up/down 未成对：{missing}")
    return [(v, ups[v], downs[v]) for v in sorted(ups)]


def _ensure_ledger(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS data_plane")
        cur.execute("""CREATE TABLE IF NOT EXISTS data_plane.schema_migrations (
                           version TEXT PRIMARY KEY,
                           name TEXT NOT NULL,
                           applied_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
    conn.commit()


def _applied(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM data_plane.schema_migrations ORDER BY version")
        return [r[0] for r in cur.fetchall()]


def _exec_file(conn, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def up(conn) -> int:
    _ensure_ledger(conn)
    done = set(_applied(conn))
    n = 0
    for version, up_f, _ in _pairs():
        if version in done:
            continue
        _exec_file(conn, up_f)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO data_plane.schema_migrations (version, name) VALUES (%s, %s)",
                        (version, up_f.name.removesuffix(".up.sql")))
        conn.commit()
        print(f"up  {version} applied ({up_f.name})")
        n += 1
    if n == 0:
        print("up  已是最新（无未应用迁移）")
    return n


def _ledger_exists(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", ("data_plane.schema_migrations",))
        return cur.fetchone()[0] is not None


def down(conn, all_: bool) -> int:
    n = 0
    while True:
        if not _ledger_exists(conn):
            # 账本已随回滚消失（meta schema 被 down——如 001）＝已到未迁移态；
            # 不再重建账本，避免"回滚到底"仍残留空 schema。
            break
        _ensure_ledger(conn)
        applied = _applied(conn)
        if not applied or (not all_ and n >= 1):
            break
        version = applied[-1]
        pair = next(p for p in _pairs() if p[0] == version)
        # 账先行：先删账再执行回滚——001 的 down 会 DROP data_plane schema（账随之消失），
        # 先删账保证回滚失败时账与结构不脱钩（重跑 down 可重入）。
        with conn.cursor() as cur:
            cur.execute("DELETE FROM data_plane.schema_migrations WHERE version = %s", (version,))
        conn.commit()
        _exec_file(conn, pair[2])
        print(f"down {version} reverted ({pair[1].name.removesuffix('.up.sql')})")
        n += 1
    if n == 0:
        print("down 已清空（无已应用迁移）")
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description="data-plane 中台库 SQL 迁移 runner")
    ap.add_argument("command", choices=["up", "down"])
    ap.add_argument("--dsn", default=None)
    ap.add_argument("--all", action="store_true", help="down 用：回滚全部")
    args = ap.parse_args()
    with _connect(_dsn(args.dsn)) as conn:
        if args.command == "up":
            up(conn)
        else:
            down(conn, args.all)
    return 0


if __name__ == "__main__":
    sys.exit(main())
