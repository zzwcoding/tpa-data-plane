"""DP-03 · 中台库迁移双向测试：up 建三层 schema＋8 张 as-is 源表，down 全量回滚。

- 无 TEST_DATABASE_URL 或未安装 psycopg 时整文件 skip（与 tests/pg 同机制）；
- 需独占目标库的 raw/clean/data_plane 三个 schema（down 会整体 DROP CASCADE）——
  用主仓 compose 本地 PG（postgresql://petro:petro@localhost:5433/petro）实跑；
- 8 表业务列以 teach 设定书 §2.2/§A 字典为真源（本测试抽查核心表的列形与类型族）。
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

TEST_DSN = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="需要 TEST_DATABASE_URL 指向可写 PG（迁移双向实跑）")

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "migrations"

# 设定书 §A 字典抽查：表 → (逻辑键注记, 业务列抽查 {列: 类型族})
EXPECTED = {
    "well_info": ("一井一行", {"well_id": "character varying", "well_type": "character",
                               "first_prod_date": "date", "block_name": "character varying"}),
    "oil_production_daily": ("一井一日", {"well_id": "character varying", "prod_date": "date",
                                          "oil_output": "numeric", "liquid_output": "numeric",
                                          "water_cut": "numeric", "plan_output": "numeric",
                                          "open_days": "numeric", "oil_pressure": "numeric",
                                          "report_shift": "character"}),
    "water_injection_daily": ("井号+日期", {"well_id": "character varying", "prod_date": "date",
                                            "inj_volume": "numeric", "pump_pressure": "numeric",
                                            "layer_split": "character varying"}),
    "well_overhaul": ("井号+起止", {"well_id": "character varying", "overhaul_start": "date",
                                    "overhaul_end": "date", "overhaul_type": "character varying",
                                    "batch_id": "character varying"}),
    "well_measures": ("井号+措施次", {"well_id": "character varying", "measure_type": "character varying",
                                      "job_date": "date", "effective_from": "date",
                                      "effective_to": "date", "incr_oil_annual": "numeric"}),
    "fluid_test": ("井号+取样日期", {"well_id": "character varying", "sample_date": "date",
                                     "water_cut_lab": "numeric", "density": "numeric",
                                     "api_gravity": "numeric"}),
    "equip_status": ("设备号+时间戳", {"equip_id": "character varying", "equip_type": "character varying",
                                       "load": "numeric", "current": "numeric",
                                       "stop_reason_code": "character varying"}),
    "monthly_settlement": ("区块+结算月", {"block_name": "character varying", "settle_date": "date",
                                           "net_weight": "numeric"}),
}


def _run(*args: str) -> str:
    r = subprocess.run([sys.executable, str(ROOT / "tools" / "migrate.py"), *args,
                        "--dsn", TEST_DSN],
                       capture_output=True, text=True, cwd=ROOT)
    assert r.returncode == 0, f"migrate {' '.join(args)} 失败：\n{r.stdout}\n{r.stderr}"
    return r.stdout


def _columns(cur, schema: str, table: str) -> dict:
    cur.execute(
        """SELECT column_name, data_type FROM information_schema.columns
           WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""",
        (schema, table))
    return dict(cur.fetchall())


def _tables(cur, schema: str) -> set[str]:
    cur.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema = %s""", (schema,))
    return {r[0] for r in cur.fetchall()}


def _migration_state_absent(cur) -> None:
    """未迁移态断言：raw/clean 消失＋迁移账本消失（data_plane schema 允许被运行时 ensure
    幂等重建，但迁移账必须不在——账本是「已迁移」的唯一标志）。"""
    present = {r[0] for r in cur.execute(
        "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%'").fetchall()}
    assert not {"raw", "clean"} & present
    assert cur.execute("SELECT to_regclass('data_plane.schema_migrations')").fetchone()[0] is None


def test_up_down_up_roundtrip():
    """迁移双向证据：down 清场 → up 全量建 → 断言 → down 全量回滚 → 断言空 → up 复原。"""
    _run("down", "--all")  # 先回滚到未迁移态（幂等：无账时为 no-op）
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        _migration_state_absent(cur)

    _run("up")  # upgrade 全量
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        assert {"raw", "clean", "data_plane"} <= {
            r[0] for r in cur.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%'").fetchall()}
        assert set(EXPECTED) <= _tables(cur, "raw")
        for table, (key_note, col_probe) in EXPECTED.items():
            cols = _columns(cur, "raw", table)
            for name, family in col_probe.items():
                assert cols.get(name) == family, f"raw.{table}.{name} 列形={cols.get(name)}≠{family}"
            # 湖技术列三件（raw 层快照寻址/批次血缘/入库时刻）；
            # 例外＝well_overhaul：业务列 batch_id（补录批次）撞名，技术列让位为 _src_batch_id
            tech = {"_snapshot_id", "_ingested_at"} | {
                "_src_batch_id" if table == "well_overhaul" else "_batch_id"}
            assert tech <= set(cols), (table, set(cols))
            # as-is 纪律：不设业务主键约束（A/B 班重复行等脏特征是原始层合法内容），
            # 逻辑键以表 COMMENT 登记（如 oil_production_daily: 一井一日）
            cur.execute("SELECT obj_description(%s::regclass, 'pg_class')",
                        (f"raw.{table}",))
            comment = cur.fetchone()[0] or ""
            assert key_note in comment, f"raw.{table} 注记缺逻辑键说明：{comment}"
        # 湖元数据两表（catalog：表注册＋快照账）
        assert {"lake_table", "lake_snapshot"} <= _tables(cur, "data_plane")
        # 数据面自有 schema：freshness/audit 两表归位（DP-03 票 1）
        assert {"freshness_registry", "audit_record"} <= _tables(cur, "data_plane")
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='data_plane' AND table_name='lake_snapshot'")
        assert {"snapshot_id", "table_name", "batch_id", "committed_at", "rows_added",
                "data_object_key", "manifest", "provenance"} <= {r[0] for r in cur.fetchall()}

    out = _run("down", "--all")  # downgrade 全量回滚
    assert "001" in out  # 回滚账单可见（down 001 reverted）
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        _migration_state_absent(cur)

    _run("up")  # 复原到已迁移态（后续湖测试依赖）


def test_lake_boot_before_migration_up_still_idempotent():
    """DP-03.fix F2：湖先 boot（LakeCatalog._ensure_meta 幂等兜底建同名索引）再跑迁移 up
    不炸——重排序部署场景（湖 boot 先于迁移）的幂等实测。boot 以 _META_DDL 直执行等价复现
    （_ensure_meta 的湖元数据段；不引 boto3 保持本文件 psycopg-only）。"""
    _run("down", "--all")  # 未迁移态
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        # 自愈半途失败的残留：账本在但空（上次 up 半途而废）→ down 不再下探，按 001.down
        # 同口径归零（DROP 三 schema），保证本测试从真正未迁移态起步
        leftover = cur.execute(
            "SELECT to_regclass('data_plane.schema_migrations')").fetchone()[0]
        if leftover is not None:
            cur.execute("DROP SCHEMA IF EXISTS raw CASCADE")
            cur.execute("DROP SCHEMA IF EXISTS clean CASCADE")
            cur.execute("DROP SCHEMA IF EXISTS data_plane CASCADE")
        assert cur.execute("SELECT to_regclass('data_plane.schema_migrations')").fetchone()[0] is None
    from app.data_plane.lake_store import _META_DDL
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS data_plane")
        cur.execute(_META_DDL.format(meta="data_plane"))  # 湖先 boot：ensure_meta 兜底（同形 DDL）
        assert cur.execute("SELECT to_regclass("
                           "'data_plane.idx_lake_snapshot_table')").fetchone()[0] is not None
    out = _run("up")  # 迁移后补跑：001 的 CREATE INDEX 若缺 IF NOT EXISTS 在此 DuplicateTable
    assert "001 applied" in out
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        assert set(EXPECTED) <= _tables(cur, "raw")  # 迁移在 boot 之上补齐全量结构


def test_midplatform_template_grants_all_eight_tables():
    """验收 3：midplatform 只读账号 SQL 模板在迁移文件旁，且覆盖恰为 8 张源表。"""
    tpl = (MIGRATIONS / "midplatform_readonly.sql").read_text(encoding="utf-8")
    assert "CREATE ROLE midplatform" in tpl
    for table in EXPECTED:
        assert f"raw.{table}" in tpl, f"模板缺 {table} 的 GRANT"
    # 只读边界：GRANT 行只触 raw schema——clean/data_plane 与湖元数据零授权（默认拒绝）
    grant_lines = [ln for ln in tpl.splitlines() if ln.strip().upper().startswith("GRANT")]
    assert grant_lines, "模板无 GRANT 行"
    for ln in grant_lines:
        assert "clean" not in ln and "data_plane" not in ln, f"越界授权：{ln}"
