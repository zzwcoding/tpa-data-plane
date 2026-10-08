"""DP-04 · agg 星型三表 PG 实跑：DDL 形断言（主仓迁移 0014 落地）＋管道对账（dim join 完整）
＋限写三表专用账号权限负测试（99 册 §2.12/§4.4，ADR-0003 裁决 15）。

- agg schema 结构由**主仓 Alembic 0014**供应（本仓测试只断言合同形，不第二真源建表）——
  未落迁移时整文件 skip（提示先 `alembic upgrade head`）；
- 限写账号＝migrations/agg_writer.sql 模板实装（模板纪律同 midplatform_readonly.sql：
  建号随部署批，测试自带建号＋清场）；
- 无 TEST_DATABASE_URL 或缺 psycopg/boto3 时整文件 skip（与 tests/pg 同机制）。
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
boto3 = pytest.importorskip("boto3")

from app.data_plane.agg import AggStarWriter, refresh_from_catalog
from app.data_plane.ingest import Ingest, Provenance
from app.data_plane.lake_store import load_catalog
from app.data_plane.transform import RULES_OIL_DAILY, RULES_WELL_INFO
from tests.conftest import dsn_database_name

TEST_DSN = os.environ.get("TEST_DATABASE_URL")
ENDPOINT = os.environ.get("TEST_MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("TEST_MINIO_ACCESS_KEY", "petro")
SECRET_KEY = os.environ.get("TEST_MINIO_SECRET_KEY", "petro12345")
BUCKET = os.environ.get("TEST_LAKE_BUCKET", "tpa-raw-test")
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"

OIL_BATCH, WELL_BATCH = "dp4-agg-e2e-oil", "dp4-agg-e2e-well"

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="需要 TEST_DATABASE_URL＋MinIO（agg 星型实跑，主仓 compose 本地）")


@pytest.fixture(scope="module")
def agg_ready():
    """agg schema 三表由主仓迁移 0014 供应；未跑迁移则整文件 skip（结构零第二真源）。"""
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('agg.fct_daily_metering') IS NOT NULL "
                    "AND to_regclass('agg.dim_well') IS NOT NULL "
                    "AND to_regclass('agg.dim_date') IS NOT NULL")
        if not cur.fetchone()[0]:
            pytest.skip("agg 三表未落地——先在主仓跑 `alembic upgrade head`（0014）", allow_module_level=False)


def _q(sql, params=None):
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        if params:  # 无参语句不进 placeholder 处理（SQL 里的 LIKE '%' 字面量）
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        if cur.description is None:  # DDL/DML 无结果集（DROP ROLE 等）——不 fetch
            return []
        return cur.fetchall()


def _drop_writer_role():
    """清号（依赖特权先 DROP OWNED 再 DROP ROLE——幂等，角色不存在即空操作）。"""
    _q("""DO $$
         BEGIN
           IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tpa_agg_writer') THEN
             DROP OWNED BY tpa_agg_writer;
             DROP ROLE tpa_agg_writer;
           END IF;
         END $$""")


class TestAggStarShape:
    """验收 (a)：三表 DDL 合同形＋fct 粒度断言（一行=井×日唯一——组合 PK 承载）。"""

    def test_fct_grain_pk_and_index(self, agg_ready):
        cols = {r[0]: r[1] for r in _q(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='agg' AND table_name='fct_daily_metering'")}
        assert set(cols) == {"well_id", "prod_date", "oil_output", "liquid_output",
                             "oil_pressure", "batch_id", "quality_flag"}
        assert cols["oil_output"] == cols["liquid_output"] == "numeric"      # §A.2 源口径
        assert cols["oil_pressure"] == "numeric"
        pk = [r[0] for r in _q(
            """SELECT a.attname FROM pg_index i
               JOIN pg_class c ON c.oid = i.indrelid
               JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
               WHERE i.indisprimary AND c.relname='fct_daily_metering'
                 AND c.relnamespace='agg'::regnamespace
               ORDER BY a.attnum""")]
        assert pk == ["well_id", "prod_date"]                                # grain=井×日唯一
        assert _q("SELECT to_regclass('agg.idx_agg_fct_prod_date') IS NOT NULL")[0][0]
        assert _q("SELECT to_regclass('agg.dim_well_pkey') IS NOT NULL")[0][0]
        assert _q("SELECT to_regclass('agg.dim_date_pkey') IS NOT NULL")[0][0]

    def test_dim_columns_hierarchy_and_calendar(self, agg_ready):
        wells = {r[0]: r[1] for r in _q(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='agg' AND table_name='dim_well'")}
        assert set(wells) == {"well_id", "block_name", "field_name"}         # 层级属性=区块/油田
        dates = {r[0] for r in _q(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='agg' AND table_name='dim_date'")}
        assert dates == {"date", "month", "quarter"}                          # 月/季对照列


class TestPipelineToAgg:
    """验收 (b)：fixture CSV→进料→清洗→质检→agg 行对账（dim join 完整）＋幂等重刷。"""

    @pytest.fixture()
    def wired(self):
        cat = load_catalog(dsn=TEST_DSN, s3_endpoint=ENDPOINT, s3_access_key=ACCESS_KEY,
                           s3_secret_key=SECRET_KEY, bucket=BUCKET)
        ing = Ingest(catalog=cat, freshness=InMemoryFresh(), rules={
            "oil_production_daily": RULES_OIL_DAILY, "well_info": RULES_WELL_INFO})
        yield ing, cat
        _q("DELETE FROM agg.fct_daily_metering WHERE batch_id LIKE 'dp4-agg-e2e%'")
        _q("DELETE FROM agg.dim_well WHERE well_id IN "
           "('N-01','N-02','Z-01','Z-02')")
        _q("DELETE FROM agg.dim_date WHERE date BETWEEN '2025-09-01' AND '2025-09-30'")
        for tbl, batch in (("oil_production_daily", OIL_BATCH), ("well_info", WELL_BATCH)):
            _q(f"DELETE FROM raw.{tbl} WHERE _batch_id = %s", (batch,))
            _q("DELETE FROM data_plane.lake_snapshot WHERE table_name = %s AND batch_id = %s",
               (tbl, batch))

    def test_ingest_then_refresh_rows_reconcile(self, wired):
        ing, cat = wired
        ing.run_csv_load("oil_production_daily", FIXTURES / "oil_production_daily.csv",
                         Provenance(quality_report_id="qr-dp4", operator="阿强"), batch_id=OIL_BATCH)
        ing.run_csv_load("well_info", FIXTURES / "well_info.csv",
                         Provenance(quality_report_id="qr-dp4", operator="阿强"), batch_id=WELL_BATCH)
        writer = AggStarWriter(TEST_DSN)
        reg = InMemoryFresh()
        summary = refresh_from_catalog(cat, writer, freshness=reg)

        lake_rows = _q("SELECT count(*) FROM raw.oil_production_daily WHERE _batch_id=%s",
                       (OIL_BATCH,))[0][0]
        fct_rows = _q("SELECT count(*) FROM agg.fct_daily_metering "
                      "WHERE batch_id LIKE 'dp4-agg-e2e%'")[0][0]
        assert fct_rows == lake_rows                    # 行对账：fct＝质检净批全量（grain 已由闸保证）
        assert summary["fct_daily_metering"] == fct_rows

        # dim join 完整：每条 fct 井/日都能挂上维度（星型可查）
        orphans = _q("""SELECT count(*) FROM agg.fct_daily_metering f
                        LEFT JOIN agg.dim_well w USING (well_id)
                        LEFT JOIN agg.dim_date d ON d.date = f.prod_date
                        WHERE w.well_id IS NULL OR d.date IS NULL""")[0][0]
        assert orphans == 0
        dim_wells = _q("SELECT count(*) FROM agg.dim_well")[0][0]
        assert dim_wells >= 4                           # fixture 四口井全进维

        # 汇总层水位登记（freshness 更新——三 tier 模型的 offline 档）
        for t in ("agg.fct_daily_metering", "agg.dim_well", "agg.dim_date"):
            info = reg.freshness(t)
            assert info.tier == "offline" and info.as_of is not None, t

    def test_refresh_idempotent_same_grain(self, wired):
        """同源重刷：grain 唯一（组合 PK upsert），行数不翻倍。"""
        ing, cat = wired
        ing.run_csv_load("oil_production_daily", FIXTURES / "oil_production_daily.csv",
                         Provenance(quality_report_id="qr-dp4", operator="阿强"), batch_id=OIL_BATCH)
        ing.run_csv_load("well_info", FIXTURES / "well_info.csv",
                         Provenance(quality_report_id="qr-dp4", operator="阿强"), batch_id=WELL_BATCH)
        writer = AggStarWriter(TEST_DSN)
        refresh_from_catalog(cat, writer)
        refresh_from_catalog(cat, writer)               # 重刷
        n = _q("SELECT count(*) FROM agg.fct_daily_metering WHERE batch_id LIKE 'dp4-agg-e2e%'")[0][0]
        dup = _q("""SELECT count(*) FROM (SELECT well_id, prod_date FROM agg.fct_daily_metering
                    GROUP BY 1,2 HAVING count(*) > 1) d""")[0][0]
        assert dup == 0 and n == _q("SELECT count(*) FROM raw.oil_production_daily "
                                    "WHERE _batch_id=%s", (OIL_BATCH,))[0][0]


class TestRestrictedWriterAccount:
    """验收 (c)：限写三表专用账号——写 agg 恰三表放行，写 agg 以外对象一律被拒。"""

    @pytest.fixture()
    def writer_role(self, agg_ready):
        tpl = (Path(__file__).resolve().parents[1] / ".." / "migrations" / "agg_writer.sql")
        sql = tpl.read_text(encoding="utf-8").replace("${AGG_WRITER_PASSWORD}", "dp4-probe-pw")
        _drop_writer_role()
        for chunk in sql.split(";"):  # 逐句执行（剥注释行——模板头注不进语句）
            stmt = "\n".join(l for l in chunk.splitlines()
                             if not l.strip().startswith("--")).strip()
            if stmt:
                _q(stmt)
        # 库名从 TEST_DSN 派生（V2-21）：硬编码 /petro＝误指残留——守卫放行 petro_test
        # 后两值分家即假红；限写账号探针必须与被测库同库
        yield "postgresql://tpa_agg_writer:dp4-probe-pw@%s/%s" % (
            TEST_DSN.split("@")[1].split("/")[0], dsn_database_name(TEST_DSN))
        _drop_writer_role()

    def _denied(self, dsn, sql, params=()):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(sql, params)
                conn.rollback()

    def test_write_agg_three_tables_allowed(self, writer_role):
        dsn = writer_role
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO agg.fct_daily_metering
                           (well_id, prod_date, oil_output, liquid_output, oil_pressure, batch_id)
                           VALUES ('ZZ-00','2025-09-01',1,2,3,'dp4-probe')""")
            cur.execute("UPDATE agg.fct_daily_metering SET oil_output=2 WHERE well_id='ZZ-00'")
            cur.execute("DELETE FROM agg.fct_daily_metering WHERE well_id='ZZ-00'")
            cur.execute("INSERT INTO agg.dim_well (well_id, block_name, field_name) "
                        "VALUES ('ZZ-00','渤中','渤海油田')")
            cur.execute("DELETE FROM agg.dim_well WHERE well_id='ZZ-00'")
            cur.execute("INSERT INTO agg.dim_date (date, month, quarter) "
                        "VALUES ('2025-09-01','2025-09','2025-Q3')")
            cur.execute("DELETE FROM agg.dim_date WHERE date='2025-09-01'")

    def test_write_outside_agg_denied(self, writer_role, agg_ready):
        dsn = writer_role
        _q("CREATE TABLE IF NOT EXISTS public.dp4_guard_probe (x int)")   # 主账号立靶
        try:
            self._denied(dsn, "INSERT INTO public.dp4_guard_probe VALUES (1)")      # 平台表
            self._denied(dsn, "INSERT INTO raw.oil_production_daily "
                              "(well_id, prod_date, _snapshot_id, _batch_id) "
                              "VALUES ('ZZ-00','2025-09-01',1,'dp4-probe')")        # 中台 raw 层
            self._denied(dsn, "INSERT INTO data_plane.lake_snapshot (table_name, batch_id, "
                              "data_object_key, manifest) VALUES ('t','b','k','{}'::jsonb)")  # 湖账
            self._denied(dsn, "CREATE TABLE agg.evil (x int)")                       # agg schema 无 CREATE
            self._denied(dsn, "DROP TABLE agg.dim_well")                             # 无 DDL 权
            self._denied(dsn, "ALTER TABLE agg.dim_well ADD COLUMN hack int")        # 无 ALTER 权
            self._denied(dsn, "INSERT INTO agg.fct_daily_metering "
                              "(well_id, prod_date) SELECT well_id, prod_date "
                              "FROM raw.oil_production_daily")                       # 跨层读源被拒
        finally:
            _q("DROP TABLE IF EXISTS public.dp4_guard_probe")

    def test_template_grants_are_closed_world(self, agg_ready):
        """模板闭世界机判：授权语句恰限 agg 三表＋clean 层只读；raw/data_plane/public 零授权。"""
        tpl = (Path(__file__).resolve().parents[1] / ".." / "migrations" / "agg_writer.sql"
               ).read_text(encoding="utf-8")
        for t in ("agg.dim_well", "agg.dim_date", "agg.fct_daily_metering"):
            assert f"ON {t} TO tpa_agg_writer" in tpl, t
        assert "GRANT SELECT ON ALL TABLES IN SCHEMA clean TO tpa_agg_writer" in tpl
        assert "GRANT USAGE ON SCHEMA clean TO tpa_agg_writer" in tpl
        grant_lines = [l for l in tpl.splitlines() if l.strip().startswith("GRANT")]
        assert len(grant_lines) == 6  # 闭世界：agg USAGE＋三表写读各一句＋clean USAGE＋clean 只读


class InMemoryFresh:
    """freshness seam 测试替身（同 tests/dp2 MemFreshness 形）。"""

    def __init__(self):
        from app.data_plane.ingest import InMemoryFreshnessRegistry
        self._reg = InMemoryFreshnessRegistry()

    def register(self, table, tier, as_of, pipeline):
        self._reg.register(table, tier, as_of, pipeline)

    def freshness(self, table):
        return self._reg.freshness(table)
