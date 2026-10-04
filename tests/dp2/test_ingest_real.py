"""DP-03 · 进料真链路端到端：fieldforge 产物 CSV → run_csv_load（validate→append→register）
→ raw schema 行数对账 ＋ MinIO 原始对象在 ＋ manifest 血缘完整。

- CSV 绕全链退役的兑现面：run 目录 CSV 的读取路径收编为「进料输入」——数据只经
  进料管道进 raw schema，消费面一律走湖（本测试用 lake scan 回查，不 read_csv 直查）；
- 无 TEST_DATABASE_URL 或缺 psycopg/boto3 时整文件 skip（与 tests/pg 同机制）；
  实跑环境＝主仓 compose PG16（5433）＋MinIO（9000），迁移需先 up（tests/migrations 先行）。
"""
from __future__ import annotations

import io
import os
from pathlib import Path

import pyarrow as pa
import pytest

boto3 = pytest.importorskip("boto3")
psycopg = pytest.importorskip("psycopg")

from app.data_plane.ingest import Ingest, Provenance
from app.data_plane.lake_store import load_catalog
from app.data_plane.pg_store import PgFreshnessRegistry
from app.data_plane.transform import RULES_OIL_DAILY

TEST_DSN = os.environ.get("TEST_DATABASE_URL")
ENDPOINT = os.environ.get("TEST_MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("TEST_MINIO_ACCESS_KEY", "petro")
SECRET_KEY = os.environ.get("TEST_MINIO_SECRET_KEY", "petro12345")
BUCKET = os.environ.get("TEST_LAKE_BUCKET", "tpa-raw-test")

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge" / "oil_production_daily.csv"
TABLE = "oil_production_daily"          # 迁移 DDL 的规范 8 表之一（设定书 §A.2 列形）
BATCH = "dp3-e2e-probe"

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="需要 TEST_DATABASE_URL＋MinIO（进料端到端，主仓 compose 本地实跑）")


@pytest.fixture()
def wired():
    cat = load_catalog(dsn=TEST_DSN, s3_endpoint=ENDPOINT, s3_access_key=ACCESS_KEY,
                       s3_secret_key=SECRET_KEY, bucket=BUCKET)
    ing = Ingest(catalog=cat, freshness=PgFreshnessRegistry(TEST_DSN),
                 rules={TABLE: RULES_OIL_DAILY})
    yield ing, cat
    # 清场：本批次数据行＋快照账＋新鲜度登记＋测试桶对象
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM raw.{TABLE} WHERE _batch_id = %s", (BATCH,))
        cur.execute("DELETE FROM data_plane.lake_snapshot WHERE table_name = %s AND batch_id = %s",
                    (TABLE, BATCH))
        cur.execute("DELETE FROM data_plane.freshness_registry WHERE table_name = %s", (TABLE,))
    boss = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
    for obj in boss.list_objects_v2(Bucket=BUCKET, Prefix=f"{TABLE}/{BATCH}/").get("Contents", []):
        boss.delete_object(Bucket=BUCKET, Key=obj["Key"])


def test_csv_through_ingest_pipeline_to_raw(wired):
    """验收 5(a)：合成小 CSV→进料→raw schema 行数对账＋MinIO 对象在＋manifest 血缘完整。"""
    ing, cat = wired
    fixture_rows = pa.csv.read_csv(FIXTURE).num_rows  # DP-01 fixture 化产物继续可用（验收 5c）

    snap = ing.run_csv_load(TABLE, FIXTURE, Provenance(quality_report_id="qr-e2e", operator="阿强"),
                            batch_id=BATCH)

    # ① raw schema 行数对账：质检净批全量落 raw（28 行 fixture 全过 RULES_OIL_DAILY）
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM raw.{TABLE} WHERE _batch_id = %s", (BATCH,))
        assert cur.fetchone()[0] == fixture_rows
        cur.execute("""SELECT well_id, prod_date, oil_output, report_shift FROM raw.oil_production_daily
                       WHERE _batch_id = %s ORDER BY well_id, prod_date LIMIT 1""", (BATCH,))
        well_id, prod_date, oil, shift = cur.fetchone()
    assert well_id == "N-01" and str(prod_date) == "2025-09-01"
    assert float(oil) == 21.0 and shift == "A"

    # ② MinIO 对象在：tpa-raw/<source>/<batch>/ 原始文件
    boss = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
    key = f"{TABLE}/{BATCH}/data.csv"
    body = boss.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert pa.csv.read_csv(io.BytesIO(body)).num_rows == fixture_rows  # 原始批原样留桶

    # ③ manifest 血缘完整（dp3 manifest 格式沿用）：source=进料输入 CSV → 湖表
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT manifest, provenance FROM data_plane.lake_snapshot "
                    "WHERE snapshot_id = %s", (snap.snapshot_id,))
        manifest, provenance = cur.fetchone()
    model = f"model.petro.{TABLE}"
    src = [k for k in manifest["parent_map"][model]][0]
    assert src.startswith("source.petro.") and FIXTURE.name in src
    assert manifest["child_map"][src] == [model]
    assert provenance["quality_report_id"] == "qr-e2e"

    # ④ 三步契约第三步：新鲜度已登记（offline 档，as_of=真时钟）
    info = ing.freshness(TABLE)
    assert info.tier == "offline" and info.pipeline == "batch-shift-report"
    assert info.as_of is not None

    # ⑤ 消费面不经 CSV：湖 scan 回查与 raw 对账一致（CSV 绕全链退役的读面兑现）
    view = cat.load_table(TABLE).scan(pred=f"_batch_id == '{BATCH}'")
    assert view.num_rows == fixture_rows


def test_dirty_csv_rejected_no_landing(wired):
    """三步契约第一步先行：脏批在 validate 被拦，append 不发生（DP-W-D2-1 在真身上成立）。"""
    ing, cat = wired
    from app.data_plane.ingest import BatchRejected
    dirty = pa.Table.from_pylist([
        {"well_id": "N-01", "prod_date": "2025-09-01", "oil_output": 21.0,
         "liquid_output": 27.13, "water_cut": 10.0, "plan_output": 24.0,
         "open_days": 24.0, "oil_pressure": 7.5, "report_shift": "A"},
        {"well_id": "N-01", "prod_date": "2025-09-01", "oil_output": 21.0,
         "liquid_output": 27.13, "water_cut": 10.0, "plan_output": 24.0,
         "open_days": 24.0, "oil_pressure": 7.5, "report_shift": "B"},  # A/B 班重复行
    ])
    before = _raw_count(BATCH)
    with pytest.raises(BatchRejected):
        ing.run_batch_load(TABLE, dirty, Provenance(quality_report_id="qr-dirty", operator="阿强"),
                           batch_id=BATCH)
    assert _raw_count(BATCH) == before  # 脏批零落库


def _raw_count(batch: str) -> int:
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM raw.oil_production_daily WHERE _batch_id = %s", (batch,))
        return cur.fetchone()[0]
