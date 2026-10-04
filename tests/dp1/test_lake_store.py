"""DP-03 · dp1 湖底座真身测试：PG raw schema＋MinIO 原始文件（方案 C1，拍板 3 无 PyIceberg）。

- 语义沿用原内存替身契约逐条机判：schema 四件齐 / append-only＋快照隔离 /
  谓词＋列裁剪 / 快照过期显式报错 / 写面白名单 / 门面无破坏性写口；
- 替身语义缺口补齐：committed_at=真时钟（DB 提交时刻，非字面 "now"）；
- 无 TEST_DATABASE_URL 或缺 psycopg/boto3 时整文件 skip（与 tests/pg 同机制）；
  实跑环境＝主仓 compose PG16（5433）＋MinIO（9000），测试专用桶避免污染真实桶。
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pyarrow as pa
import pytest

boto3 = pytest.importorskip("boto3")
psycopg = pytest.importorskip("psycopg")

from app.data_plane.lake_store import (BoundaryViolation, LakeTypeError, SnapshotGoneError,
                                       SnapshotInfo, load_catalog)

TEST_DSN = os.environ.get("TEST_DATABASE_URL")
ENDPOINT = os.environ.get("TEST_MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("TEST_MINIO_ACCESS_KEY", "petro")
SECRET_KEY = os.environ.get("TEST_MINIO_SECRET_KEY", "petro12345")
BUCKET = os.environ.get("TEST_LAKE_BUCKET", "tpa-raw-test")
TABLE = "dp1_probe_well"

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="需要 TEST_DATABASE_URL＋MinIO（湖真身集成测试，主仓 compose 本地实跑）")


def sample(days=3, well="N-01"):
    return pa.Table.from_pylist([
        {"well_id": well, "prod_date": f"2025-09-{d:02d}", "oil_output": 20.0 + d}
        for d in range(1, days + 1)])


@pytest.fixture()
def catalog():
    cat = load_catalog(dsn=TEST_DSN, s3_endpoint=ENDPOINT, s3_access_key=ACCESS_KEY,
                       s3_secret_key=SECRET_KEY, bucket=BUCKET)
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS raw.{TABLE}")
        cur.execute("DELETE FROM data_plane.lake_snapshot WHERE table_name = %s", (TABLE,))
        cur.execute("DELETE FROM data_plane.lake_table WHERE table_name = %s", (TABLE,))
    _purge_objects(cat)
    yield cat
    _purge_objects(cat)


def _purge_objects(cat):
    boss = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
    resp = boss.list_objects_v2(Bucket=BUCKET, Prefix=f"{TABLE}/")
    for obj in resp.get("Contents", []):
        boss.delete_object(Bucket=BUCKET, Key=obj["Key"])


class TestSchema:
    def test_schema_info_four_pieces(self, catalog):
        """DP-W-D1-1：schema 四件齐（列/注释/分区规格/当前快照）。"""
        catalog.create_table(TABLE, schema=sample(1).schema,
                             comment="dp1 真身探针表", partition_spec="day(prod_date)")
        s = catalog.load_table(TABLE).schema()
        assert [c[0] for c in s.columns] == ["well_id", "prod_date", "oil_output"]
        assert s.comment == "dp1 真身探针表"
        assert s.partition_spec == "day(prod_date)"
        assert s.current_snapshot is None  # 空表无快照（如实，非字面 0）

    def test_unknown_table_raises(self, catalog):
        with pytest.raises(KeyError):
            catalog.load_table("no_such_table")


class TestAppendAndSnapshots:
    def test_append_isolation(self, catalog):
        """DP-W-D1-2：两次 append，按旧快照读=旧视图（不脏读，PG 行级 _snapshot_id 寻址）。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        t = catalog.load_table(TABLE)
        s1 = catalog.append(TABLE, sample(2), {"provenance": {"batch": "b1"}})
        assert t.scan(snapshot_id=s1.snapshot_id).num_rows == 2
        s2 = catalog.append(TABLE, sample(1, well="N-02"), {"provenance": {"batch": "b2"}})
        assert s2.snapshot_id != s1.snapshot_id
        assert t.scan(snapshot_id=s1.snapshot_id).num_rows == 2  # 旧快照仍是 2 行
        assert t.scan(snapshot_id=s2.snapshot_id).num_rows == 1  # 新快照只含本批
        assert t.scan().num_rows == 3                            # 当前视图=全量

    def test_committed_at_is_real_clock(self, catalog):
        """替身语义缺口补齐：committed_at=真时钟提交时间（DB 时刻、带时区，不再是字面 "now"）。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        before = datetime.now().astimezone() - timedelta(seconds=5)
        snap = catalog.append(TABLE, sample(1), {})
        after = datetime.now().astimezone() + timedelta(seconds=5)
        assert isinstance(snap, SnapshotInfo)
        committed = datetime.fromisoformat(snap.committed_at)
        assert before <= committed <= after
        # 快照账（data_plane.lake_snapshot）同步落库，provenance 随 commit 元数据在案
        with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT committed_at, provenance FROM data_plane.lake_snapshot "
                        "WHERE snapshot_id = %s", (snap.snapshot_id,))
            row = cur.fetchone()
        assert row is not None and before <= row[0] <= after
        assert row[1] == {}  # 空 commit_meta → provenance 出处在案为空对象

    def test_raw_rows_landed_in_pg(self, catalog):
        """raw 层落 PG raw schema（结构化）：业务值＋湖技术列（_snapshot_id/_batch_id）在表。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        snap = catalog.append(TABLE, sample(3), {"provenance": {"operator": "阿强"},
                                                 "batch_id": "b-20250901"})
        with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT well_id, prod_date, oil_output, _snapshot_id, _batch_id "
                        f"FROM raw.{TABLE} ORDER BY prod_date")
            rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0][0] == "N-01" and float(rows[0][2]) == 21.0
        assert {r[3] for r in rows} == {snap.snapshot_id}
        assert {r[4] for r in rows} == {"b-20250901"}

    def test_scan_predicate_and_projection(self, catalog):
        """DP-W-D1-3：谓词下推 SQL＋字段裁剪＋metrics（列级 min/max/null）。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        catalog.append(TABLE, sample(3), {})
        t = catalog.load_table(TABLE)
        out = t.scan(pred="prod_date >= '2025-09-02'", fields=("well_id", "oil_output"))
        assert out.num_rows == 2
        assert out.column_names == ["well_id", "oil_output"]
        stats = t.metrics()
        assert stats.min_max["oil_output"][0] == 21.0  # min
        assert stats.min_max["oil_output"][1] == 23.0  # max
        assert stats.null_count["oil_output"] == 0

    def test_dirty_value_rejected_explicitly(self, catalog):
        """结构化落库失败=显式报错（"12.40方" 类脏值不静默改写——设定书 §A.2 脏特征）。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        dirty = pa.Table.from_pylist([{"well_id": "N-11", "prod_date": "2025-09-14",
                                       "oil_output": "12.40方"}])
        with pytest.raises(LakeTypeError):
            catalog.append(TABLE, dirty, {})

    def test_gone_snapshot_explicit(self, catalog):
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        catalog.append(TABLE, sample(1), {})
        t = catalog.load_table(TABLE)
        with pytest.raises(SnapshotGoneError):
            t.scan(snapshot_id=99999)

    def test_snapshots_listing(self, catalog):
        """dp 卡公开接口 TableRef.snapshots()：快照序＋provenance 可回查。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        s1 = catalog.append(TABLE, sample(1), {"provenance": {"batch": "b1"}})
        s2 = catalog.append(TABLE, sample(1), {"provenance": {"batch": "b2"}})
        snaps = catalog.load_table(TABLE).snapshots()
        assert [s.snapshot_id for s in snaps][-2:] == [s1.snapshot_id, s2.snapshot_id]
        assert snaps[-1].provenance == {"batch": "b2"}  # provenance=出处在案（质检号/操作者）

    def test_expire_keeps_window_and_visibility(self, catalog):
        """dp7.retention 白名单＋滚动窗口：过期快照显式 SnapshotGoneError，当前视图不动。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        ids = [catalog.append(TABLE, sample(1, well=f"N-{i:02d}"), {}).snapshot_id
               for i in range(4)]
        with pytest.raises(BoundaryViolation):
            catalog.expire(TABLE, keep=2)  # 未声明 dp7.retention 身份
        catalog.begin_caller("dp7.retention")
        gone = catalog.expire(TABLE, keep=2)
        assert gone == ids[:2]
        t = catalog.load_table(TABLE)
        with pytest.raises(SnapshotGoneError):
            t.scan(snapshot_id=ids[0])
        assert t.scan(snapshot_id=ids[2]).num_rows == 1
        assert t.scan().num_rows == 4  # expire 清时间旅行可见性，不动当前数据


class TestRawObjectAndManifest:
    def test_raw_file_object_in_minio(self, catalog):
        """原始文件落 MinIO 桶（tpa-raw/<source>/<batch>/）：对象在＋内容行数对账。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        snap = catalog.append(TABLE, sample(3), {"batch_id": "b-obj"})
        key = f"{TABLE}/b-obj/data.csv"
        with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT data_object_key FROM data_plane.lake_snapshot "
                        "WHERE snapshot_id = %s", (snap.snapshot_id,))
            assert cur.fetchone()[0] == key
        import io
        body = cat_object(ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET, key)
        landed = pa.csv.read_csv(io.BytesIO(body))
        assert landed.num_rows == 3  # 原始批内容原样留桶

    def test_manifest_lineage_complete(self, catalog):
        """manifest 血缘登记（dp3 manifest 格式沿用）：parent_map/child_map 互逆＋节点齐。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        snap = catalog.append(TABLE, sample(2), {
            "batch_id": "b-man",
            "manifest": {"metadata": {"tool": "tpa-dp2-ingest"},
                         "nodes": {"model.petro.dp1_probe_well": {"columns": ["well_id"],
                                                                  "resource_type": "model"},
                                   "source.petro.fieldforge/x.csv": {"resource_type": "source"}},
                         "parent_map": {"model.petro.dp1_probe_well": ["source.petro.fieldforge/x.csv"]},
                         "child_map": {"source.petro.fieldforge/x.csv": ["model.petro.dp1_probe_well"]}},
        })
        with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT manifest FROM data_plane.lake_snapshot "
                        "WHERE snapshot_id = %s", (snap.snapshot_id,))
            m = cur.fetchone()[0]
        assert m["parent_map"]["model.petro.dp1_probe_well"] == ["source.petro.fieldforge/x.csv"]
        assert m["child_map"]["source.petro.fieldforge/x.csv"] == ["model.petro.dp1_probe_well"]
        assert set(m["nodes"]) == {"model.petro.dp1_probe_well", "source.petro.fieldforge/x.csv"}


def cat_object(endpoint, ak, sk, bucket, key) -> bytes:
    boss = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=ak,
                        aws_secret_access_key=sk)
    return boss.get_object(Bucket=bucket, Key=key)["Body"].read()


class TestWriteSurface:
    def test_append_caller_allowlist(self, catalog):
        """DP-W-D1-4：append 仅 dp2/dp3 可调，越界=BoundaryViolation。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")  # 白名单身份：放行
        catalog.append(TABLE, sample(1), {})
        with pytest.raises(BoundaryViolation):
            catalog.begin_caller("dp8.tools")  # 越界身份：拒
            catalog.append(TABLE, sample(1), {})
        with pytest.raises(BoundaryViolation):  # 未声明身份（默认越界）
            catalog2 = load_catalog(dsn=TEST_DSN, s3_endpoint=ENDPOINT,
                                    s3_access_key=ACCESS_KEY, s3_secret_key=SECRET_KEY,
                                    bucket=BUCKET)
            catalog2.append(TABLE, sample(1), {})

    def test_no_delete_overwrite_surface(self, catalog):
        """append-only：门面不暴露 overwrite/delete/upsert（expire 是 dp7 专用的可见性边界）。"""
        assert not hasattr(catalog, "overwrite")
        assert not hasattr(catalog, "delete_from")
        assert not hasattr(catalog, "upsert")
