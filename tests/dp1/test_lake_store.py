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


def _purge_objects(cat, prefix: str | None = None):
    boss = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
    resp = boss.list_objects_v2(Bucket=BUCKET, Prefix=prefix or f"{TABLE}/")
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
        """DP-W-D1-2：append-only＋快照隔离——as-of 时点视图（DP-03.fix F1：快照 N＝截至 N
        的生效全视图，不是 delta 本批切片；本探针表未登记业务键→时点过滤＝累积全量）。"""
        catalog.create_table(TABLE, schema=sample(1).schema)
        catalog.begin_caller("dp2.ingest")
        t = catalog.load_table(TABLE)
        s1 = catalog.append(TABLE, sample(2), {"provenance": {"batch": "b1"}})
        assert t.scan(snapshot_id=s1.snapshot_id).num_rows == 2
        s2 = catalog.append(TABLE, sample(1, well="N-02"), {"provenance": {"batch": "b2"}})
        assert s2.snapshot_id != s1.snapshot_id
        assert t.scan(snapshot_id=s1.snapshot_id).num_rows == 2  # 旧快照仍是 2 行（时点不漂移）
        assert t.scan(snapshot_id=s2.snapshot_id).num_rows == 3  # as-of：含旧批的全部生效行
        assert t.scan().num_rows == 3                            # 当前视图=截至当前的生效全量

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
        assert t.scan(snapshot_id=ids[2]).num_rows == 3  # as-of：截至 ids[2] 的生效行（N-00..N-02）
        assert t.scan().num_rows == 4  # expire 清时间旅行可见性，不动当前数据


class TestAsOfTimepointView:
    """DP-03.fix F1 · as-of 时点视图（L0 裁决：D6 册「增量策略与两坑」坑一——追加模式新旧并存
    →按批次取最新，不混用）：scan(snapshot_id=N)=截至 N 每业务键的最新生效版；同批并列行全保留
    （RANK 并列——A/B 班重复行是原始层合法内容）。键口径＝raw 8 表键（lake_store._BUSINESS_KEYS，
    照抄 migrations/001 表 COMMENT 逻辑键）；本探针借同名 well_info 命中 well_id 键，
    落独立 raw_dp1_asof schema，零触碰规范 raw 层。"""

    KEYED_SCHEMA = pa.schema([("well_id", pa.string()), ("current_status", pa.string())])
    BUSINESS = ("well_id", "current_status")

    def _rows(self, *rows):
        return pa.Table.from_pylist(list(rows), schema=self.KEYED_SCHEMA)

    @pytest.fixture()
    def keyed(self):
        cat = load_catalog(dsn=TEST_DSN, s3_endpoint=ENDPOINT, s3_access_key=ACCESS_KEY,
                           s3_secret_key=SECRET_KEY, bucket=BUCKET, raw_schema="raw_dp1_asof")
        with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw_dp1_asof CASCADE")
            cur.execute("DELETE FROM data_plane.lake_snapshot WHERE table_name = 'well_info'")
        _purge_objects(cat, prefix="well_info/")
        yield cat
        with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw_dp1_asof CASCADE")
            cur.execute("DELETE FROM data_plane.lake_snapshot WHERE table_name = 'well_info'")
        _purge_objects(cat, prefix="well_info/")

    def _seed_two_batches(self, cat):
        cat.create_table("well_info", schema=self.KEYED_SCHEMA, comment="as-of 探针（键=well_id）")
        cat.begin_caller("dp2.ingest")
        s1 = cat.append("well_info", self._rows(
            {"well_id": "N-01", "current_status": "生产中"},
            {"well_id": "N-02", "current_status": "生产中"}), {"provenance": {"batch": "b1"}})
        s2 = cat.append("well_info", self._rows(
            {"well_id": "N-01", "current_status": "停产"}), {"provenance": {"batch": "b2"}})
        return s1, s2

    @staticmethod
    def _view(table, snapshot_id):
        return sorted(table.scan(snapshot_id=snapshot_id)
                      .select(list(TestAsOfTimepointView.BUSINESS)).to_pylist(),
                      key=lambda r: r["well_id"])

    def test_same_key_newer_batch_supersedes(self, keyed):
        """新旧两版同键只见到新版：跨批同键新版覆盖旧版（不混用），未覆盖键旧批行仍生效。"""
        s1, s2 = self._seed_two_batches(keyed)
        t = keyed.load_table("well_info")
        assert self._view(t, s2.snapshot_id) == [
            {"well_id": "N-01", "current_status": "停产"},    # 新版（b2 同键覆盖）
            {"well_id": "N-02", "current_status": "生产中"}]  # b1 未覆盖键照常生效
        assert t.scan().num_rows == 2  # 当前视图=截至当前的生效视图（非逐批累积 3 行）

    def test_older_snapshot_still_sees_old_version(self, keyed):
        """N 之前时点见旧版：时间旅行不漂移（dp7 同快照重放一致的底座）。"""
        s1, _s2 = self._seed_two_batches(keyed)
        t = keyed.load_table("well_info")
        assert self._view(t, s1.snapshot_id) == [
            {"well_id": "N-01", "current_status": "生产中"},  # 时点在 b2 之前→旧版仍在
            {"well_id": "N-02", "current_status": "生产中"}]

    def test_same_batch_duplicate_keys_both_kept(self, keyed):
        """同批并列行全保留：RANK 并列（同 _snapshot_id）不作键内裁决——A/B 班重复行
        是原始层合法内容（as-is，001 头注）。"""
        s1, _s2 = self._seed_two_batches(keyed)
        s3 = keyed.append("well_info", self._rows(
            {"well_id": "N-01", "current_status": "生产中"},
            {"well_id": "N-01", "current_status": "生产中"}), {"provenance": {"batch": "b3"}})
        assert s3.snapshot_id != s1.snapshot_id
        t = keyed.load_table("well_info")
        view = t.scan(snapshot_id=s3.snapshot_id).select(list(self.BUSINESS)).to_pylist()
        assert len(view) == 3  # N-01 两行（同批并列）＋N-02 一行
        assert sum(1 for r in view if r["well_id"] == "N-01") == 2

    def test_fake_and_real_asof_semantics_agree(self, keyed):
        """替身/真身语义一致断言：同一 append 序列，FakeSnapshotCatalog 与湖真身的
        各时点视图逐行相等（conftest 替身 as-of 对齐真身的机判）。"""
        from tests.conftest import FakeSnapshotCatalog
        fake = FakeSnapshotCatalog()
        fake.create_table("well_info", schema=self.KEYED_SCHEMA)
        fake.begin_caller("dp2.ingest")
        s1r, s2r = self._seed_two_batches(keyed)          # 真身侧（独立 schema 探针表）
        fake.append("well_info", self._rows(              # 替身侧同一序列（快照号各自计，按位对齐）
            {"well_id": "N-01", "current_status": "生产中"},
            {"well_id": "N-02", "current_status": "生产中"}), {})
        s1f = 1
        fake.append("well_info", self._rows(
            {"well_id": "N-01", "current_status": "停产"}), {})
        s2f = 2
        rt, ft = keyed.load_table("well_info"), fake.load_table("well_info")
        assert self._view(rt, s1r.snapshot_id) == self._view(ft, s1f)
        assert self._view(rt, s2r.snapshot_id) == self._view(ft, s2f)
        assert sorted(rt.scan().select(list(self.BUSINESS)).to_pylist(),
                      key=lambda r: r["well_id"]) == \
            sorted(ft.scan().select(list(self.BUSINESS)).to_pylist(),
                   key=lambda r: r["well_id"])


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
