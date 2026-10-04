"""dp1 湖底座 LakeStore 真身（DP-03，方案 C1——Owner 拍板 3：不引入 PyIceberg，
湖＝中台库独立 schema（PG raw 层，结构化）＋MinIO 原始文件桶 `tpa-raw/<source>/<batch>/`）。

- 内存替身类（原测试替身 Catalog/TableRef 的 Arrow 内存实现）**物理移除**（Owner 方针：
  抛弃不修补——盘点 A1-7 "降级施工"病灶清除）；三 backend（jdbc/rest/federated）假分支随之删除——拍板 3 下不存在
  Iceberg catalog，`load_catalog` 只留真实装配方（换 backend 不换调用方的 seam 承诺由
  Catalog/TableRef 接口形状继续承载，seam 替身位下沉到测试 conftest，不入仓实现）；
- 语义与原替身契约逐条对齐：append-only／快照隔离（PG 行级 `_snapshot_id` 寻址）／谓词＋
  列裁剪（下推 SQL WHERE，列名白名单＋参数化值）／写面收敛（append 仅 dp2/dp3，expire 仅
  dp7.retention）／门面**故意不暴露** overwrite/delete/upsert（DP1 底稿 L131-132）；
- 替身语义缺口补齐：`committed_at`＝真时钟提交时间（DB 时刻，非字面 "now"）；
- `SnapshotInfo.provenance`＝质检/操作者出处的本体（commit_meta 的 source/batch_id/manifest
  在快照账各有专列/专段，不再整包回塞）；`schema()`＝业务列视图（湖管道列 `_` 前缀不外露）；
- 结构化落库失败＝显式 LakeTypeError（"12.40方" 类脏值不静默改写——设定书 §A.2 脏特征）；
- 原始批文件以 CSV 留 MinIO 桶；血缘 manifest（dp3 transform.py 格式沿用）随快照账
  `data_plane.lake_snapshot.manifest`（JSONB）登记。
"""
from __future__ import annotations

import io
import json
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.csv  # noqa: F401  (pyarrow 命名空间懒加载：pa.csv.* 需显式导入)

# ---------------------------------------------------------------------------
# 公共形状（与原替身契约同名同形——调用方零改动）

class SnapshotGoneError(Exception):
    """快照不存在或已过保留窗口——绝不静默回退当前快照（DP-W-D7-3）。"""


class BoundaryViolation(Exception):
    """写面白名单外调用（dp1.append/expire 审计，DP-W-D1-4／RF2-05）。"""


class LakeTypeError(Exception):
    """结构化落库失败：值与 raw 表列形不合（显式报错，不静默改写/丢列）。"""


@dataclass(frozen=True)
class SchemaInfo:
    columns: tuple
    comment: str
    partition_spec: str
    current_snapshot: int | None


@dataclass(frozen=True)
class SnapshotInfo:
    snapshot_id: int
    committed_at: str
    provenance: dict


@dataclass(frozen=True)
class ColumnStats:
    min_max: dict
    null_count: dict


_WRITE_ALLOWED = ("dp2.ingest", "dp3.transform")
_EXPIRE_ALLOWED = ("dp7.retention",)

_EMPTY_MANIFEST = {"metadata": {"tool": "dp1.append", "version": "0.1.0"},
                   "nodes": {}, "parent_map": {}, "child_map": {}}

# arrow 类型 → PG 列型（运行时 create_table 的通用落型；规范 8 表列形以迁移 DDL 为真源）
_ARROW_TO_PG = {
    pa.string(): "TEXT", pa.large_string(): "TEXT",
    pa.bool_(): "BOOLEAN",
    pa.int8(): "BIGINT", pa.int16(): "BIGINT", pa.int32(): "BIGINT", pa.int64(): "BIGINT",
    pa.uint8(): "BIGINT", pa.uint16(): "BIGINT", pa.uint32(): "BIGINT", pa.uint64(): "BIGINT",
    pa.float16(): "DOUBLE PRECISION", pa.float32(): "DOUBLE PRECISION",
    pa.float64(): "DOUBLE PRECISION",
    pa.date32(): "DATE", pa.date64(): "DATE",
}
_NUMERIC_PG = {"numeric", "decimal", "integer", "bigint", "smallint",
               "double precision", "real"}


def _pg_family(data_type: str) -> str:
    t = data_type.lower()
    if t.startswith("character") or t == "text":
        return "text"
    if t in _NUMERIC_PG:
        return "numeric"
    if t == "date":
        return "date"
    if t.startswith("timestamp"):
        return "timestamp"
    if t == "boolean":
        return "bool"
    return "other"


def _arrow_type_of(family: str) -> pa.DataType:
    return {"text": pa.string(), "numeric": pa.float64(), "date": pa.string(),
            "timestamp": pa.string(), "bool": pa.bool_(), "other": pa.string()}[family]


# ---------------------------------------------------------------------------
# TableRef

class LakeTableRef:
    """单表读面：schema()/scan()/snapshots()/metrics()（只读——与 dp8 五工具消费形状一致）。"""

    def __init__(self, cat: "LakeCatalog", name: str, columns: list[tuple[str, str]]):
        self._cat = cat
        self.name = name
        self._columns = columns          # [(列名, pg data_type)]，information_schema 读出
        self._colset = {c for c, _ in columns}

    # -- 元数据 --------------------------------------------------------------

    def schema(self) -> SchemaInfo:
        reg = self._cat._table_registry(self.name)
        # schema 四件＝业务列视图（_snapshot_id/_batch_id/_ingested_at 是湖管道列，
        # 不是问数信源的一部分——Agent describe 面不暴露管道件）
        return SchemaInfo(columns=tuple((c, t) for c, t in self._columns
                                        if not c.startswith("_")),
                          comment=reg.get("comment", ""),
                          partition_spec=reg.get("partition_spec", ""),
                          current_snapshot=self._cat._current_snapshot(self.name))

    def snapshots(self) -> list[SnapshotInfo]:
        return self._cat._snapshots(self.name)

    # -- 读面 ----------------------------------------------------------------

    def scan(self, pred: str | None = None, fields: tuple[str, ...] = (),
             snapshot_id: int | None = None, limit: int | None = None) -> pa.Table:
        where, params = [], []
        if snapshot_id is not None:
            if not self._cat._snapshot_exists(self.name, snapshot_id):
                raise SnapshotGoneError("快照不存在或已过保留窗口")
            where.append("_snapshot_id = %s")
            params.append(snapshot_id)
        if pred:
            col, sql_op, value = _parse_pred(pred, self._colset)
            family = dict(self._columns).get(col, "other")
            where.append(f'"{col}" {sql_op} %s')
            params.append(_typed_literal(value, family))
        sql = f'SELECT {self._projection(fields)} FROM "{self._cat.raw_schema}"."{self.name}"'
        if where:
            sql += " WHERE " + " AND ".join(where)
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = self._cat._fetch(sql, params)
        return pa.Table.from_pylist([self._normalize(r, fields) for r in rows],
                                    schema=self._arrow_schema(fields))

    def metrics(self) -> ColumnStats:
        numeric = [c for c, t in self._columns if _pg_family(t) == "numeric"]
        agg_sql, aggs = [], []
        for c in self._colset:
            agg_sql.append(f'count(*) FILTER (WHERE "{c}" IS NULL)')
        for c in numeric:
            agg_sql += [f'min("{c}")', f'max("{c}")']
        if not agg_sql:
            return ColumnStats(min_max={}, null_count={})
        row = self._cat._fetch_one(
            f'SELECT {", ".join(agg_sql)} FROM "{self._cat.raw_schema}"."{self.name}"')
        nulls, min_max = {}, {}
        cols = list(self._colset)
        for i, c in enumerate(cols):
            nulls[c] = int(row[i] or 0)
        base = len(cols)
        for j, c in enumerate(numeric):
            lo, hi = row[base + 2 * j], row[base + 2 * j + 1]
            if lo is not None:
                min_max[c] = (float(lo), float(hi))
        return ColumnStats(min_max=min_max, null_count=nulls)

    # -- 内部 ----------------------------------------------------------------

    def _projection(self, fields: tuple[str, ...]) -> str:
        if not fields:
            return ", ".join(f'"{c}"' for c, _ in self._columns)
        unknown = [f for f in fields if f not in self._colset]
        if unknown:
            raise KeyError(f"未知列：{unknown}（表 {self.name}）")
        return ", ".join(f'"{f}"' for f in fields)

    def _arrow_schema(self, fields: tuple[str, ...]) -> pa.Schema:
        types = {c: _arrow_type_of(_pg_family(t)) for c, t in self._columns}
        names = list(fields) or [c for c, _ in self._columns]
        return pa.schema([(n, types.get(n, pa.string())) for n in names])

    def _normalize(self, row: dict, fields: tuple[str, ...] = ()) -> dict:
        """JSON 兼容形状：numeric→float、date/datetime→ISO 串（回放等值比较与 Arrow 推断稳定）。"""
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                v = v.isoformat()
            elif isinstance(v, date):
                v = v.isoformat()
            elif isinstance(v, Decimal):
                v = float(v)
            out[k] = v
        return out


def _parse_pred(pred: str, columns: set[str]) -> tuple[str, str, str]:
    """谓词小语言（原替身 _eval_pred 同语法）：col op value，op ∈ >= <= == > <。"""
    for op in (">=", "<=", "==", ">", "<"):
        if op in pred:
            col, _, raw = pred.partition(op)
            col, value = col.strip(), raw.strip().strip("'\"")
            if col not in columns:
                raise KeyError(f"谓词列不在表中：{col}")
            return col, ("=" if op == "==" else op), value
    raise ValueError(f"不支持的谓词形状：{pred}")


def _typed_literal(value: str, family: str):
    """谓词值按列族定型（参数化传入，字符串不可注入）。"""
    if family == "numeric":
        return float(value)
    return value  # text/date/timestamp 交给 PG 隐式转换比较


# ---------------------------------------------------------------------------
# Catalog（真身）

# 湖元数据两表（与 migrations/001 同形——迁移是 canonical 结构，此处为幂等 ensure 兜底，
# 同 pg_store「构造时自动 ensure_schema」先例；两处改列形必须同批）
_META_DDL = """
CREATE TABLE IF NOT EXISTS {meta}.lake_table (
    table_name     VARCHAR(128) PRIMARY KEY,
    comment        VARCHAR(255) NOT NULL DEFAULT '',
    partition_spec VARCHAR(128) NOT NULL DEFAULT '',
    created_at     TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {meta}.lake_snapshot (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(128) NOT NULL,
    batch_id        VARCHAR(64)  NOT NULL,
    committed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    rows_added      INTEGER      NOT NULL DEFAULT 0,
    data_object_key VARCHAR(512) NOT NULL,
    manifest        JSONB        NOT NULL,
    provenance      JSONB        NOT NULL DEFAULT '{{}}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_lake_snapshot_table ON {meta}.lake_snapshot (table_name, snapshot_id);
"""


class LakeCatalog:
    """中台湖门面：PG raw schema（结构化行）＋MinIO 桶（原始批文件）＋data_plane.lake_snapshot（快照账）。"""

    def __init__(self, *, dsn: str, s3_endpoint: str, s3_access_key: str, s3_secret_key: str,
                 bucket: str = "tpa-raw", raw_schema: str = "raw",
                 meta_schema: str = "data_plane", ensure_bucket: bool = True):
        self._dsn = dsn
        self.s3_endpoint = s3_endpoint
        self._s3_ak = s3_access_key
        self._s3_sk = s3_secret_key
        self.bucket = bucket
        self.raw_schema = raw_schema
        self.meta_schema = meta_schema
        self._caller: str | None = None  # 当前写方身份（默认未知=越界）
        self._ensure_meta()
        self._s3 = self._make_s3()
        if ensure_bucket:
            self.ensure_bucket()

    def _ensure_meta(self) -> None:
        """湖元数据幂等兜底（迁移未跑的库也能起湖；规范结构以 migrations/001 为真源）。"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.meta_schema}"')
            cur.execute(_META_DDL.format(meta=self.meta_schema))

    # -- 装配 ----------------------------------------------------------------

    def _connect(self):
        try:
            import psycopg
        except ImportError as e:  # 已要 PG 却没装驱动：显式失败，不静默丢湖
            raise RuntimeError("LakeCatalog 需要 psycopg（pip install \"psycopg[binary]\"）") from e
        return psycopg.connect(self._dsn)

    def _make_s3(self):
        try:
            import boto3
        except ImportError as e:  # 已要对象存储却没装客户端：显式失败
            raise RuntimeError("LakeCatalog 需要 boto3（pip install boto3）") from e
        return boto3.client("s3", endpoint_url=self.s3_endpoint,
                            aws_access_key_id=self._s3_ak, aws_secret_access_key=self._s3_sk)

    def ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=self.bucket)
        except self._s3.exceptions.ClientError:
            self._s3.create_bucket(Bucket=self.bucket)

    @classmethod
    def from_env(cls, **overrides):
        """环境装配方：DATABASE_URL＋MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY（部署批消费）。"""
        cfg = dict(dsn=os.environ.get("DATABASE_URL"),
                   s3_endpoint=os.environ.get("MINIO_ENDPOINT"),
                   s3_access_key=os.environ.get("MINIO_ACCESS_KEY"),
                   s3_secret_key=os.environ.get("MINIO_SECRET_KEY"))
        missing = [k for k, v in cfg.items() if not v]
        if missing:
            raise ValueError(f"LakeCatalog.from_env 缺环境变量：{missing}")
        cfg.update(overrides)
        return cls(**cfg)

    # -- 写方身份（写面收敛，DP-W-D1-4） --------------------------------------

    def begin_caller(self, caller: str) -> None:
        """声明写方身份（dp2/dp3/dp7 进出场时调用）；身份与写面白名单比对。"""
        self._caller = caller

    # -- Catalog 接口 --------------------------------------------------------

    def create_table(self, name: str, schema: pa.Schema, comment: str = "",
                     partition_spec: str = "") -> None:
        """建湖表：PG raw 表（幂等 IF NOT EXISTS）＋元数据注册（注册面可重复登记）。

        规范 8 表列形以迁移 DDL 为真源——已存在的表零改动（列形校验交给进料落库的显式报错）。"""
        cols = ", ".join(
            f'"{f.name}" {_ARROW_TO_PG.get(f.type, "TEXT")}' for f in schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.raw_schema}"')
            cur.execute(f"""CREATE TABLE IF NOT EXISTS "{self.raw_schema}"."{name}" (
                                {cols},
                                _snapshot_id BIGINT NOT NULL,
                                _batch_id   VARCHAR(64) NOT NULL,
                                _ingested_at TIMESTAMP NOT NULL DEFAULT now())""")
            cur.execute(f"""INSERT INTO "{self.meta_schema}".lake_table
                                (table_name, comment, partition_spec)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (table_name) DO UPDATE
                                SET comment = EXCLUDED.comment,
                                    partition_spec = EXCLUDED.partition_spec""",
                        (name, comment, partition_spec))

    def load_table(self, name: str) -> LakeTableRef:
        cols = self._table_columns(name)
        if cols is None:
            raise KeyError(f"表不存在：{name}")
        return LakeTableRef(self, name, cols)

    def append(self, table: str, arrow: pa.Table, commit_meta: dict) -> SnapshotInfo:
        """append-only 提交：MinIO 留原始批文件 → PG 单事务（快照账行＋raw 数据行，行级快照寻址）。

        commit_meta 契约：{"provenance": {...}}（dp2/dp3）；可选 batch_id（缺省生成）、
        source（进料输入标识）、manifest（dp3 格式血缘——缺省落空骨架）。"""
        self._require_writer()
        rows = arrow.to_pylist()
        cols = self._table_columns(table)
        if cols is None:
            raise KeyError(f"表不存在：{table}")
        business = [(c, t) for c, t in cols if not c.startswith("_")]
        tech = {c for c, _ in cols if c.startswith("_")}
        batch_id = str(commit_meta.get("batch_id") or f"b-{uuid.uuid4().hex[:12]}")
        manifest = commit_meta.get("manifest") or dict(_EMPTY_MANIFEST)

        object_key = f"{table}/{batch_id}/data.csv"
        self._put_raw_object(object_key, arrow)   # 先留桶：PG 失败只留孤儿对象，不留悬空快照账

        snap_col = "_snapshot_id" if "_snapshot_id" in tech else None
        # 批次技术列：规范名 _batch_id；well_overhaul 业务列 batch_id（补录批次）撞名处让位 _src_batch_id
        batch_col = next((c for c in ("_batch_id", "_src_batch_id") if c in tech), None)
        insert_cols = ", ".join(f'"{c}"' for c, _ in business)
        values = ", ".join(["%s"] * len(business))
        snap_sql = f"""INSERT INTO "{self.meta_schema}".lake_snapshot
                           (table_name, batch_id, rows_added, data_object_key, manifest, provenance)
                           VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
                           RETURNING snapshot_id, committed_at"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(snap_sql, (table, batch_id, len(rows), object_key,
                                   json.dumps(manifest, ensure_ascii=False, default=str),
                                   json.dumps(commit_meta.get("provenance", {}),
                                              ensure_ascii=False, default=str)))
            snapshot_id, committed_at = cur.fetchone()
            tech_cols = [c for c in (snap_col, batch_col) if c]
            tech_marks = ["%s"] * len(tech_cols)
            col_sql = ", ".join([insert_cols] + [f'"{c}"' for c in tech_cols])
            mark_sql = ", ".join([values] + tech_marks)
            for row in rows:
                cur.execute(
                    f"""INSERT INTO "{self.raw_schema}"."{table}" ({col_sql})
                            VALUES ({mark_sql})""",
                    [_cast(row.get(c), t) for c, t in business]
                    + ([snapshot_id] if snap_col else [])
                    + ([batch_id] if batch_col else []))
        return SnapshotInfo(snapshot_id=snapshot_id,
                            committed_at=committed_at.isoformat(),
                            provenance=commit_meta.get("provenance", {}))
        # provenance 字段=质检/操作者出处的本体（commit_meta 的 source/batch_id/manifest
        # 在快照账各有专列/专段——真身化后不再整包回塞）

    def expire(self, table: str, keep: int) -> list[int]:
        """滚动保留窗口：保留最近 keep 个快照的**可见性**，其余 expire（存储边界不是篡改口）。

        破坏性写面与 append 同闸：仅 dp7.retention 可调（RF2-05）；过期快照 scan=SnapshotGoneError，
        当前视图数据不动（raw 行仍在——expire 清的是时间旅行，不是数据）。"""
        self._require_expire()
        keep = max(int(keep), 1)  # keep=0/负数特判：至少保留当前快照（P2-1）
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"""DELETE FROM "{self.meta_schema}".lake_snapshot
                            WHERE table_name = %s AND snapshot_id NOT IN (
                                SELECT snapshot_id FROM "{self.meta_schema}".lake_snapshot
                                WHERE table_name = %s ORDER BY snapshot_id DESC LIMIT %s)
                            RETURNING snapshot_id""", (table, table, keep))
            gone = [r[0] for r in cur.fetchall()]
        return gone

    # -- 内部 ----------------------------------------------------------------

    def _require_writer(self) -> None:
        if self._caller not in _WRITE_ALLOWED:
            raise BoundaryViolation(
                f"dp1.append 调用方 {self._caller!r} 不在白名单 {_WRITE_ALLOWED}（写面收敛）")

    def _require_expire(self) -> None:
        if self._caller not in _EXPIRE_ALLOWED:
            raise BoundaryViolation(
                f"dp1.expire 调用方 {self._caller!r} 不在白名单 {_EXPIRE_ALLOWED}（保留策略执行面收敛）")

    def _table_columns(self, table: str) -> list[tuple[str, str]] | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("""SELECT table_name FROM information_schema.tables
                           WHERE table_schema = %s AND table_name = %s""",
                        (self.raw_schema, table))
            if cur.fetchone() is None:
                return None
            cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                           WHERE table_schema = %s AND table_name = %s
                           ORDER BY ordinal_position""", (self.raw_schema, table))
            return [(r[0], r[1]) for r in cur.fetchall()]

    def _table_registry(self, table: str) -> dict:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"""SELECT comment, partition_spec FROM "{self.meta_schema}".lake_table
                            WHERE table_name = %s""", (table,))
            row = cur.fetchone()
        return {"comment": row[0] if row else "", "partition_spec": row[1] if row else ""}

    def _current_snapshot(self, table: str) -> int | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"""SELECT max(snapshot_id) FROM "{self.meta_schema}".lake_snapshot
                            WHERE table_name = %s""", (table,))
            return cur.fetchone()[0]

    def _snapshots(self, table: str) -> list[SnapshotInfo]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"""SELECT snapshot_id, committed_at, provenance
                            FROM "{self.meta_schema}".lake_snapshot
                            WHERE table_name = %s ORDER BY snapshot_id""", (table,))
            return [SnapshotInfo(snapshot_id=r[0], committed_at=r[1].isoformat(),
                                 provenance=r[2]) for r in cur.fetchall()]

    def _snapshot_exists(self, table: str, snapshot_id: int) -> bool:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"""SELECT 1 FROM "{self.meta_schema}".lake_snapshot
                            WHERE table_name = %s AND snapshot_id = %s""", (table, snapshot_id))
            return cur.fetchone() is not None

    def _fetch(self, sql: str, params: list) -> list[dict]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            names = [d.name for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

    def _fetch_one(self, sql: str) -> tuple:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def _put_raw_object(self, key: str, arrow: pa.Table) -> None:
        buf = io.BytesIO()
        pa.csv.write_csv(arrow, buf)
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue(),
                            ContentType="text/csv")


def _cast(value, pg_type: str):
    """进料值 → raw 列形（结构化落库的显式转换；失败=LakeTypeError 不静默改写）。

    as-is 约定：空串/None→NULL（CSV 空字段语义）；其余按 information_schema 列族定型。"""
    if value is None or (isinstance(value, str) and value == ""):
        return None
    family = _pg_family(pg_type)
    try:
        if family == "text":
            return str(value)
        if family == "numeric":
            return value if isinstance(value, Decimal) else Decimal(str(value))
        if family == "date":
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            return date.fromisoformat(str(value)[:10])
        if family == "timestamp":
            return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        if family == "bool":
            return bool(value)
        return value
    except (ValueError, TypeError, ArithmeticError) as e:
        raise LakeTypeError(f"值 {value!r} 无法落列形 {pg_type}（as-is 纪律：不改写，显式拒绝）") from e


# ---------------------------------------------------------------------------
# 真实装配方（三 backend 假分支已随拍板 3 删除——jdbc/rest/federated 是 Iceberg catalog 概念，
# 本湖不存在；调用方只见 load_catalog(...) -> Catalog 形状）

def load_catalog(*, dsn: str | None = None, s3_endpoint: str | None = None,
                 s3_access_key: str | None = None, s3_secret_key: str | None = None,
                 bucket: str = "tpa-raw", raw_schema: str = "raw",
                 meta_schema: str = "data_plane", ensure_bucket: bool = True) -> LakeCatalog:
    """真实装配方：PG 中台库（raw schema）＋MinIO（原始文件桶）。

    缺省回落环境变量：DATABASE_URL／MINIO_ENDPOINT／MINIO_ACCESS_KEY／MINIO_SECRET_KEY
    （部署批装配点消费；与 pg_store.database_url 的回落纪律同构）。"""
    env = {"dsn": os.environ.get("DATABASE_URL"),
           "s3_endpoint": os.environ.get("MINIO_ENDPOINT"),
           "s3_access_key": os.environ.get("MINIO_ACCESS_KEY"),
           "s3_secret_key": os.environ.get("MINIO_SECRET_KEY")}
    cfg = {"dsn": dsn, "s3_endpoint": s3_endpoint, "s3_access_key": s3_access_key,
           "s3_secret_key": s3_secret_key}
    missing = [k for k, v in cfg.items() if v is None and not env[k]]
    if missing:
        raise ValueError(f"load_catalog 缺连接参数（形参或环境变量）：{missing}——"
                         f"PG=DATABASE_URL，对象存储=MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY")
    for k, v in cfg.items():
        if v is None:
            cfg[k] = env[k]
    return LakeCatalog(bucket=bucket, raw_schema=raw_schema,
                       meta_schema=meta_schema, ensure_bucket=ensure_bucket, **cfg)
