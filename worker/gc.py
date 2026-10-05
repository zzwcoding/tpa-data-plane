"""dp 系湖桶孤儿对象清点（DEP-01 件⑤——安全优先：只列不删）。

- 孤儿对象＝桶内存在但 ``data_plane.lake_snapshot.data_object_key`` 无引用的
  原始批文件（湖写序「先留桶后落账」的 PG 失败遗留，或快照账 expire 后遗留）。
- ``gc_orphans``：列出「无引用 ∧ LastModified 早于保留期」的孤儿对象——
  **仅日志与返回值，不删除**；清单即人工复核输入（先列后删两步制的第一步，
  策略见主仓 docs/deploy/db-accounts-runbook.md §4）。
- **默认不注册作业**：``worker.scheduler.build_scheduler`` 不装配本函数——
  防自动删除；真删除策略与开关归 Owner 部署批拍板（仓内现状零删除实现）。
  人工入口：``python -m worker.gc``（``DP_GC_RETENTION_DAYS`` 缺省 90）。
- 装配依赖同湖：DATABASE_URL＋MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY
  （``LakeCatalog.from_env``——缺任一显式 ValueError，不静默）。
- 模块零顶层 app 依赖（worker 纪律同 scheduler.py——app 侧 import 只在 main()
  进程入口内）。
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

log = logging.getLogger("dp.worker.gc")

DEFAULT_RETENTION_DAYS = 90  # 隔离/拒收批文件取证窗建议值（runbook §4——非删除开关）


class GcCatalog(Protocol):
    """gc 所需湖面（LakeCatalog 的 duck-typed 窄投影——bucket＋S3 客户端＋账读）。"""

    bucket: str

    def _fetch(self, sql: str, params: list) -> list[dict]: ...  # 快照账读（元数据真源）


def _referenced_keys(catalog: GcCatalog) -> set[str]:
    """快照账引用集（data_object_key 全量——孤儿判定的「有主」半边）。"""
    rows = catalog._fetch(
        f'SELECT data_object_key FROM "{getattr(catalog, "meta_schema", "data_plane")}"'
        ".lake_snapshot",
        [],
    )
    return {row["data_object_key"] for row in rows if row.get("data_object_key")}


def gc_orphans(catalog: GcCatalog, *, retention_days: int = DEFAULT_RETENTION_DAYS,
               now: datetime | None = None) -> list[dict[str, Any]]:
    """列出湖桶超期孤儿对象（只列不删——返回清单并存日志，零删除调用）。

    - 孤儿＝对象键不在快照账引用集（有文件无账行）；
    - 超期＝LastModified 早于 now - retention_days 天；
    - 有引用的对象永不清单（append-only 教义——本函数也不具备删除能力，
      S3 delete 调用面不存在）。
    返回 [{key, last_modified, age_days}]（升序——最老在前，复核从最老看起）。
    """
    at = now or datetime.now(timezone.utc)
    cutoff = at - timedelta(days=retention_days)
    referenced = _referenced_keys(catalog)
    orphans: list[dict[str, Any]] = []
    token: str | None = None
    s3 = getattr(catalog, "_s3")
    while True:
        kwargs: dict[str, Any] = {"Bucket": catalog.bucket}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            modified = obj["LastModified"]
            if key in referenced or modified >= cutoff:
                continue  # 有主不清／未超期不动
            orphans.append({
                "key": key,
                "last_modified": modified,
                "age_days": round((at - modified).total_seconds() / 86400, 1),
            })
        if not resp.get("IsTruncated"):
            break
        token = resp["NextContinuationToken"]
    orphans.sort(key=lambda o: o["last_modified"])
    for item in orphans:
        log.warning("gc 清单（仅列不删）：%s（写入于 %s，%.1f 天前，快照账无引用）",
                    item["key"], item["last_modified"].isoformat(), item["age_days"])
    log.warning("gc 汇总：桶 %s 孤儿超期 %d 个（保留期 %d 天）——仅列不删，"
                "真删除策略与开关归 Owner 部署批拍板（runbook §4 先列后删两步制）",
                catalog.bucket, len(orphans), retention_days)
    return orphans


def main(argv: list[str] | None = None) -> int:
    """人工入口：`python -m worker.gc`——单次清点后退出（不常驻、不注册作业）。"""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from app.data_plane.lake_store import LakeCatalog

    catalog = LakeCatalog.from_env()
    gc_orphans(catalog, retention_days=int(
        os.environ.get("DP_GC_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS))))
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv[1:]))
