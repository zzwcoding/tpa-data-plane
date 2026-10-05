"""DEP-01 件⑤ · 湖桶孤儿清点测试（hermetic——fixture 桶替身，零外部服务）。

gc_orphans 纪律机判：只列不删（delete 调用面不存在）／有主不清／未超期不动／
分页遍历／默认不注册调度作业（build_scheduler 无 gc 作业——防自动删除）。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker.gc import DEFAULT_RETENTION_DAYS, gc_orphans
from worker.scheduler import PICKUP_JOB_ID, PipelineConfig, build_scheduler

_NOW = datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc)


class FakeS3:
    """fixture 桶：put_object 存内存字典，list_objects_v2 按 _page_size 分页；
    delete_object 直接 raise——gc 若具备/调用删除面立即炸（只列不删机证）。"""

    def __init__(self, objects: dict[str, datetime], page_size: int = 2):
        self.objects = dict(objects)
        self._page_size = page_size

    def put_object(self, *, Bucket: str, Key: str, Body, ContentType: str = "") -> None:
        raise AssertionError("gc 不应有写面")

    def delete_object(self, **kwargs) -> None:
        raise AssertionError("gc 调用了删除面——只列不删纪律被破坏")

    def list_objects_v2(self, *, Bucket: str, ContinuationToken: str | None = None):
        keys = sorted(self.objects)
        start = int(ContinuationToken or 0)
        page = keys[start:start + self._page_size]
        truncated = start + self._page_size < len(keys)
        return {
            "Contents": [{"Key": k, "LastModified": self.objects[k]} for k in page],
            "IsTruncated": truncated,
            **({"NextContinuationToken": str(start + self._page_size)} if truncated else {}),
        }


class FakeCatalog:
    """GcCatalog 窄投影替身：bucket＋meta_schema＋_fetch（引用集来自快照账）。"""

    def __init__(self, s3: FakeS3, referenced: list[str]):
        self.bucket = "tpa-raw-fixture"
        self.meta_schema = "data_plane"
        self._s3 = s3
        self._referenced = referenced
        self.fetches: list[str] = []

    def _fetch(self, sql: str, params: list) -> list[dict]:
        self.fetches.append(sql)
        return [{"data_object_key": k} for k in self._referenced]


def _bucket_objects() -> dict[str, datetime]:
    return {
        # 有主（快照账引用）——超期也不清
        "oil_production_daily/b-keep-old/data.csv": _NOW - timedelta(days=120),
        # 孤儿＋超期——应列
        "well_info/b-orphan-old/data.csv": _NOW - timedelta(days=95),
        "oil_production_daily/b-orphan-oldest/data.csv": _NOW - timedelta(days=200),
        # 孤儿＋未超期——不动
        "well_info/b-orphan-new/data.csv": _NOW - timedelta(days=3),
    }


class TestGcOrphans:
    def test_orphan_expired_listed_only(self):
        """孤儿∧超期入清单（升序最老在前）；有主/未超期不列。"""
        cat = FakeCatalog(FakeS3(_bucket_objects()),
                          referenced=["oil_production_daily/b-keep-old/data.csv"])
        listed = gc_orphans(cat, retention_days=90, now=_NOW)
        assert [o["key"] for o in listed] == [
            "oil_production_daily/b-orphan-oldest/data.csv",
            "well_info/b-orphan-old/data.csv",
        ]
        assert listed[0]["age_days"] == 200.0
        assert cat.fetches and "lake_snapshot" in cat.fetches[0]  # 引用集出自快照账

    def test_referenced_never_listed_even_expired(self):
        cat = FakeCatalog(FakeS3(_bucket_objects()),
                          referenced=["oil_production_daily/b-keep-old/data.csv"])
        assert all("b-keep-old" not in o["key"] for o in gc_orphans(cat, now=_NOW))

    def test_fresh_orphan_under_retention_not_listed(self):
        cat = FakeCatalog(FakeS3(_bucket_objects()), referenced=[])
        assert all("b-orphan-new" not in o["key"] for o in gc_orphans(cat, now=_NOW))

    def test_list_only_no_delete_surface(self):
        """只列不删机证：跑完 gc 桶内对象原样；替身 delete_object 若被调即 raise。"""
        s3 = FakeS3(_bucket_objects())
        cat = FakeCatalog(s3, referenced=[])
        before = dict(s3.objects)
        gc_orphans(cat, now=_NOW)
        assert s3.objects == before  # 零增删改

    def test_pagination_walks_all_pages(self):
        """分页遍历：IsTruncated/ContinuationToken 走完（替身每页 2 个）。"""
        objects = {f"t/b-orphan-{i:02d}/data.csv": _NOW - timedelta(days=100 + i)
                   for i in range(5)}
        cat = FakeCatalog(FakeS3(objects, page_size=2), referenced=[])
        listed = gc_orphans(cat, retention_days=90, now=_NOW)
        assert len(listed) == 5  # 5 个全过分页走到，不漏


class TestGcDefaults:
    def test_default_retention_is_90(self):
        assert DEFAULT_RETENTION_DAYS == 90  # runbook §4 建议值（非删除开关）

    def test_not_registered_as_scheduler_job(self):
        """默认不注册作业：管道＋拾取配齐，scheduler 里也没有 gc 作业（防自动删除）。"""

        class FakeQualityStore:
            pass  # 仅判「在位」——build_scheduler 据此装配拾取作业

        sched = build_scheduler(
            PipelineConfig(run_dir=None, freshness=None,
                           quality_store=FakeQualityStore()))
        job_ids = {j.id for j in sched.get_jobs()}
        assert job_ids == {"dp-pipeline", PICKUP_JOB_ID}  # 无 gc 键——默认不注册

    def test_main_missing_env_raises(self, monkeypatch):
        """人工入口装配面：缺湖环境变量显式 ValueError（不静默起湖）。"""
        for k in ("DATABASE_URL", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
            monkeypatch.delenv(k, raising=False)
        from worker import gc as gc_mod

        with pytest.raises(ValueError, match="缺环境变量"):
            gc_mod.main([])
