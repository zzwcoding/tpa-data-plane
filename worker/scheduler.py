"""dp 系周期管道 worker（DP-04，ADR-0003 裁决 12/15：APScheduler＋独立 worker 进程）。

- 一周期六件事：**日度进料（拉新批次）→清洗→质检跑批落库→agg 刷新→水位登记→报水位**
  （freshness 更新＋water_line 三 tier 回写）；进料走 dp2 三步契约（validate→append→
  register，闸在编排层——脏批拒绝入湖并记 rejected）；
  质检＝dp3 整队跑批**落库真身**（DP-05：读 quality_rule 当前启用集→quality_task 报告
  1:1→quarantine_row 隔离落行——原 validate_bundle 内存报告弃置路径清零，报告身份＝
  QT 任务号，主平台 E2 质检台读同表同源）；
  agg 刷新＝dp3 出汇总层（agg.py：湖 as-of 视图→星型构建→**限写三表专用账号**直写主平台库）；
  水位＝freshness 登记（进料侧 as_of=快照账 committed_at——P6 修复；汇总侧=刷新完成时刻）
  ＋water_line 报水位（原始/清洗层=入湖水位、汇总层=agg 刷新水位——E2 行为约定 8，
  T-1 判定归 E2 端点 8 服务端算）。
- 框架红线：APScheduler＝ADR-0003 裁决 12 点名框架（真引入，依赖进 deploy/worker-requirements.txt）。
- 调度配置外置（interval/cron 可配，环境变量；缺省日度 05:30 cron）：
    DP_PIPELINE_RUN_DIR        进料 run 目录（缺省 /data/runs）
    DP_PIPELINE_TABLES         进料表清单（缺省 oil_production_daily,well_info——agg 两源）
    DP_SCHED_TRIGGER           interval | cron（缺省 cron）
    DP_SCHED_INTERVAL_MINUTES  interval 触发间隔分钟（缺省 1440）
    DP_SCHED_CRON              cron 触发参数「k=v,k=v」形（缺省 hour=5,minute=30）
  装配依赖同 lake/agg/quality：DATABASE_URL＋MINIO_*（湖）、AGG_DATABASE_URL（agg 直写
  ——未配时 agg 步显式跳过）、QUALITY_DATABASE_URL（质检落库＋报水位——未配时质检落库/
  报水位显式跳过，不静默装成功）。
- 进程形态：独立 worker 进程——`python -m worker.scheduler`（周期常驻）；
  `--once` 单周期干跑后退出（验收干跑证据＝该模式日志）。
"""
from __future__ import annotations

import csv
import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("dp.worker")

_SOURCE_TABLES = ("oil_production_daily", "well_info")  # agg 星型两源（99 册 §2.12）


@dataclass
class PipelineConfig:
    run_dir: Path
    freshness: object                      # FreshnessRegistry seam（替身|PG 真身）
    agg_writer: object | None = None       # AggStarWriter（未配 AGG_DATABASE_URL=None＝显式跳过）
    quality_store: object | None = None    # QualityStore（未配 QUALITY_DATABASE_URL=None＝质检落库/报水位显式跳过）
    tables: tuple = _SOURCE_TABLES
    operator: str = "dp-scheduler"
    trigger: tuple = ("cron", {"hour": 5, "minute": 30})
    catalog_factory: object | None = None  # 湖装配方（测试注入位；缺省 load_catalog.from_env）


def load_config(catalog_factory=None, freshness=None, agg_writer=None,
                quality_store=None) -> PipelineConfig:
    """环境装配（调度配置外置——interval/cron 可配）。"""
    trigger_kind = os.environ.get("DP_SCHED_TRIGGER", "cron").strip().lower()
    if trigger_kind == "interval":
        trigger = ("interval", {"minutes": int(os.environ.get("DP_SCHED_INTERVAL_MINUTES", "1440"))})
    else:
        cron: dict = {}
        for kv in os.environ.get("DP_SCHED_CRON", "hour=5,minute=30").split(","):
            k, _, v = kv.partition("=")
            if k.strip():
                cron[k.strip()] = int(v)
        trigger = ("cron", cron)
    tables = tuple(t.strip() for t in
                   os.environ.get("DP_PIPELINE_TABLES", ",".join(_SOURCE_TABLES)).split(",") if t.strip())
    return PipelineConfig(
        run_dir=Path(os.environ.get("DP_PIPELINE_RUN_DIR", "/data/runs")),
        freshness=freshness, agg_writer=agg_writer, quality_store=quality_store,
        tables=tables, trigger=trigger, catalog_factory=catalog_factory)


def run_pipeline_once(cfg: PipelineConfig, catalog_factory=None) -> dict:
    """一周期六件事；返回摘要（进料/质检落库/agg/水位各步如实记账——干跑证据载体）。"""
    from app.data_plane.ingest import (BatchRejected, Ingest, Provenance)
    from app.data_plane.quality_store import data_date_of, run_quality_task
    from app.data_plane.transform import RULES_OIL_DAILY, RULES_WELL_INFO

    catalog = (catalog_factory or cfg.catalog_factory)()
    ing = Ingest(catalog=catalog, freshness=cfg.freshness, rules={
        "oil_production_daily": RULES_OIL_DAILY, "well_info": RULES_WELL_INFO})
    summary: dict = {"ingested": [], "rejected": [], "agg": "skipped（未配 AGG_DATABASE_URL）",
                     "freshness": cfg.freshness}

    # ①＋② 日度进料（拉新批次）→清洗（闸后净批＝入湖内容，原始/清洗层水位同源）
    bundle: dict[str, list[dict]] = {}
    for name in cfg.tables:
        path = Path(cfg.run_dir) / f"{name}.csv"
        if not path.is_file():
            log.info("进料跳过：run 目录无 %s（%s）", name, path)
            continue
        with open(path, encoding="utf-8") as f:
            bundle[name] = list(csv.DictReader(f))

    # ③ 质检跑批落库（DP-05：读启用集→QT 任务 1:1 报告→隔离区落行；未配＝显式跳过）
    task_id = None
    if cfg.quality_store is None:
        summary["quality"] = "skipped（未配 QUALITY_DATABASE_URL——质检落库/报水位显式跳过）"
        log.warning("质检落库跳过：未配 QUALITY_DATABASE_URL（限写账号模板 quality_writer.sql）")
    elif not bundle:
        summary["quality"] = "skipped（无进料数据——不建空任务）"
    else:
        q = run_quality_task(cfg.quality_store, bundle)
        task_id = q["task_id"]
        summary["quality"] = q
        log.info("质检跑批落库：任务 %s，%d 条启用规则，挂 %s，隔离 %d 行",
                 task_id, q["rules_run"], q["failed_rules"] or "无", q["quarantined_rows"])

    quality_report_id = task_id or f"pipeqa-{uuid.uuid4().hex[:12]}"  # 同源：报告身份＝QT 任务号
    for name in cfg.tables:
        path = Path(cfg.run_dir) / f"{name}.csv"
        if not path.is_file():
            continue
        try:  # 进料闸在编排层：脏批 BatchRejected＝拒绝入湖（append 不发生）
            snap = ing.run_csv_load(name, path, Provenance(
                quality_report_id=quality_report_id, operator=cfg.operator))
            summary["ingested"].append(name)
            log.info("进料完成：%s → 快照 %s（as_of=%s）", name, snap.snapshot_id, snap.committed_at)
            if cfg.quality_store is not None:  # ⑥ 报水位：原始/清洗层＝入湖水位（闸后净批）
                day = data_date_of(name, bundle[name])
                for layer in ("原始层", "清洗层"):
                    advanced = cfg.quality_store.raise_water_line(layer, day)
                    log.info("水位回写：%s → %s%s", layer, day,
                             "" if advanced else "（未推进——水位单调不回退/无业务日期）")
        except BatchRejected as e:
            summary["rejected"].append(name)
            log.warning("进料拒绝：%s（质检 rule_ids=%s）", name, e.report.rule_ids)

    # ④＋⑤＋⑥ agg 刷新（限写账号直写主平台库）→ 水位登记（freshness）→ 汇总层报水位
    if cfg.agg_writer is None:
        log.warning("agg 刷新跳过：未配 AGG_DATABASE_URL（限写三表专用账号连接串）")
        return summary
    from app.data_plane.agg import refresh_from_catalog
    counts = refresh_from_catalog(catalog, cfg.agg_writer, freshness=cfg.freshness,
                                  water_line=cfg.quality_store)
    summary["agg"] = "ok" if "note" not in counts else f"skipped（{counts['note']}）"
    log.info("agg 刷新：%s", counts)
    return summary


def build_trigger(cfg: PipelineConfig):
    """APScheduler 触发器（外置配置 → 触发器实例）。"""
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    kind, params = cfg.trigger
    return IntervalTrigger(**params) if kind == "interval" else CronTrigger(**params)


def build_scheduler(cfg: PipelineConfig, catalog_factory=None):
    """独立 worker 进程装配：BlockingScheduler＋周期管道任务（APScheduler 真引入）。"""
    from apscheduler.schedulers.blocking import BlockingScheduler
    sched = BlockingScheduler(timezone=os.environ.get("DP_SCHED_TIMEZONE", "Asia/Shanghai"))
    sched.add_job(run_pipeline_once, trigger=build_trigger(cfg),
                  args=[cfg, catalog_factory], id="dp-pipeline",
                  max_instances=1, coalesce=True,  # 上轮未完不并发；积压合并为一轮
                  misfire_grace_time=3600)
    return sched


def main(argv: list[str] | None = None) -> int:
    """入口：`python -m worker.scheduler`（常驻）／`--once`（单周期干跑）。"""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from app.data_plane.agg import AggStarWriter
    from app.data_plane.lake_store import LakeCatalog
    from app.data_plane.pg_store import PgFreshnessRegistry, database_url
    from app.data_plane.quality_store import QualityStore

    freshness = None
    if database_url():
        freshness = PgFreshnessRegistry()  # 持久化真身（P0 热修；DP-03 归位 data_plane schema）
    else:
        from app.data_plane.ingest import InMemoryFreshnessRegistry
        freshness = InMemoryFreshnessRegistry()
        log.warning("未配 DATABASE_URL：freshness 用内存替身（进程重启即失——本地干跑形态）")
    agg_writer = None
    try:
        agg_writer = AggStarWriter.from_env()
    except ValueError as e:
        log.warning("%s", e)
    quality_store = None
    try:
        quality_store = QualityStore.from_env()
    except ValueError as e:
        log.warning("%s", e)
    cfg = load_config(catalog_factory=LakeCatalog.from_env, freshness=freshness,
                      agg_writer=agg_writer, quality_store=quality_store)
    if argv and "--once" in argv:
        summary = run_pipeline_once(cfg)
        log.info("干跑摘要：%s", {k: v for k, v in summary.items() if k != "freshness"})
        return 0
    sched = build_scheduler(cfg)
    log.info("周期管道 worker 启动（trigger=%s%s）", cfg.trigger[0], cfg.trigger[1])
    sched.start()
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
