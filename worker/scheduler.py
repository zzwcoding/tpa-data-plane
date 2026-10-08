"""dp 系周期管道 worker（DP-04，ADR-0003 裁决 12/15：APScheduler＋独立 worker 进程）。

- 一周期六件事：**日度进料（拉新批次）→清洗→质检跑批落库→agg 刷新→水位登记→报水位**
  （freshness 更新＋water_line 三 tier 回写）；进料走 dp2 三步契约（validate→append→
  register，闸在编排层——脏批拒绝入湖并记 rejected）；**进料幂等闸（V2-11，Q11 P0
  数据基座根因修复）**：湖 as-of 当前视图与源 CSV 逐格一致＝内容未变零追加（不建新批、
  不重刷 agg、不报水位、不重盖 freshness——同内容重跑/每日 cron 重跑零变更；视图被
  旧批覆写≠源＝整批重灌重建真值，修复方向宁重灌不漏灌）；
  质检＝dp3 整队跑批**落库真身**（DP-05：读 quality_rule 当前启用集→quality_task 报告
  1:1→quarantine_row 隔离落行——原 validate_bundle 内存报告弃置路径清零，报告身份＝
  QT 任务号，主平台 E2 质检台读同表同源）；报告 range 按实扫表数动态生成（DP-07 F-01）；
  agg 刷新＝dp3 出汇总层（agg.py：湖 as-of 视图→星型构建→**限写三表专用账号**直写主平台库）；
  水位＝freshness 登记（进料侧 as_of=快照账 committed_at——P6 修复；汇总侧=刷新完成时刻）
  ＋water_line 报水位（原始/清洗层=**闸后净批**落位批业务日期——DP-07 F-03：取落位批
  不取闸前原始行，坏行恰为最新日期行时不再虚标；汇总层=agg 刷新水位——E2 行为约定 8，
  T-1 判定归 E2 端点 8 服务端算）。
- 扫描面与进料面解耦（DP-07 F-01）：质检/拾取的扫描面缺省＝全部受检 8 表
  （DP_SCAN_TABLES——与 quality_store.SCAN_TABLES 同域），进料面保持 DP_PIPELINE_TABLES
  原语义（agg 两源）；扩进料表清单到无进料规则集的表不再 UnruledTableRejected 崩周期
  （诚实跳过记摘要，闸 fail-closed 语义不变——表不入湖）。
- 拾取作业（DP-06，与管道作业并列的第二只 APScheduler 作业）：周期轮询 quality_task
  的外部受理行（status='running' AND step=0——M-05b 冻结语义「step=0＝待拾取外部任务」，
  主平台 E2 accept_task 落行即触发面）→逐行认领（条件 UPDATE 原子闸，step 0→1）→
  按 DP-05 run_quality_task 落库编排执行（**拾取该行而非自建**，数据面＝湖 as-of 当前
  视图——修正批次随进料入湖后可复扫）→完任务 done/step=2＋报告 1:1＋隔离行
  batch_id=任务号（行形合同＝M-05b 集成替身冻结形，零形状偏差）。
  - 残行自愈（重置形——票面二选一，形态裁决见 quality_store.heal_stale_tasks）：
    running∧step=1 超阈无更新（worker 认领后中断的 crash 残行）→重置 step=0 可重拾，
    自愈先于拾取同周期闭环；执行中断的行如实停在 step=1（不造假完成），下周期自愈重拾。
    0015 status CHECK('running','done') 冻结——failed 状态落不进去，故不取 failed 形。
  - 假绿防线：湖视图空→本周期不拾取（空扫描报告会被复扫通道判成全绿——宁缺不造假）。
  - 【R2 观察项登记（M-05b 关票裁决——实质修复归数据面报告合同化批，不在本票）】
    复扫判绿受报告 hits[].sample 每规则截前 5 条影响存在假绿风险：超样本命中不进
    sample 投影，复扫按单裁决会误判「不再命中」；本拾取器产出的报告在 verdict 头
    续写注记（R2_SAMPLE_NOTE）留痕，机判修复待报告合同化批放宽 sample 或全量化。
- 框架红线：APScheduler＝ADR-0003 裁决 12 点名框架（真引入，依赖进 deploy/worker-requirements.txt）。
- 调度配置外置（interval/cron 可配，环境变量；缺省管道日度 05:30、拾取 60s 间隔）：
    DP_PIPELINE_RUN_DIR        进料 run 目录（缺省 /data/runs）
    DP_PIPELINE_TABLES         进料表清单（缺省 oil_production_daily,well_info——agg 两源；
                               F-01 起仅承载进料面语义）
    DP_SCAN_TABLES             质检/拾取扫描面表清单（缺省全部受检 8 表——独立于进料面，
                               扩扫描面不再触发进料闸 UnruledTableRejected）
    DP_SCHED_TRIGGER           interval | cron（缺省 cron）
    DP_SCHED_INTERVAL_MINUTES  interval 触发间隔分钟（缺省 1440）
    DP_SCHED_CRON              cron 触发参数「k=v,k=v」形（缺省 hour=5,minute=30）
    DP_PICKUP_TRIGGER          拾取作业 interval | cron（缺省 interval）
    DP_PICKUP_INTERVAL_SECONDS 拾取 interval 间隔秒（缺省 60）
    DP_PICKUP_CRON             拾取 cron 参数「k=v,k=v」形（缺省 minute=*/1）
    DP_PICKUP_STALE_SECONDS    残行自愈阈值秒（缺省 3600——须远大于单任务执行时长）
  装配依赖同 lake/agg/quality：DATABASE_URL＋MINIO_*（湖）、AGG_DATABASE_URL（agg 直写
  ——未配时 agg 步显式跳过）、QUALITY_DATABASE_URL（质检落库/报水位/拾取作业——未配时
  质检落库/报水位显式跳过、拾取作业不装配，不静默装成功）。
- 进程形态：独立 worker 进程——`python -m worker.scheduler`（周期常驻）；
  `--once` 单周期干跑后退出（验收干跑证据＝该模式日志）。
"""
from __future__ import annotations

import csv
import logging
import os
import uuid
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

log = logging.getLogger("dp.worker")

_SOURCE_TABLES = ("oil_production_daily", "well_info")  # agg 星型两源（99 册 §2.12）——进料面

# 扫描面缺省＝全部受检 8 表（DP-07 F-01，与 quality_store.SCAN_TABLES／_RULE_TABLE 主表
# 值域同域——字面在此保住 worker 模块零顶层 app 依赖，一致性由 tests/worker 钉子测试看守）。
_SCAN_TABLES = ("oil_production_daily", "well_info", "water_injection_daily",
                "well_overhaul", "equip_status", "fluid_test", "monthly_settlement",
                "sensor_reading_subdaily")

PICKUP_JOB_ID = "dp-task-pickup"

# R2 观察项（M-05b 关票裁决登记——实质修复归数据面报告合同化批）：拾取产报告头注记。
# 文案键「R2」「sample」为对账锚（tests 断言登记在案）。
R2_SAMPLE_NOTE = ("注：报告 hits[].sample 每规则截前 5 条——复扫判绿按样本投影对位，"
                  "超样本命中不参与判绿与建单（假绿风险已登记 R2——实质修复归报告合同化批）")


@dataclass
class PipelineConfig:
    run_dir: Path
    freshness: object                      # FreshnessRegistry seam（替身|PG 真身）
    agg_writer: object | None = None       # AggStarWriter（未配 AGG_DATABASE_URL=None＝显式跳过）
    quality_store: object | None = None    # QualityStore（未配 QUALITY_DATABASE_URL=None＝质检落库/报水位显式跳过）
    tables: tuple = _SOURCE_TABLES         # 进料面（F-01：仅承载进料语义——agg 两源缺省）
    scan_tables: tuple = _SCAN_TABLES      # 质检/拾取扫描面（F-01：缺省全部受检 8 表，独立于进料面）
    operator: str = "dp-scheduler"
    trigger: tuple = ("cron", {"hour": 5, "minute": 30})
    pickup_trigger: tuple = ("interval", {"seconds": 60})  # 拾取作业触发（DP-06）
    stale_seconds: int = 3600              # 残行自愈阈值（须远大于单任务执行时长）
    catalog_factory: object | None = None  # 湖装配方（测试注入位；缺省 load_catalog.from_env）


def _parse_trigger(*, kind_env: str, default_kind: str, interval_env: str,
                   interval_default: str, interval_unit: str,
                   cron_env: str, cron_default: str) -> tuple:
    """「interval|cron＋参数」环境装配（管道/拾取两作业共用解析；cron 值数字转 int、
    表达式（如 */5）保字符串——APScheduler 两收）。"""
    kind = os.environ.get(kind_env, default_kind).strip().lower()
    if kind == "interval":
        return ("interval", {interval_unit: int(os.environ.get(interval_env, interval_default))})
    cron: dict = {}
    for kv in os.environ.get(cron_env, cron_default).split(","):
        k, _, v = kv.partition("=")
        if k.strip():
            cron[k.strip()] = int(v) if v.strip().isdigit() else v.strip()
    return ("cron", cron)


def load_config(catalog_factory=None, freshness=None, agg_writer=None,
                quality_store=None) -> PipelineConfig:
    """环境装配（调度配置外置——interval/cron 可配）。"""
    trigger = _parse_trigger(kind_env="DP_SCHED_TRIGGER", default_kind="cron",
                             interval_env="DP_SCHED_INTERVAL_MINUTES", interval_default="1440",
                             interval_unit="minutes", cron_env="DP_SCHED_CRON",
                             cron_default="hour=5,minute=30")
    pickup_trigger = _parse_trigger(kind_env="DP_PICKUP_TRIGGER", default_kind="interval",
                                    interval_env="DP_PICKUP_INTERVAL_SECONDS",
                                    interval_default="60", interval_unit="seconds",
                                    cron_env="DP_PICKUP_CRON", cron_default="minute=*/1")
    tables = tuple(t.strip() for t in
                   os.environ.get("DP_PIPELINE_TABLES", ",".join(_SOURCE_TABLES)).split(",") if t.strip())
    scan_tables = tuple(t.strip() for t in
                        os.environ.get("DP_SCAN_TABLES", ",".join(_SCAN_TABLES)).split(",") if t.strip())
    return PipelineConfig(
        run_dir=Path(os.environ.get("DP_PIPELINE_RUN_DIR", "/data/runs")),
        freshness=freshness, agg_writer=agg_writer, quality_store=quality_store,
        tables=tables, scan_tables=scan_tables, trigger=trigger, pickup_trigger=pickup_trigger,
        stale_seconds=int(os.environ.get("DP_PICKUP_STALE_SECONDS", "3600")),
        catalog_factory=catalog_factory)


def _canonical_cell(v, data_type: str | None = None, scale: int | None = None):
    """幂等比对口径（V2-11）：空值→None；数值族→Decimal（数字等值不比字面形态——
    视图侧数值经湖 _normalize 已转 float）；**带列标度时按列形定量**（PG 落库定标口径：
    raw 列 NUMERIC(5,1) 把 CSV 13.47 存成 13.5——比对不定量则真湖上闸永不满足＝
    假阴性重灌）；其余→字符串。HALF_UP 与 PG numeric 舍入同向（远离零）。"""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return str(v)
    if data_type and data_type.split(" ")[0] in ("numeric", "decimal"):
        d = Decimal(str(v))
        if scale is not None:
            d = d.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
        return d
    return str(v)


def _cell_canonicalizer(specs: dict):
    """按表列形规格产出单元定型函数（无规格＝不定量——替身湖全文本列照常数值等值）。"""
    def canon(v, col):
        dt, sc = specs.get(col, (None, None))
        return _canonical_cell(v, dt, sc)
    return canon


def view_matches_csv(catalog, table: str, csv_path: Path) -> bool:
    """进料幂等闸判定（V2-11）：湖 as-of 当前视图与源 CSV 逐格一致（列域＝CSV 列；
    行多重集比较——行序不敏感、A/B 班同批并列行是合法内容照常参与比对）。
    比对口径与 PG 落库定标对齐（column_specs 数值标度定量）；替身湖无规格面＝
    精确比。视图缺表/缺列＝不一致（走重灌——修复方向安全：宁可重灌不漏灌）。"""
    try:
        ref = catalog.load_table(table)
    except KeyError:
        return False
    canon = _cell_canonicalizer(getattr(ref, "column_specs", dict)())
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cols = tuple(reader.fieldnames or ())
        csv_rows = [tuple(canon(r.get(c), c) for c in cols) for r in reader]
    view_rows = []
    for r in ref.scan().to_pylist():  # as-of 当前视图（DP-03.fix F1）
        if any(c not in r for c in cols):
            return False
        view_rows.append(tuple(canon(r.get(c), c) for c in cols))
    return Counter(view_rows) == Counter(csv_rows)


def run_pipeline_once(cfg: PipelineConfig, catalog_factory=None) -> dict:
    """一周期六件事；返回摘要（进料/质检落库/agg/水位各步如实记账——干跑证据载体）。
    扫描面＝cfg.scan_tables（F-01：全受检表），进料面＝cfg.tables（agg 两源原语义）。"""
    from app.data_plane.ingest import (BatchRejected, Ingest, Provenance,
                                       UnruledTableRejected)
    from app.data_plane.quality_store import data_date_of, run_quality_task
    from app.data_plane.transform import (RULES_EQUIP, RULES_FLUID, RULES_MEASURES,
                                          RULES_OIL_DAILY, RULES_OVERHAUL,
                                          RULES_SETTLE, RULES_WATER_INJ,
                                          RULES_WELL_INFO)

    catalog = (catalog_factory or cfg.catalog_factory)()
    ing = Ingest(catalog=catalog, freshness=cfg.freshness, rules={
        "oil_production_daily": RULES_OIL_DAILY, "well_info": RULES_WELL_INFO,
        "water_injection_daily": RULES_WATER_INJ, "well_overhaul": RULES_OVERHAUL,
        "equip_status": RULES_EQUIP, "fluid_test": RULES_FLUID,
        "monthly_settlement": RULES_SETTLE, "well_measures": RULES_MEASURES})
    summary: dict = {"ingested": [], "unchanged": [], "rejected": [], "unruled_skipped": [],
                     "agg": "skipped（未配 AGG_DATABASE_URL）", "freshness": cfg.freshness}
    landed_any = False

    # 质检扫描面（F-01：与进料面解耦——全受检表；缺 CSV 诚实跳过不虚报）
    bundle: dict[str, list[dict]] = {}
    for name in cfg.scan_tables:
        path = Path(cfg.run_dir) / f"{name}.csv"
        if path.is_file():
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
            log.info("进料跳过：run 目录无 %s（%s）", name, path)
            continue
        if view_matches_csv(catalog, name, path):  # 幂等闸（V2-11）：内容未变零追加
            summary["unchanged"].append(name)
            log.info("进料跳过：%s——湖当前视图与源 CSV 逐格一致（内容未变不重灌，零新批）", name)
            continue
        try:  # 进料闸在编排层：脏批 BatchRejected＝拒绝入湖（append 不发生）
            snap = ing.run_csv_load(name, path, Provenance(
                quality_report_id=quality_report_id, operator=cfg.operator))
            summary["ingested"].append(name)
            landed_any = True
            log.info("进料完成：%s → 快照 %s（as_of=%s）", name, snap.snapshot_id, snap.committed_at)
            if cfg.quality_store is not None:  # ⑥ 报水位：原始/清洗层＝闸后净批（F-03：落位批）
                day = data_date_of(name, ing.landed.get(name) or [])
                for layer in ("原始层", "清洗层"):
                    advanced = cfg.quality_store.raise_water_line(layer, day)
                    log.info("水位回写：%s → %s%s", layer, day,
                             "" if advanced else "（未推进——水位单调不回退/无业务日期）")
        except BatchRejected as e:
            summary["rejected"].append(name)
            log.warning("进料拒绝：%s（质检 rule_ids=%s）", name, e.report.rule_ids)
        except UnruledTableRejected as e:  # F-01：进料面扩表不再崩周期——诚实跳过
            summary["unruled_skipped"].append(name)
            log.warning("进料跳过：%s（%s）——表未登记进料规则集（fail-closed 不入湖）", name, e)

    # ④＋⑤＋⑥ agg 刷新（限写账号直写主平台库）→ 水位登记（freshness）→ 汇总层报水位
    if cfg.agg_writer is None:
        log.warning("agg 刷新跳过：未配 AGG_DATABASE_URL（限写三表专用账号连接串）")
        return summary
    if not landed_any:  # 幂等闸（V2-11）：as-of 视图未动（内容未变/批被拒/无进料）＝
        # 汇总层无从变化——不重刷、不重盖 freshness（第二遍零变更的另一半）
        summary["agg"] = "skipped（湖无新批——as-of 视图未变，汇总层不重刷）"
        log.info("agg 刷新跳过：本周期零落位（内容未变/批被拒/无进料）——视图未动不重刷")
        return summary
    from app.data_plane.agg import refresh_from_catalog
    counts = refresh_from_catalog(catalog, cfg.agg_writer, freshness=cfg.freshness,
                                  water_line=cfg.quality_store)
    summary["agg"] = "ok" if "note" not in counts else f"skipped（{counts['note']}）"
    log.info("agg 刷新：%s", counts)
    return summary


def lake_bundle(catalog, tables: tuple) -> dict:
    """湖 as-of 当前视图 → 质检 bundle（拾取数据面——修正批次随进料入湖后可复扫）；
    湖中缺表＝诚实跳过（规则按 bundle.get(表,[]) 空跑——不虚报该表扫描）。
    表清单＝扫描面（F-01：DP_SCAN_TABLES 全受检表——独立于进料面两源）。"""
    bundle: dict[str, list[dict]] = {}
    for name in tables:
        try:
            bundle[name] = catalog.load_table(name).scan().to_pylist()
        except KeyError:
            log.warning("拾取 bundle 跳过：湖无表 %s（未进料不虚报扫描）", name)
    return bundle


def run_pickup_once(cfg: PipelineConfig, catalog_factory=None,
                    bundle: dict | None = None) -> dict:
    """拾取作业单周期（DP-06）：自愈残行→轮询 step=0 外部受理行→逐行认领执行→
    done/step=2 落库。行形合同＝M-05b 集成替身冻结形（主仓
    tests/domains/dataops/quality/test_quality_task_channel.py 行形断言）照抄实现，
    零形状偏差：done/step=2/报告 1:1（恰八键）/hits=单据数/隔离行 batch_id=任务号。

    【R2 观察项登记（实质修复归数据面报告合同化批——不在本票）】复扫判绿受报告
    hits[].sample 每规则截 5 影响存在假绿风险——本拾取器产报告经 R2_SAMPLE_NOTE 在
    verdict 头注记留痕（见模块头注）。

    - 自愈先于拾取（同周期闭环）：running∧step=1 超阈无更新→重置 step=0（重置形
      裁决见 QualityStore.heal_stale_tasks——failed 被 0015 CHECK 冻结落不进去）；
    - 防重复拾取：条件 UPDATE 认领闸（claim_task False＝他者已认领——跳过不重跑）；
    - 执行中断＝行如实停 step=1（不造假完成），下周期自愈重拾（重试回路）；
    - 假绿防线：bundle 空（湖无数据）＝本周期不拾取——空扫描报告会被复扫通道判成
      全绿（宁缺不造假结论；行留 step=0，复扫通道照旧诚实 501）。
    - 扫描面（DP-07 F-01）：湖 bundle 取 cfg.scan_tables（全部受检 8 表）——规则 6/8/
      11/12/15/19 在扫描面真跑，M-05b 复扫对其表来源问题单（含预置单）不再假绿。
    返回摘要（picked/healed/failed/bundle_tables——干跑证据载体）。"""
    from app.data_plane.quality_store import run_quality_task
    summary: dict = {"picked": [], "healed": [], "failed": [], "bundle_tables": []}
    if cfg.quality_store is None:
        summary["note"] = "skipped（未配 QUALITY_DATABASE_URL——拾取作业未装配）"
        return summary
    healed = cfg.quality_store.heal_stale_tasks(older_than_seconds=cfg.stale_seconds)
    summary["healed"] = healed
    if healed:
        log.warning("残行自愈：重置 %d 行可重拾（running∧step=1 超阈 %ds 无更新）——%s",
                    len(healed), cfg.stale_seconds, healed)
    if bundle is None:
        catalog = (catalog_factory or cfg.catalog_factory)()
        bundle = lake_bundle(catalog, cfg.scan_tables)
    summary["bundle_tables"] = sorted(bundle)
    if not bundle or not any(bundle.values()):
        log.warning("拾取跳过：湖视图空——不产空扫描报告（假绿防线），%d 行候选原样待拾取",
                    len(cfg.quality_store.pending_tasks()))
        summary["note"] = "skipped（湖视图空——不产空扫描报告）"
        return summary
    for task_id in cfg.quality_store.pending_tasks():
        if not cfg.quality_store.claim_task(task_id):
            log.info("任务 %s 已被他者认领——跳过（认领闸防重复拾取）", task_id)
            continue
        try:
            q = run_quality_task(cfg.quality_store, bundle, task_id=task_id,
                                 verdict_note=R2_SAMPLE_NOTE)
        except Exception as e:  # noqa: BLE001  执行中断如实留残行——下周期自愈重拾
            summary["failed"].append(task_id)
            log.error("任务 %s 执行中断（%s: %s）——行停 step=1 待自愈重拾（不造假完成）",
                      task_id, type(e).__name__, e)
            continue
        summary["picked"].append(q)
        log.info("拾取完成：%s——%d 条启用规则，挂 %s，隔离 %d 行（done/step=2）",
                 task_id, q["rules_run"], q["failed_rules"] or "无", q["quarantined_rows"])
    return summary


def _build_trigger(trigger: tuple):
    """APScheduler 触发器（外置配置 → 触发器实例）。"""
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    kind, params = trigger
    return IntervalTrigger(**params) if kind == "interval" else CronTrigger(**params)


def build_trigger(cfg: PipelineConfig):
    """管道作业触发器（DP-04 既有口——测试与装配沿用）。"""
    return _build_trigger(cfg.trigger)


def build_pickup_trigger(cfg: PipelineConfig):
    """拾取作业触发器（DP-06——周期可配，与管道作业并列）。"""
    return _build_trigger(cfg.pickup_trigger)


def build_scheduler(cfg: PipelineConfig, catalog_factory=None):
    """独立 worker 进程装配：BlockingScheduler＋周期管道任务（APScheduler 真引入）。
    拾取作业仅在 QUALITY_DATABASE_URL 装配（quality_store 在位）时注册——无表可轮询
    不装空作业（不静默）。"""
    from apscheduler.schedulers.blocking import BlockingScheduler
    sched = BlockingScheduler(timezone=os.environ.get("DP_SCHED_TIMEZONE", "Asia/Shanghai"))
    sched.add_job(run_pipeline_once, trigger=build_trigger(cfg),
                  args=[cfg, catalog_factory], id="dp-pipeline",
                  max_instances=1, coalesce=True,  # 上轮未完不并发；积压合并为一轮
                  misfire_grace_time=3600)
    if cfg.quality_store is not None:
        sched.add_job(run_pickup_once, trigger=build_pickup_trigger(cfg),
                      args=[cfg], id=PICKUP_JOB_ID,
                      max_instances=1, coalesce=True,  # 拾取/自愈单飞——单写者前提的进程内半边
                      misfire_grace_time=300)
    else:
        log.warning("拾取作业未装配：未配 QUALITY_DATABASE_URL"
                    "（外部受理任务行无轮询者——复扫通道将诚实 501）")
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
        if cfg.quality_store is not None:  # 拾取作业单周期同跑（DP-06 干跑证据）
            picked = run_pickup_once(cfg)
            log.info("拾取干跑摘要：%s", picked)
        return 0
    sched = build_scheduler(cfg)
    log.info("周期管道 worker 启动（trigger=%s%s，拾取作业=%s）", cfg.trigger[0],
             cfg.trigger[1], "在" if cfg.quality_store is not None else "未装配")
    sched.start()
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
