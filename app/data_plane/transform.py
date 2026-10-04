"""dp3 加工与质量闸（S1 切片）。

- QualityRule：判据=rows(list[dict]) 上的纯函数，返回违例定位器列表；
- validate_batch：先质检（不合格行从 cleaned 中剔除——clean_batch_arrow 闭环，裁决 #13），
  返回 (GateVerdict, cleaned_arrow)；闸裁决强制执行（拒批即 BatchRejected 载体，非弃置件）；
- evaluate_bundle：S2 全量形态（20 条一起跑的整队入口）纯计算——P4（全量入口崩）随 DP-04 修复
  并补整队测试（20261004 盘点 A 组点名：rule-2 签名/孤儿井排序/未测三处）；整队跑批**落库**
  编排＝quality_store.run_quality_task（DP-05：质检跑批结果落主平台库 quality_task/
  quarantine_row——原 validate_bundle 内存 QualityReport 弃置路径随之清零，报告身份＝
  quality_task.id 同源物理落点＝同表）；
- rule_mapping：规则 ID ↔ 测试名对照（DP-W-D3-1/2）。
- 出汇总层（marts）已随 DP-04 重造：v2 parquet 物化三模型（fct_block_monthly/
  fct_valid_output_monthly/fct_wellset_annual——模型名/粒度与 99 册不符）退役删除，
  改 agg 星型三表直写主平台库（99 册 §2.12 合同值，见 agg.py——D6 star_model，
  grain=一井×一天）；lineage 血缘随进料 manifest（ingest.py _lineage_manifest）与
  快照账承载，parquet manifest/lineage 读面随之退役。
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa

Rows = list[dict]
Locator = dict


@dataclass(frozen=True)
class QualityRule:
    rule_id: str
    name: str
    dimension: str
    check: Callable[[Rows], list[Locator]]
    check_batch: Callable[[dict], list[Locator]] | None = None  # S2 bundle 形态


@dataclass(frozen=True)
class GateVerdict:
    """进料闸裁决（闸强制执行——拒批即 BatchRejected 载体，非弃置件）。

    DP-05 起 dp3 无内存弃置报告：质检跑批结果落主平台库 quality_task/quarantine_row
    （quality_store.run_quality_task——报告身份＝quality_task.id，与 E2 同源同表）。"""
    passed: bool
    failures: list      # [(rule_id, *定位器样本≤5)]——闸拒绝留痕用
    rule_ids: list[str]


@dataclass(frozen=True)
class RuleOutcome:
    """整队质检单规则裁决（evaluate_bundle 纯计算输出；落库编排消费全量定位器）。"""
    rule_id: str
    passed: bool
    locators: list      # 全量失败定位器（报告样本截断在编排侧做）
    hit_count: int


def _num(row: dict, col: str) -> float | None:
    v = row.get(col)
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _row_key(r: dict, *cols: str) -> tuple:
    """排序/分组键 None 安全（P4 修复，DP-04）：孤儿井行（well_info 查无此人→block/well
    缺失＝None）进排序环节不再 None 与 str 比大小崩——None 归位空串（排序仅定序，不判等）。"""
    return tuple((r.get(c) if r.get(c) is not None else "") for c in cols)


def _dup_well_date(rows: Rows) -> list[Locator]:
    seen, dups = set(), []
    for i, r in enumerate(rows):
        key = (r.get("well_id"), r.get("prod_date"))
        if key in seen:
            dups.append({"_index": i, "well_id": key[0], "prod_date": key[1]})
        seen.add(key)
    return dups


def _oil_output_not_null(rows: Rows) -> list[Locator]:
    return [{"well_id": r.get("well_id"), "prod_date": r.get("prod_date")}
            for r in rows if _num(r, "oil_output") is None]


def _density_band(rows: Rows) -> list[Locator]:
    """规则 3：折算密度 = 油/(液×(1−含水)) ∈ [0.82, 0.98] t/m³（设定书 §4）。"""
    bad = []
    for r in rows:
        oil, liquid, wc = _num(r, "oil_output"), _num(r, "liquid_output"), _num(r, "water_cut")
        if not oil or not liquid or wc is None or wc >= 100:
            continue
        density = oil / (liquid * (1 - wc / 100))
        if not 0.82 <= density <= 0.98:
            bad.append({"well_id": r.get("well_id"), "prod_date": r.get("prod_date"),
                        "density": round(density, 4)})
    return bad


def _value_range(rows: Rows) -> list[Locator]:
    """规则 5：water_cut∈[0,100]、open_days∈[0,24]。"""
    bad = []
    for r in rows:
        wc, od = _num(r, "water_cut"), _num(r, "open_days")
        if wc is not None and not 0 <= wc <= 100:
            bad.append({"well_id": r.get("well_id"), "prod_date": r.get("prod_date"), "field": "water_cut"})
        if od is not None and not 0 <= od <= 24:
            bad.append({"well_id": r.get("well_id"), "prod_date": r.get("prod_date"), "field": "open_days"})
    return bad


RULE_1 = QualityRule("rule-1", "唯一性 (well_id,prod_date)", "唯一性", _dup_well_date)
RULE_2 = QualityRule("rule-2", "oil_output 非空率 ≥99.5%", "完整性", _oil_output_not_null)
RULE_3 = QualityRule("rule-3", "单位一致性（折算密度带）", "准确性", _density_band)
RULE_5 = QualityRule("rule-5", "water_cut/open_days 值域", "值域", _value_range)

RULES_OIL_DAILY = [RULE_1, RULE_2, RULE_3, RULE_5]


def _well_info_enum_rows(rows: Rows) -> list[Locator]:
    """规则 16 行级半边（well_info 进料闸用，DP-04）：well_type/current_status 枚举合法。

    值域照抄 bundle 版 _r16 的 legal 表（合同规则零发明）；report_shift 半边属日产计量行，
    仍在 _r16 bundle 版检查。"""
    legal = {"well_type": {"EW", "IW"},
             "current_status": {"生产中", "停产", "检修", "待弃置"}}
    bad = []
    for r in rows:
        for f, values in legal.items():
            if r.get(f) is not None and r.get(f) not in values:
                bad.append({"well_id": r.get("well_id"), "field": f, "value": r.get(f)})
    return bad


RULES_WELL_INFO = [QualityRule("rule-16", "枚举合法（well_info 行级半边）", "枚举合法",
                               _well_info_enum_rows)]


def rule_mapping() -> dict[str, str]:
    """规则 ID ↔ 检查名对照（20 条全量；dbt test 名形状留位，S1=内置检查名）。"""
    return {r.rule_id: "check_" + r.rule_id for r in RULES_FULL}


def validate_batch(table: pa.Table, rules: list[QualityRule]) -> tuple[GateVerdict, pa.Table]:
    """质检闸：返回 (GateVerdict, cleaned_arrow)。不合格行从 cleaned 剔除（保留首条）。"""
    rows = table.to_pylist()
    failures: list = []
    rule_ids: list[str] = []
    bad_index: set[int] = set()
    for rule in rules:
        rule_ids.append(rule.rule_id)
        locators = rule.check(rows)
        if locators:
            failures.append((rule.rule_id, *locators[:5]))
            for i, r in enumerate(rows):
                for loc in locators:
                    if "_index" in loc:  # 行级精确定位（重复出现剔除保首条）
                        if i == loc["_index"]:
                            bad_index.add(i)
                    elif "well_id" in loc and \
                            (loc.get("well_id"), loc.get("prod_date")) == (r.get("well_id"), r.get("prod_date")):
                        bad_index.add(i)
    verdict = GateVerdict(passed=not failures, failures=failures, rule_ids=rule_ids)
    clean_rows = [r for i, r in enumerate(rows) if i not in bad_index]
    cleaned = pa.Table.from_pylist(clean_rows, schema=table.schema)
    return verdict, cleaned


# ============================================================ S2-01 · 规则全量
# 20 条规则对象化：1–12 按设定书 §4 判据，13–20 按提取件二 8 类型 × 附录 A 字段重建。
# 新形态 check_batch(bundle: dict) -> locators（跨表规则需要多表）；S1 行级 check 并存。

def _wrap_rows(rule: QualityRule, table: str = "oil_production_daily"):
    """把 S1 行级 check 适配为 bundle 形态。"""
    def check_batch(bundle: dict) -> list[Locator]:
        return rule.check(bundle.get(table, []))
    return check_batch


def _r2_ratio(bundle: dict) -> list[Locator]:
    """规则 2（批级阈值版）：oil_output 非空率 ≥99.5%。

    P4 修复（DP-04）：本规则是批级判据，注册在 check_batch 槽收 **bundle**（多表打包），
    原实现形参按 rows 用法直接消费 bundle → 整队入口 AttributeError（20261004 盘点 A 组点名）。"""
    rows = bundle.get("oil_production_daily", [])
    empty = _oil_output_not_null(rows)
    if rows and len(empty) / len(rows) <= 0.005:
        return []
    return empty


def _r4_lab_conflict(bundle: dict) -> list[Locator]:
    """规则 4：计量含水 vs 化验含水 ≤5pp（井号+取样日期双键联结）。"""
    lab = {(f.get("well_id"), f.get("sample_date")): _num(f, "water_cut_lab")
           for f in bundle.get("fluid_test", [])}
    bad = []
    for r in bundle.get("oil_production_daily", []):
        key = (r.get("well_id"), r.get("prod_date"))
        wc, lc = _num(r, "water_cut"), lab.get(key)
        if wc is None or lc is None:
            continue
        if abs(wc - lc) > 5.0:
            bad.append({"well_id": key[0], "prod_date": key[1], "dev_pp": round(abs(wc - lc), 2)})
    return bad


def _r6_batch_watermark(bundle: dict) -> list[Locator]:
    """规则 6：批次水位 ≤T-1（-T3 补录=超窗）。"""
    return [{"batch_id": r.get("batch_id")} for r in bundle.get("well_overhaul", [])
            if str(r.get("batch_id", "")).endswith("-T3")]


def _r7_normalize(bundle: dict) -> list[Locator]:
    """规则 7：井号归一化 ^[A-Z]-\\d{2}$（N2/N-2 旧写法挂样本）。"""
    import re
    return [{"well_id": r.get("well_id")}
            for r in bundle.get("oil_production_daily", [])
            if not re.match(r"^[A-Z]-\d{2}$", str(r.get("well_id", "")))]


_KNOWN_CODES = {"RUNNING", "BELT", "MOTOR", "PUMP", "POWER", "R3", "R4", "R5", "R6"}


def _r8_stop_code(bundle: dict) -> list[Locator]:
    """规则 8：停机原因码新旧映射全命中（未知码挂）。"""
    return [{"equip_id": r.get("equip_id"), "code": r.get("stop_reason_code")}
            for r in bundle.get("equip_status", [])
            if r.get("stop_reason_code") not in _KNOWN_CODES]


def _r9_state_lag(bundle: dict) -> list[Locator]:
    """规则 9：状态机——近 31 日全停但档案仍"生产中"=滞后。"""
    prod = bundle.get("oil_production_daily", [])
    if not prod:
        return []
    tail = max(str(r["prod_date"]) for r in prod)
    from datetime import date as _d, timedelta as _td
    cut = (_d.fromisoformat(tail) - _td(days=30)).isoformat()
    ran: dict[str, bool] = {}
    for r in prod:
        if str(r["prod_date"]) >= cut:
            ran[r["well_id"]] = ran.get(r["well_id"], False) or (_num(r, "open_days") or 0) > 0
    status = {w.get("well_id"): w.get("current_status") for w in bundle.get("well_info", [])}
    return [{"well_id": w} for w, ran30 in ran.items()
            if not ran30 and status.get(w) == "生产中"]


def _r10_constant_30d(bundle: dict) -> list[Locator]:
    """规则 10：连续 30 日恒值 → 告警级（warn，非 fail）。"""
    by_well: dict[str, list] = {}
    for r in sorted(bundle.get("oil_production_daily", []),
                    key=lambda r: _row_key(r, "well_id", "prod_date")):
        by_well.setdefault(r.get("well_id"), []).append(r)
    hits = []
    for wid, rows in by_well.items():
        run, prev, worst = 0, None, 0
        for r in rows:
            if (_num(r, "open_days") or 0) == 0.0:
                run, prev = 0, None
                continue
            v = r.get("oil_output")
            run = run + 1 if v == prev else 1
            prev = v
            worst = max(worst, run)
        if worst >= 30:
            hits.append({"well_id": wid, "severity": "warn", "run_days": worst})
    return hits


def _r11_realtime_domain(bundle: dict) -> list[Locator]:
    """规则 11：传感器零值 2 小时告警——15 分钟档 8 连零（RF2-01 真机判）。

    bundle 含 sensor_reading_subdaily 时逐通道机判（按 ts 排序后数连零）；
    纯日批 bundle 恒空=如实（日粒度不可判，设定书自标教学演绎）。"""
    rows = bundle.get("sensor_reading_subdaily", [])
    if not rows:
        return []
    series: dict[str, list] = {}
    for r in rows:
        chan = r.get("channel_id")
        series.setdefault(chan, []).append((_num(r, "value") or 0.0, r.get("ts")))
    bad = []
    for chan, pairs in series.items():
        pairs.sort(key=lambda x: x[1])
        run = worst = 0
        for v, _ in pairs:
            run = run + 1 if v == 0.0 else 0
            worst = max(worst, run)
        if worst >= 8:
            bad.append({"channel_id": chan, "max_zero_run": worst, "threshold": 8})
    return bad


def _r12_material_balance(bundle: dict) -> list[Locator]:
    """规则 12：区块月物质平衡 注入 ≈ 产液×(1±3%)。

    有意分歧注记：fieldforge r12 对齐"结算 vs 产量"，本仓按设定书 §4 原文
    "注入量≈产出量+存水率变动"对齐"注入 vs 产液"——两处不要互相"对齐"。"""
    inj: dict[tuple, float] = {}
    for r in bundle.get("water_injection_daily", []):
        key = (r.get("well_id"), str(r.get("prod_date"))[:7])
        inj[key] = inj.get(key, 0.0) + (_num(r, "inj_volume") or 0.0)
    bad = []
    wells = {w.get("well_id"): w for w in bundle.get("well_info", [])}
    by_block_month: dict[tuple, float] = {}
    for r in bundle.get("oil_production_daily", []):
        blk = (wells.get(r.get("well_id")) or {}).get("block_name")
        key = (blk, str(r.get("prod_date"))[:7])
        by_block_month[key] = by_block_month.get(key, 0.0) + (_num(r, "liquid_output") or 0.0)
    for (block, month), liquid in sorted(by_block_month.items(),
                                         key=lambda kv: (kv[0][0] or "", kv[0][1])):
        total_inj = sum(v for (w, _m), v in inj.items()
                        if (wells.get(w) or {}).get("block_name") == block and _m == month)
        if liquid == 0:
            continue
        if abs(total_inj / liquid - 1.0) > 0.03:
            bad.append({"block_name": block, "month": month,
                        "dev": round(abs(total_inj / liquid - 1.0), 4)})
    return bad


def _r13_referential(bundle: dict) -> list[Locator]:
    """规则 13（引用完整）：oil_production_daily.well_id → well_info 孤儿行。"""
    known = {w.get("well_id") for w in bundle.get("well_info", [])}
    return [{"well_id": r.get("well_id"), "prod_date": r.get("prod_date")}
            for r in bundle.get("oil_production_daily", [])
            if r.get("well_id") not in known]


def _r14_intra_table_logic(bundle: dict) -> list[Locator]:
    """规则 14（表内逻辑一致）：open_days=0 ⇒ 产量/液量/压力全零。"""
    bad = []
    for r in bundle.get("oil_production_daily", []):
        if (_num(r, "open_days") or 0) == 0.0 and any(
                (_num(r, c) or 0) > 0 for c in ("oil_output", "liquid_output", "oil_pressure")):
            bad.append({"well_id": r.get("well_id"), "prod_date": r.get("prod_date")})
    return bad


def _r15_injection_continuity(bundle: dict) -> list[Locator]:
    """规则 15（跨表逻辑一致/注采连续性）：日注水量环比无骤变（>3× 挂）。"""
    import math
    by_well: dict[str, list] = {}
    for r in sorted(bundle.get("water_injection_daily", []),
                    key=lambda r: _row_key(r, "well_id", "prod_date")):
        by_well.setdefault(r.get("well_id"), []).append(_num(r, "inj_volume") or 0.0)
    bad = []
    for wid, vals in by_well.items():
        for a, b in zip(vals, vals[1:]):
            if a > 0 and b > 0 and max(a, b) / min(a, b) > 3.0:
                bad.append({"well_id": wid, "ratio": round(max(a, b) / min(a, b), 2)})
    return bad


def _r16_enum_legal(bundle: dict) -> list[Locator]:
    """规则 16（枚举合法）：well_type / current_status / report_shift。"""
    legal = {"well_type": {"EW", "IW"},
             "current_status": {"生产中", "停产", "检修", "待弃置"},
             "report_shift": {"A", "B", "C"}}
    bad = []
    for w in bundle.get("well_info", []):
        for f, values in legal.items():
            if f in w and w.get(f) not in values:
                bad.append({"well_id": w.get("well_id"), "field": f, "value": w.get(f)})
    for r in bundle.get("oil_production_daily", []):
        if r.get("report_shift") not in legal["report_shift"]:
            bad.append({"well_id": r.get("well_id"), "field": "report_shift",
                        "value": r.get("report_shift")})
    return bad


def _r17_extremes(bundle: dict) -> list[Locator]:
    """规则 17（极值）：oil_pressure∈[0,40]（平台原型量程）。"""
    bad = []
    for r in bundle.get("oil_production_daily", []):
        p = _num(r, "oil_pressure")
        if p is not None and not 0 <= p <= 40:
            bad.append({"well_id": r.get("well_id"), "oil_pressure": p})
    return bad


def _r18_volatility(bundle: dict) -> list[Locator]:
    """规则 18（波动率）：相邻生产日油量环比 >2×（log 判据，对齐注入尖刺设计界）。"""
    import math
    rows = [r for r in bundle.get("oil_production_daily", [])
            if (_num(r, "open_days") or 0) > 0]
    by_well: dict[str, list] = {}
    for r in sorted(rows, key=lambda r: _row_key(r, "well_id", "prod_date")):
        by_well.setdefault(r.get("well_id"), []).append(_num(r, "oil_output") or 0.0)
    bad = []
    for wid, vals in by_well.items():
        for a, b in zip(vals, vals[1:]):
            if a > 0 and b > 0 and abs(math.log(b / a)) > math.log(2.0):
                bad.append({"well_id": wid, "jump_x": round(max(a, b) / min(a, b), 2)})
    return bad


def _r19_settlement_hook(bundle: dict) -> list[Locator]:
    """规则 19（钩稽）：月结算净重 = 区块月油量合计 ×(1−输差)，输差 ≤3%。"""
    wells = {w.get("well_id"): w.get("block_name") for w in bundle.get("well_info", [])}
    sums: dict[tuple, float] = {}
    for r in bundle.get("oil_production_daily", []):
        key = (wells.get(r.get("well_id")), str(r.get("prod_date"))[:7])
        sums[key] = sums.get(key, 0.0) + (_num(r, "oil_output") or 0.0)
    bad = []
    for r in bundle.get("monthly_settlement", []):
        key = (r.get("block_name"), str(r.get("settle_date"))[:7])
        total = sums.get(key, 0.0)
        net = _num(r, "net_weight")
        if not total or net is None:
            continue
        if abs(net / total - 1.0) > 0.03:
            bad.append({"block_name": key[0], "month": key[1],
                        "dev": round(abs(net / total - 1.0), 4)})
    return bad


def _r20_scrub(bundle: dict) -> list[Locator]:
    """规则 20（scrub/规范化）：字符字段未 trim（大小写规范化 S2 后续）。"""
    bad = []
    for r in bundle.get("oil_production_daily", []):
        for f in ("report_shift", "well_id"):
            v = r.get(f)
            if isinstance(v, str) and v != v.strip():
                bad.append({"well_id": r.get("well_id"), "field": f, "value": v})
    return bad


def _b(rule_id, name, dimension, fn):
    """S2 bundle 形态规则构造器：fn 进 check_batch 槽（跨表规则需要多表）。"""
    return QualityRule(rule_id, name, dimension,
                       check=lambda rows: [], check_batch=fn)


RULES_FULL = [
    _b("rule-1", "唯一性 (well_id,prod_date)", "唯一性", _wrap_rows(RULE_1)),
    _b("rule-2", "oil_output 非空率 ≥99.5%（批级阈值）", "完整性", _r2_ratio),
    _b("rule-3", "单位一致性（折算密度带）", "准确性", _wrap_rows(RULE_3)),
    _b("rule-4", "计量 vs 化验含水 ≤5pp", "一致性", _r4_lab_conflict),
    _b("rule-5", "water_cut/open_days 值域", "值域", _wrap_rows(RULE_5)),
    _b("rule-6", "批次水位 ≤T-1", "及时性", _r6_batch_watermark),
    _b("rule-7", "井号归一化", "规范性", _r7_normalize),
    _b("rule-8", "停机原因码映射全命中", "规范性", _r8_stop_code),
    _b("rule-9", "状态机合法迁移（滞后检测）", "唯一性", _r9_state_lag),
    _b("rule-10", "恒值 30 日告警", "一致性", _r10_constant_30d),
    _b("rule-11", "传感器零值 2 小时告警（实时域）", "及时性", _r11_realtime_domain),
    _b("rule-12", "区块月物质平衡 ≤3%", "一致性", _r12_material_balance),
    _b("rule-13", "引用完整（well_id 孤儿行）", "引用完整", _r13_referential),
    _b("rule-14", "表内逻辑一致（停机归零）", "表内一致", _r14_intra_table_logic),
    _b("rule-15", "注采连续性（日环比无骤变）", "跨表一致", _r15_injection_continuity),
    _b("rule-16", "枚举合法", "枚举合法", _r16_enum_legal),
    _b("rule-17", "极值（油压量程）", "极值", _r17_extremes),
    _b("rule-18", "波动率（日环比界）", "波动率", _r18_volatility),
    _b("rule-19", "钩稽（月结算 vs 产量 ≤3%）", "钩稽", _r19_settlement_hook),
    _b("rule-20", "scrub（规范化）", "scrub", _r20_scrub),
]



def evaluate_bundle(bundle: dict, rules: list[QualityRule]) -> list[RuleOutcome]:
    """S2 整队质检纯计算：逐规则出 RuleOutcome（全量定位器＋命中数）。

    DP-05 起整队跑批落库由 quality_store.run_quality_task 承接（读启用集→建任务→
    隔离区落行→报告 1:1 完任务）——本函数只算不落，原 validate_bundle 的内存
    QualityReport 弃置路径随之清零（DP-05 验收 2）。"""
    outcomes = []
    for rule in rules:
        locators = rule.check_batch(bundle)
        outcomes.append(RuleOutcome(rule_id=rule.rule_id, passed=not locators,
                                    locators=locators, hit_count=len(locators)))
    return outcomes
