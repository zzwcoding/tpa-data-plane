"""dp3 加工与质量闸（S1 切片）。

- QualityRule：判据=rows(list[dict]) 上的纯函数，返回违例定位器列表；
- validate_batch：先质检（不合格行从 cleaned 中剔除——clean_batch_arrow 闭环，裁决 #13），
  返回 (QualityReport, cleaned_arrow)；QualityReport 带 report_id（裁决 #9）；
- rule_mapping：规则 ID ↔ 测试名对照（DP-W-D3-1/2）。
- 分区/分层/lineage 全量属 S2；本切片只承载质检闸本体。
"""
from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

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
class QualityReport:
    passed: bool
    failures: list
    rule_ids: list[str]
    report_id: str = field(default_factory=lambda: str(uuid.uuid4()))


def _num(row: dict, col: str) -> float | None:
    v = row.get(col)
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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


def rule_mapping() -> dict[str, str]:
    """规则 ID ↔ 检查名对照（20 条全量；dbt test 名形状留位，S1=内置检查名）。"""
    return {r.rule_id: "check_" + r.rule_id for r in RULES_FULL}


def validate_batch(table: pa.Table, rules: list[QualityRule]) -> tuple[QualityReport, pa.Table]:
    """质检闸：返回 (QualityReport, cleaned_arrow)。不合格行从 cleaned 剔除（保留首条）。"""
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
    report = QualityReport(passed=not failures, failures=failures, rule_ids=rule_ids)
    clean_rows = [r for i, r in enumerate(rows) if i not in bad_index]
    cleaned = pa.Table.from_pylist(clean_rows, schema=table.schema)
    return report, cleaned


# ============================================================ S2-01 · 规则全量
# 20 条规则对象化：1–12 按设定书 §4 判据，13–20 按提取件二 8 类型 × 附录 A 字段重建。
# 新形态 check_batch(bundle: dict) -> locators（跨表规则需要多表）；S1 行级 check 并存。

def _wrap_rows(rule: QualityRule, table: str = "oil_production_daily"):
    """把 S1 行级 check 适配为 bundle 形态。"""
    def check_batch(bundle: dict) -> list[Locator]:
        return rule.check(bundle.get(table, []))
    return check_batch


def _r2_ratio(rows: Rows) -> list[Locator]:
    """规则 2（批级阈值版）：oil_output 非空率 ≥99.5%。"""
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
    for r in bundle.get("oil_production_daily", []):
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
    """规则 11：传感器零值 2 小时告警——实时域规则（设定书自标教学演绎），
    日批 bundle 不可判；亚日数据接入后由 dp6 realtime 分支消费。恒空=如实。"""
    return []


def _r12_material_balance(bundle: dict) -> list[Locator]:
    """规则 12：区块月物质平衡 注入 ≈ 产液×(1±3%)（存水率变动并入容差）。"""
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
    for (block, month), liquid in sorted(by_block_month.items()):
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
    for r in bundle.get("water_injection_daily", []):
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
    for r in rows:
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
    """规则 20（scrub/规范化）：字符字段未 trim、大小写不规范。"""
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
    _b("rule-14", "表内逻辑一致（停机归零）", "跨表一致", _r14_intra_table_logic),
    _b("rule-15", "注采连续性（日环比无骤变）", "跨表一致", _r15_injection_continuity),
    _b("rule-16", "枚举合法", "枚举合法", _r16_enum_legal),
    _b("rule-17", "极值（油压量程）", "极值", _r17_extremes),
    _b("rule-18", "波动率（日环比界）", "波动率", _r18_volatility),
    _b("rule-19", "钩稽（月结算 vs 产量 ≤3%）", "钩稽", _r19_settlement_hook),
    _b("rule-20", "scrub（规范化）", "scrub", _r20_scrub),
]



def validate_bundle(bundle: dict, rules: list[QualityRule]) -> list[QualityReport]:
    """S2 bundle 形态质检：逐规则出独立报告（失败定位到表+定位器）。"""
    reports = []
    for rule in rules:
        locators = rule.check_batch(bundle)
        reports.append(QualityReport(passed=not locators, failures=[(rule.rule_id, *locators[:5])],
                                     rule_ids=[rule.rule_id]))
    return reports
