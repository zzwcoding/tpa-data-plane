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
    """规则 ID ↔ 测试名对照（DP-W-D3-1/2；dbt 形状留位，S1=内置检查名）。"""
    return {"rule-1": "unique_well_prod_date", "rule-2": "not_null_oil_output",
            "rule-3": "density_band_082_098", "rule-5": "range_water_cut_open_days"}


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
