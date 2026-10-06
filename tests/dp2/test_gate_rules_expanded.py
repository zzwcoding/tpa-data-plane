"""进料闸规则集扩表单测（云部署 2026-10-06：五表＋措施台账）。

最小硬闸语义：主键列非空（完整性）＋非空值域/日期序——脏特征留 as-is 归 bundle
版规则与人工复核。每套规则一好一坏各一例，fail-closed 注册面（scheduler rules
dict 键域）同检。
"""

from app.data_plane.transform import (RULES_EQUIP, RULES_FLUID, RULES_MEASURES,
                                      RULES_OVERHAUL, RULES_SETTLE,
                                      RULES_WATER_INJ, validate_batch)
import pyarrow as pa


def _verdict(rows, rules):
    table = pa.table({k: [r.get(k) for r in rows] for k in rows[0]}) if rows else pa.table({})
    return validate_batch(table, rules)


def _row(**kw):
    return kw


def test_water_inj_pass_and_flag() -> None:
    good = [_row(well_id="N-02", prod_date="2026-08-01", inj_volume=120.5, pump_pressure=10.0)]
    assert _verdict(good, RULES_WATER_INJ)[0].passed
    bad = good + [_row(well_id="N-02", prod_date="2026-08-01", inj_volume=99.0, pump_pressure=10.0),
                  _row(well_id="N-03", prod_date="2026-08-02", inj_volume=-1.0, pump_pressure=10.0)]
    report, _ = _verdict(bad, RULES_WATER_INJ)
    assert not report.passed  # 重复行（rule-1）＋负注入量（rule-5）


def test_overhaul_end_before_start_flagged() -> None:
    good = [_row(well_id="N-11", overhaul_start="2026-08-01", overhaul_end="2026-08-05")]
    assert _verdict(good, RULES_OVERHAUL)[0].passed
    bad = [_row(well_id="N-11", overhaul_start="2026-08-05", overhaul_end="2026-08-01")]
    assert not _verdict(bad, RULES_OVERHAUL)[0].passed


def test_equip_load_band() -> None:
    assert _verdict([_row(equip_id="P-1", load=85.0, current=12.0)], RULES_EQUIP)[0].passed
    assert not _verdict([_row(equip_id="P-1", load=300.0, current=12.0)], RULES_EQUIP)[0].passed


def test_fluid_cut_band_and_missing_key() -> None:
    assert _verdict([_row(well_id="N-02", sample_date="2026-08-01",
                          water_cut_lab=78.0, density=0.91)], RULES_FLUID)[0].passed
    assert not _verdict([_row(well_id=None, sample_date="2026-08-01",
                              water_cut_lab=78.0, density=0.91)], RULES_FLUID)[0].passed
    assert not _verdict([_row(well_id="N-02", sample_date="2026-08-01",
                              water_cut_lab=120.0, density=0.91)], RULES_FLUID)[0].passed


def test_settle_and_measures_nonneg() -> None:
    assert _verdict([_row(block_name="渤中", settle_date="2025-10-01", net_weight=1200.0)],
                    RULES_SETTLE)[0].passed
    assert not _verdict([_row(block_name="渤中", settle_date="2025-10-01", net_weight=-5.0)],
                        RULES_SETTLE)[0].passed
    assert _verdict([_row(well_id="N-02", incr_oil_annual=300.0)], RULES_MEASURES)[0].passed
    assert not _verdict([_row(well_id="N-02", incr_oil_annual=-1.0)], RULES_MEASURES)[0].passed
