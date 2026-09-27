"""S2-01 · dp3 规则全量测试：20 条对象（过/挂双样本）。"""
from __future__ import annotations

from app.data_plane.transform import RULES_FULL, rule_mapping


def well(**over):
    w = {"well_id": "N-01", "well_type": "EW", "current_status": "生产中",
         "block_name": "渤中"}
    w.update(over)
    return w


def prod_row(**over):
    # 折算密度 25/(31.2×0.9)=0.89 带内
    r = {"well_id": "N-01", "prod_date": "2025-09-01", "oil_output": 25.0,
         "liquid_output": 31.2, "water_cut": 10.0, "open_days": 24.0,
         "oil_pressure": 5.0, "plan_output": 24.0, "report_shift": "A"}
    r.update(over)
    return r


def down_row(d):
    return prod_row(prod_date=f"2025-09-{d:02d}", open_days=0.0, oil_output=0.0,
                    liquid_output=0.0, water_cut=0.0, oil_pressure=0.0)


def bundle(rows=None, **overrides):
    b = {"oil_production_daily": rows if rows is not None else [prod_row()],
         "well_info": [well()],
         "fluid_test": [], "well_overhaul": [], "well_measures": [],
         "equip_status": [], "monthly_settlement": [], "water_injection_daily": []}
    b.update(overrides)
    return b


def get(rule_id):
    return next(r for r in RULES_FULL if r.rule_id == rule_id)


class TestRulesFull:
    def test_rule4_lab_conflict(self):
        r4 = get("rule-4")
        ok = bundle([prod_row()], fluid_test=[well(sample_date="2025-09-01", water_cut_lab=12.0)])
        assert r4.check_batch(ok) == []
        bad = bundle([prod_row()], fluid_test=[well(sample_date="2025-09-01", water_cut_lab=30.0)])
        assert len(r4.check_batch(bad)) == 1  # 偏差 17pp > 5pp

    def test_rule6_batch_watermark(self):
        r6 = get("rule-6")
        assert r6.check_batch(bundle()) == []
        assert len(r6.check_batch(bundle(
            well_overhaul=[{"well_id": "N-01", "batch_id": "20250911-T3"}]))) == 1

    def test_rule7_well_id_normalize(self):
        r7 = get("rule-7")
        assert r7.check_batch(bundle()) == []
        assert len(r7.check_batch(bundle(
            [prod_row(well_id="N2"), prod_row(well_id="N-2", prod_date="2025-09-02")]))) == 2

    def test_rule8_stop_code_map(self):
        r8 = get("rule-8")
        assert r8.check_batch(bundle(equip_status=[
            {"equip_id": "EQ-1", "stop_reason_code": "BELT"},
            {"equip_id": "EQ-2", "stop_reason_code": "R3"}])) == []  # 新旧映射全命中
        assert len(r8.check_batch(bundle(equip_status=[
            {"equip_id": "EQ-1", "stop_reason_code": "LEGACY-OLD"}]))) == 1  # 未知码挂

    def test_rule9_state_machine(self):
        r9 = get("rule-9")
        stopped = [down_row(d) for d in range(1, 31)]  # 近 30 日全停
        hits = r9.check_batch(bundle(stopped))
        assert len(hits) == 1  # 档案仍"生产中" = 滞后
        assert r9.check_batch(bundle([prod_row()])) == []

    def test_rule10_constant_30d_warn(self):
        r10 = get("rule-10")
        const = [prod_row(prod_date=f"2025-09-{d:02d}", oil_output=25.0) for d in range(1, 31)]
        hits = r10.check_batch(bundle(const))
        assert hits and hits[0]["severity"] == "warn"
        varied = [prod_row(prod_date=f"2025-09-{d:02d}", oil_output=25.0 + d) for d in range(1, 31)]
        assert r10.check_batch(bundle(varied)) == []

    def test_rule12_material_balance(self):
        r12 = get("rule-12")
        ok = bundle([prod_row(liquid_output=1000.0)],
                    well_info=[well(), well(well_id="I-01", well_type="IW", block_name="渤中")],
                    water_injection_daily=[well(well_id="I-01", prod_date="2025-09-01", inj_volume=1015.0)])
        assert r12.check_batch(ok) == []  # 1.5% 界内
        bad = bundle([prod_row(liquid_output=1000.0)],
                     well_info=[well(), well(well_id="I-01", well_type="IW", block_name="渤中")],
                     water_injection_daily=[well(well_id="I-01", prod_date="2025-09-01", inj_volume=1100.0)])
        assert len(r12.check_batch(bad)) == 1  # 10% > 3%

    def test_rule13_referential_integrity(self):
        r13 = get("rule-13")
        assert r13.check_batch(bundle()) == []
        assert len(r13.check_batch(bundle([prod_row(well_id="X-99")]))) == 1

    def test_rule16_enum_legal(self):
        r16 = get("rule-16")
        assert r16.check_batch(bundle()) == []
        bad = bundle(well_info=[well(well_type="XX")])
        assert len(r16.check_batch(bad)) == 1

    def test_rule17_extremes(self):
        r17 = get("rule-17")
        assert r17.check_batch(bundle()) == []
        assert len(r17.check_batch(bundle([prod_row(oil_pressure=55.0)]))) == 1

    def test_rule18_volatility(self):
        r18 = get("rule-18")
        mild = bundle([prod_row(oil_output=25.0),
                       prod_row(prod_date="2025-09-02", oil_output=25.5)])
        assert r18.check_batch(mild) == []
        jump = bundle([prod_row(oil_output=25.0),
                       prod_row(prod_date="2025-09-02", oil_output=80.0)])  # 3.2×
        assert len(r18.check_batch(jump)) == 1

    def test_rule19_settlement_hook(self):
        r19 = get("rule-19")
        ok = bundle([prod_row(oil_output=1000.0)],
                    monthly_settlement=[well(block_name="渤中", settle_date="2025-09-01",
                                             net_weight=980.0)])  # 输差 2% ≤3%
        assert r19.check_batch(ok) == []
        bad = bundle([prod_row(oil_output=1000.0)],
                     monthly_settlement=[well(block_name="渤中", settle_date="2025-09-01",
                                              net_weight=900.0)])  # 10% 挂
        assert len(r19.check_batch(bad)) == 1

    def test_rule20_scrub(self):
        r20 = get("rule-20")
        assert r20.check_batch(bundle()) == []
        assert len(r20.check_batch(bundle([prod_row(report_shift=" a ")]))) == 1

    def test_twenty_rules_registered(self):
        assert len(RULES_FULL) == 20
        assert len(rule_mapping()) == 20
