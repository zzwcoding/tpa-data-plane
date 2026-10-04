"""票 01 · dp3 质量闸测试（TDD）：规则 1/2/3/5 + cleaned_arrow + 闸裁决。"""
from __future__ import annotations

import pyarrow as pa
import pytest

from app.data_plane.transform import (RULE_1, RULE_2, RULE_3, RULE_5, GateVerdict,
                                      validate_batch, rule_mapping)


def make_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows)


def base_row(**over):
    row = {"well_id": "N-01", "prod_date": "2025-09-01", "oil_output": 21.16,
           "liquid_output": 28.43, "water_cut": 13.5, "open_days": 24.0}
    row.update(over)
    return row


RULES = [RULE_1, RULE_2, RULE_3, RULE_5]


class TestValidateBatch:
    def test_clean_batch_passes(self):
        table = make_table([base_row(), base_row(prod_date="2025-09-02")])
        report, cleaned = validate_batch(table, RULES)
        assert report.passed is True
        assert report.failures == []
        assert cleaned.num_rows == 2

    def test_rule1_duplicate_locates_key(self):
        rows = [base_row(), base_row()]  # A/B 班重复行
        report, cleaned = validate_batch(make_table(rows), RULES)
        assert report.passed is False
        hit = [f for f in report.failures if f[0] == "rule-1"]
        assert hit and hit[0][1]["well_id"] == "N-01"
        assert cleaned.num_rows == 1  # 重复行剔除，保留首条

    def test_rule3_density_out_of_band(self):
        # 设定书 §A.2 样本：12.40 方误录（漏折算密度 → 折算密度≈1.0 越上界）
        rows = [base_row(oil_output=12.40, liquid_output=96.00, water_cut=12.9)]
        report, _ = validate_batch(make_table(rows), RULES)
        assert [f[0] for f in report.failures if f[0] == "rule-3"]

    def test_rule5_range_violation(self):
        rows = [base_row(water_cut=120.0)]
        report, _ = validate_batch(make_table(rows), RULES)
        assert [f[0] for f in report.failures if f[0] == "rule-5"]

    def test_rule2_not_null_ratio(self):
        rows = [base_row(oil_output=None) for _ in range(3)]
        report, _ = validate_batch(make_table(rows), RULES)
        assert [f[0] for f in report.failures if f[0] == "rule-2"]

    def test_cleaned_schema_preserved(self):
        table = make_table([base_row(), base_row(water_cut=120.0)])
        _, cleaned = validate_batch(table, RULES)
        assert cleaned.column_names == table.column_names
        assert cleaned.schema.field("oil_output").type == table.schema.field("oil_output").type

    def test_verdict_shape_and_rule_ids(self):
        """DP-05：闸裁决＝GateVerdict（passed/failures/rule_ids）——内存弃置报告清零，
        报告身份归 quality_task.id（落库真身见 tests/pg/test_quality_store.py）。"""
        t = make_table([base_row()])
        v, _ = validate_batch(t, RULES)
        assert isinstance(v, GateVerdict)
        assert v.passed and v.rule_ids == [r.rule_id for r in RULES]

    def test_rule_mapping(self):
        m = rule_mapping()
        assert m["rule-1"] and m["rule-3"] and m["rule-5"]
