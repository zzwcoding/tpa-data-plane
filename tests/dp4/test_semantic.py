"""票 03 · dp4 语义层测试：15 指标卡目录 / 术语归一四类 / 消歧询问 / 权威源仲裁。"""
from __future__ import annotations

import pytest

from app.data_plane.semantic import Clarify, Semantic, StaticYaml

SEM = Semantic(StaticYaml.default())


class TestMetricCatalog:
    def test_fifteen_metrics_with_version(self):
        cats = SEM.metric_catalog()
        assert len(cats) == 15
        assert all(m.version for m in cats)

    def test_modifiers_only_metric2(self):
        cats = {m.name: m for m in SEM.metric_catalog()}
        assert set(cats["有效产量"].modifiers) == {"开井日历", "检修剔除"}
        assert cats["产量完成率"].modifiers == ()

    def test_additive_annotated(self):
        """F12 判据：含计划值/拟合基线/比率=非可加；纯可加计量=可加。"""
        cats = {m.name: m for m in SEM.metric_catalog()}
        assert cats["产量完成率"].additive is False  # 分母含计划配产（非可加）
        assert cats["综合含水率"].additive is True   # 分子分母均为可加计量
        assert cats["自然递减率"].additive is False  # 拟合基线非可加


class TestResolveTerm:
    def test_forbidden_word_blocked(self):
        r = SEM.resolve_term("老完成率")
        assert r.action == "block" and r.canonical == "产量完成率"  # DP-W-D4-3

    def test_alias_rewritten(self):
        r = SEM.resolve_term("剔检产量")
        assert r.action == "rewrite" and "净产量" in r.canonical  # DP-W-D4-4

    def test_black_lang_pass_with_context_note(self):
        r = SEM.resolve_term("躺井")
        assert r.action == "pass" and "作业区" in r.note

    def test_unknown_passes(self):
        assert SEM.resolve_term("产量完成率").action == "pass"


class TestBuildQuery:
    def test_ambiguous_metric_returns_clarify(self):
        """DP-W-D4-2：有效产量不带给修饰词 → 消歧询问，不擅自二选一。"""
        r = SEM.build_query("有效产量", dims=["block_name"],
                            time_window={"start": "2025-09-01", "end": "2025-09-30",
                                         "granularity": "month"}, filters={})
        assert isinstance(r, Clarify) and set(r.options) == {"开井日历", "检修剔除"}  # DP-W-D4-2

    def test_modifier_selects_measure(self):
        r = SEM.build_query("有效产量", dims=["block_name"],
                            time_window={"start": "2025-09-01", "end": "2025-09-30",
                                         "granularity": "month"},
                            filters={"口径": "检修剔除"})
        assert r.measures == ("valid_output_overhaul_excluded",)

    def test_simple_metric_executable_with_base_table(self):
        """裁决 #10：base_table/limit 显式槽位。"""
        r = SEM.build_query("产量完成率", dims=["block_name"],
                            time_window={"start": "2025-09-01", "end": "2025-09-30",
                                         "granularity": "month"}, filters={})
        assert r.base_table == "oil_production_daily"
        assert "oil_production_daily" in r.tables
        # TimeWindow 实例与 dict 双路兼容（RF-02）
        from app.data_plane.semantic import TimeWindow
        r2 = SEM.build_query("产量完成率", dims=["block_name"],
                             time_window=TimeWindow("2025-09-01", "2025-09-30", "month"), filters={})
        assert r2.time_dimensions[0]["dateRange"] == r.time_dimensions[0]["dateRange"]


class TestAuthoritativeSource:
    def test_arbitration(self):
        """DP-W-D4-5：一数四源仲裁 → 唯一权威源 + 注记。"""
        ref = SEM.authoritative_source("油气产量")
        assert ref.source == "monthly_settlement"
        assert "3.2%" in ref.note
