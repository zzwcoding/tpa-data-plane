"""票 04 · dp6 路由拒答测试：三查三出口 / 话术合同零编数 / FakeClock。"""
from __future__ import annotations

from datetime import date, datetime

from app.data_plane.freshness import FreshnessRouter, FreshnessInfo
from app.data_plane.semantic import Semantic, StaticYaml

TODAY = date(2026, 9, 27)


class FakeClock:
    def today(self) -> date:
        return TODAY


class StaticFreshness:
    """dp2 FreshnessRegistry 的测试替身（dp 卡 L70-71）。"""

    def __init__(self, registry: dict[str, FreshnessInfo]):
        self._r = registry

    def freshness(self, table: str) -> FreshnessInfo:
        return self._r.get(table, FreshnessInfo(tier="unknown", as_of=None, pipeline=None))


def make_router(registry_overrides: dict | None = None):
    reg = {"oil_production_daily": FreshnessInfo("offline", date(2026, 9, 26), "batch"),
           "monthly_settlement": FreshnessInfo("offline", date(2026, 9, 26), "batch"),
           "rt_table": FreshnessInfo("realtime", datetime(2026, 9, 27, 10, 0), "flink")}
    reg.update(registry_overrides or {})
    return FreshnessRouter(semantic=Semantic(StaticYaml.default()), freshness=StaticFreshness(reg),
                           clock=FakeClock())


def w(start, end, granularity):
    return {"start": start, "end": end, "granularity": granularity}


class TestRoute:
    def test_history_window_offline_answerable(self):
        """分支 C：已闭合历史窗 → 答离线面。"""
        d = make_router().route(w("2025-09-01", "2025-09-30", "month"), "产量完成率")
        assert d.answerable and d.source_tier == "offline"
        assert str(d.declared_as_of) == "2026-09-26"

    def test_month_metric_intraday_nonadditive_refused(self):
        """DP-W-D6-1 正典：产量完成率（含计划值分母=非可加）×今天上午 → 粒度拒，档位无关。"""
        d = make_router().route(w("2026-09-27 08:00", "2026-09-27 12:00", "intraday"), "产量完成率")
        assert d.answerable is False and d.refusal.kind == "granularity"

    def test_intraday_additive_offline_deferred(self):
        """分支 B'：日内窗 × 可加指标 × offline → defer（T+1 明日此问可答——可加量承诺成立）。"""
        d = make_router().route(w("2026-09-27 08:00", "2026-09-27 12:00", "intraday"), "商品量")
        assert d.answerable is False and d.refusal.kind == "defer"

    def test_realtime_additive_intraday_answerable_with_caliber_note(self):
        """DP-W-D6-3 分支 B：realtime×可加 → 答实时面 + 口径差异声明（事故 13）。"""
        from datetime import datetime
        router = make_router({"monthly_settlement": FreshnessInfo(
            "realtime", datetime(2026, 9, 27, 10, 0), "flink")})
        d = router.route(w("2026-09-27 08:00", "2026-09-27 12:00", "intraday"), "商品量")
        assert d.answerable is True and d.source_tier == "realtime"

    def test_unregistered_metric_refused(self):
        d = make_router().route(w("2025-09-01", "2025-09-30", "month"), "不存在的指标")
        assert d.answerable is False and d.refusal.kind == "unregistered"

    def test_unregistered_freshness_refused(self):
        d = make_router().route(w("2025-09-01", "2025-09-30", "month"), "注采比")
        assert d.refusal.kind == "unregistered"  # water_injection_daily 未登记

    def test_future_window_refused(self):
        d = make_router().route(w("2026-10-01", "2026-11-30", "month"), "产量完成率")
        assert d.answerable is False and d.refusal.kind == "future"

    def test_uncovered_window_refused(self):
        d = make_router().route(w("2026-09-20", "2026-10-05", "month"), "产量完成率")
        assert d.answerable is False and d.refusal.kind == "uncovered"


class TestRefusalText:
    def test_text_contains_as_of_and_uncovered(self):
        """DP-W-D6-2：拒答话术含"数据截至 X，未覆盖您问的 Y"。"""
        router = make_router()
        d = router.route(w("2026-09-27 08:00", "2026-09-27 12:00", "intraday"), "产量完成率")
        text = router.refusal_text(d)
        assert "数据截至 2026-09-26" in text and "未覆盖" in text

    def test_unknown_zero_fabrication(self):
        """零编数纪律：未登记新鲜度的拒答连"数据截至"都不填。"""
        router = make_router()
        d = router.route(w("2025-09-01", "2025-09-30", "month"), "注采比")
        text = router.refusal_text(d)
        assert "未登记" in text and "数据截至" not in text

    def test_realtime_answer_carries_caliber_note(self):
        from datetime import datetime
        router = make_router({"monthly_settlement": FreshnessInfo(
            "realtime", datetime(2026, 9, 27, 10, 0), "flink")})
        d = router.route(w("2026-09-27 08:00", "2026-09-27 12:00", "intraday"), "商品量")
        assert "3.2%" in d.note  # 事故 13 口径差异声明（教学值如实标注）
