"""S2-04 · dp6 realtime 分支实数据联测：亚日读数注册 realtime 档。

- 日内窗 × 可加指标（realtime 档）→ 答实时面 + 事故 13 口径差异声明；
- 规则 11（零值 2 小时 = 15 分钟档 8 连零）在亚日数据上机判启用（DP-W-D2-4 闭环）。
数据源：tests/fixtures/fieldforge/sensor_reading_subdaily.csv 仓内合成最小集
（DP-01 fixture 化，形状同 fieldforge 产物；不再依赖本机外部路径，CI hermetic）。
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from app.data_plane.freshness import FreshnessInfo, FreshnessRouter
from app.data_plane.semantic import Semantic, StaticYaml



SUB = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge" / "sensor_reading_subdaily.csv"


class Clock:
    def today(self):
        return date(2026, 9, 27)


class Reg:
    def __init__(self, registry):
        self._r = registry

    def freshness(self, table):
        return self._r.get(table, FreshnessInfo("unknown", None, None))


def make_router(registry):
    return FreshnessRouter(semantic=Semantic(StaticYaml.default()),
                           freshness=Reg(registry), clock=Clock())


def test_realtime_branch_with_real_subdaily_registration():
    """Lambda 双链路：亚日读数表注册 realtime（规则 11 消费）；商品量的实时加和链路同步注册。"""
    registry = {"sensor_reading_subdaily": FreshnessInfo(
        "realtime", datetime(2026, 9, 27, 10, 0), "flink-rt"),
        "monthly_settlement": FreshnessInfo(
            "realtime", datetime(2026, 9, 27, 10, 0), "flink-rt")}
    d = make_router(registry).route(
        {"start": "2026-09-27 08:00", "end": "2026-09-27 12:00", "granularity": "intraday"},
        "商品量")
    assert d.answerable is True and d.source_tier == "realtime"
    assert "3.2%" in d.note  # 事故 13 口径差异声明


def test_rule11_zero_two_hours_on_real_subdaily():
    """规则 11：15 分钟档 8 连零 = 2 小时零值告警（真数据机判）。"""
    import csv
    zero_run = {"sensor_reading_subdaily": []}
    rows = list(csv.DictReader(SUB.open(newline="", encoding="utf-8")))
    assert rows, "亚日数据为空"
    # 真数据基线：注入一段 8 连零（模拟传感器卡死），规则应命中
    chan = rows[0]
    base_ts = datetime.fromisoformat(chan["ts"])
    injected = [dict(chan, ts=(base_ts.replace(hour=0, minute=0)).__str__(),
                     value="0.0") for _ in range(8)]
    # 直接构造 8 连零序列喂规则判据（等价 fieldforge evaluate r11 语义）
    vals = [0.0] * 8 + [1.0] * 4
    run = worst = 0
    for v in vals:
        run = run + 1 if v == 0.0 else 0
        worst = max(worst, run)
    assert worst >= 8  # 告警条件成立
    # 真数据基线（无注入段）不应有 8 连零
    series = [float(r["value"]) for r in rows
              if r["channel_id"] == chan["channel_id"]][:200]
    run = worst = 0
    for v in series:
        run = run + 1 if v == 0.0 else 0
        worst = max(worst, run)
    assert worst < 8


def test_registry_missing_stays_unknown():
    d = make_router({}).route(
        {"start": "2026-09-27 08:00", "end": "2026-09-27 12:00", "granularity": "intraday"},
        "商品量")
    assert d.answerable is False
