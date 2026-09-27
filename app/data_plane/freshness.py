"""dp6 新鲜度路由与拒答（S1 切片）：三查三出口 + 话术合同。

- 路由是确定性代码，无 LLM（DP6）；Seam=Clock（FakeClock 冻结测试）；
- 拒答话术合同（DP-W-D6-2）：元素白名单=截至时点/未覆盖窗口/原因/改答路径；
  零编数纪律：未登记新鲜度的拒答连"数据截至"都不填；
- 分支主干对齐 DP6 底稿：A=月指标×日内×非可加×offline 拒 / B=日内×realtime×可加 答实时面
  （附事故 13 口径差异声明）/ B'=日内×offline defer / C=已闭合历史窗 答离线面 / 兜底 uncovered。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from app.data_plane.semantic import TimeWindow  # noqa: F401  (dp6 依赖 dp4：粒度)


@dataclass(frozen=True)
class FreshnessInfo:
    tier: str  # realtime | nearline | offline | unknown
    as_of: datetime | date | None
    pipeline: str | None


@dataclass(frozen=True)
class Refusal:
    kind: str  # granularity | defer | unregistered | future | uncovered
    reason: str
    hint: str = ""
    as_of: str = ""
    metric: str = ""


@dataclass(frozen=True)
class RouteDecision:
    answerable: bool
    source_tier: str | None = None
    declared_as_of: object | None = None
    refusal: Refusal | None = None
    note: str = ""


CALIBER_NOTE_REALTIME = (
    "以上为实时面加和的临时值，数据截至 {as_of}。"
    "提示：实时加和 ≠ 统计上报——差异原因=统计口径/计量误差/损耗/测算方式/精度累计/统计周期"
    "（A11 实时加和 vs E7 统计上报差 3.2%，事故 13 教学值）；正式月度口径以离线面月结为准。"
)


def _as_date(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v)
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        return date.fromisoformat(s)


def _win_get(window, key):
    return window[key] if isinstance(window, dict) else getattr(window, key)


class FreshnessRouter:
    def __init__(self, *, semantic, freshness, clock):
        self.semantic = semantic   # dp4（查一：指标注册 + 粒度 + 可加性）
        self.freshness = freshness  # dp2 FreshnessRegistry seam（查二）
        self.clock = clock          # Clock seam（今天）

    # ---- 三查 --------------------------------------------------------------

    def _registered(self, metric):
        md = self.semantic.metric_def(metric)
        if md is None:
            return None, Refusal("unregistered", f"指标未在语义层注册：{metric}")
        return md, None

    def _freshness(self, md):
        info = self.freshness.freshness(md.sql_table)
        if info.tier == "unknown":
            return None, Refusal("unregistered",
                                 f"表 {md.sql_table} 未登记新鲜度（未进料），暂无可答数据")
        return info, None

    def _future(self, window) -> Refusal | None:
        """未来窗=起点在今天之后（整窗未发生）；跨今天的窗属 uncovered（兜底分支）。"""
        today = self.clock.today()
        start = window["start"] if isinstance(window, dict) else window.start
        if _as_date(start) > today:
            return Refusal("future", "预测类问题不在数据面处理（不编数）")
        return None

    # ---- 主干：三查三出口 --------------------------------------------------

    def route(self, window, metric: str) -> RouteDecision:
        md, refuse = self._registered(metric)
        if refuse:
            return RouteDecision(answerable=False, refusal=refuse)
        info, refuse = self._freshness(md)
        if refuse:
            return RouteDecision(answerable=False, refusal=refuse)
        future = self._future(window)
        if future:
            return RouteDecision(answerable=False, refusal=future)

        wg = _win_get(window, "granularity")
        as_of = info.as_of

        # 分支 A：月指标 × 日内窗 × 非可加 × offline —— 任何时候都答不了官方口径
        if md.granularity == "月" and wg == "intraday" and not md.additive and info.tier == "offline":
            return self._refuse(md, info, "granularity",
                                f"该指标官方口径为{md.granularity}粒度，无法按日内粒度作答")
        # 分支 B：日内窗 × realtime/nearline × 可加 —— 答实时/近线面临时值
        if wg == "intraday" and info.tier in ("realtime", "nearline") and md.additive:
            return RouteDecision(answerable=True, source_tier=info.tier, declared_as_of=as_of,
                                 note=CALIBER_NOTE_REALTIME.format(as_of=as_of))
        # 分支 B'：日内窗 × offline —— defer（离线面 T+1）
        if wg == "intraday":
            return self._refuse(md, info, "defer", hint="离线面 T+1，明日此问可答")
        # 分支 C：已闭合历史窗 —— 答离线面（历史月结以批层覆盖后为准）
        if _as_date(_win_get(window, "end")) <= _as_date(as_of):
            return RouteDecision(answerable=True, source_tier=info.tier, declared_as_of=as_of)
        # 兜底：未闭合窗
        return self._refuse(md, info, "uncovered")

    def _refuse(self, md, info, kind: str, hint: str = "") -> RouteDecision:
        as_of = str(info.as_of) if info.as_of else ""
        reason = {"granularity": "月度官方口径与所问粒度不符",
                  "defer": "离线面 T+1 批次尚未覆盖所问时段",
                  "uncovered": "所问窗口晚于数据截至时点"}.get(kind, kind)
        return RouteDecision(answerable=False,
                             refusal=Refusal(kind=kind, reason=reason, hint=hint,
                                             as_of=as_of, metric=md.name))

    # ---- 拒答话术合同（DP-W-D6-2） ----------------------------------------

    def refusal_text(self, decision: RouteDecision) -> str:
        r = decision.refusal
        if r is None:
            return ""
        if r.kind == "unregistered":
            # 零编数纪律：连"数据截至"都不填
            return r.reason
        as_of = getattr(r, "as_of", "") or ""
        parts = [f"数据截至 {as_of}，未覆盖您问的窗口。"]
        if r.kind == "granularity":
            parts.append(r.reason + "——任何时候都答不了官方口径，您可改问月粒度。")
        elif r.kind == "defer":
            parts.append(r.hint)
        elif r.kind == "future":
            parts.append("预测类问题不在数据面处理。")
        elif r.kind == "uncovered":
            parts.append("您问的窗口晚于数据截至时点，请调整查询区间。")
        return "".join(parts)
