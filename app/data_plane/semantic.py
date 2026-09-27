"""dp4 语义层（S1=StaticYaml）：15 指标卡机器可读口径 + 术语归一 + 四件套编译 + 权威源仲裁。

- 口径唯一暴露面：指标定义只来自本字典与 dp3 manifest（边界规则第 4 行），禁发明；
- 修饰词消歧在本层主干判断（DP4：任何上游 Agent 得到同一个询问）；
- StructuredQuery 含显式槽位 base_table/limit（裁决 #10）；TimeWindow={start,end,granularity}（裁决 #11）。
"""
from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from pathlib import Path

import yaml

DEFAULT_DICT = "semantic_dictionary.yaml"


@dataclass(frozen=True)
class MetricDef:
    name: str
    version: str          # 裁决 #7
    definition: str
    formula: str
    unit: str
    granularity: str
    modifiers: tuple
    owner: str
    source: str           # 权威源（湖表）
    sql_table: str
    additive: bool
    agg: dict
    measures: tuple
    ai_context: str


@dataclass(frozen=True)
class TermResolution:
    canonical: str
    action: str  # pass | rewrite | block | clarify
    note: str


@dataclass(frozen=True)
class SourceRef:
    source: str
    source_system: str
    note: str


@dataclass(frozen=True)
class TimeWindow:
    """裁决 #11：{start, end, granularity: day|month|intraday}。"""
    start: str
    end: str
    granularity: str  # day | month | intraday


@dataclass(frozen=True)
class StructuredQuery:
    """四件套 + 显式槽位（裁决 #10：base_table/limit）。dp 卡钦定本类型定义在 dp4。"""
    metric: str
    base_table: str
    tables: tuple
    measures: tuple
    dims: tuple
    time_dimensions: tuple
    filters: dict
    limit: int | None = None


@dataclass(frozen=True)
class TimeWindow:
    start: str
    end: str
    granularity: str  # day | month | intraday


class StaticYaml:
    """S1 语义字典后端：YAML 加载（指标/术语/权威源）。"""

    def __init__(self, data: dict):
        self.data = data

    @classmethod
    def default(cls) -> "StaticYaml":
        text = resources.files("app").joinpath("data_plane/semantic_dictionary.yaml").read_text(encoding="utf-8")
        return cls(yaml.safe_load(text))

    @classmethod
    def from_path(cls, path: str | Path) -> "StaticYaml":
        return cls(yaml.safe_load(Path(path).read_text(encoding="utf-8")))

    def metrics(self) -> list[dict]:
        return self.data.get("metrics", [])

    def terms(self) -> list[dict]:
        return self.data.get("terms", [])

    def authoritative(self) -> dict:
        return self.data.get("authoritative", {})


class Semantic:
    def __init__(self, backend: StaticYaml):
        self.backend = backend
        self._by_name = {m["name"]: m for m in backend.metrics()}

    # ---- 指标目录 -----------------------------------------------------------

    def metric_catalog(self) -> list[MetricDef]:
        return [self._to_def(m) for m in self.backend.metrics()]

    def metric_def(self, name: str) -> MetricDef | None:
        m = self._by_name.get(name)
        return self._to_def(m) if m else None

    @staticmethod
    def _to_def(m: dict) -> MetricDef:
        return MetricDef(
            name=m["name"], version=m["version"], definition=m["definition"],
            formula=m["formula"], unit=m["unit"], granularity=m["granularity"],
            modifiers=tuple(m.get("modifiers", [])), owner=m["owner"],
            source=m.get("source", ""), sql_table=m["sql_table"],
            additive=bool(m.get("additive")), agg=m.get("agg", {}),
            measures=tuple(m.get("measures", ())), ai_context=m.get("ai_context", ""))

    # ---- 术语归一（问数入口第一闸） ----------------------------------------

    def resolve_term(self, term: str) -> TermResolution:
        for t in self.backend.terms():
            if t["term"] == term:
                return TermResolution(canonical=t["canonical"], action=t["action"], note=t["note"])
        return TermResolution(canonical=term, action="pass", note="")

    # ---- 四件套编译 ---------------------------------------------------------

    def build_query(self, metric: str, dims: list[str], time_window: dict, filters: dict):
        """返回 {'kind':'clarify', ...} 或 {'kind':'executable','query':StructuredQuery}。"""
        card = self._by_name.get(metric)
        if card is None:
            raise KeyError(f"指标未在语义层注册：{metric}")
        measures = tuple(card.get("measures", ()))
        if len(measures) > 1:  # 一词多 measure（裁决：消歧在本层，DP-W-D4-2）
            modifier = filters.get("口径")
            if modifier not in card.get("modifiers", []):
                return {"kind": "clarify", "metric": metric,
                        "options": list(card.get("modifiers", [])),
                        "note": card.get("ai_context", "")}
            chosen = self._measure_for(card, modifier)
        else:
            chosen = card["name"]
        tables = {card["sql_table"]}
        for d in dims:
            tables.add(card["sql_table"])  # S1：dims 均落基表列
        return {"kind": "executable",
                "query": StructuredQuery(
                    metric=metric, base_table=card["sql_table"], tables=tuple(sorted(tables)),
                    measures=(chosen,), dims=tuple((d, card["sql_table"]) for d in dims),
                    time_dimensions=({"dateRange": [time_window["start"], time_window["end"]],
                                      "granularity": time_window["granularity"]},),
                    filters=dict(filters), limit=None)}

    @staticmethod
    def _measure_for(card: dict, modifier: str) -> str:
        idx = card.get("modifiers", []).index(modifier)
        return card["measures"][idx]

    # ---- 权威源仲裁（一数四源，事故 13） ------------------------------------

    def authoritative_source(self, metric: str) -> SourceRef:
        a = self.backend.authoritative().get(metric)
        if a:
            return SourceRef(source=a["source"], source_system=a["source_system"], note=a["note"])
        card = self._by_name.get(metric)
        if card:
            return SourceRef(source=card["source"], source_system=card["owner"], note="")
        raise KeyError(f"指标未注册：{metric}")
