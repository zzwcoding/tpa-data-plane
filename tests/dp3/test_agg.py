"""DP-04 · dp3 出汇总层：agg 星型三表构建＋主平台库直写（限写账号）测试（hermetic 半边）。

- 模型合同＝teach 99 册 §2.12 末行（D6 star_model 合同值）：dim_well/dim_date/fct_daily_metering，
  grain=一井×一天（A1 问数链 SELECT 唯一落点）——替换 v2 parquet marts 三模型（名/粒度与册不符）；
- PG 实跑半边（表形断言/限写账号负测试/管道对账）在 tests/pg/test_agg_star.py。
"""
from __future__ import annotations

import pytest

from app.data_plane.agg import (_AGG_TABLES, AggStarWriter, build_agg, dim_date_rows)


def well(wid="N-01", **over):
    w = {"well_id": wid, "well_name": "渤中28-1-N1", "well_type": "EW",
         "block_name": "渤中", "field_name": "渤海油田"}
    w.update(over)
    return w


def prod(wid="N-01", date="2025-09-01", **over):
    r = {"well_id": wid, "prod_date": date, "oil_output": 25.0, "liquid_output": 31.2,
         "water_cut": 10.0, "open_days": 24.0, "oil_pressure": 5.0, "plan_output": 24.0,
         "report_shift": "A", "_batch_id": "b-src1", "_snapshot_id": 1}
    r.update(over)
    return r


class TestBuildAgg:
    def test_fct_grain_one_row_per_well_day(self):
        """grain 合同：一行＝一井×一天——A/B 班重复行折叠保首条（质检闸 rule-1 同语义）。"""
        rows = [prod(), prod(report_shift="B", oil_output=99.0),   # 同井同日 A/B 两行
                prod(date="2025-09-02")]
        out = build_agg(rows, [well()])
        fct = out["fct_daily_metering"]
        keys = [(r["well_id"], r["prod_date"]) for r in fct]
        assert len(keys) == len(set(keys)) == 2                    # 唯一且不丢天
        first = next(r for r in fct if r["prod_date"] == "2025-09-01")
        assert first["oil_output"] == 25.0                         # 折叠保首条（A 班）

    def test_fct_columns_match_contract(self):
        """事实列形照 99 册 §2.12：计量三值＋batch_id（批次水位）＋quality_flag。"""
        out = build_agg([prod()], [well()])
        fct = out["fct_daily_metering"][0]
        assert set(fct) == {"well_id", "prod_date", "oil_output", "liquid_output",
                            "oil_pressure", "batch_id", "quality_flag"}
        assert fct["batch_id"] == "b-src1"                         # 批次水位随行携带
        assert fct["quality_flag"] is None                         # 值域无冻结枚举——零发明写 NULL

    def test_dim_well_hierarchy_columns(self):
        """井维度：well_id 键＋区块/油田层级属性列（99 册合同——多行同井折叠保首条）。"""
        wells = [well(), well(), well(wid="N-02", block_name="渤南")]
        out = build_agg([prod()], wells)
        dim = {r["well_id"]: r for r in out["dim_well"]}
        assert set(dim) == {"N-01", "N-02"}
        assert dim["N-01"]["block_name"] == "渤中" and dim["N-01"]["field_name"] == "渤海油田"
        assert set(out["dim_well"][0]) == {"well_id", "block_name", "field_name"}

    def test_dim_date_spans_fct_range_deterministic(self):
        """日期维度：覆盖 fct prod_date 全域（闭区间）；月/季对照列确定性生成（零业务发明）。"""
        rows = [prod(date="2025-09-03"), prod(date="2025-09-01"), prod(date="2025-09-02")]
        a, b = build_agg(rows, [well()]), build_agg(list(reversed(rows)), [well()])
        assert a["dim_date"] == b["dim_date"]                      # 顺序无关=确定性
        dates = [r["date"] for r in a["dim_date"]]
        assert dates == ["2025-09-01", "2025-09-02", "2025-09-03"]
        assert a["dim_date"][0] == {"date": "2025-09-01", "month": "2025-09", "quarter": "2025-Q3"}

    def test_dim_date_year_boundary_quarter(self):
        assert dim_date_rows("2025-12-30", "2026-01-02") == [
            {"date": "2025-12-30", "month": "2025-12", "quarter": "2025-Q4"},
            {"date": "2025-12-31", "month": "2025-12", "quarter": "2025-Q4"},
            {"date": "2026-01-01", "month": "2026-01", "quarter": "2026-Q1"},
            {"date": "2026-01-02", "month": "2026-01", "quarter": "2026-Q1"}]

    def test_empty_input_empty_agg(self):
        out = build_agg([], [])
        assert out == {"dim_well": [], "dim_date": [], "fct_daily_metering": []}

    def test_tables_are_contract_three(self):
        """模型名闭世界：恰 99 册三表（agg schema 限定由 DDL＋限写账号承载），v2 parquet
        marts 三模型不复存在。"""
        assert set(_AGG_TABLES) == {"dim_well", "dim_date", "fct_daily_metering"}
        import app.data_plane.transform as t
        assert not any(hasattr(t, n) for n in ("build_marts", "_MARTS_SQL", "BuildResult"))


class TestAggStarWriter:
    def test_out_of_allowlist_table_rejected(self):
        """写面白名单：合同三表之外（含跨 schema 漂移名）显式拒绝（校验先于连接）。"""
        w = AggStarWriter("postgresql://x:1@localhost:1/x")
        with pytest.raises(Exception, match="不在 agg 写面白名单"):
            w._validate_tables({"dim_evil": [{"x": 1}]})
        with pytest.raises(Exception, match="不在 agg 写面白名单"):
            w._validate_tables({"agg.dim_evil": [{"x": 1}]})       # 跨 schema 漂移名同拒
        assert w._validate_tables(
            {"dim_well": [], "dim_date": [], "fct_daily_metering": []}) is None  # 恰三表放行

    def test_from_env_requires_dsn(self, monkeypatch):
        """缺 AGG_DATABASE_URL＝显式失败（不静默丢直写）。"""
        monkeypatch.delenv("AGG_DATABASE_URL", raising=False)
        with pytest.raises(ValueError, match="AGG_DATABASE_URL"):
            AggStarWriter.from_env()

    def test_from_env_reads_variable(self, monkeypatch):
        monkeypatch.setenv("AGG_DATABASE_URL", "postgresql://u:p@h:5432/d")
        assert AggStarWriter.from_env()._dsn.endswith(":5432/d")
