"""S2-02 · dp3 marts 三张示范模型测试：build_marts 物化 + lineage + dp4 对齐闸。"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.data_plane.transform import build_marts, lineage

RUN = Path.home() / "Downloads" / "fieldforge" / "out-tpa"
NEEDED = ("oil_production_daily", "well_info", "well_overhaul", "well_measures",
          "injection_connection", "monthly_settlement")

pytestmark = pytest.mark.skipif(
    not all((RUN / f"{t}.csv").is_file() for t in NEEDED),
    reason="fieldforge 合同产物不在本机（CI 跳过）")


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp("marts")
    result = build_marts(run_dir=RUN, out_dir=out)
    return result, out


class TestBuildMarts:
    def test_three_models_built(self, built):
        """裁决 #14：三张示范模型落盘。"""
        result, out = built
        assert set(result.models_built) == {
            "fct_block_monthly", "fct_valid_output_monthly", "fct_wellset_annual"}
        assert result.tests_passed is True
        for m in result.models_built:
            assert (out / f"{m}.parquet").is_file()

    def test_block_monthly_shape(self, built):
        import pyarrow.parquet as pq
        _, out = built
        t = pq.read_table(out / "fct_block_monthly.parquet")
        cols = set(t.column_names)
        assert {"block_name", "month", "oil_output_sum", "plan_output_sum",
                "completion_rate"} <= cols
        assert t.num_rows > 0

    def test_valid_output_two_measures(self, built):
        import pyarrow.parquet as pq
        _, out = built
        t = pq.read_table(out / "fct_valid_output_monthly.parquet")
        assert {"valid_output_calendar", "valid_output_overhaul_excluded"} <= set(t.column_names)

    def test_lineage_three_chains(self, built):
        """lineage 可查三链（DP-W-D3-3 同构）。"""
        result, _ = built
        ln = lineage("fct_block_monthly", manifest_ref=result.manifest_ref)
        ups = " ".join(ln.upstreams)
        assert "oil_production_daily" in ups and "well_info" in ups  # dbt 形状=source.petro.<表>

    def test_dp4_alignment_gate(self, built):
        """dp4 对齐闸：语义字典 sql_table 指向 marts 模型时，模型必须在 manifest。"""
        from app.data_plane.semantic import Semantic, StaticYaml
        sem = Semantic(StaticYaml.default())
        result, _ = built
        models = set(result.models_built)
        # 字典中 marts 专属指标（裁决映射后）应可对齐；缺失模型=登记失败不放行
        missing = [m.name for m in sem.metric_catalog()
                   if m.sql_table.startswith("fct_") and m.sql_table not in models]
        assert missing == []  # 当前字典指标都挂基表/marts 已建模型

    def test_manifest_ref_readable(self, built):
        import json
        result, out = built
        manifest = json.loads(Path(result.manifest_ref).read_text(encoding="utf-8"))
        assert "nodes" in manifest and "parent_map" in manifest
