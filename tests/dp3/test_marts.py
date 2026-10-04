"""S2-02 · dp3 marts 三张示范模型测试：build_marts 物化 + lineage + dp4 对齐闸。

数据源：tests/fixtures/fieldforge 仓内合成最小集（DP-01 fixture 化，形状同 fieldforge
合同产物；不再依赖 ~/Downloads/fieldforge 本机外部路径，CI hermetic）。
"""
from __future__ import annotations

from pathlib import Path

import json

import pytest

from app.data_plane.transform import build_marts, lineage

RUN = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"


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


class TestAlignGate:
    def test_align_passes_with_built_marts(self, built):
        """RF2-04：对齐闸落生产代码——有效产量 sql_table=fct_valid_output_monthly。"""
        from app.data_plane.semantic import Semantic, StaticYaml
        result, _ = built
        manifest = json.loads(Path(result.manifest_ref).read_text(encoding="utf-8"))
        sem = Semantic(StaticYaml.default())
        m = {c.name: c for c in sem.metric_catalog()}
        assert m["有效产量"].sql_table == "fct_valid_output_monthly"
        assert sem.align_with_manifest(manifest) == []

    def test_align_gate_rejects_missing_model(self, built):
        """负样本：登记缺失模型 → 对齐闸返回缺失项（不放行）。"""
        import json
        from app.data_plane.semantic import Semantic, StaticYaml
        result, _ = built
        manifest = json.loads(Path(result.manifest_ref).read_text(encoding="utf-8"))
        missing_manifest = {"nodes": {k: v for k, v in manifest["nodes"].items()
                                      if k != "fct_valid_output_monthly"}}
        sem = Semantic(StaticYaml.default())
        missing = sem.align_with_manifest(missing_manifest)
        assert any("fct_valid_output_monthly" in x for x in missing)
