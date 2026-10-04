"""S2-02 · dp3 marts 三张示范模型测试：build_marts 物化 + lineage。

数据源：tests/fixtures/fieldforge 仓内合成最小集（DP-01 fixture 化，形状同 fieldforge
合同产物；不再依赖 ~/Downloads/fieldforge 本机外部路径，CI hermetic）。
DP-02（裁决 15 归位）：原 dp4 对齐闸三测（test_dp4_alignment_gate＋TestAlignGate）随
dp4 语义层删除（对齐闸属 dp4，随语义职责归主平台；git 历史备查）。
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

    def test_manifest_ref_readable(self, built):
        import json
        result, out = built
        manifest = json.loads(Path(result.manifest_ref).read_text(encoding="utf-8"))
        assert "nodes" in manifest and "parent_map" in manifest
