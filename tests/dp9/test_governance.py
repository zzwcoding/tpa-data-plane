"""票 02 · dp9 治理闸测试：Join 白名单默认拒绝 + 行级作业区隔离。"""
from __future__ import annotations

import pytest

from app.data_plane.governance import Governance, StaticPolicy, UserContext
from app.data_plane.audit import StructuredQuery  # 原 dp4 定义，裁决 15 归位后落位 audit.py（DP-02）

POLICY = {
    "join_whitelist": [
        {"left": "oil_production_daily", "right": "well_info", "status": "allowed", "note": "井号"},
        {"left": "oil_production_daily", "right": "date_dim", "status": "allowed", "note": "日期维"},
        {"left": "oil_production_daily", "right": "fluid_test", "status": "blocked",
         "note": "须井号+取样日期双键+时间容差，经 intermediate"},
        {"left": "oil_production_daily", "right": "water_injection_daily", "status": "blocked",
         "note": "须经受效关系实体"},
    ],
    "row_scope": {"column": "block_name", "orgs": ["渤南", "渤中", "渤西"]},
    "users": [
        {"user_id": "laowang", "org": "渤中", "role": "engineer"},
        {"user_id": "boss", "org": "总部", "role": "admin"},
    ],
}


def q(base="oil_production_daily", tables=("oil_production_daily", "well_info"), **kw):
    return StructuredQuery(metric="产量完成率", base_table=base, tables=tuple(tables),
                           measures=("oil_output",), dims=(("block_name", "well_info"),),
                           time_dimensions=(), filters={}, **kw)


@pytest.fixture()
def gov():
    return Governance(StaticPolicy(POLICY))


class TestCheckJoin:
    def test_whitelisted_edge_allowed(self, gov):
        assert gov.check_join(q()).allowed is True

    def test_blocked_edge_rejected_with_note(self, gov):
        d = gov.check_join(q(tables=("oil_production_daily", "fluid_test")))
        assert d.allowed is False
        assert any("intermediate" in e.get("note", "") for e in d.violated_edges)

    def test_unknown_edge_rejected_by_default(self, gov):
        d = gov.check_join(q(tables=("oil_production_daily", "equip_status")))
        assert d.allowed is False  # 默认拒绝


class TestAuthorize:
    def test_row_level_isolation(self, gov):
        ctx = UserContext("laowang", org="渤中", role="engineer")
        d = gov.authorize(ctx, "oil_production_daily", "read", requested_scope="渤南")
        assert d.allowed is False  # 渤中用户查渤南井 → 拒（DP-W-D9-2）

    def test_same_org_read_allowed(self, gov):
        ctx = UserContext("laowang", org="渤中", role="engineer")
        assert gov.authorize(ctx, "oil_production_daily", "read", requested_scope="渤中").allowed

    def test_admin_bypasses_scope(self, gov):
        ctx = UserContext("boss", org="总部", role="admin")
        assert gov.authorize(ctx, "oil_production_daily", "read", requested_scope="渤南").allowed

    def test_export_downgraded_in_s1(self, gov):
        ctx = UserContext("laowang", org="渤中", role="engineer")
        d = gov.authorize(ctx, "oil_production_daily", "export")
        assert d.allowed is False and "S1" in d.reason

    def test_fail_closed_on_missing_org(self, gov):
        """RF-01：用户档案缺 org 且未传 scope → 显式拒绝（不静默放行）。"""
        gov.policy.users["noorg"] = {"user_id": "noorg", "org": "", "role": "engineer"}
        ctx = UserContext("noorg", org="", role="engineer")
        d = gov.authorize(ctx, "oil_production_daily", "read")
        assert d.allowed is False and "fail-closed" in d.reason

    def test_unknown_user_rejected(self, gov):
        ctx = UserContext("intruder", org="渤中", role="engineer")
        assert gov.authorize(ctx, "oil_production_daily", "read").allowed is False
