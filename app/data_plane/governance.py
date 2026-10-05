"""dp9 治理控制面（S1 切片）：Join 白名单默认拒绝 + 行级作业区隔离 + 导出降级。

- StaticPolicy：白名单/行级/用户表从 YAML 或 dict 加载（S1 形态）；
- check_join：边集=StructuredQuery.tables 两两成对（measures→基表、dims→维表可枚举），
  白名单外=拒（默认拒绝语义），✗ 边拒并给修复指引；
- authorize：闸只持规则不持数据——requested_scope 由调用方从查询 filters 提取传入；
- S1 导出整体降级为不可用（显式隐藏语义，DP-W-D9-3 属 S2+）；
- vend_credentials：S3 形状留位，零实装。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str = ""
    violated_edges: tuple = ()


@dataclass(frozen=True)
class UserContext:
    user_id: str
    org: str
    role: str = "engineer"


# StructuredQuery 类型原定义在 dp4（semantic.py，已随 ADR-0003 裁决 15 归位删除——
# DP-02）；现逐字内联落位 audit.py，本模块只消费其形状（.base_table/.tables/.metric），
# 不 import audit——依赖图 dp9 依赖=无。（DP-06 注记更新：原「定义在 dp4」已陈旧，
# DP-02 审查 F5 既有项。）


@dataclass(frozen=True)
class CredentialRef:
    token: str
    expires_at: str


class StaticPolicy:
    """S1 策略后端：dict/YAML 加载，只持规则不持数据。"""

    def __init__(self, policy: dict):
        self.join_whitelist = policy.get("join_whitelist", [])
        self.row_scope = policy.get("row_scope", {})
        self.users = {u["user_id"]: u for u in policy.get("users", [])}

    @classmethod
    def from_yaml(cls, path: str) -> "StaticPolicy":
        import yaml
        return cls(yaml.safe_load(open(path, encoding="utf-8")))

    def user(self, user_id: str) -> dict | None:
        return self.users.get(user_id)


class Governance:
    def __init__(self, policy: StaticPolicy):
        self.policy = policy

    # ---- Join 白名单（默认拒绝） ------------------------------------------

    def check_join(self, query: StructuredQuery) -> Decision:
        tables = [t for t in query.tables if t != query.base_table]
        edges = {(query.base_table, t) for t in tables}
        violated = []
        for left, right in sorted(edges):
            hit = self._lookup_edge(left, right)
            if hit is None:
                violated.append({"edge": f"{left}⋈{right}", "note": "不在白名单：走治理链审批新增白名单"})
            elif hit.get("status") == "blocked":
                violated.append({"edge": f"{left}⋈{right}", "note": hit.get("note", "")})
        return Decision(allowed=not violated, violated_edges=tuple(violated),
                        reason="" if not violated else "Join 白名单默认拒绝")

    def _lookup_edge(self, a: str, b: str) -> dict | None:
        for e in self.policy.join_whitelist:
            pair = {e.get("left"), e.get("right")}
            if a in pair and b in pair:
                return e
        return None

    # ---- 行级权限（作业区隔离） -------------------------------------------

    def authorize(self, ctx: UserContext, table: str, action: str,
                  requested_scope: str | None = None) -> Decision:
        """fail-closed：无身份 / 用户未知 / 用户无 org / scope 无法确立 → 一律拒绝。"""
        if ctx is None:
            return Decision(allowed=False, reason="无调用身份（fail-closed）")
        u = self.policy.user(ctx.user_id)
        if u is None:
            return Decision(allowed=False, reason=f"未知用户 {ctx.user_id}")
        if action == "export":
            return Decision(allowed=False, reason="S1 导出整体降级为不可用（DP-W-D9-3 属 S2+ 审批位）")
        user_org = u.get("org")
        if not user_org:
            return Decision(allowed=False, reason="用户档案缺归属作业区（fail-closed）")
        if ctx.role == "admin":
            return Decision(allowed=True)  # 管理员不受作业区清单/行级约束
        scope = requested_scope or user_org
        if scope not in self.policy.row_scope.get("orgs", []):
            return Decision(allowed=False, reason=f"scope {scope} 不在合法作业区清单")
        if scope != user_org:
            return Decision(allowed=False,
                            reason=f"行级隔离：{user_org} 作业区用户不可读 {scope} 井数据")
        return Decision(allowed=True)

    # ---- S3 形状留位 -------------------------------------------------------

    def vend_credentials(self, table: str, ttl: int) -> CredentialRef:
        raise NotImplementedError("S3 形状留位：credential vending 需 catalog 选型支持（零实装）")
