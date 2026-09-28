# 02 · 装配接线 + ADR 0002
Status: done | Touches modules: serve, deploy, docs | Blocks: 无（终点）

验收（源 spec）:
- [x] serve/api.py `build_tools()`：有 DATABASE_URL → `PgAuditStore`/`PgFreshnessRegistry`；无 → 原内存替身（本地开发不炸）。API 形状零改动。
- [x] ADR 0002（选址 PostgreSQL 一库三用）落稿 docs/adr/0002-持久化选址postgresql一库三用.md。
- [x] 全量 pytest 全绿（98 passed, 1 skipped）+ tools/check_specs.py PASS。
- [x] deploy/api-requirements.txt 补 `psycopg[binary]>=3.1`（PG 装配的镜像依赖）。

## Answer

部署注意（下次上云启用 PG 时）：
1. api 容器加 `DATABASE_URL=postgresql://postgres:<密码>@postgres:5432/fieldforge` 后重建镜像（已含 psycopg[binary]）。
2. **启用后 freshness 是真话：空库=全线 unknown 拒答（零编数纪律）**。上线前先登记新鲜度
   （走 dp2 `register_freshness` 或等价 SQL INSERT 到 freshness_registry），不得回填虚构 as_of。
3. audit_record 从此跨重启/跨进程回放可用；`AuditStore` Protocol（audit.py）为 dp7 唯一存储合同。
