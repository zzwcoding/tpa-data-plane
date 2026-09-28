# 01 · PG 真身：PgFreshnessRegistry / PgAuditStore
Status: done | Touches modules: dp2, dp7 | Blocks: 02

验收（源 spec）:
- [x] `app/data_plane/pg_store.py`：`PgFreshnessRegistry.register/freshness` 与内存替身签名一致；同表多管道取 as_of 最新（ON CONFLICT WHERE 守卫）；未登记=unknown。
- [x] `PgAuditStore.put/get` 与 `InMemoryAudit` 签名一致；query=`StructuredQuery` 双向序列化（JSONB，tuple 还原）；未知 ref=KeyError；同 question_id 覆盖写。
- [x] 建表幂等（CREATE TABLE IF NOT EXISTS，首次连接自动 ensure_schema）。
- [x] `DATABASE_URL` 未设但显式构造 PG 真身 = 显式报错（不静默回落）。
- [x] ingest.py：`FreshnessRegistry` 改名 `InMemoryFreshnessRegistry` + 兼容别名，标注测试替身意图（PRD 裁决 #1）。
- [x] audit.py：`AuditStore` seam Protocol + `InMemoryAudit` 标注测试替身意图。
- [x] 集成测试 `tests/pg/test_pg_persistence.py`：无 `TEST_DATABASE_URL` 或未装 psycopg 整文件 skip（本地 98 passed + 1 skipped）。

## Answer

真机验证（2026-09-28，云服务器 8.163.33.135，api 容器内临时装 psycopg[binary]+pytest，
TEST_DATABASE_URL 指向 deploy-postgres-1/fieldforge 库）：**6 passed**（round-trip 重启存续 /
as_of 最新优先 / tuple 还原 / 覆盖写 / KeyError / env 合同）。验后已 DROP 两表，云库恢复原状，
线上服务 healthz 200 未受影响。
