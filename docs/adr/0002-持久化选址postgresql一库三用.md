# ADR 0002 —— freshness / audit 持久化选址 PostgreSQL（一库三用）

- **日期**：2026-09-28
- **状态**：已采纳（P0 持久化热修，Owner 拍板系列改造第一步；P1 接入层另票）
- **背景**：现网两个硬伤——`ingest.py` 的 `FreshnessRegistry` 与 `audit.py` 的 `InMemoryAudit` 均为内存 dict，进程重启即失：前者致 dp6 路由全线"未登记"拒答（可用性事故），后者致审计四元组丢失、"结论可回放可追责"红线断裂（合规事故）。PRD 裁决 #1 本就规定内存实现=测试替身，真实实现另写。
- **结论**：两个真身落 **PostgreSQL**（`app/data_plane/pg_store.py`：`PgFreshnessRegistry` / `PgAuditStore`，表 `freshness_registry` / `audit_record`），复用云服务器现有 postgres 容器——**一库三用**：①Iceberg JDBC catalog（PRD §10.5 已定）②新鲜度登记 ③审计记录。不引入新中间件、不加进程。
- **理由**：
  - 新鲜度与审计都是"少量行、强一致、随事务提交"的元数据，PG 是现成最重合的解；Kafka/独立存储违背"不引入新中间件"边界。
  - 云上 PG 已在跑且已兼作 catalog（先例即"一库二用"），第三用是同一条路的延伸；PG 实例级故障属既有单机部署（S1 档）风险面，不在本票放大。
- **后果 / 纪律**：
  - 公开签名不变（`register/freshness`、`put/get`），内存版改名 `InMemoryFreshnessRegistry` 并与 `InMemoryAudit` 一并标注为测试替身（PRD 裁决 #1 兑现）；现有测试全部继续用替身跑绿，PG 集成测试走 `TEST_DATABASE_URL`、无配置即 skip。
  - 连接走 `DATABASE_URL`；未配置时装配点（serve/api.py）回落内存替身，本地开发不炸；已配置但未装 psycopg=显式报错不静默降级。
  - **部署注意**：启用 PG 后 freshness 是真话——空库=全线 unknown 拒答（零编数纪律）。上线前需先登记（走 dp2 `register_freshness` 或等价 SQL），不得回填虚构 as_of。
  - 与 Iceberg catalog 同库不同表（`iceberg_tables` 等为 catalog 自管表），互不干扰；如后续要隔离可平移到独立 schema，接口不变。
- **效力**：P1 接入层（任务化进料/认证限流）以本票的持久化真身为前提；`AuditStore` seam（audit.py Protocol）自此为 dp7 唯一存储合同。
- **遗留**：无（部署侧登记动作记入施工票 02）。
