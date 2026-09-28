# P0 持久化热修 —— freshness / audit 落 PostgreSQL

Status: done（2026-09-28 收口）

## 背景

现网（8.163.33.135/dp/）诊断出两个硬伤：

1. `app/data_plane/ingest.py` 的 `FreshnessRegistry` 是内存 dict——进程重启所有表新鲜度蒸发，dp6 路由全线"未登记"拒答（可用性事故）。
2. `app/data_plane/audit.py` 的 `InMemoryAudit` 是内存 dict——重启丢审计四元组，"结论可回放可追责"红线断裂（合规事故）。

PRD 裁决 #1 本来就规定：内存实现=测试替身，真实实现另写。本票就是把这两个真身写出来。

## 范围

**做**：
- 新增 PG 持久化实现（`app/data_plane/pg_store.py`）：`freshness_registry` 表 + `audit_record` 表，落 PostgreSQL（云上 PG 兼作 Iceberg JDBC catalog，一库三用）。
- 公开签名不变：`register/freshness`（dp2）、`put/get`（dp7）与内存版形状一致，构造注入。
- 内存版降级为测试替身（改名/标注意图），全部现有测试继续用它们跑绿。
- 连接配置走 `DATABASE_URL` 环境变量；未配置时装配点回落内存替身（本地开发不炸）。
- PG 集成测试（`tests/pg/`）：无 `TEST_DATABASE_URL` 时 skip，CI/本地 pytest 不变红。

**不做**（P1 另票）：
- 不改任何 HTTP/API 接口形状、不加端点、不加认证限流。
- 不动 dp 模块质检/路由/语义/治理逻辑。
- 不引入 Kafka 等新中间件。
- 不动 teach-petro-agent 仓（只读蓝本）。

## 验收清单

- [x源 PRD 裁决 #1] 内存实现降级为测试替身（标注意图），PG 真身另写、签名一致。
- [x] `pg_store.PgFreshnessRegistry`：register/freshness 落 PG；同表多管道取 as_of 最新；未登记=unknown；重启（新实例）不丢。
- [x] `pg_store.PgAuditStore`：put/get 四元组落 PG（query 结构化序列化/还原）；未知 ref=KeyError 显式失败；重启不丢。
- [x] 装配点（serve/api.py）：有 DATABASE_URL 用 PG 真身，无则回落内存替身——本地无 PG 可启动。
- [x] 现有 98 测试全绿（现有测试零改动——FreshnessRegistry 兼容别名承接）。
- [x] 新增 PG 集成测试：无 TEST_DATABASE_URL / 未装 psycopg 时 skip；已上云真机验证 6 passed（见票 01）。
- [x] 选址裁决落 ADR 0002（一库三用）。
- [x] 部署注意写清：启用 PG 后 freshness 需先登记，空库=全线 unknown 拒答（零编数纪律下的真实状态）。

## Comments

- 2026-09-28 拆 01/02 两张实施票。
- 2026-09-28 收口：PG 真身云上真机验证 6 passed（验后云库恢复原状）；本地 98 passed + 1 skipped（PG 件 skip）；check_specs PASS；ADR 0002 立稿。
