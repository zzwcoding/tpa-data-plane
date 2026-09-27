# spec —— S1 垂直切片：老王问数 tracer bullet

状态: 已定稿 · 2026-09-27 · 触及模块: dp1/dp2/dp3/dp4/dp5/dp6/dp7/dp8/dp9（全部九卡，S1 范围）

## 1. 目标

一条链路打通"问数"全闭环，作为 S1 出口验收（PRD §10.5 决策 5）：

> 输入 fieldforge 合同产物 → dp3 质检 → dp2 入湖（snapshot N）→ 用户问"渤南区块上月产量完成率" → dp4 术语归一+四件套 → dp9 白名单 ✓ → dp6 路由（月度指标×历史窗→答离线面）→ dp5 DuckDB 执行（快照 N）→ dp7 记录四元组 → AnswerBundle 四件齐。

## 2. 范围（S1 实现 / 留位）

| 模块 | S1 实现 | 留位（不实装） |
|---|---|---|
| dp1 | PyIceberg SqlCatalog（JDBC→postgres）+ MinIO；InMemoryLake 测试替身 | rest/federated backend |
| dp2 | run_batch_load + register_freshness/freshness（offline 档） | cdc_pipeline_spec 实装（形状返回） |
| dp3 | 质量闸框架 + 规则 1/2/3/5 对象 + rule_mapping + lineage 最小 | 规则 4/6-12 精化（S2）、13–20 重建（S2） |
| dp4 | StaticYaml 15 指标卡 + resolve_term 四类 + build_query 四件套/消歧 + authoritative_source | Cube |
| dp5 | DuckDB EngineAdapter + sandbox_query + 成本护栏 | trino/starrocks |
| dp6 | route 三查三出口 + refusal_text 话术合同 | S3 跨域 |
| dp7 | record/replay/retention_policy（InMemoryAudit） | S2 对账作业 |
| dp8 | 五工具 in_process 直调 + audit_write_surface 机查 | MCP server |
| dp9 | check_join 白名单 + authorize 行级 + export 降级隐藏 + vending 形状 | 策略引擎 |

## 3. 验收标准（每条对应可执行测试）

| # | 验收 | 对应断言/测试 |
|---|---|---|
| A1 | 进料：脏批（班报重复行）被质检拦截，净批入湖后 freshness=offline、provenance 在案 | DP-W-D2-1；tests/dp3+dp2 |
| A2 | 湖表：schema 四件齐；两次 append 后旧快照可读（隔离）；谓词+列裁剪 | DP-W-D1-1/2/3 |
| A3 | 语义：metric_catalog 15 条；"老完成率"block；"剔检产量"rewrite；"有效产量"无修饰词→消歧询问 | DP-W-D4-1/2/3/4 |
| A4 | 治理：oil_production_daily⋈fluid_test 直连拒；渤中用户查渤南井拒 | DP-W-D9-1/2 |
| A5 | 路由：月度指标×"今天上午"拒答且话术含"数据截至"；历史窗答离线面 | DP-W-D6-1/2 + 分支 C |
| A6 | 执行：snapshot_id 透传；超成本护栏拒绝/降级显式声明 | DP-W-D5-3/4 |
| A7 | 审计：record 四元组齐；同快照 replay match=True；快照过期显式报错 | DP-W-D7-1/2/3 |
| A8 | 工具面：五工具 JSON 合同 + 只读红线机查 write_surface=0 + query 四道闸任一拒即整体拒 | DP-W-D8-1/2/3 |
| A9 | **端到端**：老王问数 tracer bullet 全链路一条测试走通（真实 fieldforge 产物进料 → query → AnswerBundle） | 本 spec §1 |

## 4. 供给依赖

- fieldforge 合同产物（云端 run 或本地重生成）：oil_production_daily/well_info/monthly_settlement/injection_connection/fluid_test/equip_status CSV+manifest
- 测试夹具：干净配方（过样本）+ 脏配方（挂样本，本地重生成）
