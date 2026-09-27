# PRD —— tpa-data-plane（Petro-Agent 数据面真实代码）

状态: 逼问中

> **版本** v0.1 · **日期** 2026-09-27
> **蓝本**：teach-petro-agent 教学仓（dp 卡 292 行已定稿 / 35 条 W-DP 断言 / 20 条质量规则 / 15 条指标卡 / DP1–DP9 九份底稿）。抽取素材报告见会话归档（A–F 六节，F 节=24 项教学演绎差距）。
> **供给方**：fieldforge（已完成，云端 8.163.33.135 可产合同全量数据）
> **流程**：sdd-flow（教学环节裁剪，本地施工票）

---

## 0. 一句话定位

把 Petro-Agent 数据面从"教学伪代码"变成**真实可运行的数据面**：湖底座 / 进料与新鲜度 / 质量闸 / 指标语义 / 路由拒答 / 快照审计 / 五只读工具 / 治理闸——按 S1 档先落地垂直切片，S2/S3 只留接口位。

## 1. 背景

- 教学仓已完成全套设计（dp 卡零代码红线，292 行模块卡定稿，35 条 W-DP 断言"已兑现"但兑现物=伪代码）。
- fieldforge（合成数据供给方）已收口：一条命令产出生产域 8 表+受效关系+测井曲线+亚日传感器，带 manifest（seed/freshness/闸门结论）——**数据面第一次有了真实可用的进料源与测试夹具**。
- 云服务器（阿里云广州 4C8G）已在跑 fieldforge-api + postgres + redis + nginx——数据面部署位现成。

## 2. 目标 / 非目标

**目标（S1 档，本期）**：
- G1 湖底座：PyIceberg + JDBC catalog（复用云上 postgres）+ 单机 MinIO 容器；湖表 schema 四件齐（列名/类型/注释/分区）、append-only 写面收敛、快照隔离、谓词/列裁剪
- G2 进料与新鲜度：fieldforge 产物批量进料（validate→append→register_freshness）；freshness 三档登记；未登记表=unknown
- G3 质量闸：20 条规则对象化（13–20 按设定书提取件二 8 类型×附录 A 字段落条目）；不合格行拦截；rule_mapping
- G4 语义层：15 条指标卡 → MetricDef（静态 YAML）+ 术语归一（block/rewrite/pass）+ build_query 四件套 + 权威源仲裁
- G5 执行适配：DuckDB 引擎（Trino/StarRocks 留接口位）；成本护栏；快照透传
- G6 路由与拒答：三查三出口（答实时面/答离线面/拒答），拒答话术合同零编数
- G7 快照审计：四元组记录 + 同快照回放 + 保留窗口
- G8 五只读工具：进程内直调（JSON 合同），只读红线机查
- G9 治理闸：Join 白名单默认拒绝、行级作业区隔离、S1 导出降级为不可用

**非目标**：
- 不做 tpa 服务端/Agent 编排/前端（另工程）；不做造数据（fieldforge 已完成）
- 不做 S2/S3 实装：Flink CDC（只产 spec 形状）、dbt-trino（S1 校验脚本直写）、Cube、MCP server（S1 进程内直调）、credential vending
- 不做 OTel 可观测（DP7 明确不做）
- 不引入教学仓没有承诺的框架（框架红线：PyIceberg/DuckDB/PyArrow/PyYAML/psycopg 点名引入，其余见 ADR）

## 3. 规模档位与范围声明

- **S1 档 = 本期全部范围**：GB 级、120 井 × 8 表替身、单机部署（并入 fieldforge 云服务器 compose 栈）
- S2/S3 只留 Seam 接口位（EngineAdapter 三实现、ToolTransport、PolicyBackend、Catalog backend="rest"），本期零实装
- 数据量级锚点：fieldforge 合同全量 675,412 行 / ~46MB Parquet；成本护栏 LIMIT_GUARD=100,000 行（教学演绎值，可配）

## 4. 技术栈宣布（阶段 0）

Python 3.12 · PyIceberg（湖表）· DuckDB（S1 引擎）· PyArrow（数据交换）· PyYAML（语义/策略字典）· psycopg（catalog JDBC）· pytest（测试）· 部署并入 fieldforge 云 compose（新增 minio 容器）。**不做教学阶段**（无 learn-by-rebuild 教学档；lessons/ 仅作施工日志）。

## 5. 功能需求（按模块，S1 范围）

### DP1 湖底座 LakeStore
- `load_catalog(backend, **cfg)`：S1=SqlCatalog（jdbc→云上 postgres）+ MinIO s3_props 四键
- `TableRef.schema() → SchemaInfo` 四件齐；`scan(pred, fields, snapshot_id, limit) → Arrow`（快照隔离、裁剪可解释）；`snapshots()`；`append(table, arrow, commit_meta) → SnapshotInfo`（**写面收敛：仅 dp2/dp3 可调**，append-only 不暴露 overwrite/delete）
- `metrics() → ColumnStats`（列级 min/max/null，裁剪依据）

### DP2 进料与新鲜度 Ingest
- `validate_batch` 先（dp3）→ `run_batch_load(table, records, provenance) `→ `register_freshness(table, tier, as_of, pipeline)`
- freshness 三档（realtime/nearline/offline）+ unknown；同表多管道=多登记，取 as_of 最新
- CDC 只产 `cdc_pipeline_spec` 形状（FlinkJobSpec，sink 语义标注）——**零实装**

### DP3 质量闸 Transform
- `validate_batch(table, rules) → QualityReport(passed, failures, rule_ids, **report_id[补])`
- 20 条规则对象化：1–12 按 §4 原文；**13–20 按提取件二 8 类型 × 附录 A 字段重建**（引用完整/跨表一致/枚举合法/正则格式/极值/波动率/钩稽/scrub）
- `rule_mapping()` 规则 ID ↔ 测试名；lineage（S1 读自有构建记录，dbt manifest 形状留位）
- 分层：staging/intermediate/marts；marts=消费面唯一认认层

### DP4 语义层 Semantic
- `metric_catalog()`：15 条指标卡 → MetricDef（**补 version 字段[裁决 #7]**；修饰词除指标 2 外=默认口径[裁决 #6]）
- `resolve_term`：禁用词 block / 别名 rewrite / 黑话限语境（设定书 §6 四类逐字）
- `build_query`：四件套编译；一词多口径→消歧询问不擅断（指标 2 有效产量两支）
- `authoritative_source`：一数四源仲裁（E7→映射到实际湖表，裁决 #5）

### DP5 执行适配 QueryRunner
- `run(query, snapshot_id) → QueryResult(arrow, snapshot_id, stats)`：四拍主干=check_join→成本护栏→路由→执行
- `sandbox_query`：DuckDB 进程内（禁 httpfs 直扫 s3——边界规则）
- 成本护栏：LIMIT_GUARD 可配，超限拒绝或降级采样并显式声明

### DP6 路由与拒答 FreshnessRouter
- 三查（指标注册?/新鲜度 unknown?/未来窗?）+ 三分支（粒度冲突拒/实时可加答/历史闭合答）+ 兜底未覆盖拒
- `refusal_text` 话术合同：截至时点/未覆盖窗口/原因/改答路径，零编数；realtime 附口径差异声明（事故 13）

### DP7 快照审计 Audit
- `record(question_id, query, snapshot_id, semantic_version, quality_status)`；`replay(ref)` 同快照重跑、快照过期显式报错；`retention_policy`（50/200 可配）

### DP8 数据工具面 DataTools（五只读工具，JSON 合同）
- discover_tables / describe_table / query（四道闸：check_join→route→run→record）/ replay / lineage
- 只读红线机查：`audit_write_surface()` assert 零写动词；写意图请求→拒绝并指引走 dp2/dp3
- serve_mcp 形状声明（S1=in_process）

### DP9 治理闸 Governance
- `check_join`：白名单默认拒绝（§5 四条 + **date_dim[裁决 #2]** + 受效关系经实体[已有]）
- `authorize`：行级作业区隔离（UserContext S1=静态 YAML[裁决 #17]）
- S1 导出整体降级不可用（显式隐藏）

## 6. 供给契约（fieldforge → 数据面）

| 项 | 状态 |
|---|---|
| 生产域 8 表 + 受效关系 + 测井曲线 + 亚日传感器（CSV/Parquet + manifest） | ✅ 已供给 |
| manifest（seed/schema 版本/freshness/闸门结论）→ 进料 provenance 与 freshness 登记输入 | ✅ 已供给 |
| **配产表（指标 1 依赖，裁决 #3）** | ❌ **需 fieldforge 新增** |
| **date_dim 日期维（Join 白名单第二边，裁决 #2）** | ❌ 需 fieldforge 新增或数据面自建 |
| 新旧编码映射表/井号归一表内容（规则 7/8，裁决 #15） | ⚠️ 部分供给（R3/BELT、N2/N-2 样本已有，需补全） |

## 7. 质量规则与指标口径

- 20 条规则与 15 条指标卡**全文以教学仓设定书 §3/§4 为唯一原文**，本 PRD 不复述；真实化裁决差异见 §10。

## 8. 非功能需求

- 部署：并入 fieldforge 云服务器 compose（新增 minio 容器）；无独立 K8s
- 并发：无硬指标（教学演绎口径沿袭）；生成/进料互斥限流沿 fieldforge-api 模式
- 可复现：同快照回放逐行一致；测试数据全部来自 fieldforge 可复现产物

## 9. 里程碑

| 阶段 | 内容 | 出口 |
|---|---|---|
| S0（已完成） | 接口合同冻结素材抽取（本 PRD 底稿）+ 仓骨架 + CI gate | 本文档 + 仓 |
| S1 垂直切片 | DP1-lite → DP2-lite → DP3-lite → DP6-lite → DP9-lite → DP8-lite（含 DP4/5/7 最小件），吃 fieldforge 云端产物；"老王问数"最小闭环 | 问数 tracer bullet 全绿 |
| S2 全量 | 15 指标卡全量语义、20 规则全量、快照保留窗口、r13–20 重建 | 对齐 DP 全集 S1 档 |
| S3 上云 | minio 容器并入 compose、数据面 API 化对外 | 数据面成为对外服务一部分 |

## 10. 教学演绎差距裁决表（24 项）

> 状态三值：✅已裁决（写入本 PRD）｜❓待确认（§11 逼问议程）｜N/A（不适用）

| # | 差距 | 裁决 |
|---|---|---|
| 1 | Fake 件清单（InMemoryLake/StaticFreshness/FakeDbt/FakeCube/RecordedEngine/FakeClock/InMemoryAudit/StaticPolicy） | ✅ 全部保留为**测试替身**（Seam 设计意图），真实实现另写——测试双轨 |
| 2 | date_dim 表不存在 | ✅ 数据面自建日期维（进料时派生），不改 fieldforge |
| 3 | 配产表不存在（指标 1 依赖） | ❓ 待确认：fieldforge 新增配产表供给，或 S1 先以 well_info 派生占位 |
| 4 | 受效关系实体不存在 | ✅ **已解决**：fieldforge injection_connection（65 组，Σ=1）已供给 |
| 5 | 一数四源无教学表 | ✅ SourceRef 映射：E7 统计上报→monthly_settlement（湖表化），仲裁注记 3.2% 为教学值如实标注 |
| 6 | 修饰词仅指标 2 有值 | ✅ 其余 14 条 MetricDef.modifiers=空（默认口径） |
| 7 | MetricDef 缺 version | ✅ dp4 卡补 version 字段（真实化签名变更，回写教学仓另议） |
| 8 | quality_status 无来源 | ✅ 定义=进料批的 dp3 QualityReport 结论（经 dp2 provenance 传递到表级，答级随快照） |
| 9 | QualityReport 缺 report_id | ✅ 补 report_id 字段（uuid） |
| 10 | StructuredQuery 缺槽位 | ✅ 显式化 base_table + limit 槽位 |
| 11 | TimeWindow 未定义 | ✅ 定义 {start, end, granularity: day\|month\|intraday} |
| 12 | formula_additive 未定义 | ✅ 15 指标逐条标注可加性（附录随 S2 落） |
| 13 | clean_batch_arrow 未闭环 | ✅ validate_batch 返回 (report, cleaned_arrow) 二元组 |
| 14 | marts 模型缺口 | ✅ S1 先落 3 张示范模型（fct_block_monthly/fct_valid_output_monthly/fct_wellset_annual 最小列集） |
| 15 | 映射表内容不全 | ❓ 待确认：fieldforge 脏特征已有部分（R3/BELT、N2/N-2），补全范围拍板 |
| 16 | 三处水位两套账 | ✅ 唯一真源=dp2 FreshnessRegistry；m17 平台侧读它（真实化接线时对齐） |
| 17 | UserContext 身份 | ✅ S1=静态 YAML 用户表（user/org/role），身份由调用方传入 |
| 18 | rt 告警表 schema | ✅ rt_zero_value_alerts DDL 随 S2 CDC spec 落；数据源=fieldforge 亚日读数（已供给） |
| 19 | well_daily 教学记法 | ✅ 真实断言写 oil_production_daily |
| 20 | 教学常量（LIMIT_GUARD 等） | ✅ 沿用默认值+全部可配 |
| 21 | 对账阈值 3% 跨域借数 | ✅ 确认借规则 12 同源阈值，注记归属 |
| 22 | F56 决策链到 dp8 接线 | N/A（属 tpa 服务端工程，接口合同留位） |
| 23 | S1 导出不可用 | ✅ 显式隐藏（工具清单不出现，非报错） |
| 24 | 教学 DP 票已 done | ✅ 验收 checkbox 直接复用为本仓票面素材 |

## 11. 待确认清单（阶段 1 逼问议程）

1. **配产表供给**（#3）：fieldforge 新增配产表，还是 S1 派生占位？
2. **编码/井号映射表补全范围**（#15）：fieldforge 脏特征已有样本外，要不要把完整映射表作为 fieldforge 种子数据补齐？
3. **S1 部署形态确认**：minio 容器并入 fieldforge 云 compose（本机 postgres 兼作 Iceberg catalog），还是独立栈？
4. **仓库远端**：tpa-data-plane 建 GitHub 私有仓（zzwcoding 名下）还是先纯本地？
5. **S1 验收口径**：以"老王问数 tracer bullet"（月度产量完成率一问全链路）作为 S1 出口验收，是否认可？

---

*本 PRD 为粗稿；逼问收敛后翻"已定稿"。所有 [待确认] 项关闭前不开工 S1 编码（sdd-flow 纪律：约定赶在开发前）。*
