# modules.md —— tpa 数据面模块划分（阶段 2 唯一真源）

> **来源**：移植自 teach-petro-agent `specs/data-plane/modules.md`（292 行已定稿），签名逐字保留；真实化裁决差异（24 项，见 docs/prd.md §10）已并入。
> **分层**：数据层 dp1–dp3 → 语义与服务层 dp4–dp7 → Agent 接口层 dp8–dp9。
> **规模档位**：本期只实装 **S1**（DuckDB 进程内 + JDBC catalog + 单机 MinIO）；S2/S3 只留 Seam 接口位。
> **目录骨架**：`app/data_plane/{lake_store,ingest,transform,semantic,query_runner,freshness,audit,tools_mcp,governance}.py`（占位已建）。

## 依赖图（无环）

```
dp1 ← dp2 ← （上游数据源：fieldforge 产物 / 班报 CSV）
dp1 ← dp3 ← dp4
dp1 ← dp5 ← dp7        dp4 ← dp5
dp2 ← dp6 → dp4
dp1/dp4/dp5/dp6/dp7/dp9 ← dp8
dp9 ← dp5, dp8
```

## dp1

目录: app/data_plane
- 模块: （无——最底层）

- **职责**：对象存储 + Iceberg 表的统一读写门面，屏蔽 catalog/存储/表格式差异。
- **公开接口**（逐字，真实化补充项标 🆕）：
  - `load_catalog(backend: Literal["jdbc","rest","federated"], **cfg) -> Catalog`
  - `Catalog.load_table(name: str) -> TableRef`
  - `TableRef.schema() -> SchemaInfo`（列名/类型/注释/分区规格四件齐）
  - `TableRef.scan(pred: str | None, fields: tuple[str, ...], snapshot_id: int | None, limit: int) -> ArrowTable`
  - `TableRef.snapshots() -> list[SnapshotInfo]`
  - `Catalog.append(table: str, arrow: ArrowTable, commit_meta: dict) -> SnapshotInfo`（**仅 dp2/dp3 可调**）
  - `TableRef.metrics() -> ColumnStats`
- **依赖**：无（最底层）；外部=MinIO/S3、PyIceberg SqlCatalog（JDBC→云上 postgres）
- **Seam/Adapter**：`Catalog`/`TableRef` 抽象；测试替身 `InMemoryLake`、`FakeCatalog`
- **测试计划**：DP-W-D1-1…4（schema 四件齐/快照隔离/谓词列裁剪/写面越界审计）

## dp2

目录: app/data_plane
- 模块: `dp1`

- **职责**：把上游数据（fieldforge 产物 / 班报批量）写入湖表，登记并暴露新鲜度档位元数据。
- **公开接口**：
  - `register_freshness(table: str, tier: Literal["realtime","nearline","offline"], as_of: datetime, pipeline: str) -> None`
  - `freshness(table: str) -> FreshnessInfo(tier, as_of, pipeline)`（未登记=unknown；同表多管道取 as_of 最新）
  - `run_batch_load(table: str, records: ArrowTable, provenance: Provenance(quality_report_id, operator)) -> SnapshotInfo`
  - `cdc_pipeline_spec(source_db: str, tables: list[str]) -> FlinkJobSpec`（S2 形状，零实装）
- **依赖**：dp1
- **Seam/Adapter**：`FreshnessRegistry`；测试替身 `StaticFreshness`
- **测试计划**：DP-W-D2-1…4；进料源=fieldforge run 目录（manifest→freshness 登记）

## dp3

目录: app/data_plane
- 模块: `dp1`

- **职责**：质量规则执行（20 条）、staging→intermediate→marts 分层、口径与血缘清单。
- **公开接口**：
  - `validate_batch(table: str, rules: list[QualityRule]) -> (QualityReport(passed, failures, rule_ids, report_id 🆕), cleaned_arrow 🆕)`（二元组裁决 #13）
  - `rule_mapping() -> dict[rule_id, test_name]`
  - `build_marts() -> BuildResult(models_built, tests_passed, manifest_ref)`（S1=内置分层 SQL，dbt 形状留位）
  - `lineage(model: str) -> LineageInfo(upstreams, downstreams, columns)`
- **依赖**：dp1
- **Seam/Adapter**：`Runner`；测试替身 `FakeDbt`（S1 不引入 dbt）
- **测试计划**：DP-W-D3-1…4；20 条规则对象（1–12 原文 + 13–20 重建）；fieldforge 脏配方=挂样本源

## dp4

目录: app/data_plane
- 模块: `dp3`

- **职责**：15 条指标卡 → 机器可读口径；术语归一（block/rewrite/pass）是问数入口第一闸。
- **公开接口**：
  - `metric_catalog() -> list[MetricDef]`（名称/公式/单位/粒度/修饰词/Owner/权威源/ai_context/**version 🆕**）
  - `resolve_term(term: str) -> TermResolution(canonical, action: Literal["pass","rewrite","block"], note)`
  - `build_query(metric: str, dims: list[str], time_window: TimeWindow, filters: dict) -> StructuredQuery | Clarify`
  - `authoritative_source(metric: str) -> SourceRef`
- **依赖**：dp3（manifest 只读——口径唯一暴露面，禁第二真源）
- **Seam/Adapter**：`DictionaryBackend`；S1=`StaticYaml`
- **测试计划**：DP-W-D4-1…5；词表四类逐字（设定书 §6）

## dp5

目录: app/data_plane
- 模块: `dp1`, `dp4`, `dp9`

- **职责**：同一 StructuredQuery 按档位路由引擎执行，返回 Arrow + 快照号 + 统计。
- **公开接口**：
  - `run(query: StructuredQuery, snapshot_id: int | None = None) -> QueryResult(arrow, snapshot_id, stats(rows_scanned, bytes, engine))`（四拍：check_join→成本护栏→路由→执行）
  - `sandbox_query(table: str, pred: str, fields: tuple[str, ...], limit: int) -> ArrowTable`
  - `engines() -> list[EngineInfo(name, tier)]`
- **依赖**：dp1、dp4（StructuredQuery 定义在 dp4）、dp9（闸）
- **Seam/Adapter**：`EngineAdapter`（duckdb 实现；trino/starrocks 留位）；测试替身 `RecordedEngine`
- **测试计划**：DP-W-D5-1…4；StructuredQuery 含 base_table/limit 槽位（裁决 #10）

## dp6

目录: app/data_plane
- 模块: `dp2`, `dp4`

- **职责**：问题时间窗 × 表新鲜度档位 → 三出口（答实时面/答离线面/拒答并诚实声明）。
- **公开接口**：
  - `route(window: TimeWindow, metric: str) -> RouteDecision(answerable, source_tier, declared_as_of | refusal_reason)`
  - `refusal_text(decision: RouteDecision) -> str`（话术合同：截至时点/未覆盖窗口/原因/改答路径，零编数）
- **依赖**：dp2（freshness 元数据）、dp4（指标粒度）；**禁止读湖表数据**（边界规则）
- **Seam/Adapter**：`Clock`；测试替身 `FakeClock`
- **测试计划**：DP-W-D6-1…3；TimeWindow={start,end,granularity}（裁决 #11）

## dp7

目录: app/data_plane
- 模块: `dp1`, `dp5`

- **职责**：结论绑定快照四元组，同快照回放校验，保留窗口。
- **公开接口**：
  - `record(question_id: str, query: StructuredQuery, snapshot_id: int, semantic_version: str, quality_status: str) -> AuditRef`
  - `replay(ref: AuditRef) -> ReplayResult(match: bool, original, current)`（快照过期显式报错，绝不静默回退）
  - `retention_policy() -> RetentionPolicy`（50/200 可配）
- **依赖**：dp1（snapshots）、dp5（重跑执行）
- **Seam/Adapter**：`AuditStore`；测试替身 `InMemoryAudit`
- **测试计划**：DP-W-D7-1…3

## dp8

目录: app/data_plane
- 模块: `dp1`, `dp4`, `dp5`, `dp6`, `dp7`, `dp9`

- **职责**：问数 Agent 与数据面的**唯一接缝**：发现→描述→查询→回放→溯源。
- **公开接口**：
  - `discover_tables(prefix: str | None) -> list[TableCard(name, tier, freshness, metrics)]`（越权表不进清单，不报错）
  - `describe_table(name: str) -> TableSchema(columns, partition_spec, current_snapshot, metrics_linked)`
  - `query(metric, dims, window, filters) -> AnswerBundle(arrow_summary, snapshot_id, quality_status, as_of)`（**内部强制 dp9.check_join → dp6.route → dp5.run → dp7.record**；任一闸拒=整体拒）
  - `replay(audit_ref: str) -> ReplayResult`
  - `lineage(table_or_metric: str) -> LineageInfo`
  - `serve_mcp() -> MCPServerSpec`（S2+ 形状；**只读红线：无任何湖表写工具**，`audit_write_surface()` 机查）
- **依赖**：dp1/dp4/dp5/dp6/dp7/dp9
- **Seam/Adapter**：`ToolTransport`（S1=in_process；S2+=MCP 留位）
- **测试计划**：DP-W-D8-1…4

## dp9

目录: app/data_plane
- 模块: （无——最底层）

- **职责**：横切闸——合法 join 白名单（默认拒绝）、行级作业区隔离、导出审批（S1 降级不可用）、S3 凭证下发（形状留位）。
- **公开接口**：
  - `check_join(query: StructuredQuery) -> Decision(allowed, violated_edges)`
  - `authorize(ctx: UserContext, table: str, action: Literal["read","export"]) -> Decision`
  - `export_report(...)`（S1 显式隐藏）
  - `vend_credentials(...)`（S3 形状留位）
- **依赖**：无（被 dp5/dp8 依赖）
- **Seam/Adapter**：`PolicyBackend`；S1=`StaticPolicy`（YAML）
- **测试计划**：DP-W-D9-1…4（白名单 ✗ 边/行级隔离/导出降级/vending 形状）

## 边界规则

> 阶段 6 边界闸唯一事实来源。

| 禁止 | 理由 |
|---|---|
| dp5 绕过 dp1 直连 S3 SDK / 引擎原生文件访问 | 对象访问面收敛 LakeStore |
| dp8 绕过 dp9 放行任何查询/导出 | 治理闸是工具面唯一放行路径 |
| dp6 读湖表数据——只许读 dp2 档位元数据与 dp4 粒度 | 路由不得"先查了再说" |
| dp4 发明口径——指标定义只来自 dp3 manifest 与设定书 §3 | 语义层是口径唯一暴露面，禁止第二真源 |
| dp1.append 被 dp2/dp3 之外调用（例外=dp1 自身测试替身） | 湖表写面收敛进料/加工两管道，Agent 五工具全只读 |
| dp7.replay 用当前快照替代记录快照 | 回放的语义就是"那一版" |
| 任何 dp 模块 import 平台 m1–m21 内部实现 | 数据面与平台语义侧只走公开面（HTTP/工具合同） |

## 页面映射

N/A——本仓无 Web UI（数据面是库/服务层）；页面数据需求由 tpa 服务端工程负责，经 dp8 五工具合同对接。
