# 架构图 v1 —— tpa 数据面

> 阶段 2 产物（Mermaid）。过审记录落 `docs/adr/0001-架构图v1过审.md`。

```mermaid
flowchart TB
    subgraph 供给方["供给方（fieldforge，已完成）"]
        FF["fieldforge 产物<br/>Parquet/CSV + manifest<br/>(seed/freshness/闸门结论)"]
    end

    subgraph 数据面["tpa 数据面（本仓库）"]
        subgraph 数据层
            DP2["dp2 进料 Ingest<br/>validate→append→register_freshness"]
            DP3["dp3 质量闸 Transform<br/>20 条规则 / 分层 / lineage"]
            DP1["dp1 湖底座 LakeStore<br/>PyIceberg + MinIO + JDBC catalog<br/>append-only 写面收敛"]
        end
        subgraph 语义与服务层
            DP4["dp4 语义层<br/>15 指标卡 / 术语归一 / 四件套"]
            DP5["dp5 执行适配<br/>DuckDB / 成本护栏"]
            DP6["dp6 路由拒答<br/>三查三出口 / 话术合同"]
            DP7["dp7 快照审计<br/>四元组 / 同快照回放"]
        end
        subgraph 接口层
            DP9["dp9 治理闸<br/>Join 白名单 / 行级隔离"]
            DP8["dp8 五只读工具<br/>discover/describe/query/replay/lineage<br/>(只读红线机查)"]
        end
        LAKE[("湖存储<br/>MinIO + Iceberg")]
    end

    subgraph 消费方["消费方（tpa 服务端，另工程）"]
        AGENT["问数 Agent<br/>(LLM 外接)"]
    end

    FF -->|进料| DP3
    DP3 -->|validate_batch| DP2
    DP2 -->|append| DP1
    DP1 --- LAKE
    DP3 -->|manifest| DP4
    DP4 -->|StructuredQuery| DP5
    DP5 -->|read| DP1
    DP6 -.->|freshness/粒度| DP2
    DP6 -.->|粒度| DP4
    DP8 ==>|①check_join| DP9
    DP8 ==>|②route| DP6
    DP8 ==>|③run| DP5
    DP8 ==>|④record| DP7
    AGENT -->|五只读工具 JSON 合同| DP8
```

## 节点说明（过审导览）

| 节点 | 职责一句话 | 关键纪律 |
|---|---|---|
| FF 供给方 | 造数+验证，送到湖边 | 不入湖（边界在文件+manifest） |
| dp2 进料 | 唯一合法写入口之一 | provenance 随 commit 落元数据 |
| dp3 质量闸 | 唯一合法写入口之二；20 规则机判 | 两证放行=测试全绿+源新鲜 |
| dp1 湖底座 | append-only、快照隔离、写面收敛 | append 仅 dp2/dp3 可调 |
| dp4 语义层 | 口径唯一暴露面，禁发明 | 对齐闸=与 dp3 manifest 比对 |
| dp5 执行适配 | 结构化查询→引擎 | 四拍主干闸前置 |
| dp6 路由拒答 | 诚实声明，零编数 | 不读湖表数据 |
| dp7 快照审计 | 结论可回放可追责 | 绝不静默换快照 |
| dp9 治理闸 | 白名单默认拒绝+行级隔离 | 只持规则不持数据 |
| dp8 五工具 | Agent 唯一接缝 | 四道闸顺序即合同；写意图=拒绝 |

## 数据流（老王问数 tracer bullet 的路）

fieldforge 产物 → dp3 质检 → dp2 入湖（snapshot N）→ 用户问"渤南区块上月产量完成率" → dp4 build_query（术语归一+修饰词消歧）→ dp9 白名单 ✓ → dp6 路由：月度指标×历史窗 → 答离线面 → dp5 DuckDB 执行（快照 N）→ dp7 记录四元组 → AnswerBundle 四件齐 → Agent 流式解读。
