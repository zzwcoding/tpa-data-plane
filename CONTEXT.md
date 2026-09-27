# CONTEXT.md —— 共享术语表

> 阶段 1 逼问中持续填充；术语只在此处有原文，spec 以 `TERM-x` 引用。

| 术语 | 定义 | 来源 |
|---|---|---|
| 湖底座 | MinIO+Iceberg（S2）或本地 Parquet+DuckDB（S1）构成的存储查询底座 | tpa DP1 |
| 进料 | 数据从交付态（fieldforge 产物/班报 CSV）进入湖表的管道 | tpa DP2 |
| 质量闸 | 入湖前置的规则检查，不合格行拦截 | tpa DP3 |
| 新鲜度 | 湖表的三档时效元数据：realtime / nearline / offline + as_of | tpa DP2/DP6 |
| AnswerBundle | 问数结论的四件套：问题 / 查询 / snapshot_id / 口径版本+质量状态 | tpa DP8 |
| 受效关系 | 注水井→受效油井多对多，劈分系数 Σ=1 | tpa 本体 |

## 语义核心

（阶段 1/2 填充：状态机 / 不变量 / 关系约束）
