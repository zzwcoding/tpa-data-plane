# RESOURCES —— 信源清单

> 讲解论断挂信源，不凭记忆。R 编号供场景文章引用。

## 知识（第一手：本仓）

- **R1 源码**：`app/data_plane/` 九模块 + `serve/api.py` + `app/data_plane/semantic_dictionary.yaml`（行数与关键行号以场景文章实测 grep 为准）
- **R2 PRD**：`docs/prd.md`（§5 功能需求、§10 教学演绎差距裁决表 24 项、§12 实施终态）
- **R3 模块卡**：`specs/modules.md`（九模块公开签名 + 边界规则表 7 条）
- **R4 架构图**：`docs/architecture-v1.md`（Mermaid 全景 + 节点说明 + 数据流）
- **R5 测试**：`tests/dp1…dp9 + tests/e2e/test_tracer_bullet.py`（98 个；e2e=老王问数全链）
- **R6 施工票**：`.scratch/s1-tracer-bullet/`、`.scratch/s2-full-thickening/`、`.scratch/s2-review-fixes/`

## 知识（设计蓝本与供给方，只读）

- **R7 设定书**：`/Users/divh/Downloads/teach-petro-agent/docs/business-setting.md`（§3 指标卡 15 条、§4 质量规则 20 条、§6 术语表——语义字典唯一原文）
- **R8 DP 底稿**：teach-petro-agent `lessons/DP1–DP9`（九份实现底稿，模块设计依据）
- **R9 fieldforge**：`/Users/divh/Downloads/fieldforge`（造数器；`out-tpa/` run 目录 = 进料源与测试夹具）

## tpa 前期做过的研究（本仓开工前的调研与设计沉淀，teach-petro-agent 仓内只读）

- **R10 数据面设计四件套**：`specs/data-plane/DP1–DP9`（九份设计底稿——本仓 dp 卡的前身）、`tickets/DP1–DP9`（九张开发票）、`specs/walkthroughs-data-plane.md`（W-DP 断言合同，129 行）、`docs/adr/0001-架构图v1过审.md`（本仓架构图过审记录）
- **R11 技术栈选型分册**：teach-petro-agent `docs/tech-stack/`，与本课直接相关的六册——`duckdb.md`（dp5 引擎选型）、`minio-iceberg.md`（dp1 湖底座选型）、`dbt.md`（dp3 分层/marts 的 dbt 形状来源）、`cube-semantic-layer.md`（dp4 语义层的同类对照）、`streaming-batch-warehouse.md`（流批一体背景）、`dataagent-stack-map.md`（数据栈全景图）；另有 `flink.md`（dp2 CDC spec 形状来源）、`s3.md`、`guochan-db-os.md`（国产化语境）备查

## 外部书籍（可佐证通用原理；原文英文）

- **R12 《Fundamentals of Analytics Engineering》**（`/Users/divh/Downloads/资料/技术书籍/Fundamentals of Analytics Engineering/原书提取`，ch1–ch14 提取件）。能用上——它是 dbt 视角的"分析工程"教科书，与本课三处直接对得上：
  - **ch5/ch6**（数仓建模 Inmon/Kimball、实体与粒度重组）↔ 场景 1-2 dp3 的 staging→marts 分层与三张汇总表 SQL（fct_ 前缀＝事实表命名惯例）
  - **ch9–11**（源端/转换中的数据质量、数据血缘、可观测、数据目录、**语义层**）↔ 场景 1-2 质量闸、2-2 语义层——这册是 dp4"为什么要有语义层"的行业佐证
  - **ch12–14**（模型测试进 CI/CD 管线）↔ 场景 3-3 测试守边界
  - 用法：当**延伸阅读/面试佐证**（"这个做法行业里叫 XX"），不替代本仓实测；讲技术点四要素时挂对应章节，不整本通读

## 外部官方文档（讲新技术点四要素时挂）

- DuckDB（进程内分析引擎）、PyArrow（列式数据交换）、FastAPI、PyYAML、PyIceberg（湖表，测试替身语义来源）

## 实测环境

- **云端**：`http://8.163.33.135/dp/`（healthz / tables / query / replay / lineage；只读；响应带 X-Synthetic-Data 头；当前挂 fieldforge tpa_s1_dirty run）
- **本地**：fieldforge `out-tpa/` run 目录（与云端同 seed，字节级可复现）；本仓 `python3 -m pytest`（98 测试）
