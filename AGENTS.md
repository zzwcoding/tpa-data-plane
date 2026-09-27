# tpa-data-plane —— Petro-Agent 数据面（真实代码）

Petro-Agent 平台数据面的真实代码实现：湖底座 / 进料与新鲜度 / 质量闸 / 指标语义 / 路由拒答 / 消费面五只读工具。设计蓝本 = teach-petro-agent 教学仓（模块卡 + W-DP 断言 + 20 条规则 + 15 条指标卡）；测试数据供给 = fieldforge（合成数据生成器，已完成）。

按 sdd-flow 流程开发（不做教学环节，只记施工票）。

## Agent skills

### Issue tracker

本地 markdown 施工票，一个功能一个目录 `.scratch/<feature-slug>/`。见 `docs/agents/issue-tracker.md`。

### Domain docs

single-context：根目录 `CONTEXT.md` + `docs/adr/`。见 `docs/agents/domain.md`。

## 关联仓库

- `~/Downloads/teach-petro-agent`：设计蓝本（只读，教学仓）
- `~/Downloads/fieldforge`：数据供给方（合成数据生成器，GitHub: zzwcoding/fieldforge）
