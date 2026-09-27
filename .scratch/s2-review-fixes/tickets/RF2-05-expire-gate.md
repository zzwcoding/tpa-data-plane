# RF2-05 · expire 调用方闸 + enforce 入口（P1）

Status: done | 源: S2 审查（四轴）

## 修复内容

expire 仅 dp7.retention 可调（同 append 白名单）；dp7.enforce_retention(catalog, table) 为策略执行入口；modules.md dp1 卡补 expire 签名 🆕。

## 验收

- [ ] 按描述修复
- [ ] 全量测试绿
