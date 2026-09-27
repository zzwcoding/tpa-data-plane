# RF-05 · replay 等值归一（P1-5，票 08 前必修）
Status: todo | Touches modules: `dp7` | 源: 审查 P1-5
问题: match = ArrowTable == list 恒 False，dp5 落地后每次回放都误报 drift。
修法: 两侧归一（pa.Table.to_pylist() 分支）；AuditRecord.original 类型合同写入卡注。
