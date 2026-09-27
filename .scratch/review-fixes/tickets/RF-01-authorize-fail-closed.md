# RF-01 · authorize fail-closed（P1-8 安全洞）
Status: todo | Touches modules: `dp9` | 源: 审查 P1-8
问题: 用户无 org 且未传 requested_scope 时两道检查全跳过=跨作业区放行；传 scope 时 u["org"] KeyError。
修法: 用户条目缺 org 或 scope 无法确立 → 显式拒绝（fail-closed）。
