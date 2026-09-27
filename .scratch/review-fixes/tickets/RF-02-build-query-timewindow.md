# RF-02 · build_query 兼容 TimeWindow 实例（P1-1）
Status: todo | Touches modules: `dp4` | 源: 审查 P1-1
问题: 卡签名 time_window: TimeWindow，实现只收 dict（传实例 TypeError）；返回裸 dict 信封，Clarify 类型缺失。
修法: getattr 双路取值；补 Clarify dataclass；卡面标 🆕。
