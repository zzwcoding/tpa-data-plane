# RF-04 · 粒度拒答档位无关 + 假承诺修复（P1-3）
Status: todo | Touches modules: `dp6` | 源: 审查 P1-3
问题: 分支 A 限定 not additive and offline——正典输入"产量完成率×今天上午"落到 defer，"明日可答"对月粒度指标是假承诺（T+1 永远补不出日内官方口径）。
修法: 月指标×日内窗=粒度拒绝，档位无关；重锚测试。
