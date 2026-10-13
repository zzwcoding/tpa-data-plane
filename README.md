# tpa-data-plane —— Petro-Agent 数据面仓

独立数据服务：进料→清洗→出汇总→质检→报水位（湖-仓-面分层，ADR 链见本仓 docs）。本地嵌套于平台仓 `data-plane/` 目录，平台仓 `.gitignore` 排除——独立 git、独立提交节奏，内容一律回本仓提交推送。

## 项目地图（同一项目的各部分 GitHub 仓）

本项目按「设计真源 → 平台实现 → 数据面 → 合成数据」分仓协作，各仓独立 git，GitHub 平级发布：

| 仓库 | 角色 | 可见性 |
|---|---|---|
| [petro-agent-platform](https://github.com/zzwcoding/petro-agent-platform) | 平台实现仓：后端 `app/`＋`appv2/` · 前端 `web/` · 本体 `ontology/`（已并入） | 公开 |
| [teach-petro-agent](https://github.com/zzwcoding/teach-petro-agent) | 设计/教学真源仓：合同册 · ADR · 业务设定 · 场景课 | 私有 |
| [teach-petro-agent-public](https://github.com/zzwcoding/teach-petro-agent-public) | 教学仓公开向副本（架构设计项目形态） | 私有 |
| **本仓** [tpa-data-plane](https://github.com/zzwcoding/tpa-data-plane) | 数据面仓：独立数据服务（进料→清洗→出汇总→质检→报水位） | 私有 |
| [fieldforge](https://github.com/zzwcoding/fieldforge) | 油气领域合成数据生成工作台 | 公开 |
