-- agg 限写三表专用账号模板（DP-04；teach 99 册 §2.12/§4.4＋ADR-0003 裁决 15：
-- agg 三表由数据面按 99 DDL 直写主平台库——限写三表专用账号，账号建立随数据面部署批）。
-- 纪律同 midplatform_readonly.sql（DP-03 先例）：本文件是**部署批交付物**，不进迁移链、
-- 不被 tools/migrate.py 消费；${AGG_WRITER_PASSWORD} 占位符由部署环境替换（凭证不进 git）。
-- 权限闭世界：USAGE agg＋恰三表 SELECT/INSERT/UPDATE/DELETE；clean 层只读（读中台库 clean 层——票面）；
-- 其余一律默认拒绝：raw/data_plane/public 零授权、agg schema 无 CREATE、无 ALTER 权、
-- 不设 ALTER DEFAULT PRIVILEGES（新表不自动暴露）。
-- 结构演进（agg DDL）归主仓迁移链（Alembic 0014），本账号运行时零 DDL 权。

CREATE ROLE tpa_agg_writer LOGIN PASSWORD '${AGG_WRITER_PASSWORD}';

GRANT USAGE ON SCHEMA agg TO tpa_agg_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON agg.dim_well TO tpa_agg_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON agg.dim_date TO tpa_agg_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON agg.fct_daily_metering TO tpa_agg_writer;

GRANT USAGE ON SCHEMA clean TO tpa_agg_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA clean TO tpa_agg_writer;

-- clean 层未来落表（DP-04/DP-05 清洗产物落表批）随批补 GRANT SELECT（与 midplatform 模板同款：
-- 不设 ALTER DEFAULT PRIVILEGES——新表不自动暴露，闭世界靠显式授权维持）。
