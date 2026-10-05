-- quality 面限写账号模板（DP-05；teach 99 册 §2.12＋E2 行为约定 2/3/8/9＋DP-05 同源裁决：
-- quality_* 表族＋water_line 落**主平台库**（99 册＝主平台库 90 表清单）；数据面质检跑批写
-- quality_task/quarantine_row/water_line，quality_rule **只读**——启停＝主平台 E2 独写
-- （M-05a PATCH 是唯一写口），任务执行按读到的当前启用集跑）。
-- 纪律同 agg_writer.sql/midplatform_readonly.sql 先例：本文件是**部署批交付物**，不进迁移链、
-- 不被 tools/migrate.py 消费；${QUALITY_WRITER_PASSWORD} 占位符由部署环境替换（凭证不进 git）。
-- 权限闭世界：quality_rule 只读；quarantine_row 仅 INSERT 不改写（append-only 账号级
-- 承载——坏行 payload 永不改写，与湖表同教义）＋SELECT（DP-06.fix F1 幂等寻址——
-- 先查后插防自愈重拾重复落行；UPDATE/DELETE 仍零授权，append-only 不松；
-- bigserial 序列随 INSERT 需 USAGE）；quality_task/water_line 写读（任务推进/水位
-- upsert——ON CONFLICT 读旧行需 SELECT）；其余一律默认拒绝：agg/raw/clean/data_plane/
-- 其余 public 表零授权、无 CREATE、无 ALTER 权、不设 ALTER DEFAULT PRIVILEGES（新表不自动暴露）。
-- 结构演进（quality_* DDL）归主仓迁移链（Alembic 0015），本账号运行时零 DDL 权。

CREATE ROLE tpa_quality_writer LOGIN PASSWORD '${QUALITY_WRITER_PASSWORD}';

GRANT USAGE ON SCHEMA public TO tpa_quality_writer;
GRANT SELECT ON quality_rule TO tpa_quality_writer;
GRANT SELECT, INSERT, UPDATE ON quality_task TO tpa_quality_writer;
GRANT SELECT ON quarantine_row TO tpa_quality_writer;
GRANT INSERT ON quarantine_row TO tpa_quality_writer;
GRANT USAGE, SELECT ON SEQUENCE quarantine_row_id_seq TO tpa_quality_writer;
GRANT SELECT, INSERT, UPDATE ON water_line TO tpa_quality_writer;
