-- 001 回滚：三个 schema 连同其对象整体删除（schema 边界即对象边界，无跨 schema 依赖）。
-- data_plane.lake_snapshot 的回滚语义＝湖元数据全清（原始文件桶对象不在 DB 事务内，
-- 留桶对象由部署侧清理策略处置——downgrade 不触碰 MinIO）。
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS clean CASCADE;
DROP SCHEMA IF EXISTS data_plane CASCADE;
