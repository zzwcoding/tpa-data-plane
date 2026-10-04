-- =====================================================================
-- 中台库只读账号 SQL 模板：midplatform（DP-03 票 4——平台侧消费缝预告，本票只留接口不留实现）
--
-- 用途：主平台 M-01 票经 `midplatform` 角色只读直读 8 张 as-is 源表（teach 99 册 §2.0②
-- 「中台只读路由：wells/dataops 经 infra.db 的 midplatform 角色（只读连接）直读源表取行」；
-- §4.1 三角色路由 midplatform 行：映射卡取行/质检扫描专用，强制只读）。
--
-- 交付位与执行时机：本模板随迁移文件旁交付（迁移工具不管角色——99 §4.4「midplatform 不迁移，
-- 中台源表归中台管」的仓内对应＝账号建立随数据面部署批由管理员执行，不进迁移账）。
--
-- 用法：替换两处 <SET_ME> 后在中台库以超级用户执行；生产口令走密管/环境注入，不入仓不入日志。
-- 边界（默认拒绝）：仅 GRANT USAGE raw schema＋SELECT 8 表；clean/data_plane schema、
-- 湖元数据两表（lake_table/lake_snapshot）一律不授权；不设 ALTER DEFAULT PRIVILEGES
-- ——后续新增源表需显式补 GRANT，避免新表自动暴露给平台侧。
-- =====================================================================

CREATE ROLE midplatform LOGIN PASSWORD '<SET_ME>';

GRANT USAGE ON SCHEMA raw TO midplatform;

GRANT SELECT ON raw.well_info,
               raw.oil_production_daily,
               raw.water_injection_daily,
               raw.well_overhaul,
               raw.well_measures,
               raw.fluid_test,
               raw.equip_status,
               raw.monthly_settlement
TO midplatform;

-- 连接级只读兜底（拦绕过授权面的一声令下改数据；与 99 册连接层只读守卫同构）
ALTER ROLE midplatform SET default_transaction_read_only = on;
