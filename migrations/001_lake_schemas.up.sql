-- 001 中台库三层 schema＋湖元数据＋8 张 as-is 源表（DP-03，方案 C1——拍板 3：不引入 PyIceberg，
-- 湖＝中台库独立 schema（PG raw/clean/数据面自有）＋MinIO 原始文件桶）。
-- DDL 真源＝teach 设定书 §2.2/§A 生产域 8 表字典；裁决锚＝teach 99 册 §2.0（8 表 as-is 真源在中台
-- 原始层湖）/§4.4；Owner 拍板 2＝中台库归数据面全权。
-- as-is 纪律：业务列照抄字典；**不设业务主键/唯一约束**（A/B 班重复行、配产双值行等脏特征是
-- 原始层合法内容，质检闸在进料前拦截、清洗层才去重）——逻辑键以表 COMMENT 登记只不约束。
-- 湖技术列三件（业务列之外）：_snapshot_id（快照寻址）/ _batch_id（批次血缘）/ _ingested_at（入库时刻）。

CREATE SCHEMA IF NOT EXISTS raw;        -- 中台原始层湖：8 张 as-is 源表
COMMENT ON SCHEMA raw IS '中台原始层湖（as-is 源表 8 张；DP-03 方案 C1，99 册 §2.0）';

CREATE SCHEMA IF NOT EXISTS clean;      -- 清洗产物表（落表归 DP-04/DP-05，本票只立 schema）
COMMENT ON SCHEMA clean IS '清洗产物层（staging 去重/归一产物；DP-04/05 落表）';

CREATE SCHEMA IF NOT EXISTS data_plane; -- 数据面自有表（Owner 拍板 2：中台库归数据面全权）
COMMENT ON SCHEMA data_plane IS '数据面自有表：湖元数据（lake_table/lake_snapshot）＋freshness_registry/audit_record';

-- ============ 湖元数据（catalog 两表） ============

CREATE TABLE IF NOT EXISTS data_plane.lake_table (
    table_name     VARCHAR(128) PRIMARY KEY,
    comment        VARCHAR(255) NOT NULL DEFAULT '',
    partition_spec VARCHAR(128) NOT NULL DEFAULT '',
    created_at     TIMESTAMP NOT NULL DEFAULT now()
);
COMMENT ON TABLE data_plane.lake_table IS '湖表注册（schema 四件齐的元数据半张：注释/分区规格；当前快照取自 lake_snapshot）';

CREATE TABLE IF NOT EXISTS data_plane.lake_snapshot (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(128) NOT NULL,
    batch_id        VARCHAR(64)  NOT NULL,
    committed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),  -- 真时钟提交时间（DB 时刻，替身 "now" 字面的语义缺口补齐）
    rows_added      INTEGER      NOT NULL DEFAULT 0,
    data_object_key VARCHAR(512) NOT NULL,                -- MinIO 原始文件键：tpa-raw/<source>/<batch>/data.csv
    manifest        JSONB        NOT NULL,                -- 血缘 manifest（dp3 transform.py 格式沿用：metadata/nodes/parent_map/child_map）
    provenance      JSONB        NOT NULL DEFAULT '{}'::jsonb  -- commit 元数据（质检报告号/操作人）
);
CREATE INDEX IF NOT EXISTS idx_lake_snapshot_table ON data_plane.lake_snapshot (table_name, snapshot_id);  -- IF NOT EXISTS=湖先 boot（_ensure_meta 同形兜底）再跑迁移不炸（DP-03.fix F2 幂等）
COMMENT ON TABLE data_plane.lake_snapshot IS '快照账（append-only；expire 只删行＝清时间旅行可见性，不动当前数据——DP-W-D7-3/RF2-05）';

-- ============ 数据面自有两表（与 pg_store 幂等 DDL 同形；P0 热修起归位自有 schema） ============

CREATE TABLE IF NOT EXISTS data_plane.freshness_registry (
    table_name TEXT PRIMARY KEY,
    tier       TEXT NOT NULL,
    as_of      TIMESTAMP,
    pipeline   TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS data_plane.audit_record (
    audit_id         TEXT PRIMARY KEY,
    asked_at         TEXT NOT NULL,
    query            JSONB NOT NULL,
    snapshot_id      BIGINT NOT NULL,
    semantic_version TEXT NOT NULL,
    quality_status   TEXT NOT NULL,
    original         JSONB NOT NULL DEFAULT '[]'::jsonb
);

-- ============ 8 张 as-is 源表（设定书 §A 字典；通用三技术列不再逐表重复注释） ============

-- §A.1 井档案（一井一行；逻辑 PK well_id）
CREATE TABLE IF NOT EXISTS raw.well_info (
    well_id         VARCHAR(8)  NOT NULL,   -- `N-02`；旧写法 N2/N-2 并存（归一化素材）
    well_name       VARCHAR(64),
    well_type       CHAR(2),                -- EW 采油 · IW 注水（个别改型未留版本）
    first_prod_date DATE,
    platform        VARCHAR(16),            -- CEP-A/B
    block_name      VARCHAR(16),            -- 渤南/渤中/渤西
    field_name      VARCHAR(32),            -- 渤海油田（行内冗余＝反规范化样例）
    current_status  VARCHAR(16),            -- 生产中·停产·检修·待弃置（状态滞后：N-11 已停仍"生产中"）
    remarks         VARCHAR(255),
    _snapshot_id    BIGINT NOT NULL,
    _batch_id       VARCHAR(64) NOT NULL,
    _ingested_at    TIMESTAMP NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.well_info IS '井档案（一井一行；逻辑 PK well_id）——设定书 §A.1';

-- §A.2 日产计量（一井一日；逻辑 PK well_id+prod_date）★核心事实源
CREATE TABLE IF NOT EXISTS raw.oil_production_daily (
    well_id       VARCHAR(8) NOT NULL,
    prod_date     DATE NOT NULL,
    oil_output    NUMERIC(10,2),            -- 吨（个别行误录为方——"12.40方" 类脏值显式报错不静默改写）
    liquid_output NUMERIC(10,2),            -- 方
    water_cut     NUMERIC(5,1),             -- %（化验折算，与 fluid_test 打架）
    plan_output   NUMERIC(10,2),            -- 吨（配产版本切换日双值行）
    open_days     NUMERIC(4,1),             -- 天（停井日仍>0 矛盾行）
    oil_pressure  NUMERIC(6,2),             -- MPa
    report_shift  CHAR(1),                  -- A/B/C（班次手抄汇总：同井同日 A/B 两行）
    _snapshot_id  BIGINT NOT NULL,
    _batch_id     VARCHAR(64) NOT NULL,
    _ingested_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_opd_well_date ON raw.oil_production_daily (well_id, prod_date);
COMMENT ON TABLE raw.oil_production_daily IS '日产计量（一井一日；逻辑 PK well_id+prod_date，重复行不约束）——设定书 §A.2';

-- §A.3 日注水（井号+日期；逻辑 PK well_id+prod_date）
CREATE TABLE IF NOT EXISTS raw.water_injection_daily (
    well_id       VARCHAR(8) NOT NULL,
    prod_date     DATE NOT NULL,
    inj_volume    NUMERIC(10,2),            -- 方
    pump_pressure NUMERIC(6,2),             -- MPa
    layer_split   VARCHAR(64),              -- `P1:60,P2:40`（两层/三层口径不一、偶为累计值）
    _snapshot_id  BIGINT NOT NULL,
    _batch_id     VARCHAR(64) NOT NULL,
    _ingested_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_wid_well_date ON raw.water_injection_daily (well_id, prod_date);
COMMENT ON TABLE raw.water_injection_daily IS '日注水（井号+日期；逻辑 PK well_id+prod_date）——设定书 §A.3';

-- §A.4 检修台账（井号+起止；跨月切割争议源；T-3 补录 batch_id）
CREATE TABLE IF NOT EXISTS raw.well_overhaul (
    well_id         VARCHAR(8) NOT NULL,
    overhaul_start  DATE,
    overhaul_end    DATE,
    overhaul_type   VARCHAR(32),            -- 检泵/清防砂…
    affected_output VARCHAR(32),            -- 吨（各作业区估算口径不一；样本含 "约210吨" 文本形态→留 as-is 文本）
    batch_id        VARCHAR(64),            -- 补录：N-11 batch=20260911-T3
    _snapshot_id    BIGINT NOT NULL,
    _src_batch_id   VARCHAR(64) NOT NULL,   -- 湖技术列与业务 batch_id 同形不同义：_src_batch_id=进料批次
    _ingested_at    TIMESTAMP NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.well_overhaul IS '检修台账（井号+起止；补录批次 batch_id 业务列）——设定书 §A.4；湖技术列名 _src_batch_id 避让业务 batch_id';

-- §A.5 措施台账（井号+措施次；30% 行缺失 effective_from/to）
CREATE TABLE IF NOT EXISTS raw.well_measures (
    well_id          VARCHAR(8) NOT NULL,
    measure_type     VARCHAR(32),           -- 压裂/酸化/补孔/调剖
    job_date         DATE,
    effective_from   DATE,                  -- 30% 缺失（票 42 体检素材）
    effective_to     DATE,
    incr_oil_annual  NUMERIC(10,2),         -- 吨（递减增油口径，两作业区算法不一致=事故 7）
    _snapshot_id     BIGINT NOT NULL,
    _batch_id        VARCHAR(64) NOT NULL,
    _ingested_at     TIMESTAMP NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.well_measures IS '措施台账（井号+措施次）——设定书 §A.5';

-- §A.6 化验（井号+取样日期；频次不齐）
CREATE TABLE IF NOT EXISTS raw.fluid_test (
    well_id       VARCHAR(8) NOT NULL,
    sample_date   DATE NOT NULL,
    water_cut_lab NUMERIC(5,1),             -- %
    density       NUMERIC(5,3),             -- t/m³
    api_gravity   NUMERIC(5,1),
    _snapshot_id  BIGINT NOT NULL,
    _batch_id     VARCHAR(64) NOT NULL,
    _ingested_at  TIMESTAMP NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.fluid_test IS '化验（井号+取样日期；与计量含水打架）——设定书 §A.6';

-- §A.7 设备状态（字典粒度＝设备号+时间戳；当前 fieldforge 供给无时间戳列——列缺疑点已登记回报，
-- 时间戳列名待数据供给方定名后随批补列，不擅自发明业务列名）
CREATE TABLE IF NOT EXISTS raw.equip_status (
    equip_id        VARCHAR(32) NOT NULL,
    equip_type      VARCHAR(32),            -- 电泵/螺杆泵/加热炉
    load            NUMERIC(8,2),           -- %
    current         NUMERIC(8,2),           -- A
    stop_reason_code VARCHAR(32),           -- 旧 R3=皮带断 / 新 BELT（归一化素材）
    _snapshot_id    BIGINT NOT NULL,
    _batch_id       VARCHAR(64) NOT NULL,
    _ingested_at    TIMESTAMP NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.equip_status IS '设备状态（字典粒度＝设备号+时间戳，供给暂无时间戳列；load 为 SQL 非保留可裸用）——设定书 §A.7';

-- §A.8 月度结算（区块+结算月；与井口口径天然不等=事故 7 另一面）
CREATE TABLE IF NOT EXISTS raw.monthly_settlement (
    block_name  VARCHAR(16) NOT NULL,
    settle_date DATE NOT NULL,              -- 结算月（demo 口径落当日：2025-10-01）
    net_weight  NUMERIC(12,2),              -- 吨（扣自用油与输差）
    _snapshot_id BIGINT NOT NULL,
    _batch_id   VARCHAR(64) NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT now()
);
COMMENT ON TABLE raw.monthly_settlement IS '月度结算（区块+结算月；商品量口径）——设定书 §A.8';
