"""dp3 出汇总层：agg 星型三表构建＋主平台库直写（DP-04，ADR-0003 裁决 15）。

- 模型合同＝teach 99 册 §2.12 末行（D6 star_model 合同值）：agg.dim_well（井维度，
  well_id PK＋区块/油田层级属性列）/ agg.dim_date（日期维度，date PK＋月/季对照列——
  **数据面自建**：自 calendar 确定性生成，无业务发明〔Owner 方针，票面点名〕）/
  agg.fct_daily_metering（事实表：产油/产液/油压，**grain=一井×一天**，A1 问数链 SELECT
  唯一落点）。物理列名沿源口径（设定书 §A.1/§A.2）。
- 替换 v2 parquet marts 三模型（fct_block_monthly/fct_valid_output_monthly/fct_wellset_annual
  ——模型名/粒度与 99 册不符，盘点 v2 差距 3），parquet 物化路径随之退役（transform.py）。
- 直写主平台库＝**限写三表专用账号**（99 册 §2.12/§4.4：账号建立随数据面部署批；SQL 模板
  migrations/agg_writer.sql——只能写 agg 恰三表＋读中台库 clean 层，其余默认拒绝）；连接经
  **AGG_DATABASE_URL** 环境变量装配（主仓 config/database.yaml 角色白名单 _ROLES 无 agg 角色，
  主仓 app/ 一行不动红线下不走 database.yaml——随部署批再议，回报疑点登记）。
- 写面白名单：本模块运行时只许 upsert 恰三张合同表（表名白名单硬编码，越名显式拒绝）；
  结构演进（DDL）归主仓迁移链 0014（Alembic），运行时零 DDL、零建号。
- 幂等：三表全部 upsert（ON CONFLICT DO UPDATE）——同源重刷行数不翻倍（grain 由组合 PK
  与质检闸 rule-1 双保险）。
- quality_flag：99 册合同列但**值域无冻结枚举**——本管道写 NULL（业务事实零发明），
  值域随 DP-05 质检实体落库票/Owner 裁决对齐。
"""
from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal

Rows = list[dict]

# 合同三表（99 册 §2.12）：内部键＝裸表名（build_agg 输出形）；schema 限定 agg 由
# DDL（主仓迁移 0014）与限写账号（agg_writer.sql）双层承载。
_AGG_TABLES = ("dim_well", "dim_date", "fct_daily_metering")

_FCT_COLUMNS = ("well_id", "prod_date", "oil_output", "liquid_output", "oil_pressure",
                "batch_id", "quality_flag")


def _num(v) -> float | None:
    """数值列定型（as-is 读入的文本/Decimal/None 归一；解析失败显式拒绝不静默改写）。"""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float, Decimal)):
        return float(v)
    try:
        return float(str(v))
    except ValueError as e:
        raise ValueError(f"agg 数值列收到不可定型值 {v!r}（as-is 纪律：不改写，显式拒绝）") from e


def _date(v) -> date | None:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def _text(v) -> str | None:
    return None if v is None or v == "" else str(v)


def dim_date_rows(start: str | date, end: str | date) -> list[dict]:
    """日期维度行（闭区间）：date＋月/季对照列——确定性日历生成（数据面自建，零业务发明）。"""
    d, stop = _date(start), _date(end)
    rows = []
    while d <= stop:
        rows.append({"date": d.isoformat(), "month": d.strftime("%Y-%m"),
                     "quarter": f"{d.year}-Q{(d.month - 1) // 3 + 1}"})
        d = date.fromordinal(d.toordinal() + 1)
    return rows


def build_agg(prod_rows: Rows, well_rows: Rows) -> dict[str, list[dict]]:
    """星型三表构建（纯函数）。

    - fct：grain=一井×一天。质检闸 rule-1（进料前剔重保首条）＋湖 as-of 时点视图
      （每业务键取最新批）已保证唯一，此处**防御性再折叠**（同键出现即保首条——与闸
      同语义）；batch_id＝批次水位（湖技术列 _batch_id，随行溯源）；quality_flag=NULL
      （值域无合同枚举，零发明）。
    - dim_well：well_id 键＋block_name/field_name 层级属性（99 册合同列——源口径 §A.1），
      同井多行折叠保首条。
    - dim_date：覆盖 fct prod_date 全域（闭区间；无数据＝空维——不预生成未来日历，
      不发明范围外日期）。
    """
    fct: dict[tuple, dict] = {}
    for r in prod_rows:
        key = (_text(r.get("well_id")), _date(r.get("prod_date")))
        if key == (None, None):
            continue
        if key in fct:  # 防御性折叠：与质检闸 rule-1 保首条同语义（正常数据流不触达）
            continue
        fct[key] = {"well_id": key[0], "prod_date": key[1].isoformat(),
                    "oil_output": _num(r.get("oil_output")),
                    "liquid_output": _num(r.get("liquid_output")),
                    "oil_pressure": _num(r.get("oil_pressure")),
                    "batch_id": _text(r.get("_batch_id") or r.get("batch_id")),
                    "quality_flag": None}
    dim_well: dict[str, dict] = {}
    for w in well_rows:
        wid = _text(w.get("well_id"))
        if wid is None or wid in dim_well:
            continue
        dim_well[wid] = {"well_id": wid,
                         "block_name": _text(w.get("block_name")),
                         "field_name": _text(w.get("field_name"))}
    dates = sorted(k[1] for k in fct if k[1] is not None)
    dim_date = dim_date_rows(dates[0], dates[-1]) if dates else []
    return {"dim_well": list(dim_well.values()), "dim_date": dim_date,
            "fct_daily_metering": list(fct.values())}


class AggStarWriter:
    """主平台库 agg 三表直写（限写账号语义的应用侧半边：写面白名单＋恰三表 upsert）。"""

    TABLES = _AGG_TABLES

    def __init__(self, dsn: str):
        self._dsn = dsn

    @classmethod
    def from_env(cls, dsn: str | None = None) -> "AggStarWriter":
        """环境装配方：AGG_DATABASE_URL（限写三表专用账号连接串——部署批消费）。"""
        dsn = dsn or os.environ.get("AGG_DATABASE_URL")
        if not dsn:
            raise ValueError("AggStarWriter 需要 AGG_DATABASE_URL（或显式传 dsn）——"
                             "限写账号模板见 migrations/agg_writer.sql")
        return cls(dsn)

    def _validate_tables(self, agg_rows: dict[str, Rows]) -> None:
        """写面白名单（应用侧半边）：恰合同三表，越名显式拒绝（先于任何连接）。"""
        unknown = set(agg_rows) - set(self.TABLES)
        if unknown:
            raise ValueError(f"表 {sorted(unknown)} 不在 agg 写面白名单 {list(self.TABLES)}"
                             "（限写三表专用账号——越名拒绝；合同表＝99 册 §2.12）")

    def write(self, agg_rows: dict[str, Rows]) -> dict[str, int]:
        """三表 upsert（单连接逐表提交，失败即抛不静默半刷账）；返回 {裸表名: 行数}。"""
        self._validate_tables(agg_rows)
        counts = {}
        with self._connect() as conn, conn.cursor() as cur:
            for row in agg_rows.get("dim_well", []):
                cur.execute("""INSERT INTO agg.dim_well (well_id, block_name, field_name)
                               VALUES (%s, %s, %s)
                               ON CONFLICT (well_id) DO UPDATE
                               SET block_name = EXCLUDED.block_name,
                                   field_name = EXCLUDED.field_name""",
                            (row["well_id"], row["block_name"], row["field_name"]))
            counts["dim_well"] = len(agg_rows.get("dim_well", []))
            for row in agg_rows.get("dim_date", []):
                cur.execute("""INSERT INTO agg.dim_date (date, month, quarter)
                               VALUES (%s, %s, %s)
                               ON CONFLICT (date) DO UPDATE
                               SET month = EXCLUDED.month, quarter = EXCLUDED.quarter""",
                            (row["date"], row["month"], row["quarter"]))
            counts["dim_date"] = len(agg_rows.get("dim_date", []))
            for row in agg_rows.get("fct_daily_metering", []):
                cur.execute(f"""INSERT INTO agg.fct_daily_metering ({", ".join(_FCT_COLUMNS)})
                                VALUES ({", ".join(["%s"] * len(_FCT_COLUMNS))})
                                ON CONFLICT (well_id, prod_date) DO UPDATE
                                SET oil_output = EXCLUDED.oil_output,
                                    liquid_output = EXCLUDED.liquid_output,
                                    oil_pressure = EXCLUDED.oil_pressure,
                                    batch_id = EXCLUDED.batch_id,
                                    quality_flag = EXCLUDED.quality_flag""",
                            tuple(row[c] for c in _FCT_COLUMNS))
            counts["fct_daily_metering"] = len(agg_rows.get("fct_daily_metering", []))
        return counts

    def _connect(self):
        try:
            import psycopg
        except ImportError as e:  # 已要直写却没装驱动：显式失败（同 pg_store 纪律）
            raise RuntimeError("AggStarWriter 需要 psycopg（pip install \"psycopg[binary]\"）") from e
        return psycopg.connect(self._dsn)


def refresh_from_catalog(catalog, writer, freshness=None, water_line=None,
                         pipeline: str = "dp3-agg-refresh"):
    """出汇总层编排：湖 as-of 当前视图（清洗语义＝闸后净批＋每键最新批）→ 星型构建 → 直写。

    返回 {表名: 行数} 摘要；freshness seam 在位则登记汇总层三表（offline 档——
    三 tier 模型的批域档位，as_of=刷新完成时刻）；water_line seam 在位则回写汇总层
    报水位（E2 行为约定 8：汇总层＝agg 刷新水位——data_date=fct 最大业务日期，
    promise=T-1 判定归 E2 端点 8 服务端算）。"""
    prod = catalog.load_table("oil_production_daily").scan().to_pylist()
    wells = catalog.load_table("well_info").scan().to_pylist()
    agg_rows = build_agg(prod, wells)
    if not agg_rows["fct_daily_metering"]:
        return {"fct_daily_metering": 0, "note": "无数据未刷（不硬刷空账）"}
    counts = writer.write(agg_rows)
    if freshness is not None:
        as_of = datetime.now().replace(microsecond=0)
        for t in ("agg.dim_well", "agg.dim_date", "agg.fct_daily_metering"):
            freshness.register(t, tier="offline", as_of=as_of, pipeline=pipeline)
    if water_line is not None:
        days = [r["prod_date"] for r in agg_rows["fct_daily_metering"] if r["prod_date"]]
        water_line.raise_water_line("汇总层", max(days))
    return counts
