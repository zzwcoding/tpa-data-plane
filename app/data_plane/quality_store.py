"""dp3 质检实体落库真身＋水位回写（DP-05：质检跑批落表同源＋报水位——盘点 v2 差距 4）。

- 同源裁决（DP-05 票面）：quality_* 表族＋water_line 落**主平台库**（teach 99 册 §2.12
  ＝主平台库 90 表清单）；数据面质检跑批产出 quality_task（report jsonb 1:1）/
  quarantine_row（append-only）实体并回写 water_line 三 tier 水位，主平台 E2 质检台
  （M-05a/b）读**同一批表**——不建第二份（票 105 同源的物理落点＝同表）。
- 限写账号边界（DP-05 同源裁决，账号模板 migrations/quality_writer.sql）：数据面只写
  quality_task/quarantine_row/water_line，**quality_rule 只读**——启停＝主平台 E2 独写
  （M-05a PATCH 唯一写口），任务执行按读到的当前启用集跑（E2 端点 2 停用＝跳过不删）。
- 装配：QUALITY_DATABASE_URL 环境变量（AGG_DATABASE_URL 同款纪律——主仓 config/database.yaml
  角色白名单不动的红线不走 yaml）；未配置＝显式跳过落库（run_pipeline_once 记 skipped，
  不静默装成功）。
- 规则执行体＝transform.RULES_FULL（rule-N ↔ quality_rule.no 同号同源——种子由主仓迁移
  0015 自本体 bohai M3 投影（E2 行为约定 9 权威源），规则 13-20 仅登记不实施＝种子
  enabled=false，V1 任务只跑启用集）。
- 水位（E2 行为约定 8）：原始/清洗层＝入湖管道回写（进料闸后净批），汇总层＝agg 刷新
  水位；water_line 单调不回退（同层旧数据日期不覆盖新水位）；promise=T-1 常量随迁移
  落默认值，T-1 达标判定后端算归 E2 端点 8（本票只报水位不判达标）。
- 内存弃置路径清零（DP-05 验收 2）：质检跑批不再产内存 QualityReport——报告身份＝
  quality_task.id（QT-*），报告实体 1:1 落任务行。
"""
from __future__ import annotations

import os
from datetime import date, datetime

from app.data_plane.transform import RULES_FULL, RuleOutcome, evaluate_bundle

# 整队规则 → 主扫描表（隔离区 loc 前缀与坏行 payload 寻址；跨表规则取其发现面主表）
_RULE_TABLE: dict[str, str] = {
    "rule-1": "oil_production_daily", "rule-2": "oil_production_daily",
    "rule-3": "oil_production_daily", "rule-4": "oil_production_daily",
    "rule-5": "oil_production_daily", "rule-6": "well_overhaul",
    "rule-7": "oil_production_daily", "rule-8": "equip_status",
    "rule-9": "oil_production_daily", "rule-10": "oil_production_daily",
    "rule-11": "sensor_reading_subdaily", "rule-12": "water_injection_daily",
    "rule-13": "oil_production_daily", "rule-14": "oil_production_daily",
    "rule-15": "water_injection_daily", "rule-16": "well_info",
    "rule-17": "oil_production_daily", "rule-18": "oil_production_daily",
    "rule-19": "monthly_settlement", "rule-20": "oil_production_daily",
}

# 主扫描表 → 展示名（E2 端点 9 loc 串先例「生产日报 · N-02 · 2026-09-14」同款口径）
_TABLE_LABEL = {"oil_production_daily": "生产日报", "well_info": "井档案",
                "water_injection_daily": "注水日报", "well_overhaul": "检修台账",
                "equip_status": "设备状态", "fluid_test": "化验样",
                "monthly_settlement": "月度结算", "sensor_reading_subdaily": "传感读数"}

# 各表业务日期键（water_line data_date 提取；_BUSINESS_KEYS 日期族——无日期键的表
# 不产出水位主张：宁缺不虚标，P6 同款纪律）
_DATE_KEY: dict[str, str] = {"oil_production_daily": "prod_date",
                             "water_injection_daily": "prod_date",
                             "fluid_test": "sample_date",
                             "monthly_settlement": "settle_date",
                             "well_overhaul": "overhaul_start"}

# 定位器中的派生量（非行字段——payload 行匹配与单据去重键剔除）
_DERIVED_KEYS = {"_index", "density", "dev_pp", "dev", "ratio", "jump_x", "severity",
                 "run_days", "threshold", "max_zero_run", "value", "field"}


def database_url() -> str | None:
    """主平台库 quality 面连接串；未配置＝无落库通道，装配点显式跳过（不静默）。"""
    return os.environ.get("QUALITY_DATABASE_URL") or None


def data_date_of(table: str, rows: list[dict]) -> date | None:
    """批数据业务日期（该表日期键的最大值；无日期键/无值＝None——不产出水位主张）。"""
    key = _DATE_KEY.get(table)
    if not key:
        return None
    days = [str(r.get(key))[:10] for r in rows if r.get(key) not in (None, "")]
    return date.fromisoformat(max(days)) if days else None


def quarantine_loc(table: str, locator: dict) -> str:
    """隔离行 loc 串（E2 端点 9 展示口径「生产日报 · N-02 · 2026-09-14」同款）。"""
    label = _TABLE_LABEL.get(table, table)
    well = locator.get("well_id")
    day = locator.get("prod_date")
    if well and day:
        return f"{label} · {well} · {day}"
    if well:
        return f"{label} · {well}"
    for k in ("batch_id", "channel_id", "equip_id"):
        if locator.get(k):
            return f"{label} · {locator[k]}"
    block, month = locator.get("block_name"), locator.get("month")
    if block or month:
        return f"{label} · {block or '?'} · {month or '?'}"
    return label


def _payload_row(table: str, locator: dict, rows: list[dict]) -> dict:
    """坏行原始 payload：定位器与行字段交集全等的首行（找不到＝定位器自身如实落库）。"""
    overlap = [k for k in locator if k not in _DERIVED_KEYS
               and rows and k in rows[0]]
    for r in rows:
        if all(r.get(k) == locator[k] for k in overlap):
            return r
    return dict(locator)


def _issue_key(rule_no: int, locator: dict) -> tuple:
    """单据去重键（E2 端点 4：规则＋对象＋数据日期——定位器身份字段，派生量剔除）。"""
    ident = tuple(sorted((k, str(v)) for k, v in locator.items()
                         if k not in _DERIVED_KEYS))
    return (rule_no, ident)


def evaluate_enabled(bundle: dict, specs: list[dict]) -> list[tuple[dict, RuleOutcome]]:
    """按启用规则集跑整队质检（纯计算）：返回 (规则 spec, 单规则裁决) 序列。

    规则号 ↔ 执行体同号映射（rule-N ↔ quality_rule.no——0015 种子自同一 M3 权威源投影）；
    表有而执行体无的规则号＝如实跳过（不静默装跑过）。"""
    numbered = {int(r.rule_id.rsplit("-", 1)[-1]): r for r in RULES_FULL}
    out = []
    for spec in specs:
        rule = numbered.get(spec["no"])
        if rule is None:
            continue
        outcomes = evaluate_bundle(bundle, [rule])
        out.append((spec, outcomes[0]))
    return out


class QualityStore:
    """主平台库 quality 表族＋water_line 的数据面写读面（限写账号语义的应用侧半边）。

    写面：quality_task（INSERT 建任务＋UPDATE 完任务）/quarantine_row（仅 INSERT
    不改写——append-only；F1 幂等寻址附读权比对，不同寻址键照常追加）/water_line
    （upsert 单调）；读面：quality_rule 启用集（只读）＋quality_task 当日序号寻址。
    越界由账号授权拒绝（quality_writer.sql 闭世界）。"""

    def __init__(self, dsn: str):
        self._dsn = dsn

    @classmethod
    def from_env(cls, dsn: str | None = None) -> "QualityStore":
        dsn = dsn or database_url()
        if not dsn:
            raise ValueError("QualityStore 需要 QUALITY_DATABASE_URL（或显式传 dsn）——"
                             "限写账号模板见 migrations/quality_writer.sql")
        return cls(dsn)

    def _connect(self):
        try:
            import psycopg
        except ImportError as e:  # 已要落库却没装驱动：显式失败（同 pg_store/agg 纪律）
            raise RuntimeError("QualityStore 需要 psycopg（pip install \"psycopg[binary]\"）") from e
        return psycopg.connect(self._dsn)

    # ---- 启停读取（quality_rule 只读——启停写口＝主平台 E2 独占） ----------------

    def enabled_rules(self) -> list[dict]:
        """当前启用规则集（E2 端点 2：停用＝任务跳过不删；13-20 种子恒不启用）。"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT no, kind, rule, crit, action, autofix FROM quality_rule "
                        "WHERE enabled ORDER BY no")
            return [{"no": r[0], "kind": r[1], "rule": r[2], "crit": r[3],
                     "action": r[4], "autofix": r[5]} for r in cur.fetchall()]

    # ---- 任务 1:1 报告落库 -----------------------------------------------------

    def create_task(self, rules_count: int, *, now: datetime | None = None) -> str:
        """建任务行（status=running, step=1）；任务号 QT-YYYYMMDD-NN 当日序号递增
        （E2 端点 3 任务号规则；同刻多任务由 PK 兜底——worker max_instances=1 单飞）。
        钟源契约（DP-06.fix F2）：行 time＝**数据库东八区钟** now() AT TIME ZONE
        'Asia/Shanghai'（naive）——与外部受理行写入口径（主仓 infra.clock EAST8）同源，
        宿主机时区零耦合；now 显式传参＝测试回放缝。"""
        with self._connect() as conn, conn.cursor() as cur:
            if now is None:
                cur.execute("SELECT now() AT TIME ZONE 'Asia/Shanghai'")
                now = cur.fetchone()[0]
            ts = now.replace(microsecond=0)
            day = ts.strftime("%Y%m%d")
            cur.execute("SELECT count(*) FROM quality_task WHERE id LIKE %s",
                        (f"QT-{day}-%",))
            task_id = f"QT-{day}-{cur.fetchone()[0] + 1:02d}"
            cur.execute("INSERT INTO quality_task (id, status, step, rules_count, time) "
                        "VALUES (%s, 'running', 1, %s, %s)", (task_id, rules_count, ts))
        return task_id

    def finish_task(self, task_id: str, *, hits: int, report: dict,
                    step: int = 2) -> None:
        """完任务（status=done；报告 1:1 落 report jsonb——99 册合同键
        quarantined_rows/rejected_batches 必带）。step 语义：数据面质检跑批完成至第 2 步
        （扫描→跑规则），命中开单/责任岗分发两步归主平台 E2 接续（M-05a/b）。"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("UPDATE quality_task SET status='done', step=%s, hits=%s, "
                        "report=%s::jsonb WHERE id=%s",
                        (step, hits, _json_dumps(report), task_id))

    # ---- 拾取面（DP-06：外部任务行轮询拾取——M-05b 受理半边的闭环对边） ---------

    def pending_tasks(self) -> list[str]:
        """拾取候选：status='running' AND step=0（M-05b 冻结语义：step=0＝待拾取外部
        任务——主平台 E2 受理落行即触发面）；FIFO（time,id 序）。管道自建行
        （create_task→step=1）不在候选——拾取器只吃外部受理行。
        （DP-06.fix F3：死参 now 清除——本操作无 staleness 过滤，签名不再误导。）"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM quality_task "
                        "WHERE status='running' AND step=0 ORDER BY time, id")
            return [r[0] for r in cur.fetchall()]

    def claim_task(self, task_id: str) -> bool:
        """原子认领闸（M-05b claim_task_followup 先例同构——条件 UPDATE 单语句）：
        running∧step=0 → step=1（扫描执行中）。rowcount=1＝认领赢；0＝已被他者
        认领/已完——防重复拾取（双 worker 竞态下恰单执行）。"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("UPDATE quality_task SET step=1 "
                        "WHERE id=%s AND status='running' AND step=0", (task_id,))
            return cur.rowcount > 0

    def heal_stale_tasks(self, *, older_than_seconds: int) -> list[str]:
        """残行自愈（DP-06 票面二选一的**重置形**）：running∧step=1 且 time 超阈无更新
        →重置 step=0（可重拾）。不取 failed 形：0015 status CHECK('running','done')
        冻结（主仓零改动红线），failed 落不进去；重置形下中断行由拾取器收养重跑，
        复扫通道（轮询至 done）只是迟到、不造假结论。
        钟源契约（DP-06.fix F2）：阈值比较钟＝**数据库东八区钟** now() AT TIME ZONE
        'Asia/Shanghai'（naive，比较在 SQL 内单语句原子）——与外部受理行 time 写入口径
        （主仓 infra.clock EAST8）及本侧 create_task 写钟同源；宿主机时区零耦合
        （TZ≠部署时区不再使阈值整体偏移）。DP-06 rev F2 的宿主机 datetime.now() 形废除。
        单写者前提：heal+pick 仅 worker 进程单实例执行（build_scheduler 两作业同进程
        max_instances=1 串行）；多实例部署需先加租约列（DDL 冻结——挂 DP-06 疑点）。
        time＝行创建钟（0015 无 updated_at 列），「超阈无更新」以创建钟近似；
        阈值须远大于单任务执行时长。"""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("UPDATE quality_task SET step=0 "
                        "WHERE status='running' AND step=1 AND "
                        "time < (now() AT TIME ZONE 'Asia/Shanghai') "
                        "- (%s * interval '1 second') RETURNING id",
                        (older_than_seconds,))
            return [r[0] for r in cur.fetchall()]

    # ---- 隔离区 append-only（仅 INSERT——永不改写，与湖表同教义） ----------------

    def append_quarantine(self, rows: list[dict]) -> int:
        """隔离行落表（batch_id+rule_no+action+loc+payload）——**幂等写**（DP-06.fix
        F1）：按寻址键 (batch_id, rule_no, action, loc, payload) 先查后插（单事务内，
        照 pg_store 先查后写惯例）——同任务重执行（自愈重拾的 at-least-once 语义）
        不再重复落同键行，报告计数×物理行数对账保持；**不同寻址键照常追加**
        （append-only 语义不变——真坏行不吞）。append-only 教义不变：仅 INSERT 不
        改写（读权只用于寻址比对——quality_writer.sql 的 quarantine_row SELECT
        授权随 F1 补记；UPDATE/DELETE 仍零授权）。返回实际新插行数。"""
        if not rows:
            return 0
        inserted = 0
        with self._connect() as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute("SELECT 1 FROM quarantine_row WHERE batch_id=%s AND rule_no=%s "
                            "AND action=%s AND loc IS NOT DISTINCT FROM %s "
                            "AND payload=%s::jsonb",
                            (r["batch_id"], r["rule_no"], r["action"], r.get("loc"),
                             _json_dumps(r["payload"])))
                if cur.fetchone() is not None:
                    continue
                cur.execute("INSERT INTO quarantine_row (batch_id, rule_no, action, loc, "
                            "payload) VALUES (%s, %s, %s, %s, %s::jsonb)",
                            (r["batch_id"], r["rule_no"], r["action"], r.get("loc"),
                             _json_dumps(r["payload"])))
                inserted += 1
        return inserted

    # ---- 水位（单调不回退） -----------------------------------------------------

    def raise_water_line(self, layer: str, data_date: str | date | None) -> bool:
        """回写三 tier 水位（原始层/清洗层/汇总层）；同层旧日期不覆盖新水位（单调）。
        无业务日期（data_date=None）＝不产出水位主张（宁缺不虚标）。promise=T-1 由
        迁移默认值承载；达标判定归 E2 端点 8。返回是否实际推进。"""
        if data_date is None:
            return False
        day = data_date if isinstance(data_date, date) else date.fromisoformat(str(data_date)[:10])
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO water_line (layer, data_date) VALUES (%s, %s) "
                        "ON CONFLICT (layer) DO UPDATE SET data_date = EXCLUDED.data_date, "
                        "refreshed_at = now() WHERE EXCLUDED.data_date > water_line.data_date",
                        (layer, day))
            return cur.rowcount > 0


def run_quality_task(store, bundle: dict, *, scanned: dict | None = None,
                     task_id: str | None = None, verdict_note: str = "") -> dict:
    """质检跑批落库编排（DP-05 施工 2——质检执行体接真身，内存弃置路径清零）。

    流程：读启用集 → 建任务（running）→ 逐规则跑整队 → 隔离/拒收落 quarantine_row
    （告警类不产生隔离行——E2 端点 9）→ 报告 1:1 完任务（done）。返回摘要 dict
    （worker 干跑证据载体）。

    拾取形（DP-06——M-05b 疑点①「拾取该行而非自建」）：传 task_id＝执行**已存在的
    外部受理行**（E2 accept_task 落的 running/step=0 行，拾取器先经 claim_task 认领
    至 step=1），完成后 finish_task 照写 done/step=2——受理行的 rules_count/time 原
    样不动；verdict_note＝报告头注记（报告键集不变，只续写 verdict 文案——行形合同
    零偏差）。"""
    specs = store.enabled_rules()
    if task_id is None:
        task_id = store.create_task(rules_count=len(specs))
    pairs = evaluate_enabled(bundle, specs)

    qrows: list[dict] = []
    rejected_batches = 0
    issue_keys: set[tuple] = set()
    hits_detail: list[dict] = []
    for spec, outcome in pairs:
        if outcome.passed:
            continue
        table = _RULE_TABLE.get(outcome.rule_id, "oil_production_daily")
        rows = bundle.get(table, [])
        for loc in outcome.locators:
            if spec["action"] == "隔离":
                qrows.append({"batch_id": task_id, "rule_no": spec["no"],
                              "action": "隔离", "loc": quarantine_loc(table, loc),
                              "payload": _payload_row(table, loc, rows)})
            issue_keys.add(_issue_key(spec["no"], loc))
        if spec["action"] == "整批拒收":  # 批次级一条记录（E2 端点 9）
            rejected_batches += 1
            qrows.append({"batch_id": task_id, "rule_no": spec["no"],
                          "action": "整批拒收", "loc": f"{quarantine_loc(table, {})} · 批次级",
                          "payload": {"locators": outcome.locators[:5]}})
        hits_detail.append({"rule_no": spec["no"], "kind": spec["kind"],
                            "action": spec["action"], "hit_count": outcome.hit_count,
                            "sample": outcome.locators[:5]})
    store.append_quarantine(qrows)

    report = {
        "range": "生产库 8 表",
        "tables": scanned or {t: len(r) for t, r in bundle.items()},
        "scanned_rows": sum(len(r) for r in bundle.values()),
        "rules_run": len(pairs),
        "quarantined_rows": len(qrows),           # 99 册合同键（数据粒度）
        "rejected_batches": rejected_batches,      # 99 册合同键（数据粒度）
        "hits": hits_detail,                       # 命中明细（单据粒度计数见 hits 列）
        "verdict": (f"质检完成：{len(pairs)} 条启用规则，命中 {len(issue_keys)} 单，"
                    "隔离区落行见 quarantine_row——问题单建单与责任岗分发归主平台 E2 接续"
                    + (f"；{verdict_note}" if verdict_note else "")),
    }
    store.finish_task(task_id, hits=len(issue_keys), report=report)
    return {"task_id": task_id, "rules_run": len(pairs),
            "failed_rules": [spec["no"] for spec, o in pairs if not o.passed],
            "quarantined_rows": len(qrows), "rejected_batches": rejected_batches,
            "hits": len(issue_keys)}


def _json_dumps(v) -> str:
    import json
    return json.dumps(v, ensure_ascii=False, default=str)
