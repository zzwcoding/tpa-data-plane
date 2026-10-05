"""DP-05 · quality 表族落库＋water_line 报水位 PG 实跑（票面验收 (a)-(d)）。

- (a) 质检跑批落库：quality_task 报告 1:1＋quarantine_row append-only 不改写；
- (b) 规则启停读取：改 quality_rule.enabled → 任务按当前启用集跑（停用＝跳过不删）；
- (c) water_line 三 tier 更新：原始/清洗/汇总层回写＋单调不回退＋promise=T-1 常量；
- (d) 限写账号边界：quality_writer.sql 模板实装——quality_rule 只读（UPDATE/INSERT 拒）、
  quarantine_row 仅 INSERT（UPDATE/DELETE 拒＝账号级 append-only）、越界表零授权。
- 六表＋种子由**主仓 Alembic 0015** 供应（结构零第二真源——未跑迁移整文件 skip，
  提示先在主仓 `alembic upgrade head`）；限写账号模板纪律同 agg_writer.sql（DP-04 先例）：
  建号随部署批，测试自带建号＋清场；
- 无 TEST_DATABASE_URL 或缺 psycopg 时整文件 skip（与 tests/pg 同机制）。
"""
from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from app.data_plane import quality_store as quality_store_mod
from app.data_plane.quality_store import QualityStore, data_date_of, run_quality_task

TEST_DSN = os.environ.get("TEST_DATABASE_URL")
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "fieldforge"


def _task_likes() -> list[str]:
    """清场模式与 create_task 同时钟源（python 本地 now）取今昨两日——PG UTC 与本地
    跨日界不漏删（DP-04 疑点 5 时区纪律同款）。"""
    return [f"QT-{datetime.now().strftime('%Y%m%d')}%",
            f"QT-{(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')}%"]

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="需要 TEST_DATABASE_URL 指向可写 PG（quality 表族实跑，主仓 compose）")


@pytest.fixture(scope="module")
def quality_ready():
    """六表由主仓迁移 0015 供应；未跑迁移则整文件 skip（结构零第二真源）。"""
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quality_rule') IS NOT NULL "
                    "AND to_regclass('quality_task') IS NOT NULL "
                    "AND to_regclass('quarantine_row') IS NOT NULL "
                    "AND to_regclass('water_line') IS NOT NULL")
        if not cur.fetchone()[0]:
            pytest.skip("quality 表族未落地——先在主仓跑 `alembic upgrade head`（0015）")


@pytest.fixture()
def store(quality_ready):
    st = QualityStore(TEST_DSN)
    likes = _task_likes()
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        for like in likes:
            cur.execute("DELETE FROM quarantine_row WHERE batch_id LIKE %s", (like,))
            cur.execute("DELETE FROM quality_task WHERE id LIKE %s", (like,))
        cur.execute("DELETE FROM water_line")
    yield st
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        for like in likes:
            cur.execute("DELETE FROM quarantine_row WHERE batch_id LIKE %s", (like,))
            cur.execute("DELETE FROM quality_task WHERE id LIKE %s", (like,))
        cur.execute("DELETE FROM water_line")


def _q(sql, params=None):
    with psycopg.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return []
        return cur.fetchall()


def _bundle(dirty: bool = True) -> dict:
    """fixture 全表 bundle；dirty=True 追加一行空油量（rule-2 非空率越界挂——隔离档；
    独立井/日避免与 fixture 首行同键混淆 payload 寻址）。"""
    bundle = {f.stem: list(csv.DictReader(open(f, encoding="utf-8")))
              for f in sorted(FIXTURES.glob("*.csv"))}
    if dirty:
        rows = bundle["oil_production_daily"]
        bad = dict(rows[0])
        bad.update({"oil_output": None, "well_id": "Z-98", "prod_date": "2025-09-30"})
        bundle["oil_production_daily"] = rows + [bad]
    return bundle


class TestQualityTaskPersistence:
    """验收 (a)：质检跑批落库——报告 1:1＋隔离区 append-only 不改写。"""

    def test_run_lands_task_report_and_quarantine(self, store):
        summary = run_quality_task(store, _bundle())
        assert summary["task_id"].startswith("QT-") and summary["rules_run"] == 12
        task = _q("SELECT status, step, rules_count, hits, report FROM quality_task "
                  "WHERE id=%s", (summary["task_id"],))
        assert len(task) == 1                              # 任务 1:1 报告（一行）
        status, step, rules_count, hits, report = task[0]
        assert (status, step, rules_count) == ("done", 2, 12)
        assert hits == summary["hits"] >= 1
        assert report["quarantined_rows"] == summary["quarantined_rows"] >= 1
        assert report["rejected_batches"] == 0             # v0.4 后种子无整批拒收档规则
        # 隔离行：rule-2 隔离档坏行落区，告警档（rule-12 等）零隔离行（E2 端点 9）
        rows = _q("SELECT rule_no, action, loc, payload FROM quarantine_row "
                  "WHERE batch_id=%s ORDER BY id", (summary["task_id"],))
        assert report["quarantined_rows"] == len(rows)     # 报告计数与隔离区对账
        assert {r[0] for r in rows} == {2}                 # 恰隔离档规则落行
        assert all(r[1] == "隔离" and r[2].startswith("生产日报 · ") for r in rows)
        assert all(r[3]["well_id"] and r[3]["oil_output"] is None for r in rows)  # 原始坏行 payload

    def test_quarantine_append_only_never_rewritten(self, store):
        """两次跑批：第二任务只增不改——首任务隔离行逐列原样（append-only 与湖表同教义）。"""
        s1 = run_quality_task(store, _bundle())
        before = _q("SELECT id, batch_id, rule_no, action, loc, payload, created_at "
                    "FROM quarantine_row WHERE batch_id=%s ORDER BY id", (s1["task_id"],))
        s2 = run_quality_task(store, _bundle())
        assert s2["task_id"] != s1["task_id"]              # 任务号当日序号递增
        after = _q("SELECT id, batch_id, rule_no, action, loc, payload, created_at "
                   "FROM quarantine_row WHERE batch_id=%s ORDER BY id", (s1["task_id"],))
        assert before == after                             # 首任务隔离行零改写
        n2 = _q("SELECT count(*) FROM quarantine_row WHERE batch_id=%s",
                (s2["task_id"],))[0][0]
        assert n2 == len(before)                           # 第二任务另起行（只增）


class TestRuleToggle:
    """验收 (b)：规则启停读取——改 quality_rule.enabled → 任务按当前启用集跑。"""

    def test_seed_enabled_set(self, store):
        rules = store.enabled_rules()
        assert [r["no"] for r in rules] == list(range(1, 13))   # 种子：1-12 启用
        nos = {r[0] for r in _q("SELECT no FROM quality_rule WHERE enabled")}
        assert nos == set(range(1, 13))                         # 13-20 仅登记不实施（enabled=false）

    def test_disabled_rule_skipped_by_task(self, store):
        try:
            _q("UPDATE quality_rule SET enabled=false WHERE no=2")
            assert 2 not in {r["no"] for r in store.enabled_rules()}   # 停用＝跳过不删
            summary = run_quality_task(store, _bundle())               # 脏批仍在（rule-2 已停）
            assert summary["rules_run"] == 11
            assert 2 not in summary["failed_rules"]
            assert summary["quarantined_rows"] == 0                    # 唯一隔离档命中已停用
            task = _q("SELECT rules_count FROM quality_task WHERE id=%s",
                      (summary["task_id"],))[0][0]
            assert task == 11
        finally:
            _q("UPDATE quality_rule SET enabled=true WHERE no=2")      # 复位种子
        assert 2 in {r["no"] for r in store.enabled_rules()}


class TestWaterLine:
    """验收 (c)：water_line 三 tier 更新——回写/单调不回退/T-1 常量。"""

    def test_three_tiers_raised_and_monotonic(self, store):
        assert store.raise_water_line("原始层", "2025-09-07")
        assert store.raise_water_line("清洗层", "2025-09-07")
        assert store.raise_water_line("汇总层", "2025-09-07")
        rows = sorted(_q("SELECT layer, data_date, promise FROM water_line"),
                      key=lambda r: ["原始层", "清洗层", "汇总层"].index(r[0]))
        assert [(r[0], str(r[1]), r[2]) for r in rows] == [
            ("原始层", "2025-09-07", "T-1"), ("清洗层", "2025-09-07", "T-1"),
            ("汇总层", "2025-09-07", "T-1")]
        assert not store.raise_water_line("原始层", "2025-09-01")   # 旧数据日期不回退水位
        assert str(_q("SELECT data_date FROM water_line WHERE layer='原始层'")[0][0]) == "2025-09-07"
        assert store.raise_water_line("原始层", "2025-09-08")       # 新批推进
        assert str(_q("SELECT data_date FROM water_line WHERE layer='原始层'")[0][0]) == "2025-09-08"

    def test_no_business_date_no_watermark_claim(self, store):
        assert data_date_of("well_info", [{"well_id": "N-01"}]) is None   # 无日期键不虚标
        assert not store.raise_water_line("汇总层", None)
        assert _q("SELECT count(*) FROM water_line")[0][0] == 0


class TestRestrictedWriterAccount:
    """验收 (d)：限写账号边界——数据面写不了 quality_rule（启停＝主平台 E2 独写）。"""

    @pytest.fixture()
    def writer_role(self, quality_ready):
        tpl = (Path(__file__).resolve().parents[1] / ".." / "migrations" / "quality_writer.sql")
        sql = tpl.read_text(encoding="utf-8").replace("${QUALITY_WRITER_PASSWORD}", "dp5-probe-pw")
        self._drop_role()
        for chunk in sql.split(";"):  # 逐句执行（剥注释行——模板头注不进语句）
            stmt = "\n".join(l for l in chunk.splitlines()
                             if not l.strip().startswith("--")).strip()
            if stmt:
                _q(stmt)
        yield "postgresql://tpa_quality_writer:dp5-probe-pw@%s/petro" % (
            TEST_DSN.split("@")[1].split("/")[0],)
        self._drop_role()

    @staticmethod
    def _drop_role():
        _q("""DO $$
             BEGIN
               IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tpa_quality_writer') THEN
                 DROP OWNED BY tpa_quality_writer;
                 DROP ROLE tpa_quality_writer;
               END IF;
             END $$""")

    def _denied(self, dsn, sql, params=()):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(sql, params)
                conn.rollback()

    def test_write_boundary_exact(self, writer_role):
        dsn = writer_role
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM quality_rule")               # 规则只读放行
            cur.execute("INSERT INTO quality_task (id, status, step, rules_count, time) "
                        "VALUES ('QT-probe-01', 'running', 1, 0, now())")  # 任务写放行
            cur.execute("UPDATE quality_task SET status='done' WHERE id='QT-probe-01'")
            cur.execute("INSERT INTO quarantine_row (batch_id, rule_no, action, loc, payload) "
                        "VALUES ('QT-probe-01', 2, '隔离', 'probe', '{\"x\": 1}'::jsonb)")
            cur.execute("INSERT INTO water_line (layer, data_date) VALUES ('原始层', '2025-09-07') "
                        "ON CONFLICT (layer) DO UPDATE SET data_date = EXCLUDED.data_date")
        _q("DELETE FROM quarantine_row WHERE batch_id='QT-probe-01'")
        _q("DELETE FROM quality_task WHERE id='QT-probe-01'")
        _q("DELETE FROM water_line WHERE layer='原始层' AND data_date='2025-09-07'")

    def test_quality_rule_write_denied(self, writer_role):
        """启停＝主平台 E2 独写：数据面限写账号 UPDATE/INSERT quality_rule 一律拒绝。"""
        dsn = writer_role
        self._denied(dsn, "UPDATE quality_rule SET enabled=false WHERE no=2")
        self._denied(dsn, "INSERT INTO quality_rule (no, kind, rule, crit, enabled, action, "
                          "autofix) VALUES (99, '钩稽', 'x', 'y', true, '告警', false)")

    def test_quarantine_append_only_account_level(self, writer_role):
        """append-only 账号级承载：隔离行 UPDATE/DELETE 拒（仅 INSERT 放行）。"""
        dsn = writer_role
        self._denied(dsn, "UPDATE quarantine_row SET loc='hacked' WHERE id=1")
        self._denied(dsn, "DELETE FROM quarantine_row WHERE id=1")

    def test_quarantine_idempotent_append_under_restricted_account(self, writer_role):
        """F1（DP-06.fix）：幂等落库＝先查后插——限写账号需 quarantine_row 读权
        （寻址键比对）；append-only 不松（UPDATE/DELETE 仍拒，见上测）。"""
        dsn = writer_role
        store = QualityStore(dsn)
        row = {"batch_id": "QT-probe-02", "rule_no": 2, "action": "隔离",
               "loc": "生产日报 · Z-98 · 2025-09-30",
               "payload": {"well_id": "Z-98", "oil_output": None}}
        try:
            assert store.append_quarantine([row]) == 1             # 首插 1
            assert store.append_quarantine([row]) == 0             # 同键重放＝0（F1 幂等）
            assert store.append_quarantine(
                [dict(row, loc="生产日报 · Z-97 · 2025-09-29")]) == 1  # 异键照插（不吞真坏行）
            n = _q("SELECT count(*) FROM quarantine_row WHERE batch_id='QT-probe-02'")[0][0]
            assert n == 2
        finally:
            _q("DELETE FROM quarantine_row WHERE batch_id='QT-probe-02'")
        assert _q("SELECT count(*) FROM quarantine_row WHERE batch_id='QT-probe-02'")[0][0] == 0

    def test_outside_tables_denied(self, writer_role):
        dsn = writer_role
        self._denied(dsn, "INSERT INTO raw.oil_production_daily "
                          "(well_id, prod_date, _snapshot_id, _batch_id) "
                          "VALUES ('ZZ-00','2025-09-01',1,'dp5-probe')")   # 中台 raw 层
        self._denied(dsn, "INSERT INTO agg.dim_well (well_id) VALUES ('ZZ-00')")  # agg 三表
        self._denied(dsn, "CREATE TABLE public.dp5_evil (x int)")                  # 无 CREATE
        self._denied(dsn, "ALTER TABLE quality_task ADD COLUMN hack int")          # 无 ALTER

    def test_template_grants_are_closed_world(self):
        """模板闭世界机判：授权恰 7 句——rule 只读＋task 写读＋quarantine 幂等读＋仅
        INSERT（bigserial 序列 USAGE 随 INSERT 需要）＋water 写读。F1 幂等寻址需
        quarantine_row 读权（先查后插）；UPDATE/DELETE 仍零授权（append-only 不松）。"""
        tpl = (Path(__file__).resolve().parents[1] / ".." / "migrations"
               / "quality_writer.sql").read_text(encoding="utf-8")
        grant_lines = [l.rstrip(";") for l in tpl.splitlines() if l.strip().startswith("GRANT")]
        assert len(grant_lines) == 7
        assert "GRANT SELECT ON quality_rule TO tpa_quality_writer" in grant_lines
        assert "GRANT SELECT ON quarantine_row TO tpa_quality_writer" in grant_lines
        assert "GRANT INSERT ON quarantine_row TO tpa_quality_writer" in grant_lines
        assert "GRANT USAGE, SELECT ON SEQUENCE quarantine_row_id_seq TO tpa_quality_writer" \
            in grant_lines
        assert not any("UPDATE" in l and "quarantine_row" in l for l in grant_lines)
        assert not any("DELETE" in l and "quarantine_row" in l for l in grant_lines)
        assert not any("UPDATE" in l and "quality_rule" in l for l in grant_lines)


# ---------- DP-06：拾取器行形合同 PG 实跑（M-05b 集成替身同 DDL 对账） ----------

# 行形合同冻结字面（照抄主仓 tests/domains/dataops/quality/test_quality_task_channel.py
# 集成替身 _report()/_hit() 键集——零共享库：不 import 主仓任何代码）
_STUB_REPORT_KEYS = {"range", "tables", "scanned_rows", "rules_run",
                     "quarantined_rows", "rejected_batches", "hits", "verdict"}
_STUB_HIT_KEYS = {"rule_no", "kind", "action", "hit_count", "sample"}


class TestTaskPickup:
    """DP-06 拾取器对真 0015 DDL 复证（hermetic 替身的 PG 半边）：
    受理行形（M-05b 冻结：running/step=0）→ 拾取执行 → done/step=2/报告 1:1/隔离行
    batch_id=任务号；认领闸单发；残行自愈重置可重拾。任务号用 -9x 序段（当日 LIKE
    清场模式内，不与取号测试碰撞）。"""

    def _accept(self, task_id: str, *, step: int = 0, minutes_ago: int = 0) -> None:
        """受理行直插（主平台 E2 accept_task 的行形：running/step=0/rules_count/time）。"""
        ts = datetime.now().replace(microsecond=0) - timedelta(minutes=minutes_ago)
        _q("INSERT INTO quality_task (id, status, step, rules_count, time) "
           "VALUES (%s, 'running', %s, 12, %s)", (task_id, step, ts))

    def _pickup(self, store):
        from worker.scheduler import PipelineConfig, run_pickup_once
        cfgx = PipelineConfig(run_dir=Path("/nonexistent"), freshness=None,
                              quality_store=store)
        return run_pickup_once(cfgx, bundle=_bundle())

    def _row(self, task_id: str):
        rows = _q("SELECT status, step, rules_count, hits, report FROM quality_task "
                  "WHERE id=%s", (task_id,))
        assert len(rows) == 1
        return rows[0]

    def test_full_chain_pickup_lands_done_step2_report_one_to_one(self, store):
        tid = f"QT-{datetime.now().strftime('%Y%m%d')}-91"
        self._accept(tid, minutes_ago=120)
        summary = self._pickup(store)
        assert [p["task_id"] for p in summary["picked"]] == [tid]
        status, step, rules_count, hits, report = self._row(tid)
        assert (status, step) == ("done", 2)                       # 行形：拾取完＝done/step=2
        assert rules_count == 12                                   # 受理行字段不被改写
        assert hits == summary["picked"][0]["hits"] >= 1           # hits=单据数（替身同语义）
        assert set(report) == _STUB_REPORT_KEYS                    # 恰八键——零形状偏差
        assert report["quarantined_rows"] == summary["picked"][0]["quarantined_rows"]
        # 三落地同源：隔离行 batch_id=任务号；恰隔离档落行
        qrows = _q("SELECT rule_no, action, payload FROM quarantine_row WHERE batch_id=%s",
                   (tid,))
        assert qrows and {r[0] for r in qrows} == {2}
        assert all(r[1] == "隔离" and r[2]["well_id"] for r in qrows)  # 原始坏行 payload

    def test_report_hits_entry_shape_matches_stub(self, store):
        tid = f"QT-{datetime.now().strftime('%Y%m%d')}-92"
        self._accept(tid, minutes_ago=120)
        self._pickup(store)
        _, _, _, _, report = self._row(tid)
        assert report["hits"]
        for entry in report["hits"]:
            assert set(entry) == _STUB_HIT_KEYS                    # hits 条目五键逐字段
            assert len(entry["sample"]) <= 5                       # sample 截 5（R2 观察项）
        assert "R2" in report["verdict"]                           # 报告头注记登记

    def test_claim_gate_is_one_shot(self, store):
        tid = f"QT-{datetime.now().strftime('%Y%m%d')}-93"
        self._accept(tid)
        assert store.claim_task(tid) is True                       # 首认领赢（条件 UPDATE）
        assert store.claim_task(tid) is False                      # 二次认领＝False
        assert self._row(tid)[1] == 1                              # step=1（执行中）

    def test_stale_step1_heals_and_repicks(self, store):
        """crash 残行（step=1 超阈无更新）→自愈重置 step=0→重拾→done/step=2。"""
        tid = f"QT-{datetime.now().strftime('%Y%m%d')}-94"
        self._accept(tid, step=1, minutes_ago=120)                 # 认领后中断的残行
        fresh = f"QT-{datetime.now().strftime('%Y%m%d')}-95"
        self._accept(fresh, step=1)                                # 刚认领（在途）——不动
        summary = self._pickup(store)
        assert [p["task_id"] for p in summary["picked"]] == [tid]
        assert summary["healed"] == [tid]
        assert self._row(tid)[:2] == ("done", 2)
        assert self._row(fresh)[1] == 1                            # 在途行不被自愈
        assert self._row(fresh)[0] == "running"

    # ---- DP-06.fix：F1 隔离行幂等 ＋ F2 heal 时钟源（rev 发现清单修复回归钉） ----

    def test_reexecution_does_not_duplicate_quarantine_rows(self, store):
        """F1（DP-06.rev）：at-least-once 重执行（append 后 finish 前中断→自愈重拾）——
        同任务重执行不得重复落同寻址键隔离行；报告计数×物理行数对账保持。"""
        tid = f"QT-{datetime.now().strftime('%Y%m%d')}-96"
        self._accept(tid, minutes_ago=120)
        s1 = run_quality_task(store, _bundle(), task_id=tid)       # 首执行（隔离行已落）
        assert s1["quarantined_rows"] == 1
        _q("UPDATE quality_task SET status='running', step=0 WHERE id=%s", (tid,))  # 残行回卷
        summary = self._pickup(store)                              # 次周期重拾重执行
        assert [p["task_id"] for p in summary["picked"]] == [tid]
        rows = _q("SELECT rule_no, loc, payload FROM quarantine_row WHERE batch_id=%s", (tid,))
        assert len(rows) == s1["quarantined_rows"] == 1            # 两周期 1→1 不翻倍（F1）
        assert self._row(tid)[4]["quarantined_rows"] == len(rows)  # 报告×物理对账

    def test_heal_judgment_reads_db_clock_survives_host_shift(self, store, monkeypatch):
        """F2（DP-06.rev）：heal 阈值比较钟＝数据库东八区钟（与受理行 time 写入口径
        同源）——宿主机钟偏移（TZ≠部署时区模拟）不使阈值失效。受理行按 DB 东八区钟落
        （主仓 infra.clock 同源口径）；老实现拿宿主机 naive 钟比较，偏移钟下残行判
        「恒新鲜」永不自愈——此处红先。"""
        tid = f"QT-{datetime.now().strftime('%Y%m%d')}-97"
        _q("INSERT INTO quality_task (id, status, step, rules_count, time) VALUES "
           "(%s, 'running', 1, 12, (now() AT TIME ZONE 'Asia/Shanghai') - interval '2 hours')",
           (tid,))
        monkeypatch.setattr(quality_store_mod, "datetime",
                            _shifted_host(timedelta(hours=-8)))    # 宿主机钟慢 8h（UTC 容器形）
        summary = self._pickup(store)
        assert summary["healed"] == [tid]                          # DB 钟判定：2h 前残行照常自愈
        assert [p["task_id"] for p in summary["picked"]] == [tid]
        assert self._row(tid)[:2] == ("done", 2)

    def test_inflight_row_not_healed_under_host_shift(self, store, monkeypatch):
        """F2 另一半（rev「在途行 60s 内可被误重置」放大半边）：宿主机钟「快」于行写入
        钟时，在途行不得被误判超阈——老实现拿宿主机钟比较会把刚认领的行立即重置
        →双执行。新实现读 DB 钟——不受宿主机偏移影响。"""
        tid = f"QT-{datetime.now().strftime('%Y%m%d')}-98"
        _q("INSERT INTO quality_task (id, status, step, rules_count, time) VALUES "
           "(%s, 'running', 1, 12, (now() AT TIME ZONE 'Asia/Shanghai') - interval '30 seconds')",
           (tid,))
        monkeypatch.setattr(quality_store_mod, "datetime",
                            _shifted_host(timedelta(hours=1)))     # 宿主机钟快 1h（TZ=UTC+9 形）
        summary = self._pickup(store)
        assert summary["healed"] == [] and summary["picked"] == []
        row = self._row(tid)
        assert (row[0], row[1]) == ("running", 1)                  # 在途行原样（执行中）


def _shifted_host(shift: timedelta):
    """宿主机钟偏移模拟（F2 红先取证）：monkeypatch quality_store 模块全局 datetime——
    老实现 heal 从模块全局取宿主机钟；DB 钟实现不经过此名（偏移对其不可见）。"""
    class _Shifted(datetime):
        _shift = shift

        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) + cls._shift
    return _Shifted
