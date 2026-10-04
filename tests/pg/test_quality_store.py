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

    def test_outside_tables_denied(self, writer_role):
        dsn = writer_role
        self._denied(dsn, "INSERT INTO raw.oil_production_daily "
                          "(well_id, prod_date, _snapshot_id, _batch_id) "
                          "VALUES ('ZZ-00','2025-09-01',1,'dp5-probe')")   # 中台 raw 层
        self._denied(dsn, "INSERT INTO agg.dim_well (well_id) VALUES ('ZZ-00')")  # agg 三表
        self._denied(dsn, "CREATE TABLE public.dp5_evil (x int)")                  # 无 CREATE
        self._denied(dsn, "ALTER TABLE quality_task ADD COLUMN hack int")          # 无 ALTER

    def test_template_grants_are_closed_world(self):
        """模板闭世界机判：授权恰 6 句——rule 只读＋task 写读＋quarantine 仅 INSERT
        （bigserial 序列 USAGE 随 INSERT 需要）＋water 写读。"""
        tpl = (Path(__file__).resolve().parents[1] / ".." / "migrations"
               / "quality_writer.sql").read_text(encoding="utf-8")
        grant_lines = [l.rstrip(";") for l in tpl.splitlines() if l.strip().startswith("GRANT")]
        assert len(grant_lines) == 6
        assert "GRANT SELECT ON quality_rule TO tpa_quality_writer" in grant_lines
        assert "GRANT INSERT ON quarantine_row TO tpa_quality_writer" in grant_lines
        assert "GRANT USAGE, SELECT ON SEQUENCE quarantine_row_id_seq TO tpa_quality_writer" \
            in grant_lines
        assert not any("UPDATE" in l and "quarantine_row" in l for l in grant_lines)
        assert not any("UPDATE" in l and "quality_rule" in l for l in grant_lines)
