"""V2-21 · 集成套防误指机闸测试（P-011 重大事件防线）。

机闸本体在 tests/conftest.py（套件入口 pytest_configure）——TEST_DATABASE_URL 指向
非白名单测试库名即拒启动（退出码说话），防再次误指共享 dev 库 DROP schema。

三面钉：
- dsn_database_name：URL 形与 keyword=value 形两种 DSN 库名解析；解析不出＝空串
  （守卫 fail-closed：设了 TEST_DATABASE_URL 却解析不出库名也拒）；
- 守卫单元面：误指共享 dev 库（petro）／解析不出库名 → pytest.UsageError；
  正指 petro_test／未设 TEST_DATABASE_URL → 放行；
- 套件入口实证（subprocess 真跑 pytest）：误指即拒（非零退出码＋守卫文案在场），
  正指 petro_test 照常收集（退出码 0）——钉「机闸在套件入口」而非仅函数级。
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import _ALLOWED_TEST_DBS, dsn_database_name, guard_test_database_url

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SHARED_DEV_DSN = "postgresql://petro:petro@localhost:5433/petro"  # 共享 dev 库（P-011 事发指法）
_TEST_DSN = "postgresql://petro:petro@localhost:5433/petro_test"


class TestDsnDatabaseName:
    def test_url_form(self):
        assert dsn_database_name(_SHARED_DEV_DSN) == "petro"
        assert dsn_database_name(_TEST_DSN) == "petro_test"

    def test_url_form_with_query_and_subpath(self):
        assert dsn_database_name(
            "postgresql://u:p@h:5432/petro_test?sslmode=disable") == "petro_test"

    def test_keyword_value_form(self):
        assert dsn_database_name(
            "host=localhost port=5433 dbname=petro_test user=petro") == "petro_test"
        assert dsn_database_name("host=x database=petro") == "petro"  # database= 别名形

    def test_local_socket_uri(self):
        assert dsn_database_name("postgresql:///petro_test") == "petro_test"

    def test_unparseable_returns_empty(self):
        # 守卫 fail-closed：设了 DSN 却解析不出库名 → 空串 → 守卫拒
        assert dsn_database_name("host=localhost port=5433") == ""
        assert dsn_database_name("") == ""


class TestGuardUnit:
    def test_rejects_shared_dev_db(self, monkeypatch):
        monkeypatch.setenv("TEST_DATABASE_URL", _SHARED_DEV_DSN)
        with pytest.raises(pytest.UsageError) as exc:
            guard_test_database_url()
        assert "petro" in str(exc.value)
        assert "petro_test" in str(exc.value)  # 文案指路正确指法

    def test_rejects_unparseable_dsn(self, monkeypatch):
        monkeypatch.setenv("TEST_DATABASE_URL", "host=localhost port=5433")
        with pytest.raises(pytest.UsageError):
            guard_test_database_url()

    def test_allows_petro_test(self, monkeypatch):
        monkeypatch.setenv("TEST_DATABASE_URL", _TEST_DSN)
        guard_test_database_url()  # 不抛即放行

    def test_allows_when_unset(self, monkeypatch):
        monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
        guard_test_database_url()  # 未设＝hermetic 套件照常，不校验

    def test_whitelist_contains_petro_test(self):
        assert "petro_test" in _ALLOWED_TEST_DBS


class TestSuiteEntrypoint:
    """subprocess 真跑 pytest：钉机闸在套件入口（conftest pytest_configure）。

    只收集本测试文件（--collect-only）——pytest_configure 在收集前执行，
    守卫红即启动拒，与跑多少测试无关。
    """

    def _run_pytest(self, test_database_url: str | None) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        if test_database_url is None:
            env.pop("TEST_DATABASE_URL", None)
        else:
            env["TEST_DATABASE_URL"] = test_database_url
        return subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q",
             "-p", "no:cacheprovider",
             str(Path(__file__).relative_to(_REPO_ROOT))],  # tests/test_db_guard.py
            cwd=_REPO_ROOT, env=env, capture_output=True, text=True, timeout=120)

    def test_mispointed_suite_refuses_to_start(self):
        proc = self._run_pytest(_SHARED_DEV_DSN)
        assert proc.returncode != 0, f"误指共享库应拒启动，实际退出码 {proc.returncode}"
        assert "TEST_DATABASE_URL" in proc.stdout + proc.stderr  # 守卫文案在场

    def test_pointed_at_test_db_collects_normally(self):
        proc = self._run_pytest(_TEST_DSN)
        assert proc.returncode == 0, (
            f"正指 petro_test 应照常，实际退出码 {proc.returncode}\n"
            f"{proc.stdout}\n{proc.stderr}")
