#!/bin/bash
# 本地 CI 闸（无远端阶段的等效门禁）：spec gate + 测试；边界闸在阶段 2 后接入
set -euo pipefail
cd "$(dirname "$0")"
python3 tools/check_specs.py
if ls tests >/dev/null 2>&1; then
  python3 -m pytest -q && echo "TESTS PASS" || rc=$?
  [ "${rc:-0}" -eq 5 ] && echo "(tests/ 无测试用例，骨架期跳过)" || { [ "${rc:-0}" -eq 0 ] || exit "$rc"; }
else
  echo "(tests/ 尚不存在，跳过)"
fi
echo "CI PASS"
