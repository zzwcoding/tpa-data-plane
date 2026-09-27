#!/usr/bin/env python3
"""sdd-flow spec gate：把 sdd-flow 的文档约定变成机器门禁。

来源：sdd-flow skill 的 references/spec-validator.py
用法：复制到目标仓库 tools/check_specs.py，CI 里 `python3 tools/check_specs.py`。
格式契约：sdd-flow references/module-spec-format.md「机器可校验约定」。

校验项：
- docs/prd.md：状态取值合法；标记已定稿时全文不得残留 [待确认]（阶段 1 硬门禁的机器化）
- specs/modules.md：依赖无环；依赖指向已声明模块；模块卡有 `目录:` 字段（目录不存在仅警告）；
  `## 边界规则` 节缺席 FAIL（边界禁令没有机器消费的规则段 = 阶段 2 未完成）；
  节内表行"例外"有内容但"理由"为空 FAIL（无理由的例外不许存在）
- specs/<功能>.md：状态取值合法；触及的模块 ⊆ 已声明模块；验收测试逐条绑定反引号测试标识；
  状态=已完成 时，测试标识必须能在源码中找到（状态流转表的机器守门）
- .scratch/ 本地票：Touches modules 行中的模块 ⊆ 已声明模块；对账按「Belongs to spec / 所属 spec」
  归属判定（无归属字段才退回 touches 推断）；spec 已完成但所属票未 done → 警告不拦截
- CONTEXT.md 可选「语义核心」节（存在才校验）：不变量 ID 查重、状态机流转端点查声明（FAIL）；
  不变量未被任何非草稿 spec 以 `INV-x` 引用（WARN）
specs/ 与 docs/prd.md 均不存在 → 整体跳过（阶段 0 CI 先行，此时尚无产物）；
有功能 spec 但无 modules.md → FAIL。
"""
import re
import sys
from pathlib import Path

FAILS = []
WARNS = []

H2 = re.compile(r"^##\s+(.+?)\s*$")
TICK = re.compile(r"`([^`]+)`")
ROW = re.compile(r"^\|(.+)\|\s*$")
SEP = re.compile(r":?-{2,}:?")
STATUS_LINE = re.compile(r"^状态[:：]\s*(\S+)\s*$", re.M)
LEGAL_STATUS = {"草稿", "已定稿", "实现中", "已完成"}
LEGAL_PRD_STATUS = {"粗稿", "逼问中", "已定稿"}
SRC_EXTS = {".py", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".go", ".java", ".kt", ".rs", ".rb", ".php"}
SRC_DIRS = ("src", "lib", "app", "backend", "frontend/src", "tests", "test", "server")
CORE_HEADING = re.compile(r"^##\s*语义核心\s*$", re.M)
ARROW = re.compile(r"\s*(?:→|->|⇒)\s*")
ITEM_SPLIT = re.compile(r"[,，、]")


def fail(msg):
    FAILS.append(msg)


def warn(msg):
    WARNS.append(msg)


def h2_sections(text):
    """按 ## 标题切段，返回 [(标题, 行列表)]；文件头部标题为空串。"""
    out, title, cur = [], "", []
    for line in text.splitlines():
        m = H2.match(line)
        if m:
            out.append((title, cur))
            title, cur = m.group(1).strip(), []
        else:
            cur.append(line)
    out.append((title, cur))
    return out


def table_after(lines, heading):
    """取指定 ## 小节紧跟的 markdown 表格并去掉表头行；小节不存在返回 None。"""
    for i, line in enumerate(lines):
        if line.strip() == heading:
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            rows = []
            while j < len(lines) and ROW.match(lines[j]):
                cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                if not all(SEP.fullmatch(c) for c in cells):
                    rows.append(cells)
                j += 1
            return rows[1:]
    return None


def parse_modules(root):
    """解析 specs/modules.md，返回 {模块名: {dir, deps}}；文件不存在返回 None。"""
    path = root / "specs" / "modules.md"
    if not path.exists():
        return None
    mods = {}
    for title, lines in h2_sections(path.read_text(encoding="utf-8")):
        if not title:
            continue
        body = "\n".join(lines)
        dm = re.search(r"^目录[:：]\s*(\S+)", body, re.M)
        if not dm:
            continue  # 没有目录字段的 ## 段落不是模块卡
        mm = re.search(r"^- 模块[:：]\s*(.+)$", body, re.M)
        deps = TICK.findall(mm.group(1)) if mm else []
        mods[title] = {"dir": dm.group(1).strip("` "), "deps": deps}
    return mods


def check_graph(mods):
    color = {u: 0 for u in mods}  # 0=未访 1=在栈 2=完成
    stack = []

    def dfs(u):
        color[u] = 1
        stack.append(u)
        for v in mods[u]["deps"]:
            if v not in mods:
                fail(f"modules.md: 模块 `{u}` 依赖 `{v}`，后者未声明（拼写错误或漏卡）")
            elif color[v] == 1:
                i = stack.index(v)
                fail("modules.md: 依赖成环：" + " → ".join(stack[i:] + [v]))
            elif color[v] == 0:
                dfs(v)
        color[u] = 2
        stack.pop()

    for u in mods:
        if color[u] == 0:
            dfs(u)


_blob = None


def source_blob(root):
    global _blob
    if _blob is None:
        parts = []
        for d in SRC_DIRS:
            base = root / d
            if base.is_dir():
                for f in base.rglob("*"):
                    if f.is_file() and f.suffix in SRC_EXTS:
                        try:
                            parts.append(f.read_text(encoding="utf-8", errors="ignore"))
                        except OSError:
                            pass
        _blob = "\n".join(parts)
    return _blob


_PATH_SUFFIX = re.compile(r"\.(test|spec)\.[a-z]+$|_test\.(py|go)$|(?:^|/)test_[^/]+\.py$")


def test_found(root, ident):
    """测试标识三种合法写法：`名字` / `路径::名字` / `路径`。"""
    if ident in source_blob(root):
        return True
    if "::" in ident:
        p, _, n = ident.partition("::")
        if (root / p).exists() and (not n or n in source_blob(root)):
            return True
        # 路径部分只是文件名(相对模块目录)时,用名字在源码中兜底
        return bool(n) and n in source_blob(root)
    if _PATH_SUFFIX.search(ident) and (root / ident).exists():
        return True
    return False


def check_prd(root):
    p = root / "docs" / "prd.md"
    if not p.exists():
        return
    text = p.read_text(encoding="utf-8")
    m = STATUS_LINE.search(text)
    if not m:
        warn("docs/prd.md: 缺 `状态:` 行（粗稿/逼问中/已定稿）")
        return
    if m.group(1) not in LEGAL_PRD_STATUS:
        fail(f"docs/prd.md: 状态 `{m.group(1)}` 不合法（粗稿/逼问中/已定稿）")
    elif m.group(1) == "已定稿" and "[待确认]" in text:
        fail("docs/prd.md: 标记已定稿但仍有 [待确认]（逼问未清零，回阶段 1）")


def check_semantic_core(root):
    """CONTEXT.md 可选「语义核心」节：结构问题 FAIL，覆盖不足 WARN。节不存在 → 整体跳过。"""
    p = root / "CONTEXT.md"
    if not p.exists():
        return
    text = p.read_text(encoding="utf-8")
    if not CORE_HEADING.search(text):
        return
    lines = text.splitlines()

    # 状态机：流转端点必须在该行状态集合中声明过
    sm = table_after(lines, "### 状态机")
    if sm:
        for cells in sm:
            if len(cells) < 3:
                continue
            entity = cells[0].strip("` ") or "?"
            states = {s.strip("` ") for s in ITEM_SPLIT.split(cells[1]) if s.strip("` ")}
            for trans in ITEM_SPLIT.split(cells[2]):
                trans = trans.strip()
                if not trans:
                    continue
                for ep in (x.strip("` ") for x in ARROW.split(trans) if x.strip("` ")):
                    if ep not in states:
                        fail(f"CONTEXT.md 语义核心: 实体 `{entity}` 的流转 `{trans}` 引用了未声明状态 `{ep}`")

    # 不变量：ID 查重（FAIL）；须被非草稿 spec 以 `INV-x` 引用（WARN）
    inv = table_after(lines, "### 不变量")
    ids = []
    if inv:
        for cells in inv:
            if cells:
                found = TICK.findall(cells[0])
                ids.append(found[0] if found else cells[0].strip())
    for dup in sorted({i for i in ids if ids.count(i) > 1}):
        fail(f"CONTEXT.md 语义核心: 不变量 ID `{dup}` 重复声明")

    if ids:
        spec_texts = []
        specs_dir = root / "specs"
        if specs_dir.is_dir():
            for f in sorted(specs_dir.glob("*.md")):
                if f.name == "modules.md":
                    continue
                stext = f.read_text(encoding="utf-8")
                m = STATUS_LINE.search(stext)
                if m and m.group(1) == "草稿":
                    continue  # 草稿 spec 不算引用方
                spec_texts.append(stext)
        if spec_texts:
            for i in ids:
                if not any(f"`{i}`" in t for t in spec_texts):
                    warn(f"CONTEXT.md 语义核心: 不变量 `{i}` 未被任何非草稿 spec 引用（声明了就要有关闭它的验收行）")


def check_spec(path, mods, root):
    """校验单个功能 spec，返回 (状态, 触及的模块列表) 供票交叉对账。"""
    name = path.name
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    st = STATUS_LINE.search(text)
    if not st:
        fail(f"{name}: 缺少 `状态:` 行")
        return None, []
    status = st.group(1)
    if status not in LEGAL_STATUS:
        fail(f"{name}: 状态 `{status}` 不合法（草稿/已定稿/实现中/已完成）")

    touched_mods = []
    touched = table_after(lines, "## 触及的模块")
    if touched is None:
        if status != "草稿":
            fail(f"{name}: 缺少 `## 触及的模块` 表")
    else:
        for cells in touched:
            mod = cells[0] if cells else ""
            if mod:
                touched_mods.append(mod)
                if mod not in mods:
                    fail(f"{name}: 触及的模块 `{mod}` 未在 specs/modules.md 声明")

    rows = table_after(lines, "## 验收测试")
    if not rows:
        if status != "草稿":
            fail(f"{name}: `## 验收测试` 表缺失或为空")
        return status, touched_mods
    for i, cells in enumerate(rows, 1):
        tests = TICK.findall(cells[-1]) if cells else []
        if not tests:
            fail(f"{name}: 验收测试第 {i} 行未绑定测试名（对应测试列用反引号包测试标识）")
        elif status == "已完成":
            for t in tests:
                if not test_found(root, t):
                    fail(f"{name}: 第 {i} 行测试 `{t}` 在源码中找不到（状态已完成必须有对应测试）")
    return status, touched_mods


def check_tickets(root, mods):
    """校验本地票，返回 [(相对路径, status, belongs, touches)] 供交叉对账。"""
    tickets = []
    scratch = root / ".scratch"
    if not scratch.is_dir():
        return tickets
    for f in sorted(scratch.rglob("*.md")):
        rel = str(f.relative_to(root))
        text = f.read_text(encoding="utf-8")
        sm = re.search(r"^\**[^\S\n]*Status[:：][^\S\n]*\**[^\S\n]*(\S+)", text, re.M)
        status = sm.group(1).strip("*") if sm else None
        bm = re.search(r"^\**[^\S\n]*(?:Belongs to spec|所属[^\S\n]*spec)[:：]?[^\S\n]*\**[^\S\n]*([^\s*:][^\n]*)[^\S\n]*$", text, re.M)
        belongs = None
        if bm:
            bt = TICK.findall(bm.group(1))
            belongs = (bt[0] if bt else bm.group(1).strip()).split("/")[-1]
        touches = []
        for line in text.splitlines():
            m = re.match(r"^\**[^\S\n]*(Touches modules|触及模块|模块)[:：]?[^\S\n]*\**[^\S\n]*(.+)$", line)
            if m:
                touches.extend(TICK.findall(m.group(2)))
        for dep in touches:
            if dep not in mods:
                fail(f"{rel}: 模块 `{dep}` 未在 specs/modules.md 声明")
        tickets.append((rel, status, belongs, touches))
    return tickets


def check_open_tickets(tickets, spec_states):
    open_by_spec = {}
    open_by_mod = {}
    for rel, status, belongs, touches in tickets:
        if (status or "").lower() == "done":
            continue
        if belongs:
            open_by_spec.setdefault(belongs, []).append(rel)  # 有归属:按 spec 对账(准)
        else:
            for mod in touches:                               # 无归属:退回 touches 对账(兼容)
                open_by_mod.setdefault(mod, []).append(rel)
    for name, status, touched in spec_states:
        if status != "已完成":
            continue
        for rel in open_by_spec.get(name, []):
            warn(f"{name}: spec 已完成，但所属票仍未 done：{rel}")
        for mod in touched:
            for rel in open_by_mod.get(mod, []):
                warn(f"{name}: spec 已完成，但未完成票 touches `{mod}`：{rel}")


def check_boundary_rules(root):
    """modules.md「边界规则」节（阶段 6 边界闸的唯一事实来源）：
    缺席 → FAIL；表行"例外"有内容但"理由"为空 → FAIL。空表/纯文字说明合法（无例外）。"""
    path = root / "specs" / "modules.md"
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8")
    if not re.search(r"^##\s*边界规则\s*$", text, re.M):
        fail("modules.md: 缺 `## 边界规则` 节（边界禁令没有机器消费的规则段 = 阶段 2 未完成）")
        return
    rows = table_after(text.splitlines(), "## 边界规则")
    for i, cells in enumerate(rows or [], 1):
        exc = cells[1].strip() if len(cells) > 1 else ""
        why = cells[2].strip() if len(cells) > 2 else ""
        if exc and exc not in {"—", "-", "（无）", "无", "none", "n/a"} and (not why or why in {"—", "-", "无"}):
            fail(f"modules.md: 边界规则第 {i} 行例外 `{exc}` 未附理由（无理由的例外不许存在）")


def check_page_map(root):
    """PRD 含 `## 页面清单` 节(Web 产品标记)且 modules.md 存在时:
    缺 `## 页面映射` 节 → FAIL;表行"提供接口"列反引号模块名未声明 → FAIL;表为空 → WARN。"""
    prd = root / "docs" / "prd.md"
    path = root / "specs" / "modules.md"
    if not prd.exists() or not path.exists():
        return
    if not re.search(r"^##\s*页面清单\s*$", prd.read_text(encoding="utf-8"), re.M):
        return
    text = path.read_text(encoding="utf-8")
    if not re.search(r"^##\s*页面映射\s*$", text, re.M):
        fail("modules.md: PRD 含 `## 页面清单` 但缺 `## 页面映射` 节(页面数据需求没有进接口闸 = Web 产品的阶段 2 未完成)")
        return
    rows = table_after(text.splitlines(), "## 页面映射")
    if not rows:
        warn("modules.md: `## 页面映射` 节为空(Web 产品每个页面至少一行)")
        return
    mods = parse_modules(root) or {}
    for i, cells in enumerate(rows, 1):
        provider = cells[2] if len(cells) > 2 else ""
        for name in re.findall(r"`([^`]+)`", provider):
            if name not in mods:
                fail(f"modules.md: 页面映射第 {i} 行引用未声明模块 `{name}`")


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    has_prd = (root / "docs" / "prd.md").exists()
    has_specs = (root / "specs").is_dir()
    if not has_prd and not has_specs:
        print("spec gate: specs/ 与 docs/prd.md 均不存在，跳过（阶段 0：CI 先于产物存在）")
        return 0

    check_prd(root)
    check_semantic_core(root)

    spec_states = []
    mods = parse_modules(root)
    if mods is None:
        if has_specs and [p for p in (root / "specs").glob("*.md") if p.name != "modules.md"]:
            fail("存在功能 spec 但 specs/modules.md 缺失（回阶段 2）")
    else:
        check_graph(mods)
        check_boundary_rules(root)
        check_page_map(root)
        for name, info in mods.items():
            if not info["dir"]:
                warn(f"modules.md: 模块 `{name}` 缺 `目录:` 字段")
            elif not (root / info["dir"]).exists():
                warn(f"modules.md: 模块 `{name}` 目录 `{info['dir']}` 尚不存在（规划中的模块合法）")
        for f in sorted((root / "specs").glob("*.md")):
            if f.name != "modules.md":
                status, touched = check_spec(f, mods, root)
                spec_states.append((f.name, status, touched))
        check_open_tickets(check_tickets(root, mods), spec_states)

    for w in WARNS:
        print(f"WARN  {w}")
    for x in FAILS:
        print(f"FAIL  {x}")
    if FAILS:
        print(f"\nspec gate: {len(FAILS)} 失败 / {len(WARNS)} 警告")
        return 1
    print(f"spec gate: PASS（{len(WARNS)} 警告）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
