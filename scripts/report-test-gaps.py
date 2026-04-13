#!/usr/bin/env python3
import pathlib
import re


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "nanomq"
TEST_ROOT = SRC_ROOT / "tests"

FUNC_RE = re.compile(
    r"^(?:static\s+|extern\s+|inline\s+|)"
    r"[A-Za-z_][\w\s\*]*\s+([A-Za-z_]\w*)\s*\([^;]*\)\s*\{",
    re.MULTILINE,
)


def source_files():
    for path in sorted(SRC_ROOT.rglob("*")):
        if path.is_dir():
            continue
        if "tests" in path.parts:
            continue
        if path.suffix not in {".c", ".cpp"}:
            continue
        yield path


def count_functions(path: pathlib.Path) -> int:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return len(FUNC_RE.findall(text))


def matching_tests(path: pathlib.Path):
    stem = path.stem
    matches = []
    for test in sorted(TEST_ROOT.glob("*test.c")) + sorted(TEST_ROOT.glob("*test.cpp")):
        if stem in test.stem or test.stem.startswith(stem):
            matches.append(test.name)
    return matches


def main():
    rows = []
    total_functions = 0
    covered_files = 0
    for src in source_files():
        functions = count_functions(src)
        tests = matching_tests(src)
        total_functions += functions
        if tests:
            covered_files += 1
        rows.append((src.relative_to(ROOT).as_posix(), functions, tests))

    print("# NanoMQ Test Gap Report\n")
    print(f"- Source files scanned: {len(rows)}")
    print(f"- Estimated function definitions: {total_functions}")
    print(f"- Files with heuristic test matches: {covered_files}")
    print(f"- Files without heuristic test matches: {len(rows) - covered_files}\n")
    print("| Source | Est. functions | Matching tests |")
    print("|---|---:|---|")
    for source, functions, tests in rows:
        print(f"| `{source}` | {functions} | {', '.join(f'`{t}`' for t in tests) if tests else '-' } |")


if __name__ == "__main__":
    main()
