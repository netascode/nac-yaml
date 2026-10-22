#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

"""Check or fix SPDX license identifier and copyright notice in Python files.

Usage:
    python scripts/license_headers.py                  # check all git-tracked .py files
    python scripts/license_headers.py --fix             # fix mode (add missing headers)
    python scripts/license_headers.py FILE [FILE ...]   # check specific files
    python scripts/license_headers.py --fix FILE [...]  # fix specific files

When called without file arguments, uses 'git ls-files' to find all tracked
Python files. When called with file arguments (e.g. from pre-commit), only
checks those files. Pure pathlib/subprocess implementation (no shell-outs),
so it runs identically on Windows.
"""

from __future__ import annotations

import argparse
import subprocess  # nosec B404
import sys
from pathlib import Path

EXPECTED_SPDX = "# SPDX-License-Identifier: MPL-2.0"
EXPECTED_COPYRIGHT = "# Copyright (c) 2025 Daniel Schmidt"


def tracked_python_files() -> list[Path]:
    """Return all git-tracked '*.py' files relative to the repo root."""
    result = subprocess.run(  # nosec B603 B607
        ["git", "ls-files", "*.py"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [Path(line) for line in result.stdout.splitlines() if line]


def header_slot(lines: list[str]) -> tuple[str | None, str | None]:
    """Return the (spdx, copyright) lines a file currently has, shebang-aware."""
    if lines and lines[0].startswith("#!"):
        offset = 1
    else:
        offset = 0
    spdx = lines[offset] if len(lines) > offset else None
    copyright_line = lines[offset + 1] if len(lines) > offset + 1 else None
    return spdx, copyright_line


def has_correct_headers(path: Path) -> bool:
    lines = path.read_text(encoding="utf-8").splitlines()
    spdx, copyright_line = header_slot(lines)
    return spdx == EXPECTED_SPDX and copyright_line == EXPECTED_COPYRIGHT


def add_headers(path: Path) -> None:
    original = path.read_text(encoding="utf-8")
    lines = original.splitlines(keepends=True)
    if lines and lines[0].startswith("#!"):
        new_content = (
            lines[0] + EXPECTED_SPDX + "\n" + EXPECTED_COPYRIGHT + "\n\n"
        ) + "".join(lines[1:])
    else:
        new_content = EXPECTED_SPDX + "\n" + EXPECTED_COPYRIGHT + "\n\n" + original
    path.write_text(new_content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check or fix SPDX license headers in Python files."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Add missing headers instead of just checking.",
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Specific files to check (defaults to all git-tracked .py files).",
    )
    args = parser.parse_args(argv)

    files = [Path(f) for f in args.files] if args.files else tracked_python_files()

    checked = 0
    fixed = 0
    failed: list[Path] = []

    for path in files:
        if not path.is_file():
            print(f"ERROR: not a regular file: {path}", file=sys.stderr)
            failed.append(path)
            continue
        if path.stat().st_size == 0 or not path.read_text(encoding="utf-8").strip():
            continue

        checked += 1
        if has_correct_headers(path):
            continue

        if args.fix:
            add_headers(path)
            print(f"✓ {path} (header added)")
            fixed += 1
        else:
            print(f"✗ {path} (missing or incorrect header)")
            failed.append(path)

    print()
    print(f"Files checked: {checked}")
    if args.fix:
        print(f"Files fixed: {fixed}")
        if failed:
            print("Files failed:")
            for path in failed:
                print(f"  - {path}")
            return 1
        return 0

    if failed:
        print(f"Files needing fixes ({len(failed)}):")
        for path in failed:
            print(f"  - {path}")
        print()
        print("Run with --fix to automatically add headers")
        return 1

    print("All Python files have correct headers!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
