#!/usr/bin/env python3
"""Compare YAML merge output between nac-yaml v1.1.1 and v2.0.0a0.

nac-yaml v2.0.0a0 changed the merge logic for load_yaml_files:
  - Old (v1.1.1): Prevented merging dict list items when BOTH sides had
    unique primitive keys; deduplication was a post-processing pass.
  - New (v2.0.0a0): Merges items as long as shared primitive keys match
    (regardless of unique keys); duplicate detection is integrated into
    the merge itself.

This script runs load_yaml_files() with both versions in isolated uv
environments and reports any differences so users can verify their data
produces identical output before upgrading.

Prerequisites:
  - Python 3.10+
  - uv (https://docs.astral.sh/uv/)
    Install: curl -LsSf https://astral.sh/uv/install.sh | sh

Usage:
  python scripts/compare_merge.py path1.yaml path2.yaml [dir/] ...
  python scripts/compare_merge.py --dump-old /tmp/old.yaml --dump-new /tmp/new.yaml paths...
  python scripts/compare_merge.py --json paths...

Exit codes:
  0 - Identical output from both versions
  1 - Differences found between versions
  2 - Error (invalid paths, subprocess failure, etc.)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import textwrap
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

OLD_VERSION = "1.1.1"
NEW_VERSION = "2.0.0a0"

# Inline Python script executed inside each isolated uv environment.
# Receives YAML paths as sys.argv[1:], loads them with load_yaml_files(),
# converts ruamel.yaml special types to plain Python, and writes JSON to stdout.
_LOADER_SCRIPT_TEMPLATE = textwrap.dedent("""\
    import json
    import sys
    from pathlib import Path
    from nac_yaml.yaml import load_yaml_files

    def make_serializable(obj):
        if isinstance(obj, dict):
            return {k: make_serializable(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [make_serializable(v) for v in obj]
        if isinstance(obj, bool):
            return obj
        if isinstance(obj, int):
            return int(obj)
        if isinstance(obj, float):
            return float(obj)
        if isinstance(obj, str):
            return str(obj)
        if obj is None:
            return None
        return str(obj)

    paths = [Path(p) for p in sys.argv[1:]]
    result = load_yaml_files(paths, deduplicate=True)
    json.dump(make_serializable(result), sys.stdout, indent=2, sort_keys=True)
""")


class _Colors:
    """ANSI color codes, disabled when output is not a terminal."""

    def __init__(self) -> None:
        use_color = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
        self.RED = "\033[31m" if use_color else ""
        self.GREEN = "\033[32m" if use_color else ""
        self.YELLOW = "\033[33m" if use_color else ""
        self.CYAN = "\033[36m" if use_color else ""
        self.BOLD = "\033[1m" if use_color else ""
        self.RESET = "\033[0m" if use_color else ""


C = _Colors()


def resolve_paths(raw_paths: list[str]) -> list[Path]:
    """Validate input paths and convert to absolute paths.

    Args:
        raw_paths: CLI-provided paths (files or directories).

    Returns:
        List of validated absolute Path objects.

    Raises:
        SystemExit: If any path does not exist.
    """
    resolved: list[Path] = []
    for raw in raw_paths:
        p = Path(raw).resolve()
        if not p.exists():
            print(f"{C.RED}Error:{C.RESET} path does not exist: {raw}", file=sys.stderr)
            sys.exit(2)
        resolved.append(p)
    return resolved


def run_version(version: str, paths: list[Path]) -> dict[str, Any]:
    """Run load_yaml_files() with a specific nac-yaml version in an isolated uv env.

    Args:
        version: nac-yaml PyPI version string (e.g. "1.1.1").
        paths: Absolute paths to YAML files/directories.

    Returns:
        Parsed JSON dict of the merged YAML output.

    Raises:
        SystemExit: On subprocess failure.
    """
    str_paths = [str(p) for p in paths]
    cmd = [
        "uv",
        "run",
        "--no-project",
        "--with",
        f"nac-yaml=={version}",
        "python",
        "-c",
        _LOADER_SCRIPT_TEMPLATE,
        *str_paths,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=tempfile.gettempdir(),
        )
    except FileNotFoundError:
        print(
            f"{C.RED}Error:{C.RESET} 'uv' not found. Install it: "
            "curl -LsSf https://astral.sh/uv/install.sh | sh",
            file=sys.stderr,
        )
        sys.exit(2)
    except subprocess.TimeoutExpired:
        print(
            f"{C.RED}Error:{C.RESET} nac-yaml=={version} timed out after 120s",
            file=sys.stderr,
        )
        sys.exit(2)

    if result.returncode != 0:
        print(
            f"{C.RED}Error running nac-yaml=={version}:{C.RESET}\n{result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(
            f"{C.RED}Error:{C.RESET} invalid JSON from nac-yaml=={version}: {exc}",
            file=sys.stderr,
        )
        sys.exit(2)


def diff_values(
    old: Any, new: Any, path: str = "$"
) -> list[dict[str, Any]]:
    """Recursively compare two structures and return a list of differences.

    Args:
        old: Value from the old version.
        new: Value from the new version.
        path: JSON-path-style location string for reporting.

    Returns:
        List of diff entries, each a dict with keys:
          - path: location string
          - type: "added" | "removed" | "changed" | "type_changed" | "list_length"
          - old/new/key as applicable
    """
    diffs: list[dict[str, Any]] = []

    if type(old) is not type(new):
        diffs.append({"path": path, "type": "type_changed", "old": old, "new": new})
        return diffs

    if isinstance(old, dict):
        old_keys = set(old.keys())
        new_keys = set(new.keys())
        for key in sorted(old_keys - new_keys):
            diffs.append({"path": f"{path}.{key}", "type": "removed", "old": old[key]})
        for key in sorted(new_keys - old_keys):
            diffs.append({"path": f"{path}.{key}", "type": "added", "new": new[key]})
        for key in sorted(old_keys & new_keys):
            diffs.extend(diff_values(old[key], new[key], f"{path}.{key}"))
    elif isinstance(old, list):
        if len(old) != len(new):
            diffs.append({
                "path": path,
                "type": "list_length",
                "old": len(old),
                "new": len(new),
            })
        for i in range(min(len(old), len(new))):
            diffs.extend(diff_values(old[i], new[i], f"{path}[{i}]"))
        # Report extra elements on the longer side
        if len(old) > len(new):
            for i in range(len(new), len(old)):
                diffs.append({
                    "path": f"{path}[{i}]",
                    "type": "removed",
                    "old": old[i],
                })
        elif len(new) > len(old):
            for i in range(len(old), len(new)):
                diffs.append({
                    "path": f"{path}[{i}]",
                    "type": "added",
                    "new": new[i],
                })
    else:
        if old != new:
            diffs.append({"path": path, "type": "changed", "old": old, "new": new})

    return diffs


def format_diff(diffs: list[dict[str, Any]]) -> str:
    """Format diff entries as colored human-readable text.

    Args:
        diffs: List of diff dicts from diff_values().

    Returns:
        Formatted multi-line string.
    """
    if not diffs:
        return f"{C.GREEN}No differences found.{C.RESET}"

    lines: list[str] = [
        f"{C.BOLD}{C.YELLOW}Found {len(diffs)} difference(s):{C.RESET}",
        "",
    ]
    for d in diffs:
        path = f"{C.CYAN}{d['path']}{C.RESET}"
        dtype = d["type"]
        if dtype == "added":
            lines.append(f"  {path}: {C.GREEN}+ {json.dumps(d['new'], sort_keys=True)}{C.RESET}")
        elif dtype == "removed":
            lines.append(f"  {path}: {C.RED}- {json.dumps(d['old'], sort_keys=True)}{C.RESET}")
        elif dtype == "changed":
            lines.append(f"  {path}:")
            lines.append(f"    {C.RED}old: {json.dumps(d['old'], sort_keys=True)}{C.RESET}")
            lines.append(f"    {C.GREEN}new: {json.dumps(d['new'], sort_keys=True)}{C.RESET}")
        elif dtype == "type_changed":
            lines.append(f"  {path}: type changed")
            lines.append(f"    {C.RED}old ({type(d['old']).__name__}): {json.dumps(d['old'], sort_keys=True)}{C.RESET}")
            lines.append(f"    {C.GREEN}new ({type(d['new']).__name__}): {json.dumps(d['new'], sort_keys=True)}{C.RESET}")
        elif dtype == "list_length":
            lines.append(f"  {path}: list length {C.RED}{d['old']}{C.RESET} -> {C.GREEN}{d['new']}{C.RESET}")

    return "\n".join(lines)


def find_list_diffs(
    old: Any, new: Any, path: str = "$"
) -> tuple[list[tuple[str, list[Any], list[Any]]], list[dict[str, Any]]]:
    """Walk two structures and collect differing lists as whole groups.

    Instead of positional element-by-element comparison, records each
    differing list as a single (path, old_list, new_list) tuple.
    Non-list scalar diffs are collected separately.

    Args:
        old: Value from the old version.
        new: Value from the new version.
        path: JSON-path-style location string.

    Returns:
        Tuple of (list_diffs, scalar_diffs) where:
          - list_diffs: list of (path, old_list, new_list) tuples
          - scalar_diffs: list of diff dicts (same format as diff_values)
    """
    list_diffs: list[tuple[str, list[Any], list[Any]]] = []
    scalar_diffs: list[dict[str, Any]] = []

    if type(old) is not type(new):
        scalar_diffs.append({"path": path, "type": "type_changed", "old": old, "new": new})
        return list_diffs, scalar_diffs

    if isinstance(old, dict):
        old_keys = set(old.keys())
        new_keys = set(new.keys())
        for key in sorted(old_keys - new_keys):
            scalar_diffs.append({"path": f"{path}.{key}", "type": "removed", "old": old[key]})
        for key in sorted(new_keys - old_keys):
            scalar_diffs.append({"path": f"{path}.{key}", "type": "added", "new": new[key]})
        for key in sorted(old_keys & new_keys):
            ld, sd = find_list_diffs(old[key], new[key], f"{path}.{key}")
            list_diffs.extend(ld)
            scalar_diffs.extend(sd)
    elif isinstance(old, list):
        if old != new:
            list_diffs.append((path, old, new))
    else:
        if old != new:
            scalar_diffs.append({"path": path, "type": "changed", "old": old, "new": new})

    return list_diffs, scalar_diffs


def _classify_cause(old_list: list[Any], new_list: list[Any]) -> str:
    """Classify which PR #34 behavior change caused a list diff.

    Args:
        old_list: List from the old version.
        new_list: List from the new version.

    Returns:
        Human-readable cause description.
    """
    if len(old_list) > len(new_list):
        return (
            "Relaxed matching \u2014 v2 merges items when shared keys\n"
            "         match, even if both sides have unique keys."
        )
    elif len(old_list) < len(new_list):
        return (
            "Duplicate preservation \u2014 v2 detected within-file\n"
            "         duplicates and disabled merging (concatenated instead)."
        )
    return "Behavior change between versions."


def _format_yaml_value(v: Any) -> str:
    """Format a scalar value in YAML style (unquoted strings)."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return v
    if v is None:
        return "null"
    return json.dumps(v)


def _format_yaml_item(item: Any, indent: int = 0) -> list[str]:
    """Render a single list item as YAML-style lines.

    Args:
        item: A list element (dict, scalar, or nested structure).
        indent: Number of leading spaces.

    Returns:
        List of formatted lines.
    """
    prefix = " " * indent
    if isinstance(item, dict):
        lines: list[str] = []
        # Sort keys but prioritize common identifier keys first
        id_keys = ("name", "id", "prefix", "sequence")
        keys = sorted(item.keys(), key=lambda k: (k not in id_keys, k))
        for i, k in enumerate(keys):
            v = item[k]
            leader = f"{prefix}- " if i == 0 else f"{prefix}  "
            if isinstance(v, (dict, list)):
                lines.append(f"{leader}{k}:")
                for sub in _format_yaml_item(v, indent + 4):
                    lines.append(sub)
            else:
                lines.append(f"{leader}{k}: {_format_yaml_value(v)}")
        return lines
    return [f"{prefix}- {_format_yaml_value(item)}"]


def _format_yaml_list(items: list[Any]) -> list[str]:
    """Render a list as YAML-style lines.

    Args:
        items: List of items to format.

    Returns:
        List of formatted YAML lines.
    """
    lines: list[str] = []
    for item in items:
        lines.extend(_format_yaml_item(item))
    return lines


def _format_side_by_side(
    left: list[str], right: list[str], col_width: int = 38
) -> list[str]:
    """Format two column lists side-by-side.

    Args:
        left: Lines for the left column.
        right: Lines for the right column.
        col_width: Width of the left column (padded with spaces).

    Returns:
        List of combined lines.
    """
    max_lines = max(len(left), len(right))
    result: list[str] = []
    for i in range(max_lines):
        l_line = left[i] if i < len(left) else ""
        r_line = right[i] if i < len(right) else ""
        result.append(f"  {l_line:<{col_width}} {r_line}")
    return result


def format_enhanced_diff(
    list_diffs: list[tuple[str, list[Any], list[Any]]],
    scalar_diffs: list[dict[str, Any]],
) -> str:
    """Format diff results as side-by-side list comparisons.

    Args:
        list_diffs: List of (path, old_list, new_list) tuples.
        scalar_diffs: List of scalar diff dicts.

    Returns:
        Formatted multi-line string with colored output.
    """
    if not list_diffs and not scalar_diffs:
        return f"{C.GREEN}No differences found.{C.RESET}"

    lines: list[str] = []
    box_w = 62

    if list_diffs:
        n = len(list_diffs)
        lines.append(
            f"{C.BOLD}{C.YELLOW}Found {n} list(s) with differences:{C.RESET}"
        )
        lines.append("")

        for path, old_list, new_list in sorted(list_diffs):
            cause = _classify_cause(old_list, new_list)
            heavy_rule = "\u2501" * box_w
            light_rule = "\u2500" * box_w

            # Header
            lines.append(f"  {C.BOLD}{heavy_rule}{C.RESET}")
            lines.append(
                f"  {C.CYAN}{path}{C.RESET}  "
                f"({C.RED}{len(old_list)} items{C.RESET} \u2192 "
                f"{C.GREEN}{len(new_list)} items{C.RESET})"
            )
            lines.append(f"  Cause: {cause}")
            lines.append(f"  {light_rule}")

            # Column headers
            col_w = 38
            left_hdr = f"v{OLD_VERSION}:"
            right_hdr = f"v{NEW_VERSION}:"
            lines.append(
                f"  {C.RED}{left_hdr:<{col_w}}{C.RESET} {C.GREEN}{right_hdr}{C.RESET}"
            )

            # Side-by-side content
            left_lines = _format_yaml_list(old_list)
            right_lines = _format_yaml_list(new_list)
            sbs = _format_side_by_side(left_lines, right_lines, col_w)
            for sbs_line in sbs:
                lines.append(sbs_line)

            # Footer
            lines.append(f"  {C.BOLD}{heavy_rule}{C.RESET}")
            lines.append("")

    if scalar_diffs:
        if list_diffs:
            lines.append("")
        lines.append(format_diff(scalar_diffs))

    return "\n".join(lines)


def dump_yaml(data: dict[str, Any], path: Path) -> None:
    """Write merged data as YAML using ruamel.yaml.

    Falls back to JSON if ruamel.yaml is not available in the current env.

    Args:
        data: Merged data dict to write.
        path: Output file path.
    """
    try:
        from ruamel.yaml import YAML

        y = YAML()
        y.explicit_start = True
        y.default_flow_style = False
        y.indent(mapping=2, sequence=4, offset=2)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fh:
            y.dump(data, fh)
    except ImportError:
        # Fallback: write as JSON if ruamel.yaml not installed
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
        print(
            f"{C.YELLOW}Warning:{C.RESET} ruamel.yaml not available, wrote JSON instead to {path}",
            file=sys.stderr,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            f"Compare nac-yaml merge output between v{OLD_VERSION} and v{NEW_VERSION}."
        ),
        epilog=(
            "Exit codes: 0 = identical, 1 = differences found, 2 = error.\n"
            "Requires: Python 3.10+, uv"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="YAML files or directories to load and merge",
    )
    parser.add_argument(
        "--dump-old",
        metavar="PATH",
        help=f"Write v{OLD_VERSION} merged output as YAML to PATH",
    )
    parser.add_argument(
        "--dump-new",
        metavar="PATH",
        help=f"Write v{NEW_VERSION} merged output as YAML to PATH",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output structured JSON result instead of colored diff",
    )
    args = parser.parse_args()

    resolved = resolve_paths(args.paths)

    # Run both versions in parallel
    print(
        f"{C.BOLD}Running merge with nac-yaml v{OLD_VERSION} and v{NEW_VERSION}...{C.RESET}",
        file=sys.stderr,
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        future_old = pool.submit(run_version, OLD_VERSION, resolved)
        future_new = pool.submit(run_version, NEW_VERSION, resolved)
        old_data = future_old.result()
        new_data = future_new.result()

    # Dump merged outputs if requested
    if args.dump_old:
        dump_yaml(old_data, Path(args.dump_old))
        print(f"Wrote v{OLD_VERSION} output to {args.dump_old}", file=sys.stderr)
    if args.dump_new:
        dump_yaml(new_data, Path(args.dump_new))
        print(f"Wrote v{NEW_VERSION} output to {args.dump_new}", file=sys.stderr)

    # Compare
    diffs = diff_values(old_data, new_data)

    if args.json_output:
        result = {
            "identical": len(diffs) == 0,
            "old_version": OLD_VERSION,
            "new_version": NEW_VERSION,
            "paths": [str(p) for p in resolved],
            "differences": diffs,
        }
        json.dump(result, sys.stdout, indent=2, sort_keys=True)
        print()
    else:
        list_diffs, scalar_diffs = find_list_diffs(old_data, new_data)
        print(format_enhanced_diff(list_diffs, scalar_diffs))

    sys.exit(0 if len(diffs) == 0 else 1)


if __name__ == "__main__":
    main()
