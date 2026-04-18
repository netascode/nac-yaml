#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

# /// script
# requires-python = ">=3.10"
# dependencies = ["ruamel.yaml"]
# ///
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
  python scripts/compare_merge.py --diff paths...

Exit codes:
  0 - Identical output from both versions
  1 - Differences found between versions
  2 - Error (invalid paths, subprocess failure, etc.)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess  # nosec B404
import sys
import tempfile
import textwrap
from collections import Counter, defaultdict
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
        result = subprocess.run(  # nosec B603
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=tempfile.gettempdir(),
        )
    except FileNotFoundError:
        print(
            f"{C.RED}Error:{C.RESET} 'uv' not found. Install it: curl -LsSf https://astral.sh/uv/install.sh | sh",
            file=sys.stderr,
        )
        sys.exit(2)
    except subprocess.TimeoutExpired:
        print(
            f"{C.RED}Error:{C.RESET} nac-yaml=={version} timed out after 300s",
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
        parsed: dict[str, Any] = json.loads(result.stdout)
        return parsed
    except json.JSONDecodeError as exc:
        print(
            f"{C.RED}Error:{C.RESET} invalid JSON from nac-yaml=={version}: {exc}",
            file=sys.stderr,
        )
        sys.exit(2)


def diff_values(old: Any, new: Any, path: str = "$") -> list[dict[str, Any]]:
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
            diffs.append(
                {
                    "path": path,
                    "type": "list_length",
                    "old": len(old),
                    "new": len(new),
                }
            )
        for i in range(min(len(old), len(new))):
            diffs.extend(diff_values(old[i], new[i], f"{path}[{i}]"))
        # Report extra elements on the longer side
        if len(old) > len(new):
            for i in range(len(new), len(old)):
                diffs.append(
                    {
                        "path": f"{path}[{i}]",
                        "type": "removed",
                        "old": old[i],
                    }
                )
        elif len(new) > len(old):
            for i in range(len(old), len(new)):
                diffs.append(
                    {
                        "path": f"{path}[{i}]",
                        "type": "added",
                        "new": new[i],
                    }
                )
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
            lines.append(
                f"    {C.GREEN}new ({type(d['new']).__name__}): {json.dumps(d['new'], sort_keys=True)}{C.RESET}"
            )
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

    Analyzes list contents to detect specific patterns:
      - Concatenation instead of merge (list grew, duplicate keys detected)
      - Deduplication (list shrunk, duplicate scalars removed)
      - Relaxed matching (list shrunk, dict items merged more aggressively)

    Args:
        old_list: List from the old version.
        new_list: List from the new version.

    Returns:
        Human-readable cause description.
    """
    if len(new_list) > len(old_list):
        # List grew → concatenation instead of merge pattern
        if all(isinstance(item, dict) for item in new_list):
            id_keys = ("name", "id", "prefix", "sequence")
            for key in id_keys:
                values = [item.get(key) for item in new_list if key in item]
                if len(values) > 1:
                    counts = Counter(str(v) for v in values)
                    dupes = {v: c for v, c in counts.items() if c > 1}
                    if dupes:
                        top3 = sorted(dupes.items(), key=lambda x: -x[1])[:3]
                        dupe_desc = ", ".join(f"'{v}' \u00d7{c}" for v, c in top3)
                        return (
                            f"Concatenation instead of merge \u2014 v2 detected duplicate\n"
                            f"         '{key}' values across files and kept items separate.\n"
                            f"         Duplicates: {dupe_desc}"
                        )
        return "v2 concatenated instead of merging (duplicate detection triggered)."

    if len(old_list) > len(new_list):
        # List shrunk → deduplication or relaxed matching
        if all(not isinstance(item, (dict, list)) for item in old_list):
            old_counts = Counter(str(v) for v in old_list)
            has_dupes = any(c > 1 for c in old_counts.values())
            new_counts = Counter(str(v) for v in new_list)
            new_is_unique = all(c == 1 for c in new_counts.values())
            if has_dupes and new_is_unique:
                removed = len(old_list) - len(new_list)
                return (
                    f"Deduplication \u2014 v2 removed {removed} duplicate scalar\n"
                    f"         entries (v1 preserved duplicates)."
                )
        return "Relaxed matching \u2014 v2 merges items more aggressively\n         when shared keys match."

    return "Items matched differently between versions."


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
            if isinstance(v, (dict, list)):  # noqa: UP038
                lines.append(f"{leader}{k}:")
                for sub in _format_yaml_item(v, indent + 4):
                    lines.append(sub)
            else:
                lines.append(f"{leader}{k}: {_format_yaml_value(v)}")
        return lines
    return [f"{prefix}- {_format_yaml_value(item)}"]


def _format_side_by_side(left: list[str], right: list[str], col_width: int = 38) -> list[str]:
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


def _primitives(item: Any) -> dict[str, Any] | None:
    """Extract primitive (non-dict, non-list) key-value pairs from a dict item.

    Args:
        item: A list element.

    Returns:
        Dict of primitive fields, or None for non-dict items.
    """
    if isinstance(item, dict):
        return {
            k: v
            for k, v in item.items()
            if not isinstance(v, (dict, list))  # noqa: UP038
        }
    return None


def _items_match(item1: Any, item2: Any) -> bool:
    """Check if two list items match using the same logic as nac-yaml merge.

    For dict items, matches when at least one shared primitive key exists
    and all shared primitive values are equal. For scalar items, matches
    on equality.

    Args:
        item1: First list element.
        item2: Second list element.

    Returns:
        True if items match.
    """
    p1 = _primitives(item1)
    p2 = _primitives(item2)

    if p1 is not None and p2 is not None:
        shared_keys = p1.keys() & p2.keys()
        if not shared_keys:
            return False
        return bool(all(p1[k] == p2[k] for k in shared_keys))

    # Scalar items: exact equality
    return bool(item1 == item2)


def _diff_list_items(
    old_list: list[Any], new_list: list[Any]
) -> tuple[list[Any], list[Any], list[tuple[Any, Any]], int]:
    """Compute the differences between two lists.

    Matches dict items using shared-primitive-key matching (same logic as
    nac-yaml merge). Uses two passes to handle duplicates correctly:
      1. Exact matches first — pairs items that are fully identical.
      2. Primitive-key matches — pairs remaining items by shared keys.

    This avoids greedy mis-pairings when duplicate items exist (e.g. two
    items with the same name but different nested content).

    Args:
        old_list: List from the old version.
        new_list: List from the new version.

    Returns:
        Tuple of (only_old, only_new, changed_pairs, identical_count) where
        changed_pairs is a list of (old_item, new_item) tuples.
    """
    old_matched: list[bool] = [False] * len(old_list)
    new_matched: list[bool] = [False] * len(new_list)
    identical = 0
    changed: list[tuple[Any, Any]] = []

    # Pass 1: exact matches (deep equality) — highest confidence pairing
    for i, old_item in enumerate(old_list):
        for j, new_item in enumerate(new_list):
            if new_matched[j]:
                continue
            if old_item == new_item:
                old_matched[i] = True
                new_matched[j] = True
                identical += 1
                break

    # Pass 2: primitive-key matches on remaining unmatched items
    for i, old_item in enumerate(old_list):
        if old_matched[i]:
            continue
        for j, new_item in enumerate(new_list):
            if new_matched[j]:
                continue
            if _items_match(old_item, new_item):
                old_matched[i] = True
                new_matched[j] = True
                changed.append((old_item, new_item))
                break

    only_old = [old_list[i] for i in range(len(old_list)) if not old_matched[i]]
    only_new = [new_list[j] for j in range(len(new_list)) if not new_matched[j]]

    return only_old, only_new, changed, identical


def _item_identity(item: Any) -> str:
    """Return a short identity string for a list item (e.g. 'name: build-router-1').

    Uses common identifier keys to produce a human-readable label.

    Args:
        item: A list element.

    Returns:
        Identity string, or repr for non-dict/scalar items.
    """
    if isinstance(item, dict):
        id_keys = ("name", "id", "prefix", "sequence", "interface_name", "vrf", "host")
        parts = []
        for k in id_keys:
            if k in item:
                parts.append(f"{k}: {_format_yaml_value(item[k])}")
        if parts:
            return ", ".join(parts)
        # Fallback: show first few primitive keys
        prims = _primitives(item)
        if prims:
            first = list(prims.items())[:3]
            return ", ".join(f"{k}: {_format_yaml_value(v)}" for k, v in first)
        return "(dict item)"
    return repr(item)


def _parse_diff_path(path: str) -> list[str]:
    """Parse a JSON-path-style string into path segments.

    Examples:
        "$"                             -> []
        "$.configuration"               -> ["configuration"]
        "$.groups[0].core_interfaces"   -> ["groups", "[0]", "core_interfaces"]

    Args:
        path: JSON-path string (e.g. "$.a.b[0].c").

    Returns:
        List of segment strings.
    """
    if path == "$":
        return []
    raw = path[2:] if path.startswith("$.") else path.lstrip("$")
    return re.findall(r"[^.\[\]]+|\[\d+\]", raw)


def _truncate(s: str, max_len: int = 80) -> str:
    """Truncate a string, adding ellipsis if it exceeds *max_len*."""
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _format_value_short(val: Any, max_len: int = 80) -> str:
    """Render a value as a compact string, truncated to *max_len*."""
    if isinstance(val, (dict, list)):
        s = json.dumps(val, sort_keys=True)
    else:
        s = _format_yaml_value(val)
    return _truncate(s, max_len)


def _collapse_scalar_runs(diffs: list[dict[str, Any]], threshold: int = 5) -> list[dict[str, Any]]:
    """Collapse runs of scalar add/remove diffs from the same list into summaries.

    When a list has many individual scalar entries added/removed (e.g. 88
    product_types removed), collapse them into a single summary line instead
    of showing each one individually.

    Args:
        diffs: List of diff entries from diff_values().
        threshold: Minimum number of scalar diffs in a group to trigger collapse.

    Returns:
        Modified diff list with collapsed summary entries replacing individual ones.
    """
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    non_collapsible: list[dict[str, Any]] = []

    for d in diffs:
        path = d["path"]
        dtype = d["type"]
        if dtype in ("added", "removed") and re.search(r"\[\d+\]$", path):
            val = d.get("old" if dtype == "removed" else "new")
            if not isinstance(val, (dict, list)):  # noqa: UP038
                parent = re.sub(r"\[\d+\]$", "", path)
                groups[(parent, dtype)].append(d)
                continue
        non_collapsible.append(d)

    result = list(non_collapsible)
    for (parent, dtype), group_diffs in sorted(groups.items()):
        if len(group_diffs) >= threshold:
            values = [d.get("old" if dtype == "removed" else "new") for d in group_diffs]
            counts = Counter(str(v) for v in values)
            top_items = counts.most_common(5)
            summary_parts = [f"{v} \u00d7{c}" for v, c in top_items]
            if len(counts) > 5:
                summary_parts.append(f"... +{len(counts) - 5} more unique values")
            result.append(
                {
                    "path": parent,
                    "type": "collapsed_scalars",
                    "label": dtype,
                    "count": len(group_diffs),
                    "summary": ", ".join(summary_parts),
                }
            )
        else:
            result.extend(group_diffs)

    return result


def _format_item_summary(item: Any) -> str:
    """Summarize a dict item by identity and top-level key categories.

    Produces a compact one-line summary showing the item's identity
    (name, id, etc.) and its top-level keys grouped by type.

    Args:
        item: A list element (dict or scalar).

    Returns:
        Compact summary string.
    """
    if isinstance(item, dict):
        identity = _item_identity(item)
        scalar_keys: list[str] = []
        dict_keys: list[str] = []
        list_keys: list[str] = []
        for k, v in sorted(item.items()):
            if isinstance(v, dict):
                dict_keys.append(k)
            elif isinstance(v, list):
                list_keys.append(f"{k}[{len(v)}]")
            else:
                scalar_keys.append(k)
        parts: list[str] = []
        if scalar_keys:
            parts.append(", ".join(scalar_keys))
        if dict_keys:
            parts.append("dicts: " + ", ".join(dict_keys))
        if list_keys:
            parts.append("lists: " + ", ".join(list_keys))
        key_summary = " | ".join(parts) if parts else "(empty dict)"
        if identity and identity != "(dict item)":
            return f"[{identity}] — {key_summary}"
        return key_summary
    return _truncate(repr(item), 80)


def _render_diff_tree(diffs: list[dict[str, Any]]) -> list[str]:
    """Render leaf diffs as an indented YAML-like context tree.

    Groups diffs by their path hierarchy so the nesting structure of
    the data model is visible, with only differing branches expanded.

    Args:
        diffs: Leaf diff entries from diff_values().

    Returns:
        List of formatted, colored lines.
    """
    if not diffs:
        return []

    lines: list[str] = []
    prev_segments: list[str] = []
    base_indent = "      "  # 6-space base

    for d in sorted(diffs, key=lambda x: x["path"]):
        segments = _parse_diff_path(d["path"])

        # How many leading segments match the previous path?
        common = 0
        for i in range(min(len(prev_segments), len(segments))):
            if prev_segments[i] == segments[i]:
                common = i + 1
            else:
                break

        # Print new intermediate path segments (structural context)
        for i in range(common, max(0, len(segments) - 1)):
            indent = base_indent + "  " * i
            lines.append(f"{indent}{C.CYAN}{segments[i]}:{C.RESET}")

        # Print the leaf segment with diff annotation
        depth = max(0, len(segments) - 1)
        indent = base_indent + "  " * depth
        leaf = segments[-1] if segments else "$"
        dtype = d["type"]

        if dtype == "added":
            val = _format_item_summary(d["new"]) if isinstance(d["new"], dict) else _format_value_short(d["new"])
            lines.append(f"{indent}{C.GREEN}{leaf}: + {val}{C.RESET}")
        elif dtype == "removed":
            val = _format_item_summary(d["old"]) if isinstance(d["old"], dict) else _format_value_short(d["old"])
            lines.append(f"{indent}{C.RED}{leaf}: - {val}{C.RESET}")
        elif dtype == "changed":
            lines.append(f"{indent}{leaf}:")
            lines.append(f"{indent}  {C.RED}old: {_format_value_short(d['old'])}{C.RESET}")
            lines.append(f"{indent}  {C.GREEN}new: {_format_value_short(d['new'])}{C.RESET}")
        elif dtype == "type_changed":
            lines.append(f"{indent}{leaf}: type changed")
            lines.append(f"{indent}  {C.RED}old ({type(d['old']).__name__}): {_format_value_short(d['old'])}{C.RESET}")
            lines.append(
                f"{indent}  {C.GREEN}new ({type(d['new']).__name__}): {_format_value_short(d['new'])}{C.RESET}"
            )
        elif dtype == "list_length":
            lines.append(f"{indent}{leaf}: {C.RED}{d['old']} items{C.RESET} → {C.GREEN}{d['new']} items{C.RESET}")
        elif dtype == "collapsed_scalars":
            color = C.RED if d["label"] == "removed" else C.GREEN
            lines.append(f"{indent}{color}{leaf}: ({d['count']} {d['label']} scalars: {d['summary']}){C.RESET}")

        prev_segments = segments

    return lines


def format_enhanced_diff(
    list_diffs: list[tuple[str, list[Any], list[Any]]],
    scalar_diffs: list[dict[str, Any]],
    *,
    verbose: bool = False,
) -> str:
    """Format diff results with precise leaf-level diffs for changed items.

    By default, shows only the exact differing paths for changed items.
    With verbose=True, also shows the full side-by-side rendering.

    Args:
        list_diffs: List of (path, old_list, new_list) tuples.
        scalar_diffs: List of scalar diff dicts.
        verbose: If True, include full side-by-side output for changed items.

    Returns:
        Formatted multi-line string with colored output.
    """
    if not list_diffs and not scalar_diffs:
        return f"{C.GREEN}No differences found.{C.RESET}"

    lines: list[str] = []
    box_w = 62

    if list_diffs:
        n = len(list_diffs)
        lines.append(f"{C.BOLD}{C.YELLOW}Found {n} list(s) with differences:{C.RESET}")
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

            # Compute list-level diff
            only_old, only_new, changed_pairs, identical = _diff_list_items(old_list, new_list)
            col_w = 38

            # Items only in old (removed)
            if only_old:
                lines.append(f"  {C.RED}Removed ({len(only_old)} item(s)):{C.RESET}")
                for item in only_old:
                    lines.append(f"    {C.RED}{_format_item_summary(item)}{C.RESET}")
                    if verbose:
                        for fl in _format_yaml_item(item):
                            lines.append(f"      {C.RED}{fl}{C.RESET}")
                lines.append("")

            # Items only in new (added)
            if only_new:
                lines.append(f"  {C.GREEN}Added ({len(only_new)} item(s)):{C.RESET}")
                for item in only_new:
                    lines.append(f"    {C.GREEN}{_format_item_summary(item)}{C.RESET}")
                    if verbose:
                        for fl in _format_yaml_item(item):
                            lines.append(f"      {C.GREEN}{fl}{C.RESET}")
                lines.append("")

            # Items with same identity but different nested content
            if changed_pairs:
                lines.append(f"  {C.YELLOW}Changed ({len(changed_pairs)} item(s)):{C.RESET}")

                for _idx, (old_item, new_item) in enumerate(changed_pairs):
                    identity = _item_identity(old_item)
                    lines.append(f"  {C.BOLD}  [{identity}]{C.RESET}")

                    # Precise leaf-level diffs as YAML-like context tree
                    leaf_diffs = diff_values(old_item, new_item, path="$")
                    diff_count = len(leaf_diffs)
                    leaf_diffs = _collapse_scalar_runs(leaf_diffs)
                    if leaf_diffs:
                        lines.append(f"    {diff_count} difference(s):")
                        lines.extend(_render_diff_tree(leaf_diffs))
                    else:
                        lines.append(f"    {C.GREEN}(no leaf-level differences){C.RESET}")
                    lines.append("")

                    # Verbose: also show full side-by-side
                    if verbose:
                        left_hdr = f"v{OLD_VERSION}:"
                        right_hdr = f"v{NEW_VERSION}:"
                        lines.append(f"  {C.RED}{left_hdr:<{col_w}}{C.RESET} {C.GREEN}{right_hdr}{C.RESET}")
                        left_lines = _format_yaml_item(old_item)
                        right_lines = _format_yaml_item(new_item)
                        sbs = _format_side_by_side(left_lines, right_lines, col_w)
                        for sbs_line in sbs:
                            lines.append(sbs_line)
                        lines.append("")

            # Identical items summary
            if identical > 0:
                lines.append(f"  {C.BOLD}({identical} identical item(s) not shown){C.RESET}")

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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(f"Compare nac-yaml merge output between v{OLD_VERSION} and v{NEW_VERSION}."),
        epilog=("Exit codes: 0 = identical, 1 = differences found, 2 = error.\nRequires: Python 3.10+, uv"),
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
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Show full side-by-side YAML output for changed items (in addition to precise diffs)",
    )
    parser.add_argument(
        "--diff",
        action="store_true",
        help="Render both merge results as YAML and show unified diff (like diff -u)",
    )
    args = parser.parse_args()

    if args.diff and (args.json_output or args.raw):
        print(
            f"{C.RED}Error:{C.RESET} --diff cannot be combined with --json or --raw",
            file=sys.stderr,
        )
        return 2

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

    # Unified diff mode: dump both as YAML to temp files, run diff -u
    if args.diff:
        with tempfile.TemporaryDirectory() as td:
            old_path = Path(td) / f"merged_v{OLD_VERSION}.yaml"
            new_path = Path(td) / f"merged_v{NEW_VERSION}.yaml"
            dump_yaml(old_data, old_path)
            dump_yaml(new_data, new_path)
            diff_result = subprocess.run(  # nosec B603
                ["diff", "-u", f"--label=v{OLD_VERSION}", f"--label=v{NEW_VERSION}", str(old_path), str(new_path)],
                capture_output=True,
                text=True,
            )
            if diff_result.stdout:
                print(diff_result.stdout)
            else:
                print(f"{C.GREEN}✓ Merge outputs are identical.{C.RESET}")
            rc = diff_result.returncode
    else:
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
            print(format_enhanced_diff(list_diffs, scalar_diffs, verbose=args.raw))
        rc = 0 if len(diffs) == 0 else 1

    return rc

if __name__ == "__main__":
    sys.exit(main())
