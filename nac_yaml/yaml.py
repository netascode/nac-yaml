# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

import importlib.util
import json
import logging
import os
import re
import shutil
import subprocess  # nosec B404
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from ruamel import yaml

logger = logging.getLogger(__name__)

VAULT_HELPER_PATH = Path(__file__).with_name("_vault_helper.py")
VAULT_HELPER_TIMEOUT = 120


class VaultError(Exception):
    """Raised when Ansible Vault encrypted values cannot be decrypted."""


class VaultTag(yaml.YAMLObject):
    """Custom YAML tag handler for Ansible Vault encrypted values.

    Handles !vault tags in YAML files. Parsing a !vault tag only records the
    encrypted content; all values are decrypted in a single batch by
    ``load_yaml_files`` after all files have been parsed.

    Attributes:
        yaml_tag: The YAML tag string "!vault"
        value: The encrypted vault content

    Environment Variables:
        ANSIBLE_VAULT_PASSWORD: Password for vault decryption (required)
        ANSIBLE_VAULT_ID: Optional vault ID for multi-vault scenarios

    Example YAML:
        password: !vault |
          $ANSIBLE_VAULT;1.1;AES256
          663662346662316662616662346662316...

    Security:
        - Requires ansible-core, either in the current Python environment or
          as an ``ansible-vault`` executable on PATH
        - Decryption runs in a separate process; the password is passed via stdin
    """

    yaml_tag = "!vault"

    def __init__(self, v: str):
        """Initialize VaultTag with encrypted content.

        Args:
            v: Encrypted vault content string
        """
        self.value = v

    def __repr__(self) -> str:
        """Return a representation without the encrypted content."""
        return "VaultTag(...)"

    @classmethod
    def from_yaml(cls, constructor: Any, node: Any) -> "VaultTag":
        """Construct VaultTag from YAML node.

        Args:
            constructor: YAML constructor instance
            node: YAML node containing vault content

        Returns:
            VaultTag placeholder, replaced by the decrypted value after loading
        """
        return cls(node.value)


def _find_vault_python() -> str:
    """Find a Python interpreter that has ansible-core installed.

    Uses the current interpreter if ansible-core is importable, otherwise the
    interpreter of the ``ansible-vault`` executable found on PATH.

    Returns:
        Path to the Python interpreter

    Raises:
        VaultError: If no interpreter with ansible-core can be found
    """
    if importlib.util.find_spec("ansible") is not None:
        return sys.executable
    ansible_vault = shutil.which("ansible-vault")
    if ansible_vault is not None:
        # Console scripts of venvs, pipx, uv tools and Homebrew (after resolving
        # symlinks) are located next to the interpreter they belong to
        script = Path(ansible_vault).resolve()
        for name in ("python3", "python"):
            if (script.parent / name).is_file():
                return str(script.parent / name)
        python = _interpreter_from_script(script)
        if python is not None:
            return python
    raise VaultError(
        "ansible-core is required to decrypt !vault values: install ansible-core "
        "in this environment or make the ansible-vault command available on PATH"
    )


def _interpreter_from_script(script: Path) -> str | None:
    """Extract the Python interpreter from a console script's shebang.

    Supports absolute shebangs (``#!/path/to/python``), ``#!/usr/bin/env python3``
    and the ``#!/bin/sh`` exec trampoline with an absolute interpreter path used by
    pip for long or quoted paths.

    Args:
        script: Path to the console script

    Returns:
        Path to the Python interpreter, or None if it cannot be determined
    """
    try:
        with open(script, encoding="utf-8") as f:
            head = [f.readline() for _ in range(3)]
    except (OSError, UnicodeDecodeError):
        return None
    if not head[0].startswith("#!"):
        return None
    args = head[0][2:].split()
    if not args:
        return None
    if Path(args[0]).name == "sh":
        match = re.match(r"""'''exec' (["']?)(/.+?)\1 "\$0" "\$@"$""", head[1].strip())
        return str(match.group(2)) if match else None
    if Path(args[0]).name == "env" and len(args) > 1:
        return shutil.which(args[-1])
    return args[0]


def _decrypt_vault_values(values: list[str]) -> dict[str, str | VaultError]:
    """Decrypt Ansible Vault values in a single helper process.

    Args:
        values: Encrypted vault content strings

    Returns:
        Mapping of each encrypted value to its decrypted value, or to a
        VaultError if that value could not be decrypted
    """
    try:
        password = os.environ.get("ANSIBLE_VAULT_PASSWORD")
        if password is None:
            raise VaultError("ANSIBLE_VAULT_PASSWORD environment variable is not set")
        request = {
            "vault_id": os.environ.get("ANSIBLE_VAULT_ID", "default"),
            # Strip trailing line breaks like ansible-vault does for password scripts
            "password": password.rstrip("\r\n"),
            "values": values,
        }
        # Run with an explicit interpreter and script path in isolated mode (-I),
        # so neither PYTHON* variables nor the current directory affect imports.
        # The password is passed via stdin only, not via arguments or environment.
        env = {k: v for k, v in os.environ.items() if k != "ANSIBLE_VAULT_PASSWORD"}
        try:
            proc = subprocess.run(  # nosec B603
                [_find_vault_python(), "-I", str(VAULT_HELPER_PATH)],
                input=json.dumps(request).encode(),
                capture_output=True,
                env=env,
                timeout=VAULT_HELPER_TIMEOUT,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as e:
            raise VaultError(f"Vault helper process failed: {e}") from e
        if proc.returncode != 0:
            stderr = proc.stderr.decode(errors="replace").strip()
            raise VaultError(f"Vault helper process failed: {stderr[-500:]}")
        return _parse_vault_response(values, proc.stdout)
    except VaultError as e:
        return dict.fromkeys(values, e)


def _parse_vault_response(
    values: list[str], stdout: bytes
) -> dict[str, str | VaultError]:
    """Parse the vault helper response.

    Args:
        values: Encrypted vault content strings sent to the helper
        stdout: Raw output of the helper process

    Returns:
        Mapping of each encrypted value to its decrypted value or VaultError

    Raises:
        VaultError: If the helper reported an error or the response is invalid
    """
    try:
        response = json.loads(stdout)
        if "error" in response:
            raise VaultError(str(response["error"]))
        result: dict[str, str | VaultError] = {}
        for value, item in zip(values, response["results"], strict=True):
            if item["ok"]:
                result[value] = str(item["value"])
            else:
                result[value] = VaultError(
                    f"Could not decrypt vault value: {item['error']}"
                )
        return result
    except (ValueError, KeyError, TypeError) as e:
        raise VaultError(f"Invalid response from vault helper: {e}") from e


def _collect_vault_values(node: Any, values: set[str]) -> None:
    """Recursively collect the encrypted content of all VaultTag placeholders."""
    if isinstance(node, VaultTag):
        values.add(node.value)
    elif isinstance(node, dict):
        for v in node.values():
            _collect_vault_values(v, values)
    elif isinstance(node, list):
        for v in node:
            _collect_vault_values(v, values)


def _resolve_vault_values(node: Any, decrypted: dict[str, str | VaultError]) -> Any:
    """Recursively replace VaultTag placeholders with their decrypted values."""
    if isinstance(node, VaultTag):
        return decrypted[node.value]
    if isinstance(node, dict):
        for k, v in node.items():
            node[k] = _resolve_vault_values(v, decrypted)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            node[i] = _resolve_vault_values(v, decrypted)
    return node


class EnvTag(yaml.YAMLObject):
    """Custom YAML tag handler for environment variable substitution.

    Handles !env tags in YAML files by replacing them with environment variable values.

    Attributes:
        yaml_tag: The YAML tag string "!env"
        value: The environment variable name

    Example YAML:
        api_key: !env API_KEY
        database_url: !env DATABASE_URL

    Behavior:
        - Returns the environment variable value if it exists
        - Returns empty string if the environment variable is not set
        - No error is raised for missing environment variables
    """

    yaml_tag = "!env"

    def __init__(self, v: str):
        """Initialize EnvTag with environment variable name.

        Args:
            v: Name of the environment variable to lookup
        """
        self.value = v

    def __repr__(self) -> str:
        """Lookup and return the environment variable value.

        Returns:
            Environment variable value if set, empty string otherwise
        """
        env = os.getenv(self.value)
        if env is None:
            return ""
        return env

    @classmethod
    def from_yaml(cls, constructor: Any, node: Any) -> str:
        """Construct EnvTag from YAML node.

        Args:
            constructor: YAML constructor instance
            node: YAML node containing environment variable name

        Returns:
            String representation of the environment variable value
        """
        return str(cls(node.value))


class _DuplicateCache:
    """Cache of the duplicate status of lists during a single load_yaml_files() run.

    Entries are keyed by ``id()`` and hold a reference to the list, so that the
    id cannot be reused by another list while the entry exists.
    """

    def __init__(self) -> None:
        self._entries: dict[int, tuple[list[Any], bool]] = {}

    def has_duplicates(self, items: list[Any]) -> bool:
        entry = self._entries.get(id(items))
        if entry is not None:
            return entry[1]
        value = _has_duplicates_in_list(items)
        self.set(items, value)
        return value

    def set(self, items: list[Any], value: bool) -> None:
        self._entries[id(items)] = (items, value)

    def invalidate(self, items: list[Any]) -> None:
        self._entries.pop(id(items), None)


def _contains_aliases(node: Any) -> bool:
    """Check if the same dict or list object is reachable via multiple paths.

    This is the case for YAML aliases (e.g. ``*anchor`` or ``<<: *anchor``).
    Aliases cannot span multiple files, so checking each file is sufficient.
    """
    seen: set[int] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict | list):
            if id(current) in seen:
                return True
            seen.add(id(current))
            stack.extend(current.values() if isinstance(current, dict) else current)
    return False


def load_yaml_files(
    paths: list[Path],
    deduplicate: bool = True,
    typ: str | None = None,
) -> dict[str, Any]:
    """Load and merge YAML files from provided paths.

    Args:
        paths: List of file or directory paths to load YAML from
        deduplicate: When True, intelligently merges lists based on duplicate detection:
                    - If ANY file has duplicates in a list, that list is concatenated (no merging)
                    - If NO duplicates exist, matching dict items are merged across files
                    When False, simply concatenates all list items.
        typ: Optional ruamel.yaml parser type passed to ``ruamel.yaml.YAML(typ=...)``.
             Use ``"safe"`` to load native Python types (dict/list) instead of round-trip
             containers.

             Caveat: when ``typ`` is not round-trip (e.g. ``"safe"``), formatting-related
             options like ``preserve_quotes`` do not apply and comments/quoting/style are not
             preserved.

    Returns:
        Merged dictionary structure

    Raises:
        VaultError: If !vault values in an explicitly provided file cannot be
            decrypted. Files found in directories are skipped with a warning instead.

    Vault values:
        All !vault values of all files are decrypted in a single batch before
        merging, using ANSIBLE_VAULT_PASSWORD and the optional ANSIBLE_VAULT_ID.

    Behavior with deduplicate=True:
        - Preserves ALL duplicate items within ANY file
        - Duplicates in any file disable merging for that entire list
        - Order-independent: same result regardless of file load order
        - Dict items match based on shared primitive key-value pairs
        - Primitive list items are always appended (never merged)

    Example 1 - No duplicates, merging works:
        file1.yaml:
            devices:
              - name: switch1

        file2.yaml:
            devices:
              - name: switch1
                port: 1/0/1

        Result: devices: [{name: switch1, port: 1/0/1}]  # merged

    Example 2 - Duplicates present, concatenation instead:
        file1.yaml:
            devices:
              - name: switch1
              - name: switch1  # duplicate!

        file2.yaml:
            devices:
              - name: switch1
                port: 1/0/1

        Result: devices: [{name: switch1}, {name: switch1}, {name: switch1, port: 1/0/1}]  # all preserved
    """
    # Create YAML parser once and reuse for all files
    y = yaml.YAML(typ=typ) if typ is not None else yaml.YAML()
    y.preserve_quotes = True
    y.register_class(VaultTag)
    y.register_class(EnvTag)

    def _parse_file(file_path: Path) -> dict[str, Any] | None:
        if file_path.suffix not in [".yaml", ".yml"]:
            return None
        with open(file_path) as file:
            parsed = y.load(file.read())
        if parsed is None:
            logger.debug("Skipping empty or comment-only file: %s", file_path)
            return None
        if not isinstance(parsed, dict):
            logger.warning(
                "Skipping file with non-mapping root (%s): %s",
                type(parsed).__name__,
                file_path,
            )
            return None
        return parsed

    # Parse all files first. Errors in explicitly provided files are raised,
    # files found in directories are skipped with a warning.
    documents: list[tuple[Path, dict[str, Any], bool]] = []
    for path in paths:
        if os.path.isfile(path):
            parsed = _parse_file(Path(path))
            if parsed is not None:
                documents.append((Path(path), parsed, True))
        else:
            for dir, _subdir, files in os.walk(path):
                for filename in files:
                    file_path = Path(dir, filename)
                    try:
                        parsed = _parse_file(file_path)
                    except Exception as e:
                        logger.warning("Could not load file %s: %s", file_path, e)
                        continue
                    if parsed is not None:
                        documents.append((file_path, parsed, False))

    # Decrypt all vault values of all files in a single batch before merging,
    # so that merging operates on the decrypted values
    document_vault_values: list[set[str]] = []
    for _file_path, parsed, _explicit in documents:
        values: set[str] = set()
        _collect_vault_values(parsed, values)
        document_vault_values.append(values)
    all_vault_values = set().union(*document_vault_values)
    decrypted = (
        _decrypt_vault_values(sorted(all_vault_values)) if all_vault_values else {}
    )

    # Cache the duplicate status of merged lists across files, so that merging a
    # file only checks the list items it changed instead of rescanning whole
    # lists. Merging a file with YAML aliases could change a cached list via
    # another reference, so the cache is disabled from then on.
    cache = _DuplicateCache() if deduplicate else None
    result: dict[str, Any] = {}
    for (file_path, parsed, explicit), values in zip(
        documents, document_vault_values, strict=True
    ):
        errors = [e for v in values if isinstance(e := decrypted[v], VaultError)]
        if errors:
            if explicit:
                raise VaultError(f"{file_path}: {errors[0]}") from errors[0]
            logger.warning("Could not load file %s: %s", file_path, errors[0])
            continue
        document = _resolve_vault_values(parsed, decrypted)
        if cache is not None and _contains_aliases(document):
            cache = None
        _merge_dict(document, result, deduplicate, cache)
    return result


def _items_would_merge(item1: dict[str, Any], item2: dict[str, Any]) -> bool:
    """Check if two dict items would merge based on primitive field matching.

    Args:
        item1: First dict item
        item2: Second dict item

    Returns:
        True if items would merge (all shared primitives match), False otherwise

    Matching logic:
        - At least one primitive (non-dict, non-list) key must exist in both items
        - All shared primitive values must match exactly
        - Ignores dict and list fields for matching
    """
    comparison = False
    for k, v1 in item1.items():
        if isinstance(v1, dict | list):
            continue
        if k not in item2:
            continue
        v2 = item2[k]
        if isinstance(v2, dict | list):
            continue
        comparison = True
        if v1 != v2:
            return False
    return comparison


def _extract_primitives(item: dict[str, Any]) -> dict[str, Any]:
    """Extract primitive (non-dict, non-list) key-value pairs from a dict.

    Args:
        item: Dictionary to extract primitives from

    Returns:
        Dictionary containing only primitive key-value pairs
    """
    return {k: v for k, v in item.items() if not isinstance(v, dict | list)}


def _has_duplicates_in_list(items: list[Any]) -> bool:
    """Check if a list contains duplicate dict items using an inverted index.

    Args:
        items: List to check for duplicates

    Returns:
        True if list contains matching dict items, False otherwise

    Note:
        Uses same matching logic as merge_list_item() to determine if items are duplicates.
        Primitive items (strings, numbers) are not considered for duplicate detection.
    """
    # Only check dict items for duplicates, precompute primitives
    dict_items: list[dict[str, Any]] = []
    primitives_list: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            dict_items.append(item)
            primitives_list.append(_extract_primitives(item))

    if len(dict_items) < 2:
        return False

    # Build inverted index: (key, value) -> [indices]
    index: dict[tuple[str, Any], list[int]] = defaultdict(list)
    for i, prims in enumerate(primitives_list):
        for k, v in prims.items():
            try:
                index[(k, v)].append(i)
            except TypeError:
                continue

    # Check candidate pairs from buckets with 2+ entries
    checked: set[tuple[int, int]] = set()
    for bucket in index.values():
        if len(bucket) < 2:
            continue
        for bi in range(len(bucket)):
            for bj in range(bi + 1, len(bucket)):
                idx_pair = (bucket[bi], bucket[bj])
                if idx_pair in checked:
                    continue
                checked.add(idx_pair)
                i, j = idx_pair
                # Intersect primitive key sets, verify all shared keys match
                shared_keys = primitives_list[i].keys() & primitives_list[j].keys()
                if not shared_keys:
                    continue
                if all(
                    primitives_list[i][k] == primitives_list[j][k] for k in shared_keys
                ):
                    return True

    return False


def _has_duplicates_involving(items: list[Any], indices: list[int]) -> bool:
    """Check if any dict item at the given indices matches another dict item.

    Uses the same matching logic as _has_duplicates_in_list(), but only checks
    pairs involving the given items. If a list without duplicates had only the
    items at these indices modified, only such pairs can have become duplicates.

    Args:
        items: List to check for duplicates
        indices: Indices of the items to check against all other items

    Returns:
        True if any of the given items matches another dict item, False otherwise
    """
    if not indices:
        return False

    primitives_list: list[dict[str, Any] | None] = [
        _extract_primitives(item) if isinstance(item, dict) else None for item in items
    ]

    # Build inverted index: (key, value) -> [indices]
    index: dict[tuple[str, Any], list[int]] = defaultdict(list)
    for i, prims in enumerate(primitives_list):
        if prims is None:
            continue
        for k, v in prims.items():
            try:
                index[(k, v)].append(i)
            except TypeError:
                continue

    for i in set(indices):
        prims = primitives_list[i]
        if prims is None:
            continue
        candidates: set[int] = set()
        for k, v in prims.items():
            try:
                bucket = index.get((k, v))
                if bucket is not None:
                    candidates.update(bucket)
            except TypeError:
                continue
        candidates.discard(i)
        for j in candidates:
            other = primitives_list[j]
            if other is None:
                continue
            shared_keys = prims.keys() & other.keys()
            if all(prims[k] == other[k] for k in shared_keys):
                return True

    return False


def _merge_list_items_indexed(
    source_items: list[Any],
    destination: list[Any],
    deduplicate: bool,
    cache: _DuplicateCache | None = None,
) -> list[int]:
    """Merge source items into destination list using an inverted index.

    Args:
        source_items: List of items to merge into destination
        destination: Target list (modified in-place)
        deduplicate: When True, merges matching dict items
        cache: Optional duplicate status cache of nested lists

    Returns:
        Indices of destination items that source items were merged into
    """
    # Build inverted index over destination's dict items
    dest_primitives: list[dict[str, Any] | None] = []
    for item in destination:
        if isinstance(item, dict):
            dest_primitives.append(_extract_primitives(item))
        else:
            dest_primitives.append(None)

    index: dict[tuple[str, Any], list[int]] = defaultdict(list)
    for i, prims in enumerate(dest_primitives):
        if prims is None:
            continue
        for k, v in prims.items():
            try:
                index[(k, v)].append(i)
            except TypeError:
                continue

    merged: list[int] = []
    for source_item in source_items:
        # Appended items also need an entry in dest_primitives, as candidate
        # indices refer to positions in destination
        if not isinstance(source_item, dict):
            destination.append(source_item)
            dest_primitives.append(None)
            continue

        src_prims = _extract_primitives(source_item)
        if not src_prims:
            destination.append(source_item)
            dest_primitives.append(src_prims)
            continue

        # Collect candidate dest indices
        candidate_set: set[int] = set()
        for k, v in src_prims.items():
            try:
                bucket = index.get((k, v))
                if bucket is not None:
                    candidate_set.update(bucket)
            except TypeError:
                continue

        # Check candidates in destination order (first-match semantics)
        matched = False
        for ci in sorted(candidate_set):
            dp = dest_primitives[ci]
            if dp is None:
                continue
            shared_keys = src_prims.keys() & dp.keys()
            if not shared_keys:
                continue
            if all(src_prims[k] == dp[k] for k in shared_keys):
                _merge_dict(source_item, destination[ci], deduplicate, cache)
                # Update primitives cache after merge
                dest_primitives[ci] = _extract_primitives(destination[ci])
                merged.append(ci)
                matched = True
                break

        if not matched:
            # Append and update index so later source items can match
            new_idx = len(destination)
            destination.append(source_item)
            dest_primitives.append(src_prims)
            for k, v in src_prims.items():
                try:
                    index[(k, v)].append(new_idx)
                except TypeError:
                    continue

    return merged


def merge_list_item(
    source_item: Any, destination: list[Any], deduplicate: bool = True
) -> None:
    """Merge item into list, optionally merging matching dict items.

    Args:
        source_item: Item to merge into destination list
        destination: Target list (modified in-place)
        deduplicate: When True, merges matching dict items based on shared
                    primitive key-value pairs. When False, always appends.

    Matching rules for dict items (when deduplicate=True):
        - At least one primitive (non-dict, non-list) key must exist in both items
        - All shared primitive values must match exactly
        - Both sides can have unique primitive keys (they will be combined)
        - If items match, source attributes are merged into first matching dest item
        - Primitive items (strings, numbers) are always appended (never merged)

    Example:
        destination = [{"name": "a", "x": 1}]
        source_item = {"name": "a", "y": 2}
        merge_list_item(source_item, destination, deduplicate=True)
        # Result: [{"name": "a", "x": 1, "y": 2}] - merged

        destination = [{"name": "a", "vlan": 100}]
        source_item = {"name": "a", "port": "eth0"}
        merge_list_item(source_item, destination, deduplicate=True)
        # Result: [{"name": "a", "vlan": 100, "port": "eth0"}] - merged (both have unique keys)

        destination = [{"name": "a", "id": 1}]
        source_item = {"name": "a", "id": 2}
        merge_list_item(source_item, destination, deduplicate=True)
        # Result: [{"name": "a", "id": 1}, {"name": "a", "id": 2}] - not merged (conflicting values)
    """
    if isinstance(source_item, dict):
        # check if we have an item in destination with matching primitives
        for dest_item in destination:
            if isinstance(dest_item, dict) and _items_would_merge(
                source_item, dest_item
            ):
                merge_dict(source_item, dest_item, deduplicate)
                return
    destination.append(source_item)


def merge_dict(
    source: dict[str, Any], destination: dict[str, Any], deduplicate: bool = True
) -> dict[str, Any]:
    """Merge two nested dict/list structures.

    Args:
        source: Source dictionary to merge from
        destination: Destination dictionary to merge into (modified in-place)
        deduplicate: When True, list items are merged intelligently:
                    - If either source or destination list has duplicates, all items
                      are concatenated (no merging) to preserve duplicates
                    - If no duplicates exist, matching dict items are merged (combining attributes)
                    - Primitive items are always appended (never deduplicated)
                    When False, lists are simply concatenated.

    Returns:
        The modified destination dictionary

    Behavior:
        Duplicates in ANY list disable merging for that list, ensuring order-independent
        results and preserving all within-file duplicates.

    Example:
        # No duplicates: merging works
        source = {"list": [{"name": "a", "x": 1}]}
        dest = {"list": [{"name": "a", "y": 2}]}
        merge_dict(source, dest, deduplicate=True)
        # Result: {"list": [{"name": "a", "x": 1, "y": 2}]}

        # Duplicates present: concatenation instead
        source = {"list": [{"name": "a"}, {"name": "a"}]}  # duplicate!
        dest = {"list": [{"name": "a", "y": 2}]}
        merge_dict(source, dest, deduplicate=True)
        # Result: {"list": [{"name": "a", "y": 2}, {"name": "a"}, {"name": "a"}]}
    """
    return _merge_dict(source, destination, deduplicate, None)


def _merge_dict(
    source: dict[str, Any],
    destination: dict[str, Any],
    deduplicate: bool,
    cache: _DuplicateCache | None,
) -> dict[str, Any]:
    """Implementation of merge_dict() with an optional duplicate status cache.

    The cache is only valid if all changes to the destination lists are made
    through this function with the same cache. Without a cache, the duplicate
    status of destination lists is recomputed on every merge.
    """
    if not source:
        return destination
    for key, value in source.items():
        if key not in destination or destination[key] is None:
            destination[key] = value
        elif isinstance(value, dict):
            if isinstance(destination[key], dict):
                _merge_dict(value, destination[key], deduplicate, cache)
        elif isinstance(value, list):
            if isinstance(destination[key], list):
                if deduplicate:
                    # Skip empty lists
                    if not value or not destination[key]:
                        destination[key] += value
                        if cache is not None and value:
                            cache.invalidate(destination[key])
                    # Check if either source or destination list has duplicates
                    # If duplicates exist, skip merging to preserve them
                    elif _has_duplicates_in_list(value) or (
                        cache.has_duplicates(destination[key])
                        if cache is not None
                        else _has_duplicates_in_list(destination[key])
                    ):
                        # Concatenate without merging to preserve all duplicates
                        destination[key] += value
                        if cache is not None:
                            cache.set(destination[key], True)
                    else:
                        # No duplicates: merge matching items across files
                        merged = _merge_list_items_indexed(
                            value, destination[key], deduplicate, cache
                        )
                        # Only merged items can have become duplicates of others
                        if cache is not None:
                            cache.set(
                                destination[key],
                                _has_duplicates_involving(destination[key], merged),
                            )
                else:
                    # Simple append (original behavior)
                    destination[key] += value
        elif value is not None:
            destination[key] = value
    return destination


def write_yaml_file(data: dict[str, Any], path: Path) -> None:
    """Write data structure to a YAML file with standard formatting.

    Args:
        data: Dictionary to write as YAML
        path: Path to output file

    Formatting:
        - Explicit document start (---)
        - Block style (not flow style)
        - Mapping indent: 2 spaces
        - Sequence indent: 4 spaces with 2 space offset

    Error Handling:
        - Logs error and continues if write fails
        - Does not raise exceptions

    Example:
        data = {"key": "value", "list": [1, 2, 3]}
        write_yaml_file(data, Path("output.yaml"))
        # Creates output.yaml:
        # ---
        # key: value
        # list:
        #   - 1
        #   - 2
        #   - 3
    """
    try:
        with open(path, "w") as fh:
            y = yaml.YAML()
            y.explicit_start = True
            y.default_flow_style = False
            y.indent(mapping=2, sequence=4, offset=2)
            y.dump(data, fh)
    except:  # noqa: E722
        logger.error(f"Cannot write file: {path}")
