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
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ruamel import yaml
from ruamel.yaml import main as _ruamel_main

logger = logging.getLogger(__name__)

# Environment variable forcing the pure Python parser, e.g. NAC_YAML_PURE=1
PURE_ENV = "NAC_YAML_PURE"

# Documents containing these are parsed with the pure Python parser, as the C
# parser would parse them differently instead of failing: libyaml accepts tabs
# the pure Python scanner rejects (e.g. "a:\tb" or "a: b\tc"), and ignores
# %YAML 1.1 directives. Both are rare, so few documents miss the C parser.
_C_PARSER_DIVERGES = ("\t", "%YAML")

VAULT_HELPER_PATH = Path(__file__).with_name("_vault_helper.py")
VAULT_HELPER_TIMEOUT = 120

YAML_SUFFIXES = (".yaml", ".yml")

# Path of a node in a YAML document: map keys and list indexes
KeyPath = tuple[Any, ...]


class VaultError(Exception):
    """Raised when Ansible Vault encrypted values cannot be decrypted."""


class YamlRootError(ValueError):
    """Raised when the root of a YAML document is not a mapping.

    Attributes:
        source: Name of the document
        root_type: Type name of the root value, e.g. ``"list"``
    """

    def __init__(self, source: str, root_type: str):
        # Both arguments go into args, so the error can be pickled
        super().__init__(source, root_type)
        self.source = source
        self.root_type = root_type

    def __str__(self) -> str:
        return f"{self.source}: root is a {self.root_type}, not a mapping"


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

    def __eq__(self, other: object) -> bool:
        """Compare by encrypted content, so identical values match when merging."""
        if not isinstance(other, VaultTag):
            return NotImplemented
        return bool(self.value == other.value)

    def __hash__(self) -> int:
        """Hash by encrypted content, consistent with ``__eq__``."""
        return hash(("!vault", self.value))

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


@dataclass(frozen=True)
class Origin:
    """Where a merged value comes from.

    Attributes:
        source: Name of the document, as passed to merge_documents()
        path: Path of the value in that document
    """

    source: str
    path: KeyPath


@dataclass(frozen=True)
class Dropped:
    """A value merge_documents() discarded because of a type mismatch.

    Either a value is ignored because an earlier document has a value of another
    type there (a list where it has a mapping, or a mapping or list where it has
    a scalar), or a mapping or list of an earlier document is replaced by a
    scalar of a later one (``replaced``).

    Attributes:
        origin: Where the discarded value is
        path: Path in the merged data where the value was or would have been
        replaced: True if the value was replaced by a later scalar, whose origin
            is the last one of ``Origins.get(path)``
    """

    origin: Origin
    path: KeyPath
    replaced: bool = False


def _exists(node: Any, path: KeyPath) -> bool:
    """Check if a path exists in a document."""
    for key in path:
        if isinstance(node, dict):
            if key not in node:
                return False
            node = node[key]
        elif isinstance(node, list):
            if isinstance(key, bool) or not isinstance(key, int):
                return False
            if not 0 <= key < len(node):
                return False
            node = node[key]
        else:
            return False
    return True


class Origins:
    """The documents each value of a merge_documents() result comes from.

    Two kinds of records are made while merging:

    - inserted: the whole subtree at a merged path comes from a document; paths
      below it map 1:1 to that document
    - merged: a document contributed to the node at a merged path only

    This works because merging never removes or reorders anything: map keys
    keep their names and merged lists only grow at the end. A value added to an
    inserted subtree by a later document doesn't exist in the earlier one, which
    the lookup checks.

    A container that YAML aliases make reachable via several merged paths is
    only recorded under the path a merge went through; lookups also try the
    other paths of such containers.

    Lookups read the merged data, which must not be modified while origins are
    looked up.
    """

    # Maximum number of alias-equivalent paths tried for one lookup
    _MAX_EQUIVALENT_PATHS = 64

    def __init__(self, documents: Sequence[tuple[str, Any]], data: Any) -> None:
        self._documents = dict(documents)
        self._order = {source: i for i, (source, _) in enumerate(documents)}
        self._data = data
        self._inserted: defaultdict[KeyPath, list[tuple[str, KeyPath]]] = defaultdict(
            list
        )
        self._merged: defaultdict[KeyPath, list[tuple[str, KeyPath]]] = defaultdict(
            list
        )
        self.dropped: list[Dropped] = []
        # Set by merge_documents(): whether a document contains YAML aliases, and
        # whether merging is complete (only then are shared containers indexed)
        self._aliased = False
        self._complete = False
        self._shared: dict[int, list[KeyPath]] | None = None

    def get(self, path: Sequence[Any]) -> list[Origin]:
        """Return the documents that define the merged value at a path.

        Args:
            path: Path in the merged data, e.g. ``("nxos", "devices", 0, "name")``

        Returns:
            Origins in merge order, without duplicates. For a scalar value the
            last origin is the one whose value was kept. Empty if the path
            doesn't exist in the merged data.
        """
        path = tuple(path)
        if not _exists(self._data, path):
            return []
        found = self._direct(path)
        if self._aliased and self._complete:
            # Values merged into an aliased container via another path
            sources = {origin.source for origin in found}
            for equivalent in self._equivalent_paths(path):
                found += [
                    o for o in self._direct(equivalent) if o.source not in sources
                ]
        unique = list(dict.fromkeys(found))
        unique.sort(key=lambda origin: self._order[origin.source])
        return unique

    def _direct(self, path: KeyPath) -> list[Origin]:
        """Origins recorded for exactly this path or inserted at an ancestor."""
        found = [Origin(source, p) for source, p in self._merged.get(path, ())]
        for i in range(len(path) + 1):
            for source, prefix in self._inserted.get(path[:i], ()):
                candidate = prefix + path[i:]
                if _exists(self._documents[source], candidate):
                    found.append(Origin(source, candidate))
        return found

    def _shared_containers(self) -> dict[int, list[KeyPath]]:
        """Map containers reachable via several merged paths to those paths."""
        if self._shared is None:
            paths: dict[int, list[KeyPath]] = {}
            stack: list[tuple[KeyPath, Any]] = [((), self._data)]
            while stack:
                path, node = stack.pop()
                if not isinstance(node, dict | list):
                    continue
                known = paths.get(id(node))
                if known is not None:
                    known.append(path)
                    continue
                paths[id(node)] = [path]
                children = node.items() if isinstance(node, dict) else enumerate(node)
                stack.extend((path + (key,), value) for key, value in children)
            self._shared = {k: v for k, v in paths.items() if len(v) > 1}
        return self._shared

    def _equivalent_paths(self, path: KeyPath) -> list[KeyPath]:
        """Other merged paths that lead to the same value via shared containers."""
        shared = self._shared_containers()
        if not shared:
            return []
        seen = {path}
        queue = [path]
        result: list[KeyPath] = []
        while queue and len(result) < self._MAX_EQUIVALENT_PATHS:
            current = queue.pop()
            node = self._data
            for i in range(len(current) + 1):
                if i:
                    node = node[current[i - 1]]
                if not isinstance(node, dict | list) or id(node) not in shared:
                    continue
                for alternative in shared[id(node)]:
                    # An alternative extending the prefix means the container is
                    # inside itself (recursive alias); following it never ends
                    if len(alternative) > i and alternative[:i] == current[:i]:
                        continue
                    candidate = alternative + current[i:]
                    if candidate not in seen:
                        seen.add(candidate)
                        queue.append(candidate)
                        result.append(candidate)
        return result


# Origin tracking state passed through a merge: the origins being recorded, the
# name of the document being merged, the path in that document and the path in
# the merged data. None when origins are not tracked.
_Track = tuple[Origins, str, KeyPath, KeyPath]


def _track_inserted(track: _Track, src_key: Any, dst_key: Any) -> None:
    origins, source, src_path, dst_path = track
    origins._inserted[dst_path + (dst_key,)].append((source, src_path + (src_key,)))


def _track_merged(track: _Track, src_key: Any, dst_key: Any) -> _Track:
    """Record a merge into an existing node, return the tracking state below it."""
    origins, source, src_path, dst_path = track
    src, dst = src_path + (src_key,), dst_path + (dst_key,)
    origins._merged[dst].append((source, src))
    return (origins, source, src, dst)


def _track_appended(track: _Track, key: Any, start: int, count: int) -> None:
    """Record items appended to the list at ``key``, starting at index ``start``."""
    origins, source, src_path, dst_path = track
    for i in range(count):
        origins._inserted[dst_path + (key, start + i)].append(
            (source, src_path + (key, i))
        )


def _track_dropped(track: _Track, key: Any) -> None:
    origins, source, src_path, dst_path = track
    origins.dropped.append(
        Dropped(Origin(source, src_path + (key,)), dst_path + (key,))
    )


def _track_replaced(track: _Track, key: Any) -> None:
    """Record that the mapping or list at ``key`` is replaced by a scalar."""
    origins, _source, _src_path, dst_path = track
    path = dst_path + (key,)
    replaced = list(dict.fromkeys(origins._direct(path)))
    replaced.sort(key=lambda origin: origins._order[origin.source])
    origins.dropped += [Dropped(origin, path, replaced=True) for origin in replaced]


def _force_pure() -> bool:
    """Return True if the pure Python parser is forced via NAC_YAML_PURE."""
    return os.environ.get(PURE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def _yaml_parser(typ: str | None, pure: bool = False) -> yaml.YAML:
    """Create a ruamel.yaml parser with the !vault and !env tags registered.

    ruamel.yaml uses the libyaml based C parser for ``typ="safe"`` if the
    ``ruamel.yaml.clib`` extension can be imported, and the pure Python parser
    otherwise. The round-trip parser is always pure Python.
    """
    y = yaml.YAML(typ=typ, pure=pure or _force_pure())
    y.preserve_quotes = True
    y.register_class(VaultTag)
    y.register_class(EnvTag)
    return y


def _uses_c_parser(y: yaml.YAML) -> bool:
    """Return True if the parser uses the C extension."""
    c_parser = _ruamel_main.CParser
    return c_parser is not None and y.Parser is c_parser


def yaml_backend(typ: str | None = None) -> str:
    """Return the parser backend parse_yaml() and load_yaml_files() use for a typ.

    Args:
        typ: ruamel.yaml parser type, see load_yaml_files()

    Returns:
        ``"c"`` for the libyaml based C parser of ``ruamel.yaml.clib``, which
        is used for ``typ="safe"`` where available, or ``"python"`` for the
        pure Python parser: always for round-trip, without the extension or
        with the ``NAC_YAML_PURE`` environment variable set
    """
    return "c" if _uses_c_parser(_yaml_parser(typ)) else "python"


def _load(y: yaml.YAML, text: str, source: str, typ: str | None) -> Any:
    """Load a YAML document, with the C parser where it parses like pure Python.

    Documents the C parser would parse differently go to the pure Python
    parser. If the C parser fails, the document is parsed again with the pure
    Python parser: its error, with the offending line, is raised for invalid
    YAML, and its result is used should only the C parser fail.
    """
    if not _uses_c_parser(y):
        return y.load(text)
    if any(marker in text for marker in _C_PARSER_DIVERGES):
        logger.debug("Using pure Python parser for %s", source)
        return _yaml_parser(typ, pure=True).load(text)
    try:
        return y.load(text)
    except Exception as e:
        logger.debug("C parser failed for %s, using pure Python parser: %s", source, e)
        return _yaml_parser(typ, pure=True).load(text)


def _parse(
    y: yaml.YAML, text: str, source: str, typ: str | None
) -> dict[str, Any] | None:
    """Parse a YAML document with the given parser, see parse_yaml()."""
    parsed = _load(y, text, source, typ)
    if parsed is None:
        return None
    if not isinstance(parsed, dict):
        raise YamlRootError(source, type(parsed).__name__)
    return parsed


def parse_yaml(
    text: str, source: str = "<string>", typ: str | None = None
) -> dict[str, Any] | None:
    """Parse a YAML document like load_yaml_files() parses each file.

    The ``!vault`` and ``!env`` tags are registered. ``!env`` values are
    resolved while parsing, ``!vault`` values stay ``VaultTag`` placeholders
    (they are not decrypted).

    Args:
        text: YAML document
        source: Name of the document used in error messages, e.g. its path
        typ: Optional ruamel.yaml parser type, see load_yaml_files(). Documents
             parsed with the default or ``"rt"`` keep the positions of their
             nodes, see node_position().

    Returns:
        The parsed mapping, or None for an empty or comment-only document

    Raises:
        ruamel.yaml.MarkedYAMLError: If the document is not valid YAML; the
            error's ``problem_mark`` holds the line and column
        YamlRootError: If the root of the document is not a mapping
    """
    return _parse(_yaml_parser(typ), text, source, typ)


def find_yaml_files(paths: Sequence[Path | str]) -> list[tuple[Path, bool]]:
    """Find the YAML files load_yaml_files() reads for the given paths.

    Files are returned in the order load_yaml_files() merges them: paths in the
    given order, and the files of a directory sorted by their path relative to
    it, with ``/`` as separator, compared as plain strings. This is the order of
    Terraform's ``fileset()``, e.g. ``a.yaml``, ``a/b.yaml``, ``b.yaml``.

    Explicitly provided files are included if they have a YAML suffix,
    directories are searched recursively. Paths that are neither are ignored.

    Args:
        paths: File or directory paths

    Returns:
        ``(file, explicit)`` tuples; ``explicit`` is True for files provided
        directly and False for files found in a directory
    """
    files: list[tuple[Path, bool]] = []
    for path in paths:
        if os.path.isfile(path):
            if Path(path).suffix in YAML_SUFFIXES:
                files.append((Path(path), True))
            continue
        root = os.fspath(path)
        found: list[tuple[str, Path]] = []
        for dir, _subdir, filenames in os.walk(root):
            relative = os.path.relpath(dir, root)
            prefix = "" if relative == "." else relative.replace(os.sep, "/") + "/"
            for filename in filenames:
                file_path = Path(dir, filename)
                if file_path.suffix in YAML_SUFFIXES:
                    found.append((prefix + filename, file_path))
        # Sort by the whole relative path, not per directory, like fileset()
        found.sort(key=lambda item: item[0])
        files.extend((file_path, False) for _key, file_path in found)
    return files


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
             containers. ``"safe"`` is also several times faster, as it uses the
             libyaml based C parser where available, see yaml_backend().

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

    File order:
        Files are merged in the order of find_yaml_files(): paths in the given
        order, the files of a directory sorted by relative path like
        Terraform's fileset(). The order decides the order of list items and,
        for a scalar defined in several files, which value is kept (the last).

    Behavior with deduplicate=True:
        - Preserves ALL duplicate items within ANY file
        - Duplicates in any file disable merging for that entire list
        - Whether a list is merged or concatenated doesn't depend on file order
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
    y = _yaml_parser(typ)

    def _parse_file(file_path: Path) -> dict[str, Any] | None:
        with open(file_path) as file:
            text = file.read()
        try:
            parsed = _parse(y, text, str(file_path), typ)
        except YamlRootError as e:
            logger.warning(
                "Skipping file with non-mapping root (%s): %s",
                e.root_type,
                file_path,
            )
            return None
        if parsed is None:
            logger.debug("Skipping empty or comment-only file: %s", file_path)
        return parsed

    # Parse all files first. Errors in explicitly provided files are raised,
    # files found in directories are skipped with a warning.
    documents: list[tuple[Path, dict[str, Any], bool]] = []
    for file_path, explicit in find_yaml_files(paths):
        if explicit:
            parsed = _parse_file(file_path)
        else:
            try:
                parsed = _parse_file(file_path)
            except Exception as e:
                logger.warning("Could not load file %s: %s", file_path, e)
                continue
        if parsed is not None:
            documents.append((file_path, parsed, explicit))

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
    track: _Track | None = None,
) -> list[int]:
    """Merge source items into destination list using an inverted index.

    Args:
        source_items: List of items to merge into destination
        destination: Target list (modified in-place)
        deduplicate: When True, merges matching dict items
        cache: Optional duplicate status cache of nested lists
        track: Optional origin tracking state, with the paths of the two lists

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
    for j, source_item in enumerate(source_items):
        # Appended items also need an entry in dest_primitives, as candidate
        # indices refer to positions in destination
        if not isinstance(source_item, dict):
            if track is not None:
                _track_inserted(track, j, len(destination))
            destination.append(source_item)
            dest_primitives.append(None)
            continue

        src_prims = _extract_primitives(source_item)
        if not src_prims:
            if track is not None:
                _track_inserted(track, j, len(destination))
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
                item_track = None if track is None else _track_merged(track, j, ci)
                _merge_dict(
                    source_item, destination[ci], deduplicate, cache, item_track
                )
                # Update primitives cache after merge
                dest_primitives[ci] = _extract_primitives(destination[ci])
                merged.append(ci)
                matched = True
                break

        if not matched:
            # Append and update index so later source items can match
            new_idx = len(destination)
            if track is not None:
                _track_inserted(track, j, new_idx)
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
    track: _Track | None = None,
) -> dict[str, Any]:
    """Implementation of merge_dict() with an optional duplicate status cache.

    The cache is only valid if all changes to the destination lists are made
    through this function with the same cache. Without a cache, the duplicate
    status of destination lists is recomputed on every merge.

    With ``track``, the origins of the merged values are recorded (see Origins).
    """
    if not source:
        return destination
    for key, value in source.items():
        if key not in destination or destination[key] is None:
            destination[key] = value
            if track is not None:
                _track_inserted(track, key, key)
        elif isinstance(value, dict):
            if isinstance(destination[key], dict):
                child = None if track is None else _track_merged(track, key, key)
                _merge_dict(value, destination[key], deduplicate, cache, child)
            elif track is not None:
                _track_dropped(track, key)
        elif isinstance(value, list):
            if isinstance(destination[key], list):
                child = None if track is None else _track_merged(track, key, key)
                if deduplicate:
                    # Skip empty lists
                    if not value or not destination[key]:
                        if track is not None:
                            _track_appended(
                                track, key, len(destination[key]), len(value)
                            )
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
                        if track is not None:
                            _track_appended(
                                track, key, len(destination[key]), len(value)
                            )
                        destination[key] += value
                        if cache is not None:
                            cache.set(destination[key], True)
                    else:
                        # No duplicates: merge matching items across files
                        merged = _merge_list_items_indexed(
                            value, destination[key], deduplicate, cache, child
                        )
                        # Only merged items can have become duplicates of others
                        if cache is not None:
                            cache.set(
                                destination[key],
                                _has_duplicates_involving(destination[key], merged),
                            )
                else:
                    # Simple append (original behavior)
                    if track is not None:
                        _track_appended(track, key, len(destination[key]), len(value))
                    destination[key] += value
            elif track is not None:
                _track_dropped(track, key)
        elif value is not None:
            if track is not None and isinstance(destination[key], dict | list):
                _track_replaced(track, key)
            destination[key] = value
            if track is not None:
                _track_inserted(track, key, key)
    return destination


@dataclass(frozen=True)
class MergeResult:
    """Result of merge_documents().

    Attributes:
        data: The merged data; must not be modified while origins are used
        origins: The documents each merged value comes from
    """

    data: dict[str, Any]
    origins: Origins = field(repr=False)

    @property
    def dropped(self) -> list[Dropped]:
        """Values discarded because of a type mismatch, in merge order."""
        return self.origins.dropped


def _copy_containers(node: Any, memo: dict[int, Any]) -> Any:
    """Copy the maps and lists of a document as plain dicts and lists.

    Scalars are shared, as merging never changes them. Containers reachable
    via several paths (YAML aliases) stay shared in the copy, like with
    copy.deepcopy(), which is much slower for round-trip documents.
    """
    if isinstance(node, dict):
        if id(node) in memo:
            return memo[id(node)]
        copied: dict[Any, Any] = {}
        memo[id(node)] = copied
        for key, value in node.items():
            copied[key] = _copy_containers(value, memo)
        return copied
    if isinstance(node, list):
        if id(node) in memo:
            return memo[id(node)]
        items: list[Any] = []
        memo[id(node)] = items
        items.extend(_copy_containers(value, memo) for value in node)
        return items
    return node


def merge_documents(
    documents: Iterable[tuple[str, dict[str, Any]]],
    deduplicate: bool = True,
) -> MergeResult:
    """Merge parsed documents like load_yaml_files() merges files.

    The documents are merged in the given order and are not modified. The
    origin of every merged value is recorded, see Origins.

    Unlike load_yaml_files(), ``!vault`` values are not decrypted; identical
    encrypted values compare equal when merging.

    Args:
        documents: ``(source, document)`` tuples, e.g. from parse_yaml(); the
                   source names the document in the origins and must be unique
        deduplicate: See load_yaml_files()

    Returns:
        The merged data, as plain dicts and lists, and its origins. The origins
        refer to the documents passed in and to the merged data, so neither
        must be modified while origins are looked up.

    Raises:
        ValueError: If two documents have the same source
    """
    documents = list(documents)
    sources = [source for source, _ in documents]
    if len(set(sources)) != len(sources):
        raise ValueError("Document sources must be unique")
    result: dict[str, Any] = {}
    origins = Origins(documents, result)
    cache = _DuplicateCache() if deduplicate else None
    for source, document in documents:
        # Merging inserts and changes parts of the source, so merge a copy and
        # keep the original for looking up origins
        document = _copy_containers(document, {})
        if _contains_aliases(document):
            origins._aliased = True
            cache = None
        origins._merged[()].append((source, ()))
        _merge_dict(document, result, deduplicate, cache, (origins, source, (), ()))
    origins._complete = True
    return MergeResult(result, origins)


def node_position(document: Any, path: Sequence[Any]) -> tuple[int, int] | None:
    """Return the position of the key or list item at a path in a document.

    Only documents parsed in round-trip mode (the default typ of parse_yaml()
    and load_yaml_files(), or ``"rt"``) keep positions.

    Args:
        document: Parsed document
        path: Path of a map key or list item; empty for the document itself

    Returns:
        0-based ``(line, column)``, or None if the path doesn't exist or the
        document has no positions
    """
    path = tuple(path)
    if not _exists(document, path):
        return None
    if not path:
        lc = getattr(document, "lc", None)
        if lc is None or lc.line is None:
            return None
        return (int(lc.line), int(lc.col))
    parent = document
    for key in path[:-1]:
        parent = parent[key]
    lc = getattr(parent, "lc", None)
    if lc is None:
        return None
    try:
        position = lc.key(path[-1]) if isinstance(parent, dict) else lc.item(path[-1])
    except (KeyError, IndexError, TypeError):
        # E.g. keys added by a YAML merge key (<<) have no position of their own
        return None
    return (int(position[0]), int(position[1]))


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
            # The round-trip emitter is pure Python; the C emitter would ignore
            # the sequence indent and sort the keys
            y = yaml.YAML()
            y.explicit_start = True
            y.default_flow_style = False
            y.indent(mapping=2, sequence=4, offset=2)
            y.dump(data, fh)
    except:  # noqa: E722
        logger.error(f"Cannot write file: {path}")
