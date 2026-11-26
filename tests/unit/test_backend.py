# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

import math
import os
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import main as ruamel_main
from ruamel.yaml.error import MarkedYAMLError

from nac_yaml import yaml

pytestmark = pytest.mark.unit

C_AVAILABLE = ruamel_main.CParser is not None
requires_c = pytest.mark.skipif(not C_AVAILABLE, reason="ruamel.yaml.clib missing")

# Documents the C and the pure Python parser must parse identically
DOCUMENTS = {
    "yaml11_scalars": "a: yes\nb: no\nc: on\nd: off\ne: y\nf: 010\ng: 1:20\nh: 0b11\n",
    "yaml12_scalars": "a: 0o10\nb: 0x1F\nc: 1e3\nd: .inf\ne: -.inf\nf: .nan\ng: ~\nh: null\n",
    "numbers": "a: 12345678901234567890\nb: -1\nc: +1\nd: 1_000\ne: 1.5\nf: '1'\n",
    "booleans": "a: true\nb: false\nc: True\nd: FALSE\n",
    "timestamps": "a: 2024-01-01\nb: 2024-01-01T10:00:00Z\nc: 2024-01-01 10:00:00.5\n",
    "strings": "a: 'single'\nb: \"double \\t\\u00e9\"\nc: plain text\nd: 'it''s'\n",
    "block_scalars": "a: |\n  line1\n  line2\nb: >\n  folded\n  text\nc: |-\n  strip\nd: >+\n  keep\n\n",
    "flow": "a: {b: [1, 2, {c: d}], e: []}\nf: {}\n",
    "nested": "a:\n  - name: x\n    b:\n      - 1\n      - c: 2\n  - name: y\n",
    "anchors": "base: &b {x: 1, y: 2}\nk:\n  <<: *b\n  y: 3\nl: *b\n",
    "merge_list": "a: &a {x: 1}\nb: &b {y: 2}\nc:\n  <<: [*a, *b]\n",
    "unicode": 'name: Zürich\nemoji: "\U0001f600"\nkey_ü: wert\n',
    "bom": "\ufeffa: 1\n",
    "crlf": "a: 1\r\nb:\r\n  - x\r\n  - y\r\n",
    "tabs_in_value": 'a: x\ty\nb: "\\t"\n',
    "comments": "# head\na: 1  # trailing\n# middle\nb: 2\n",
    "document_markers": "---\na: 1\n...\n",
    "yaml_directive": "%YAML 1.2\n---\na: 0o10\n",
    "empty": "",
    "comment_only": "# only a comment\n",
    "start_only": "---\n",
    "tags": "a: !env NAC_YAML_TEST_ENV\nb: !vault |\n  $ANSIBLE_VAULT;1.1;AES256\n  6636\n",
    "explicit_tags": "a: !!str 1\nb: !!int '2'\nc: !!float 3\n",
    "deep": "a:" + "".join(f"\n{'  ' * i}- b:" for i in range(1, 60)) + " 1\n",
    "root_list": "- a\n- b\n",
    "root_scalar": "just a string\n",
    "duplicate_keys": "a: 1\na: 2\n",
    "unclosed_flow": "a: 1\nb: [1, 2\nc: 3\n",
    "bad_indentation": "a:\n  b: 1\n c: 2\n",
    "tab_indentation": "a:\n\tb: 1\n",
    "unknown_tag": "a: !unknown x\n",
    "undefined_alias": "a: *missing\n",
    "multiple_documents": "a: 1\n---\nb: 2\n",
    "unhashable_key": "? [1, 2]\n: x\n",
    "invalid_escape": 'a: "\\q"\n',
    # The C parser parses these differently, so they go to the pure parser
    "tab_after_colon": "a:\tx\n",
    "tab_after_key": "a\t: 1\n",
    "tab_trailing": "a: x\t\n",
    "tab_block_scalar": "a: |\n  x\ty\n",
    "yaml11_directive": "%YAML 1.1\n---\na: yes\nb: 010\n",
    "yaml13_directive": "%YAML 1.3\n---\na: 1\n",
    # The C parser fails on these, so the pure parser's result or error is used
    "next_line": "a: x\x85y\n",
    "line_separator": "a: x\u2028y\n",
    "surrogate": "a: \ud800\n",
    "control_character": "a: x\x07y\n",
    "long_key": "k" * 1100 + ": 1\n",
}


def _normalize(value: Any) -> Any:
    """Make parse results comparable: NaN != NaN, VaultTag reprs hide content."""
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    if isinstance(value, float) and math.isnan(value):
        return "nan"
    if isinstance(value, yaml.VaultTag):
        return ("vault", value.value)
    return (type(value).__name__, value)


def _outcome(text: str) -> Any:
    """Parse with parse_yaml(typ="safe") and return the result or the error."""
    try:
        return ("ok", _normalize(yaml.parse_yaml(text, typ="safe")))
    except Exception as e:
        mark = e.problem_mark if isinstance(e, MarkedYAMLError) else None
        position = (mark.line, mark.column) if mark is not None else None
        return ("error", type(e).__name__, str(e), position)


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NAC_YAML_TEST_ENV", "value")
    monkeypatch.delenv(yaml.PURE_ENV, raising=False)


@pytest.mark.skipif(
    "NAC_YAML_EXPECT_C" not in os.environ, reason="NAC_YAML_EXPECT_C not set"
)
def test_expected_backend() -> None:
    """CI sets NAC_YAML_EXPECT_C, so a wrong dependency marker fails the build."""
    expected = "c" if os.environ["NAC_YAML_EXPECT_C"] == "1" else "python"
    assert yaml.yaml_backend("safe") == expected


@pytest.mark.parametrize("typ", [None, "rt"])
def test_yaml_backend_round_trip_is_python(typ: str | None) -> None:
    assert yaml.yaml_backend(typ) == "python"


@requires_c
def test_yaml_backend_safe_is_c() -> None:
    assert yaml.yaml_backend("safe") == "c"


@pytest.mark.parametrize("value", ["1", "true", "YES", " on "])
def test_yaml_backend_pure_env(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv(yaml.PURE_ENV, value)
    assert yaml.yaml_backend("safe") == "python"


@pytest.mark.parametrize("value", ["", "0", "false"])
def test_yaml_backend_pure_env_disabled(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv(yaml.PURE_ENV, value)
    assert yaml.yaml_backend("safe") == ("c" if C_AVAILABLE else "python")


def test_yaml_backend_without_extension(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ruamel_main, "CParser", None)
    assert yaml.yaml_backend("safe") == "python"


@requires_c
@pytest.mark.parametrize("name", sorted(DOCUMENTS))
def test_c_parser_matches_pure_parser(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    text = DOCUMENTS[name]
    c_outcome = _outcome(text)
    monkeypatch.setenv(yaml.PURE_ENV, "1")
    pure_outcome = _outcome(text)
    assert c_outcome == pure_outcome


@requires_c
@pytest.mark.parametrize(
    "name",
    sorted(
        name
        for name, text in DOCUMENTS.items()
        if not any(marker in text for marker in yaml._C_PARSER_DIVERGES)
    ),
)
def test_raw_c_parser_never_differs(name: str) -> None:
    """The C parser itself either fails or parses like the pure parser.

    Documents it would parse differently never reach it. If it fails, the
    fallback uses the pure parser's result or error.
    """
    text = DOCUMENTS[name]

    def load(pure: bool) -> Any:
        try:
            return ("ok", _normalize(yaml._yaml_parser("safe", pure=pure).load(text)))
        except Exception:
            return ("error",)

    c_result = load(pure=False)
    if c_result[0] == "ok":
        assert c_result == load(pure=True)


@requires_c
@pytest.mark.parametrize("text", ["a: |\n  x\ty\n", "%YAML 1.1\n---\na: yes\n"])
def test_diverging_documents_skip_c_parser(mocker: Any, text: str) -> None:
    spy = mocker.spy(yaml, "_yaml_parser")
    yaml.parse_yaml(text, typ="safe")
    assert spy.call_args_list[-1].kwargs == {"pure": True}


@requires_c
def test_c_parser_error_has_snippet() -> None:
    with pytest.raises(MarkedYAMLError) as excinfo:
        yaml.parse_yaml(DOCUMENTS["unclosed_flow"], typ="safe")
    assert excinfo.value.problem_mark is not None
    assert (excinfo.value.problem_mark.line, excinfo.value.problem_mark.column) == (
        2,
        1,
    )
    # Only the pure Python parser's marks include the offending line
    assert "c: 3" in str(excinfo.value)


class _BrokenCParser:
    """Stand-in for a C parser that fails on any input."""

    def __init__(self, stream: Any) -> None:
        raise RuntimeError("broken C parser")


def test_fallback_when_c_parser_fails(
    monkeypatch: pytest.MonkeyPatch, tmpdir: Path
) -> None:
    monkeypatch.setattr(ruamel_main, "CParser", _BrokenCParser)
    assert yaml.yaml_backend("safe") == "c"
    assert yaml.parse_yaml("a: 1\n", typ="safe") == {"a": 1}

    path = Path(tmpdir, "data.yaml")
    path.write_text("a:\n  - name: x\n")
    assert yaml.load_yaml_files([path], typ="safe") == {"a": [{"name": "x"}]}


def test_fallback_raises_pure_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ruamel_main, "CParser", _BrokenCParser)
    with pytest.raises(MarkedYAMLError):
        yaml.parse_yaml(DOCUMENTS["unclosed_flow"], typ="safe")


@pytest.mark.parametrize("c_parser", [None, _BrokenCParser])
def test_load_yaml_files_without_working_c_parser(
    monkeypatch: pytest.MonkeyPatch, c_parser: Any
) -> None:
    """Loading gives the same result without the extension or with a broken one."""
    paths = [
        Path("tests/unit/fixtures/data_merge/file1.yaml"),
        Path("tests/unit/fixtures/data_merge/file2.yaml"),
    ]
    expected = yaml.load_yaml_files(paths, typ="safe")
    monkeypatch.setattr(ruamel_main, "CParser", c_parser)
    assert yaml.load_yaml_files(paths, typ="safe") == expected


def test_round_trip_errors_are_not_retried(mocker: Any) -> None:
    spy = mocker.spy(yaml, "_yaml_parser")
    with pytest.raises(MarkedYAMLError):
        yaml.parse_yaml(DOCUMENTS["unclosed_flow"])
    assert spy.call_count == 1
