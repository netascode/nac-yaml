# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

import copy
import os
import pickle
import random
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml.error import MarkedYAMLError

from nac_yaml import yaml
from tests.unit.test_yaml import _random_value

pytestmark = pytest.mark.unit


def _docs(*texts: str, typ: str | None = None) -> list[tuple[str, Any]]:
    """Parse documents named a, b, c, ..."""
    docs = []
    for i, text in enumerate(texts):
        doc = yaml.parse_yaml(text, typ=typ)
        assert doc is not None
        docs.append((chr(ord("a") + i), doc))
    return docs


def _origins(result: yaml.MergeResult, *path: Any) -> list[tuple[str, tuple[Any, ...]]]:
    return [(o.source, o.path) for o in result.origins.get(path)]


def _walk(node: Any, path: tuple[Any, ...] = ()) -> Any:
    yield path, node
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _walk(v, path + (k,))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _walk(v, path + (i,))


def _get(node: Any, path: tuple[Any, ...]) -> Any:
    for key in path:
        node = node[key]
    return node


def _check_origins(result: yaml.MergeResult, docs: list[tuple[str, Any]]) -> None:
    """Every merged value has an origin; a scalar's last origin holds its value."""
    by_source = dict(docs)
    for path, value in _walk(result.data):
        origins = result.origins.get(path)
        assert origins, f"no origin for {path}"
        if not isinstance(value, dict | list):
            last = origins[-1]
            assert _get(by_source[last.source], last.path) == value, path


def _write_files(tmpdir: Path, contents: list[str]) -> list[Path]:
    paths = []
    for i, content in enumerate(contents):
        path = Path(tmpdir, f"file{i}.yaml")
        path.write_text(content)
        paths.append(path)
    return paths


def _parse_files(paths: list[Path]) -> list[tuple[str, dict[str, Any]]]:
    docs = []
    for path in paths:
        doc = yaml.parse_yaml(path.read_text(), str(path))
        if doc is not None:
            docs.append((str(path), doc))
    return docs


def _merge_files(paths: list[Path], **kwargs: Any) -> yaml.MergeResult:
    return yaml.merge_documents(_parse_files(paths), **kwargs)


# merge_documents: same data as load_yaml_files


@pytest.mark.parametrize("deduplicate", [True, False])
def test_merge_documents_fixtures(deduplicate: bool) -> None:
    paths = [
        Path("tests/unit/fixtures/data_merge/file1.yaml"),
        Path("tests/unit/fixtures/data_merge/file2.yaml"),
    ]
    result = _merge_files(paths, deduplicate=deduplicate)
    assert result.data == yaml.load_yaml_files(paths, deduplicate=deduplicate)


@pytest.mark.parametrize("seed", range(50))
def test_merge_documents_matches_load_yaml_files(tmpdir: Path, seed: int) -> None:
    rng = random.Random(seed)
    paths = []
    for i in range(6):
        path = Path(tmpdir, f"file{i}.yaml")
        yaml.write_yaml_file(
            {"root": {"list": _random_value(rng), "map": _random_value(rng)}}, path
        )
        paths.append(path)
    docs = _parse_files(paths)
    docs_before = copy.deepcopy(docs)
    result = yaml.merge_documents(docs)
    assert result.data == yaml.load_yaml_files(paths)
    assert docs == docs_before
    _check_origins(result, docs)


def test_merge_documents_directory_order(tmpdir: Path) -> None:
    """Files of a directory are merged in the order of find_yaml_files()."""
    Path(tmpdir, "sub").mkdir()
    for i, name in enumerate(["b.yaml", "a.yaml", "sub/c.yaml", "d.yml"]):
        Path(tmpdir, name).write_text(f"x: {i}\nl:\n  - name: n{i}\n")
    files = [path for path, _ in yaml.find_yaml_files([Path(tmpdir)])]
    assert _merge_files(files).data == yaml.load_yaml_files([Path(tmpdir)])


def test_merge_documents_does_not_modify_inputs_rt() -> None:
    docs = _docs(
        "d:\n  - name: s1\n    ifs:\n      - id: 1\n",
        "d:\n  - name: s1\n    ifs:\n      - id: 1\n        mtu: 9000\n      - id: 2\n",
    )
    before = copy.deepcopy(docs)
    result = yaml.merge_documents(docs)
    assert docs == before
    assert type(result.data) is dict
    assert type(result.data["d"]) is list
    assert result.data == {
        "d": [{"name": "s1", "ifs": [{"id": 1, "mtu": 9000}, {"id": 2}]}]
    }
    _check_origins(result, docs)


def test_merge_documents_aliases(tmpdir: Path) -> None:
    contents = [
        "a:\n  - &item\n    name: n\nb:\n  - *item\n  - id: 1\n",
        "b:\n  - name: z\na:\n  - name: n\n    id: 1\n",
        "b:\n  - id: 1\n    c: 3\n",
    ]
    paths = _write_files(tmpdir, contents)
    assert _merge_files(paths).data == yaml.load_yaml_files(paths)


def test_origins_aliases() -> None:
    """Values merged into an aliased container have origins at all its paths."""
    docs = _docs(
        "a:\n  - &item\n    name: n\nb:\n  - *item\n  - id: 1\n",
        # Merges into a[0], which is the same item as b[0]
        "b:\n  - name: z\na:\n  - name: n\n    id: 1\n",
        "b:\n  - id: 1\n    c: 3\n",
    )
    result = yaml.merge_documents(docs)
    assert result.data["b"][0] == {"name": "n", "id": 1}
    assert _origins(result, "a", 0, "id") == [("b", ("a", 0, "id"))]
    assert _origins(result, "b", 0, "id") == [("b", ("a", 0, "id"))]
    assert _origins(result, "b", 0, "name") == [
        ("a", ("b", 0, "name")),
        ("b", ("a", 0, "name")),
    ]
    _check_origins(result, docs)


def test_origins_recursive_container() -> None:
    """A container that contains itself doesn't make lookups loop.

    ruamel.yaml turns a recursive alias into null, so build one in Python.
    """
    items: list[Any] = [{"name": "n"}]
    items.append(items)
    result = yaml.merge_documents(
        [("a", {"a": items}), ("b", {"a": [{"name": "n", "v": 1}]})]
    )
    assert result.data["a"][1] is result.data["a"]
    assert _origins(result, "a", 0, "v") == [("b", ("a", 0, "v"))]
    assert _origins(result, "a", 1, 1, 0, "v") == [("b", ("a", 0, "v"))]


def test_origins_aliases_replaced() -> None:
    """Aliased containers that were replaced leave nothing shared to look up."""
    docs = _docs("x:\n  p: &i\n    n: 1\n  q: *i\n", "x: 5\n")
    result = yaml.merge_documents(docs)
    assert _origins(result, "x") == [("a", ("x",)), ("b", ("x",))]


def test_merge_documents_iterable() -> None:
    docs = _docs("x: 1\n", "y: 2\n")
    result = yaml.merge_documents(doc for doc in docs)
    assert result.data == {"x": 1, "y": 2}
    assert _origins(result, "y") == [("b", ("y",))]


def test_merge_documents_unique_sources() -> None:
    with pytest.raises(ValueError, match="unique"):
        yaml.merge_documents([("a", {"x": 1}), ("a", {"y": 1})])


# Origins of each kind of merge


def test_origins_items_merged() -> None:
    result = yaml.merge_documents(
        _docs(
            "l:\n  - name: x\n    v: 1\n    sub: [1, 2]\n",
            "l:\n  - name: x\n    w: 3\n    sub: [3]\n",
        )
    )
    assert result.data == {"l": [{"name": "x", "v": 1, "sub": [1, 2, 3], "w": 3}]}
    assert _origins(result) == [("a", ()), ("b", ())]
    assert _origins(result, "l") == [("a", ("l",)), ("b", ("l",))]
    assert _origins(result, "l", 0) == [("a", ("l", 0)), ("b", ("l", 0))]
    assert _origins(result, "l", 0, "name") == [
        ("a", ("l", 0, "name")),
        ("b", ("l", 0, "name")),
    ]
    assert _origins(result, "l", 0, "v") == [("a", ("l", 0, "v"))]
    assert _origins(result, "l", 0, "w") == [("b", ("l", 0, "w"))]
    assert _origins(result, "l", 0, "sub") == [
        ("a", ("l", 0, "sub")),
        ("b", ("l", 0, "sub")),
    ]
    assert _origins(result, "l", 0, "sub", 1) == [("a", ("l", 0, "sub", 1))]
    assert _origins(result, "l", 0, "sub", 2) == [("b", ("l", 0, "sub", 0))]


def test_origins_conflicting_primitives() -> None:
    """Items with different shared primitives are not merged."""
    result = yaml.merge_documents(
        _docs("l:\n  - name: x\n    v: 1\n", "l:\n  - name: x\n    v: 2\n")
    )
    assert result.data == {"l": [{"name": "x", "v": 1}, {"name": "x", "v": 2}]}
    assert _origins(result, "l", 0) == [("a", ("l", 0))]
    assert _origins(result, "l", 1) == [("b", ("l", 0))]
    assert _origins(result, "l", 1, "v") == [("b", ("l", 0, "v"))]


def test_origins_scalar_override() -> None:
    result = yaml.merge_documents(_docs("x:\n  v: 1\n", "x:\n  v: 2\n"))
    assert result.data == {"x": {"v": 2}}
    assert _origins(result, "x", "v") == [("a", ("x", "v")), ("b", ("x", "v"))]


def test_origins_scalar_replaces_map() -> None:
    result = yaml.merge_documents(_docs("x:\n  y: 1\n", "x: 5\n"))
    assert result.data == {"x": 5}
    assert _origins(result, "x") == [("a", ("x",)), ("b", ("x",))]
    assert _origins(result, "x", "y") == []


def test_origins_null_replaced() -> None:
    result = yaml.merge_documents(_docs("x: null\n", "x:\n  y: 1\n"))
    assert result.data == {"x": {"y": 1}}
    assert _origins(result, "x") == [("a", ("x",)), ("b", ("x",))]
    assert _origins(result, "x", "y") == [("b", ("x", "y"))]


def test_origins_duplicates_concatenated() -> None:
    result = yaml.merge_documents(
        _docs("l:\n  - name: x\n  - name: x\n", "l:\n  - name: x\n    v: 1\n")
    )
    assert result.data == {"l": [{"name": "x"}, {"name": "x"}, {"name": "x", "v": 1}]}
    assert _origins(result, "l", 1) == [("a", ("l", 1))]
    assert _origins(result, "l", 2) == [("b", ("l", 0))]
    assert _origins(result, "l", 2, "v") == [("b", ("l", 0, "v"))]


def test_origins_empty_list() -> None:
    result = yaml.merge_documents(_docs("l: []\n", "l:\n  - 1\n  - name: z\n"))
    assert result.data == {"l": [1, {"name": "z"}]}
    assert _origins(result, "l") == [("a", ("l",)), ("b", ("l",))]
    assert _origins(result, "l", 1, "name") == [("b", ("l", 1, "name"))]


def test_origins_primitive_items() -> None:
    result = yaml.merge_documents(_docs("l: [1, 2]\n", "l: [2]\n"))
    assert result.data == {"l": [1, 2, 2]}
    assert _origins(result, "l", 2) == [("b", ("l", 0))]


def test_origins_no_deduplicate() -> None:
    docs = _docs("l:\n  - name: x\n", "l:\n  - name: x\n    v: 1\n")
    result = yaml.merge_documents(docs, deduplicate=False)
    assert result.data == {"l": [{"name": "x"}, {"name": "x", "v": 1}]}
    assert _origins(result, "l", 1, "v") == [("b", ("l", 0, "v"))]
    _check_origins(result, docs)


def test_origins_nested_lists_in_merged_items() -> None:
    docs = _docs(
        "d:\n  - name: s1\n    ifs:\n      - id: 1\n        mtu: 9000\n",
        "d:\n  - name: s1\n    ifs:\n      - id: 1\n        desc: up\n      - id: 2\n",
        "d:\n  - name: s1\n    ifs:\n      - id: 2\n        mtu: 1500\n",
    )
    result = yaml.merge_documents(docs)
    assert _origins(result, "d", 0, "ifs", 0, "desc") == [
        ("b", ("d", 0, "ifs", 0, "desc"))
    ]
    assert _origins(result, "d", 0, "ifs", 1) == [
        ("b", ("d", 0, "ifs", 1)),
        ("c", ("d", 0, "ifs", 0)),
    ]
    assert _origins(result, "d", 0, "ifs", 1, "mtu") == [
        ("c", ("d", 0, "ifs", 0, "mtu"))
    ]
    _check_origins(result, docs)


def test_origins_missing_path() -> None:
    result = yaml.merge_documents(_docs("x:\n  l: [1]\n"))
    assert result.origins.get(("y",)) == []
    assert result.origins.get(("x", "l", 1)) == []
    assert result.origins.get(("x", "l", "a")) == []


@pytest.mark.parametrize(
    "first, second",
    [
        pytest.param("x:\n  y: 1\n", "x: [1]\n", id="list_onto_map"),
        pytest.param("x: [1]\n", "x:\n  y: 1\n", id="map_onto_list"),
        pytest.param("x: 1\n", "x:\n  y: 1\n", id="map_onto_scalar"),
    ],
)
def test_dropped(first: str, second: str) -> None:
    result = yaml.merge_documents(_docs(first, second))
    assert result.data == yaml.parse_yaml(first)
    assert result.dropped == [yaml.Dropped(yaml.Origin("b", ("x",)), ("x",))]
    assert _origins(result, "x") == [("a", ("x",))]


@pytest.mark.parametrize(
    "first",
    [
        pytest.param("x:\n  y: 1\n", id="map"),
        pytest.param("x: [1]\n", id="list"),
    ],
)
def test_dropped_replaced_by_scalar(first: str) -> None:
    result = yaml.merge_documents(_docs(first, "x: 5\n"))
    assert result.data == {"x": 5}
    assert result.dropped == [
        yaml.Dropped(yaml.Origin("a", ("x",)), ("x",), replaced=True)
    ]
    assert _origins(result, "x")[-1] == ("b", ("x",))


def test_dropped_replaced_map_from_several_documents() -> None:
    result = yaml.merge_documents(_docs("x:\n  y: 1\n", "x:\n  z: 2\n", "x: 5\n"))
    assert result.dropped == [
        yaml.Dropped(yaml.Origin("a", ("x",)), ("x",), replaced=True),
        yaml.Dropped(yaml.Origin("b", ("x",)), ("x",), replaced=True),
    ]


def test_scalar_override_not_dropped() -> None:
    assert yaml.merge_documents(_docs("x: 1\n", "x: 2\n")).dropped == []


def test_origins_with_positions() -> None:
    """Origins of round-trip documents can be turned into line numbers."""
    docs = _docs(
        "l:\n  - name: x\n",
        "other: 1\nl:\n  - name: x\n    v: 1\n",
    )
    result = yaml.merge_documents(docs)
    (origin,) = result.origins.get(("l", 0, "v"))
    assert yaml.node_position(dict(docs)[origin.source], origin.path) == (3, 4)


# Vault values


def test_vault_tag_equality() -> None:
    assert yaml.VaultTag("x") == yaml.VaultTag("x")
    assert hash(yaml.VaultTag("x")) == hash(yaml.VaultTag("x"))
    assert yaml.VaultTag("x") != yaml.VaultTag("y")
    assert yaml.VaultTag("x") != "x"


def _vault(ciphertext: str) -> str:
    return (
        f"    password: !vault |\n      $ANSIBLE_VAULT;1.1;AES256\n      {ciphertext}\n"
    )


def test_merge_documents_same_vault_values_merge() -> None:
    """Items with identical encrypted values match, without decrypting them."""
    result = yaml.merge_documents(
        _docs(
            "users:\n  - name: u\n" + _vault("6162"),
            "users:\n  - name: u\n" + _vault("6162") + "    role: admin\n",
        )
    )
    (user,) = result.data["users"]
    assert isinstance(user["password"], yaml.VaultTag)
    assert user["role"] == "admin"


def test_merge_documents_different_vault_values_stay_apart() -> None:
    result = yaml.merge_documents(
        _docs(
            "users:\n  - name: u\n" + _vault("6162"),
            "users:\n  - name: u\n" + _vault("6364") + "    role: admin\n",
        )
    )
    assert [u.get("role") for u in result.data["users"]] == [None, "admin"]


# parse_yaml


@pytest.mark.parametrize("text", ["", "# comment only\n", "---\n"])
def test_parse_yaml_empty(text: str) -> None:
    assert yaml.parse_yaml(text) is None


def test_parse_yaml_root_not_mapping() -> None:
    with pytest.raises(yaml.YamlRootError, match="data.yaml: root is a") as e:
        yaml.parse_yaml("- a\n", "data.yaml")
    assert e.value.root_type == "CommentedSeq"
    with pytest.raises(yaml.YamlRootError) as e:
        yaml.parse_yaml("- a\n", typ="safe")
    assert e.value.root_type == "list"


def test_yaml_root_error_pickle() -> None:
    error = pickle.loads(pickle.dumps(yaml.YamlRootError("f.yaml", "list")))
    assert (error.source, error.root_type) == ("f.yaml", "list")
    assert str(error) == "f.yaml: root is a list, not a mapping"


def test_parse_yaml_syntax_error() -> None:
    with pytest.raises(MarkedYAMLError) as e:
        yaml.parse_yaml("a:\n  b: 1\n  c\nd: 2\n")
    assert e.value.problem_mark is not None
    assert e.value.problem_mark.line == 3


def test_parse_yaml_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NAC_YAML_TEST_ENV", "value")
    doc = yaml.parse_yaml("e: !env NAC_YAML_TEST_ENV\nv: !vault |\n  abc\n")
    assert doc is not None
    assert doc["e"] == "value"
    assert isinstance(doc["v"], yaml.VaultTag)


@pytest.mark.parametrize("typ", [None, "rt", "safe"])
def test_parse_yaml_matches_load_yaml_files(typ: str | None) -> None:
    path = Path("tests/unit/fixtures/data_merge/file1.yaml")
    assert yaml.parse_yaml(path.read_text(), typ=typ) == yaml.load_yaml_files(
        [path], typ=typ
    )


# find_yaml_files


def test_find_yaml_files(tmpdir: Path) -> None:
    root = Path(tmpdir)
    (root / "sub").mkdir()
    for name in [
        "a.yaml",
        "b.yml",
        "c.txt",
        "sub/d.yaml",
        "sub/e.json",
        "x.yaml",
        "x.txt",
    ]:
        (root / name).write_text("a: 1\n")
    data = root / "data"
    data.mkdir()
    for name in ["a.yaml", "b.yml", "c.txt"]:
        (data / name).write_text("a: 1\n")
    (data / "sub").mkdir()
    (data / "sub" / "d.yaml").write_text("a: 1\n")

    found = yaml.find_yaml_files([data])
    assert {(p.relative_to(data).as_posix(), e) for p, e in found} == {
        ("a.yaml", False),
        ("b.yml", False),
        ("sub/d.yaml", False),
    }
    assert yaml.find_yaml_files(
        [root / "x.yaml", root / "x.txt", str(root / "missing")]
    ) == [(root / "x.yaml", True)]


def test_find_yaml_files_sorted(tmpdir: Path) -> None:
    """Files are sorted by whole relative path, like Terraform's fileset()."""
    root = Path(tmpdir, "data")
    names = [
        "sub/c.yaml",
        "b.yaml",
        "sub.yaml",
        "a/x/y.yml",
        "Z.yaml",
        "a.yaml",
        "a/z.yaml",
        "sub/b.yaml",
    ]
    for name in names:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text("a: 1\n")
    found = yaml.find_yaml_files([root])
    # Upper case sorts before lower case and "." before "/", so "a.yaml" before "a/..." and "sub.yaml" before "sub/..."
    assert [p.relative_to(root).as_posix() for p, _ in found] == [
        "Z.yaml",
        "a.yaml",
        "a/x/y.yml",
        "a/z.yaml",
        "b.yaml",
        "sub.yaml",
        "sub/b.yaml",
        "sub/c.yaml",
    ]
    # With a trailing separator, and as a string, the order is the same
    assert yaml.find_yaml_files([str(root) + os.sep]) == [
        (Path(str(root) + os.sep, p.relative_to(root)), e) for p, e in found
    ]


def test_find_yaml_files_path_order(tmpdir: Path) -> None:
    """Paths keep their order; only the files of each directory are sorted."""
    root = Path(tmpdir)
    for name in ["d2/a.yaml", "d1/b.yaml", "d1/a.yaml", "x.yaml"]:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text("a: 1\n")
    found = yaml.find_yaml_files([root / "d2", root / "x.yaml", root / "d1"])
    assert [(p.relative_to(root).as_posix(), e) for p, e in found] == [
        ("d2/a.yaml", False),
        ("x.yaml", True),
        ("d1/a.yaml", False),
        ("d1/b.yaml", False),
    ]


def test_load_yaml_files_sorted(tmpdir: Path) -> None:
    """List items and overridden scalars follow the sorted file order."""
    root = Path(tmpdir)
    for name in ["c.yaml", "a.yaml", "b/x.yaml", "b.yaml"]:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(f"last: {name}\nl:\n  - name: {name}\n")
    data = yaml.load_yaml_files([root])
    assert data["l"] == [
        {"name": "a.yaml"},
        {"name": "b.yaml"},
        {"name": "b/x.yaml"},
        {"name": "c.yaml"},
    ]
    assert data["last"] == "c.yaml"


# node_position


def test_node_position() -> None:
    doc = yaml.parse_yaml("a:\n  b: 1\n  l:\n    - x\n    - name: y\n")
    assert yaml.node_position(doc, ()) == (0, 0)
    assert yaml.node_position(doc, ("a",)) == (0, 0)
    assert yaml.node_position(doc, ("a", "b")) == (1, 2)
    assert yaml.node_position(doc, ("a", "l")) == (2, 2)
    assert yaml.node_position(doc, ("a", "l", 0)) == (3, 6)
    assert yaml.node_position(doc, ("a", "l", 1)) == (4, 6)
    assert yaml.node_position(doc, ("a", "l", 1, "name")) == (4, 6)
    assert yaml.node_position(doc, ("a", "z")) is None
    assert yaml.node_position(doc, ("a", "l", 5)) is None


def test_node_position_without_positions() -> None:
    assert yaml.node_position(yaml.parse_yaml("a: 1\n", typ="safe"), ("a",)) is None
    assert yaml.node_position({"a": 1}, ()) is None


def test_node_position_merge_key() -> None:
    doc = yaml.parse_yaml("base: &b\n  x: 1\nitem:\n  <<: *b\n  y: 2\n")
    assert yaml.node_position(doc, ("item", "y")) == (4, 2)
    assert yaml.node_position(doc, ("item", "x")) is None
