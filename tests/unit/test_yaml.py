# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

import filecmp
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from nac_yaml import yaml

pytestmark = pytest.mark.unit


def _is_only_dict_list_tree(value: Any) -> bool:
    if isinstance(value, dict):
        return all(_is_only_dict_list_tree(v) for v in value.values())
    if isinstance(value, list):
        return all(_is_only_dict_list_tree(v) for v in value)
    return True


@pytest.mark.parametrize("typ", [None, "rt", "safe"])
def test_load_yaml_files(tmpdir: Path, typ: str | None) -> None:
    input_path_1 = Path("tests/unit/fixtures/data_merge/file1.yaml")
    input_path_2 = Path("tests/unit/fixtures/data_merge/file2.yaml")
    output_path = Path(tmpdir, "output.yaml")
    result_path = Path("tests/unit/fixtures/data_merge/result.yaml")
    result_no_deduplicate_path = Path(
        "tests/unit/fixtures/data_merge/result_no_deduplicate.yaml"
    )

    data = yaml.load_yaml_files([input_path_1, input_path_2], typ=typ)
    if typ == "safe":
        assert _is_only_dict_list_tree(data)
    yaml.write_yaml_file(data, output_path)
    assert filecmp.cmp(output_path, result_path, shallow=False)

    data = yaml.load_yaml_files(
        [input_path_1, input_path_2], deduplicate=False, typ=typ
    )
    if typ == "safe":
        assert _is_only_dict_list_tree(data)
    yaml.write_yaml_file(data, output_path)
    assert filecmp.cmp(output_path, result_no_deduplicate_path, shallow=False)

    input_path = Path("tests/unit/fixtures/data_env/")
    os.environ["ABC"] = "DEF"
    data = yaml.load_yaml_files([input_path], typ=typ)
    if typ == "safe":
        assert _is_only_dict_list_tree(data)
    assert data["root"]["children"][0]["name"] == "DEF"


@pytest.mark.skipif(sys.platform == "win32", reason="ansible-core requires POSIX")
@pytest.mark.parametrize("typ", [None, "rt", "safe"])
def test_load_yaml_files_vault(
    monkeypatch: pytest.MonkeyPatch, typ: str | None
) -> None:
    input_path = Path("tests/unit/fixtures/data_vault/")
    monkeypatch.setenv("ANSIBLE_VAULT_ID", "dev")
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD", "Password123")
    data = yaml.load_yaml_files([input_path], typ=typ)
    if typ == "safe":
        assert _is_only_dict_list_tree(data)
    assert data["root"]["children"][0]["name"] == "ABC\n"


@pytest.mark.parametrize(
    "source,destination,expected,deduplicate",
    [
        pytest.param(
            {"e2": "def"},
            {"e1": "abc"},
            {"e1": "abc", "e2": "def"},
            True,
            id="merge_dicts",
        ),
        pytest.param(
            {"e1": "abc"},
            {"e1": None},
            {"e1": "abc"},
            True,
            id="merge_empty_destination_dict",
        ),
        pytest.param(
            {"e1": {"e2": "abc"}},
            {"e1": None},
            {"e1": {"e2": "abc"}},
            True,
            id="merge_empty_destination_dict_nested",
        ),
        pytest.param(
            {"e1": None},
            {"e1": "abc"},
            {"e1": "abc"},
            True,
            id="merge_empty_source_dict",
        ),
        pytest.param(
            {"e1": None},
            {"e1": {"e2": "abc"}},
            {"e1": {"e2": "abc"}},
            True,
            id="merge_empty_source_dict_nested",
        ),
        pytest.param(
            {"root": {"child2": "def"}},
            {"root": {"child1": "abc"}},
            {"root": {"child1": "abc", "child2": "def"}},
            True,
            id="merge_nested_dicts",
        ),
        pytest.param(
            {"list": [{"child2": "def"}]},
            {"list": [{"child1": "abc"}]},
            {"list": [{"child1": "abc"}, {"child2": "def"}]},
            False,
            id="append_when_merging_lists_with_deduplicate_false",
        ),
        pytest.param(
            {"list": [{"child1": "abc"}]},
            {"list": [{"child1": "abc"}]},
            {"list": [{"child1": "abc"}, {"child1": "abc"}]},
            False,
            id="append_when_merging_lists_with_duplicate_items_deduplicate_false",
        ),
        pytest.param(
            {"list": [{"child1": "abc", "child2": "def"}]},
            {"list": [{"child1": "abc"}]},
            {"list": [{"child1": "abc", "child2": "def"}]},
            True,
            id="merge_when_deduplicating_lists_deduplicate_true",
        ),
        pytest.param(
            {
                "switch_link_aggregations": [
                    {
                        "switch_ports": [
                            {"port_id": "7", "serial": "asd"},
                            {"port_id": "8", "serial": "qwe"},
                        ]
                    }
                ]
            },
            {},
            {
                "switch_link_aggregations": [
                    {
                        "switch_ports": [
                            {"port_id": "7", "serial": "asd"},
                            {"port_id": "8", "serial": "qwe"},
                        ]
                    }
                ]
            },
            True,
            id="no_hang_when_merging_lists_of_lists",
        ),
    ],
)
def test_merge_dict(
    source: dict[Any, Any],
    destination: dict[Any, Any],
    expected: dict[Any, Any],
    deduplicate: bool,
) -> None:
    yaml.merge_dict(source, destination, deduplicate=deduplicate)
    assert destination == expected


@pytest.mark.parametrize(
    "source_item,destination,expected",
    [
        pytest.param(
            "ghi",
            ["abc", "def"],
            ["abc", "def", "ghi"],
            id="merge_primitive_list_items",
        ),
        pytest.param(
            "abc",
            ["abc", "def"],
            ["abc", "def", "abc"],
            id="do_not_merge_matching_primitive_list_items",
        ),
        pytest.param(
            {"name": "abc", "map": {"elem3": "value3"}},
            [{"name": "abc", "map": {"elem1": "value1", "elem2": "value2"}}],
            [
                {
                    "name": "abc",
                    "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
                }
            ],
            id="merge_matching_dict_list_items",
        ),
        pytest.param(
            {"name": "abc", "name2": "def", "map": {"elem3": "value3"}},
            [{"name": "abc", "map": {"elem1": "value1", "elem2": "value2"}}],
            [
                {
                    "name": "abc",
                    "name2": "def",
                    "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
                }
            ],
            id="merge_matching_dict_list_items_with_extra_src_primitive_attribute",
        ),
        pytest.param(
            {"name": "abc", "map": {"elem3": "value3"}},
            [
                {
                    "name": "abc",
                    "name2": "def",
                    "map": {"elem1": "value1", "elem2": "value2"},
                }
            ],
            [
                {
                    "name": "abc",
                    "name2": "def",
                    "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
                }
            ],
            id="merge_matching_dict_list_items_with_extra_dst_primitive_attribute",
        ),
        pytest.param(
            {"name": "abc", "name3": "ghi"},
            [{"name": "abc", "name2": "def"}],
            [{"name": "abc", "name2": "def", "name3": "ghi"}],
            id="merge_matching_dict_list_items_even_when_both_have_unique_primitive_attributes",
        ),
    ],
)
def test_merge_list_item(
    source_item: Any, destination: list[Any], expected: list[Any]
) -> None:
    yaml.merge_list_item(source_item, destination)
    assert destination == expected


def test_load_yaml_files_comment_only(tmpdir: Path) -> None:
    """Comment-only YAML file should return empty dict."""
    p = Path(tmpdir, "comments.yaml")
    p.write_text("# just a comment\n# another comment\n")
    result = yaml.load_yaml_files([p])
    assert result == {}


def test_load_yaml_files_empty_file(tmpdir: Path) -> None:
    """Empty (0-byte) YAML file should return empty dict."""
    p = Path(tmpdir, "empty.yaml")
    p.write_text("")
    result = yaml.load_yaml_files([p])
    assert result == {}


def test_load_yaml_files_root_list(tmpdir: Path) -> None:
    """Root-level list YAML file should be skipped with warning."""
    p = Path(tmpdir, "list.yaml")
    p.write_text("- item1\n- item2\n")
    result = yaml.load_yaml_files([p])
    assert result == {}


def test_load_yaml_files_mixed_dir(tmpdir: Path) -> None:
    """Directory with comment-only and valid files returns only valid data."""
    Path(tmpdir, "comments.yaml").write_text("# comment\n")
    Path(tmpdir, "valid.yaml").write_text("key: value\n")
    result = yaml.load_yaml_files([Path(tmpdir)])
    assert result == {"key": "value"}


# Vault value encrypted with the content of the former nac_yaml/ansible_vault.py as password,
# which was effectively used instead of ANSIBLE_VAULT_PASSWORD previously
LEGACY_VAULT_VALUE = (
    "$ANSIBLE_VAULT;1.2;AES256;dev\n"
    "65356165663361393035666663366331636634313063373564616334396266386534303336633566\n"
    "3236623830643834666262396234633163653635613962640a653766653732316366623734313632\n"
    "36346561616335333730356431346333666138333365306433303465383861663662386139353761\n"
    "3730616562653765390a663637656565353937643930373861373831306132613938333734303830\n"
    "6533\n"
)

skip_vault_on_windows = pytest.mark.skipif(
    sys.platform == "win32", reason="ansible-core requires POSIX"
)


def _encrypt(plaintext: str, password: str = "Password123") -> str:
    from ansible.parsing.vault import VaultLib, VaultSecret

    vault = VaultLib([("dev", VaultSecret(password.encode()))])
    return str(vault.encrypt(plaintext.encode(), vault_id="dev").decode())


def _vault_yaml(key: str, ciphertext: str, indent: int = 0) -> str:
    pad = " " * (indent + 2)
    body = "\n".join(pad + line for line in ciphertext.strip().split("\n"))
    return f"{' ' * indent}{key}: !vault |\n{body}\n"


@pytest.fixture
def vault_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANSIBLE_VAULT_ID", "dev")
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD", "Password123")


@skip_vault_on_windows
def test_load_yaml_files_vault_without_vault_id(
    monkeypatch: pytest.MonkeyPatch, vault_env: None
) -> None:
    monkeypatch.delenv("ANSIBLE_VAULT_ID")
    data = yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/")])
    assert data["root"]["children"][0]["name"] == "ABC\n"


@skip_vault_on_windows
def test_load_yaml_files_vault_wrong_password_file(
    monkeypatch: pytest.MonkeyPatch, vault_env: None
) -> None:
    """Explicitly provided file with undecryptable vault value raises VaultError."""
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD", "wrong")
    with pytest.raises(yaml.VaultError, match="Could not decrypt vault value"):
        yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/data.yaml")])


@skip_vault_on_windows
def test_load_yaml_files_vault_wrong_password_dir(
    tmpdir: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    vault_env: None,
) -> None:
    """File in a directory with undecryptable vault value is skipped with warning."""
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD", "wrong")
    Path(tmpdir, "vault.yaml").write_text(_vault_yaml("secret", _encrypt("abc")))
    Path(tmpdir, "valid.yaml").write_text("key: value\n")
    result = yaml.load_yaml_files([Path(tmpdir)])
    assert result == {"key": "value"}
    assert "vault.yaml" in caplog.text
    assert "Could not decrypt vault value" in caplog.text


@skip_vault_on_windows
def test_load_yaml_files_vault_missing_password(
    monkeypatch: pytest.MonkeyPatch, vault_env: None
) -> None:
    monkeypatch.delenv("ANSIBLE_VAULT_PASSWORD")
    with pytest.raises(yaml.VaultError, match="ANSIBLE_VAULT_PASSWORD"):
        yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/data.yaml")])


@skip_vault_on_windows
def test_load_yaml_files_vault_legacy_password_rejected(
    tmpdir: Path, vault_env: None
) -> None:
    """Values encrypted with the legacy password are no longer decrypted."""
    p = Path(tmpdir, "legacy.yaml")
    p.write_text(_vault_yaml("secret", LEGACY_VAULT_VALUE))
    with pytest.raises(yaml.VaultError, match="Could not decrypt vault value"):
        yaml.load_yaml_files([p])


def test_load_yaml_files_no_vault_values(mocker: Any) -> None:
    """Without vault values, no vault helper process is started."""
    run = mocker.patch("nac_yaml.yaml.subprocess.run")
    yaml.load_yaml_files([Path("tests/unit/fixtures/data_merge/")])
    run.assert_not_called()


@skip_vault_on_windows
@pytest.mark.parametrize("typ", [None, "safe"])
def test_load_yaml_files_vault_batch_and_merge(
    tmpdir: Path, mocker: Any, vault_env: None, typ: str | None
) -> None:
    """All vault values are decrypted in one batch and merged by plaintext."""
    # Two different encryptions of the same plaintext used as merge key
    Path(tmpdir, "file1.yaml").write_text(
        "users:\n  - role: admin\n" + _vault_yaml("name", _encrypt("alice"), indent=4)
    )
    Path(tmpdir, "file2.yaml").write_text(
        "users:\n  - shell: bash\n"
        + _vault_yaml("name", _encrypt("alice"), indent=4)
        + _vault_yaml("password", _encrypt("secret1"))
    )
    run = mocker.spy(yaml.subprocess, "run")
    data = yaml.load_yaml_files([Path(tmpdir)], typ=typ)
    assert run.call_count == 1
    assert data["users"] == [{"name": "alice", "role": "admin", "shell": "bash"}]
    assert data["password"] == "secret1"


@skip_vault_on_windows
def test_load_yaml_files_vault_ansible_vault_on_path(
    tmpdir: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any, vault_env: None
) -> None:
    """Falls back to the interpreter of ansible-vault if ansible is not importable."""
    find_spec = yaml.importlib.util.find_spec
    mocker.patch(
        "nac_yaml.yaml.importlib.util.find_spec",
        side_effect=lambda name: None if name == "ansible" else find_spec(name),
    )
    script = Path(tmpdir, "ansible-vault")
    script.write_text(f"#!{sys.executable}\n")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmpdir))
    assert yaml._find_vault_python() == sys.executable
    data = yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/")])
    assert data["root"]["children"][0]["name"] == "ABC\n"


@skip_vault_on_windows
def test_find_vault_python_sibling_interpreter(
    tmpdir: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    mocker.patch("nac_yaml.yaml.importlib.util.find_spec", return_value=None)
    bin_dir = Path(tmpdir).resolve()
    script = Path(bin_dir, "ansible-vault")
    script.write_text("#!/somewhere/else/python\n")
    script.chmod(0o755)
    Path(bin_dir, "python3").write_text("")
    monkeypatch.setenv("PATH", str(bin_dir))
    assert yaml._find_vault_python() == str(Path(bin_dir, "python3"))


def test_find_vault_python_not_available(
    tmpdir: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    mocker.patch("nac_yaml.yaml.importlib.util.find_spec", return_value=None)
    monkeypatch.setenv("PATH", str(tmpdir))
    with pytest.raises(yaml.VaultError, match="ansible-core is required"):
        yaml._find_vault_python()


@pytest.mark.parametrize(
    "content,expected",
    [
        pytest.param("#!/usr/bin/python3\n", "/usr/bin/python3", id="absolute"),
        pytest.param(
            "#!/bin/sh\n'''exec' \"/a b/python\" \"$0\" \"$@\"\n' '''\n",
            "/a b/python",
            id="sh-trampoline",
        ),
        pytest.param(
            "#!/bin/sh\n'''exec' \"$(dirname -- \"$(realpath -- \"$0\")\")\"/'python' \"$0\" \"$@\"\n' '''\n",
            None,
            id="sh-trampoline-relative",
        ),
        pytest.param("#!/usr/bin/env python3\n", "/resolved/python3", id="env"),
        pytest.param("import sys\n", None, id="no-shebang"),
    ],
)
def test_interpreter_from_script(
    tmpdir: Path, mocker: Any, content: str, expected: str | None
) -> None:
    mocker.patch("nac_yaml.yaml.shutil.which", return_value="/resolved/python3")
    script = Path(tmpdir, "ansible-vault")
    script.write_text(content)
    assert yaml._interpreter_from_script(script) == expected


def test_vault_helper_ansible_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from nac_yaml import _vault_helper

    monkeypatch.setitem(sys.modules, "ansible.parsing.vault", None)
    response = _vault_helper.decrypt_values(
        {"vault_id": "dev", "password": "Password123", "values": ["x"]}
    )
    assert "ansible-core is not installed" in response["error"]


@skip_vault_on_windows
def test_vault_helper_per_value_results() -> None:
    from nac_yaml import _vault_helper

    response = _vault_helper.decrypt_values(
        {
            "vault_id": "dev",
            "password": "Password123",
            "values": [_encrypt("abc"), _encrypt("abc", password="other")],
        }
    )
    assert response["results"][0] == {"ok": True, "value": "abc"}
    assert response["results"][1]["ok"] is False
    assert "abc" not in response["results"][1]["error"]


@skip_vault_on_windows
def test_load_yaml_files_vault_password_trailing_newline(
    monkeypatch: pytest.MonkeyPatch, vault_env: None
) -> None:
    """Trailing line breaks are stripped from the password like ansible-vault does."""
    monkeypatch.setenv("ANSIBLE_VAULT_PASSWORD", "Password123\r\n")
    data = yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/")])
    assert data["root"]["children"][0]["name"] == "ABC\n"


@skip_vault_on_windows
def test_load_yaml_files_vault_password_not_in_child_env(
    mocker: Any, vault_env: None
) -> None:
    """The password is only passed via stdin, not via the child environment."""
    run = mocker.spy(yaml.subprocess, "run")
    yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/")])
    assert run.call_count == 1
    assert "ANSIBLE_VAULT_PASSWORD" not in run.call_args.kwargs["env"]
    assert "ANSIBLE_VAULT_ID" in run.call_args.kwargs["env"]
    assert "Password123" not in " ".join(run.call_args.args[0])


@skip_vault_on_windows
@pytest.mark.parametrize(
    "returncode,stdout,stderr,match",
    [
        pytest.param(0, b"not json", b"", "Invalid response", id="invalid-json"),
        pytest.param(0, b'{"results": []}', b"", "Invalid response", id="count"),
        pytest.param(0, b'{"foo": 1}', b"", "Invalid response", id="missing-key"),
        pytest.param(0, b'{"error": "boom"}', b"", "boom", id="helper-error"),
        pytest.param(1, b"", b"Traceback: crash", "crash", id="non-zero-exit"),
    ],
)
def test_load_yaml_files_vault_helper_failures(
    mocker: Any,
    vault_env: None,
    returncode: int,
    stdout: bytes,
    stderr: bytes,
    match: str,
) -> None:
    mocker.patch(
        "nac_yaml.yaml.subprocess.run",
        return_value=subprocess.CompletedProcess([], returncode, stdout, stderr),
    )
    with pytest.raises(yaml.VaultError, match=match):
        yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/data.yaml")])


@skip_vault_on_windows
def test_load_yaml_files_vault_helper_timeout(mocker: Any, vault_env: None) -> None:
    mocker.patch(
        "nac_yaml.yaml.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="python", timeout=1),
    )
    with pytest.raises(yaml.VaultError, match="Vault helper process failed"):
        yaml.load_yaml_files([Path("tests/unit/fixtures/data_vault/data.yaml")])


@skip_vault_on_windows
def test_load_yaml_files_vault_invalid_response_dir(
    tmpdir: Path, mocker: Any, caplog: pytest.LogCaptureFixture, vault_env: None
) -> None:
    """Invalid helper response skips affected directory files with a warning."""
    mocker.patch(
        "nac_yaml.yaml.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, b"not json", b""),
    )
    Path(tmpdir, "vault.yaml").write_text(_vault_yaml("secret", _encrypt("abc")))
    Path(tmpdir, "valid.yaml").write_text("key: value\n")
    assert yaml.load_yaml_files([Path(tmpdir)]) == {"key": "value"}
    assert "Invalid response from vault helper" in caplog.text
