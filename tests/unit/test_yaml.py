# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

import filecmp
import os
from pathlib import Path
from typing import Any

import pytest

from nac_yaml import yaml

pytestmark = pytest.mark.unit


def test_load_yaml_files(tmpdir: Path) -> None:
    input_path_1 = Path("tests/unit/fixtures/data_merge/file1.yaml")
    input_path_2 = Path("tests/unit/fixtures/data_merge/file2.yaml")
    output_path = Path(tmpdir, "output.yaml")
    result_path = Path("tests/unit/fixtures/data_merge/result.yaml")
    result_no_deduplicate_path = Path(
        "tests/unit/fixtures/data_merge/result_no_deduplicate.yaml"
    )

    data = yaml.load_yaml_files([input_path_1, input_path_2])
    yaml.write_yaml_file(data, output_path)
    assert filecmp.cmp(output_path, result_path, shallow=False)

    data = yaml.load_yaml_files([input_path_1, input_path_2], deduplicate=False)
    yaml.write_yaml_file(data, output_path)
    assert filecmp.cmp(output_path, result_no_deduplicate_path, shallow=False)

    input_path = Path("tests/unit/fixtures/data_vault/")
    os.environ["ANSIBLE_VAULT_ID"] = "dev"
    os.environ["ANSIBLE_VAULT_PASSWORD"] = "Password123"
    data = yaml.load_yaml_files([input_path])

    input_path = Path("tests/unit/fixtures/data_env/")
    os.environ["ABC"] = "DEF"
    data = yaml.load_yaml_files([input_path])
    assert data["root"]["children"][0]["name"] == "DEF"


@pytest.mark.parametrize(
    "destination,source,expected",
    [
        (
            {"e1": "abc"},
            {"e2": "def"},
            {"e1": "abc", "e2": "def"},
        ),
        (
            {"e1": None},
            {"e1": "abc"},
            {"e1": "abc"},
        ),
        (
            {"e1": None},
            {"e1": {"e2": "abc"}},
            {"e1": {"e2": "abc"}},
        ),
        (
            {"e1": "abc"},
            {"e1": None},
            {"e1": "abc"},
        ),
        (
            {"e1": {"e2": "abc"}},
            {"e1": None},
            {"e1": {"e2": "abc"}},
        ),
        (
            {"root": {"child1": "abc"}},
            {"root": {"child2": "def"}},
            {"root": {"child1": "abc", "child2": "def"}},
        ),
        (
            {"list": [{"child1": "abc"}]},
            {"list": [{"child2": "def"}]},
            {"list": [{"child1": "abc"}, {"child2": "def"}]},
        ),
        (
            {"list": [{"child1": "abc"}]},
            {"list": [{"child1": "abc"}]},
            {"list": [{"child1": "abc"}, {"child1": "abc"}]},
        ),
        (
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
        ),
    ],
    ids=[
        "merge_dicts",
        "merge_empty_destination_dict",
        "merge_empty_destination_dict_nested",
        "merge_empty_source_dict",
        "merge_empty_source_dict_nested",
        "merge_nested_dicts",
        "append_when_merging_lists",
        "append_when_merging_lists_with_duplicates",
        "merge_lists_of_lists_no_hang",
    ],
)
def test_merge_dict(
    destination: dict[Any, Any], source: dict[Any, Any], expected: dict[Any, Any]
) -> None:
    yaml.merge_dict(source, destination)
    assert destination == expected


@pytest.mark.parametrize(
    "destination,source_item,expected",
    [
        (
            ["abc", "def"],
            "ghi",
            ["abc", "def", "ghi"],
        ),
        (
            ["abc", "def"],
            "abc",
            ["abc", "def", "abc"],
        ),
        (
            [{"name": "abc", "map": {"elem1": "value1", "elem2": "value2"}}],
            {"name": "abc", "map": {"elem3": "value3"}},
            [
                {
                    "name": "abc",
                    "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
                }
            ],
        ),
        (
            [{"name": "abc", "map": {"elem1": "value1", "elem2": "value2"}}],
            {"name": "abc", "name2": "def", "map": {"elem3": "value3"}},
            [
                {
                    "name": "abc",
                    "name2": "def",
                    "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
                }
            ],
        ),
        (
            [
                {
                    "name": "abc",
                    "name2": "def",
                    "map": {"elem1": "value1", "elem2": "value2"},
                }
            ],
            {"name": "abc", "map": {"elem3": "value3"}},
            [
                {
                    "name": "abc",
                    "name2": "def",
                    "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
                }
            ],
        ),
        (
            [{"name": "abc", "name2": "def"}],
            {"name": "abc", "name3": "ghi"},
            [{"name": "abc", "name2": "def"}, {"name": "abc", "name3": "ghi"}],
        ),
    ],
    ids=[
        "merge_primitive_list_items",
        "do_not_merge_matching_primitive_list_items",
        "merge_matching_dict_list_items",
        "merge_matching_dict_list_items_with_extra_src_attribute",
        "merge_matching_dict_list_items_with_extra_dst_attribute",
        "do_not_merge_dict_list_items_with_extra_dst_and_src_attributes",
    ],
)
def test_merge_list_item(
    destination: list[Any], source_item: Any, expected: list[Any]
) -> None:
    yaml.merge_list_item(source_item, destination)
    assert destination == expected


@pytest.mark.parametrize(
    "data,expected",
    [
        (
            {"list": [{"name": "abc"}, {"name": "abc"}]},
            {"list": [{"name": "abc"}]},
        ),
        (
            {"list": [{"nested_list": [{"name": "abc"}, {"name": "abc"}]}]},
            {"list": [{"nested_list": [{"name": "abc"}]}]},
        ),
        (
            {"list": ["abc", "abc"]},
            {"list": ["abc", "abc"]},
        ),
    ],
    ids=[
        "deduplicate_dict_list_items",
        "deduplicate_nested_dict_list_items",
        "do_not_deduplicate_string_list_items",
    ],
)
def test_deduplicate_list_items(data: dict[Any, Any], expected: dict[Any, Any]) -> None:
    yaml.deduplicate_list_items(data)
    assert data == expected
