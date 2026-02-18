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
