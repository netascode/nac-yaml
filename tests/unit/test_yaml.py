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


def test_merge_dict() -> None:
    # merge dicts
    destination: dict[Any, Any] = {"e1": "abc"}
    source: dict[Any, Any] = {"e2": "def"}
    result: dict[Any, Any] = {"e1": "abc", "e2": "def"}
    yaml.merge_dict(source, destination)
    assert destination == result
    # merge empty destination dict
    destination = {"e1": None}
    source = {"e1": "abc"}
    result = {"e1": "abc"}
    yaml.merge_dict(source, destination)
    assert destination == result
    # merge empty destination dict nested
    destination = {"e1": None}
    source = {"e1": {"e2": "abc"}}
    result = {"e1": {"e2": "abc"}}
    yaml.merge_dict(source, destination)
    assert destination == result
    # merge empty source dict
    destination = {"e1": "abc"}
    source = {"e1": None}
    result = {"e1": "abc"}
    yaml.merge_dict(source, destination)
    assert destination == result
    # merge empty source dict nested
    destination = {"e1": {"e2": "abc"}}
    source = {"e1": None}
    result = {"e1": {"e2": "abc"}}
    yaml.merge_dict(source, destination)
    assert destination == result
    # merge nested dicts
    destination = {"root": {"child1": "abc"}}
    source = {"root": {"child2": "def"}}
    result = {"root": {"child1": "abc", "child2": "def"}}
    yaml.merge_dict(source, destination)
    assert destination == result
    # append when merging lists
    destination = {"list": [{"child1": "abc"}]}
    source = {"list": [{"child2": "def"}]}
    result = {"list": [{"child1": "abc"}, {"child2": "def"}]}
    yaml.merge_dict(source, destination)
    assert destination == result
    # append when merging lists with duplicate items
    destination = {"list": [{"child1": "abc"}]}
    source = {"list": [{"child1": "abc"}]}
    result = {"list": [{"child1": "abc"}, {"child1": "abc"}]}
    yaml.merge_dict(source, destination)
    assert destination == result
    # make sure that the code doesn't hang when merging lists of lists
    source = {
        "switch_link_aggregations": [
            {
                "switch_ports": [
                    {"port_id": "7", "serial": "asd"},
                    {"port_id": "8", "serial": "qwe"},
                ]
            }
        ]
    }
    destination = {}
    yaml.merge_dict(source, destination)
    assert destination == source


def test_merge_list_item() -> None:
    # merge primitive list items
    destination: list[Any] = ["abc", "def"]
    source_item: Any = "ghi"
    result: list[Any] = ["abc", "def", "ghi"]
    yaml.merge_list_item(source_item, destination)
    assert destination == result
    # do not merge matching primitive list items
    destination = ["abc", "def"]
    source_item = "abc"
    result = ["abc", "def", "abc"]
    yaml.merge_list_item(source_item, destination)
    assert destination == result
    # merge matching dict list items
    destination = [{"name": "abc", "map": {"elem1": "value1", "elem2": "value2"}}]
    source_item = {"name": "abc", "map": {"elem3": "value3"}}
    result = [
        {
            "name": "abc",
            "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
        }
    ]
    yaml.merge_list_item(source_item, destination)
    assert destination == result
    # merge matching dict list items with extra src primitive attribute
    destination = [{"name": "abc", "map": {"elem1": "value1", "elem2": "value2"}}]
    source_item = {"name": "abc", "name2": "def", "map": {"elem3": "value3"}}
    result = [
        {
            "name": "abc",
            "name2": "def",
            "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
        }
    ]
    yaml.merge_list_item(source_item, destination)
    assert destination == result
    # merge matching dict list items with extra dst primitive attribute
    destination = [
        {"name": "abc", "name2": "def", "map": {"elem1": "value1", "elem2": "value2"}}
    ]
    source_item = {"name": "abc", "map": {"elem3": "value3"}}
    result = [
        {
            "name": "abc",
            "name2": "def",
            "map": {"elem1": "value1", "elem2": "value2", "elem3": "value3"},
        }
    ]
    yaml.merge_list_item(source_item, destination)
    assert destination == result
    # do not merge matching dict list items with extra dst and src primitive attribute
    destination = [{"name": "abc", "name2": "def"}]
    source_item = {"name": "abc", "name3": "ghi"}
    result = [{"name": "abc", "name2": "def"}, {"name": "abc", "name3": "ghi"}]
    yaml.merge_list_item(source_item, destination)
    assert destination == result


def test_deduplicate_list_items() -> None:
    # deduplicate dict list items
    data: dict[Any, Any] = {"list": [{"name": "abc"}, {"name": "abc"}]}
    result: dict[Any, Any] = {"list": [{"name": "abc"}]}
    yaml.deduplicate_list_items(data)
    assert data == result
    # deduplicate nested dict list items
    data = {"list": [{"nested_list": [{"name": "abc"}, {"name": "abc"}]}]}
    result = {"list": [{"nested_list": [{"name": "abc"}]}]}
    yaml.deduplicate_list_items(data)
    assert data == result
    # do not deduplicate string list items
    data = {"list": ["abc", "abc"]}
    result = {"list": ["abc", "abc"]}
    yaml.deduplicate_list_items(data)
    assert data == result
