# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

import importlib.util
import logging
import os
import subprocess  # nosec B404
from pathlib import Path
from typing import Any

from ruamel import yaml

logger = logging.getLogger(__name__)


class VaultTag(yaml.YAMLObject):
    """Custom YAML tag handler for Ansible Vault encrypted values.

    Handles !vault tags in YAML files by decrypting them using ansible-vault CLI.

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
        - Requires ansible-vault CLI tool to be installed
        - Uses subprocess to call ansible-vault decrypt
        - Vault password must be provided via environment variable
    """

    yaml_tag = "!vault"

    def __init__(self, v: str):
        """Initialize VaultTag with encrypted content.

        Args:
            v: Encrypted vault content string
        """
        self.value = v

    def __repr__(self) -> str:
        """Decrypt and return the vault value.

        Returns:
            Decrypted string if successful, empty string if vault spec not found

        Raises:
            CalledProcessError: If ansible-vault decrypt fails
        """
        spec = importlib.util.find_spec("nac_yaml.ansible_vault")
        if spec:
            if "ANSIBLE_VAULT_ID" in os.environ:
                vault_id = os.environ["ANSIBLE_VAULT_ID"] + "@" + str(spec.origin)
            else:
                vault_id = str(spec.origin)
            t = subprocess.check_output(  # nosec B603, B607
                [
                    "ansible-vault",
                    "decrypt",
                    "--vault-id",
                    vault_id,
                ],
                input=self.value.encode(),
            )
            return t.decode()
        return ""

    @classmethod
    def from_yaml(cls, loader: Any, node: Any) -> str:
        """Construct VaultTag from YAML node.

        Args:
            loader: YAML loader instance
            node: YAML node containing vault content

        Returns:
            String representation of the decrypted value
        """
        return str(cls(node.value))


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
    def from_yaml(cls, loader: Any, node: Any) -> str:
        """Construct EnvTag from YAML node.

        Args:
            loader: YAML loader instance
            node: YAML node containing environment variable name

        Returns:
            String representation of the environment variable value
        """
        return str(cls(node.value))


def load_yaml_files(paths: list[Path], deduplicate: bool = True) -> dict[str, Any]:
    """Load and merge YAML files from provided paths.

    Args:
        paths: List of file or directory paths to load YAML from
        deduplicate: When True, intelligently merges lists based on duplicate detection:
                    - If ANY file has duplicates in a list, that list is concatenated (no merging)
                    - If NO duplicates exist, matching dict items are merged across files
                    When False, simply concatenates all list items.

    Returns:
        Merged dictionary structure

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

    def _load_file(file_path: Path, data: dict[str, Any]) -> None:
        with open(file_path) as file:
            if file_path.suffix in [".yaml", ".yml"]:
                data_yaml = file.read()
                y = yaml.YAML()
                y.preserve_quotes = True
                y.register_class(VaultTag)
                y.register_class(EnvTag)
                dict = y.load(data_yaml)
                merge_dict(dict, data, deduplicate)

    result: dict[str, Any] = {}
    for path in paths:
        if os.path.isfile(path):
            _load_file(path, result)
        else:
            for dir, _subdir, files in os.walk(path):
                for filename in files:
                    try:
                        _load_file(Path(dir, filename), result)
                    except:  # noqa: E722
                        logger.warning(f"Could not load file: {filename}")
    return result


def _has_duplicates_in_list(items: list[Any]) -> bool:
    """Check if a list contains duplicate dict items using merge matching logic.

    Args:
        items: List to check for duplicates

    Returns:
        True if list contains matching dict items, False otherwise

    Note:
        Uses same matching logic as merge_list_item() to determine if items are duplicates.
        Primitive items (strings, numbers) are not considered for duplicate detection.
    """
    # Only check dict items for duplicates
    dict_items = [item for item in items if isinstance(item, dict)]
    if len(dict_items) < 2:
        return False

    # Check each dict against all subsequent dicts
    for i, source_item in enumerate(dict_items):
        for dest_item in dict_items[i + 1 :]:
            # Use same matching logic as merge_list_item
            match = True
            comparison = False

            for k, v in source_item.items():
                if isinstance(v, dict | list) or k not in dest_item:
                    continue
                comparison = True
                if v != dest_item[k]:
                    match = False

            for k, v in dest_item.items():
                if isinstance(v, dict | list) or k not in source_item:
                    continue
                comparison = True
                if v != source_item[k]:
                    match = False

            # If these two items would merge, we have a duplicate
            if comparison and match:
                return True

    return False


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
            match = True
            comparison = False

            for k, v in source_item.items():
                if isinstance(v, dict | list) or k not in dest_item:
                    continue
                comparison = True
                if v != dest_item[k]:
                    match = False

            for k, v in dest_item.items():
                if isinstance(v, dict | list) or k not in source_item:
                    continue
                comparison = True
                if v != source_item[k]:
                    match = False

            if comparison and match:
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
    if not source:
        return destination
    for key, value in source.items():
        if key not in destination or destination[key] is None:
            destination[key] = value
        elif isinstance(value, dict):
            if isinstance(destination[key], dict):
                merge_dict(value, destination[key], deduplicate)
        elif isinstance(value, list):
            if isinstance(destination[key], list):
                if deduplicate:
                    # Check if either source or destination list has duplicates
                    # If duplicates exist, skip merging to preserve them
                    if _has_duplicates_in_list(value) or _has_duplicates_in_list(
                        destination[key]
                    ):
                        # Concatenate without merging to preserve all duplicates
                        destination[key] += value
                    else:
                        # No duplicates: merge matching items across files
                        for item in value:
                            merge_list_item(item, destination[key], deduplicate)
                else:
                    # Simple append (original behavior)
                    destination[key] += value
        elif value is not None:
            destination[key] = value
    return destination


def deduplicate_list_items(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively deduplicate list items in a nested data structure.

    Args:
        data: Dictionary containing nested dicts and lists

    Returns:
        The modified data dictionary with deduplicated lists

    Behavior:
        - Traverses all nested dictionaries recursively
        - For each list, merges matching dict items using merge_list_item logic
        - Preserves order (first occurrence wins)
        - Primitive list items (strings, numbers) are NOT deduplicated
        - Dict items match based on shared primitive key-value pairs

    Note:
        This function is kept for backward compatibility. New code should use
        load_yaml_files() with deduplicate=True for cross-file deduplication.

    Example:
        data = {"devices": [{"name": "a", "x": 1}, {"name": "a", "y": 2}]}
        deduplicate_list_items(data)
        # Result: {"devices": [{"name": "a", "x": 1, "y": 2}]}
    """
    for key, value in data.items():
        if isinstance(value, dict):
            deduplicate_list_items(value)
        elif isinstance(value, list):
            deduplicated_list: list[Any] = []
            for i in value:
                merge_list_item(i, deduplicated_list)
            for i in deduplicated_list:
                if isinstance(i, dict):
                    deduplicate_list_items(i)
            data[key] = deduplicated_list
    return data


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
