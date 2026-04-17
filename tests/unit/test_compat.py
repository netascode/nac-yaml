# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2026

from pathlib import Path

import pytest

from nac_yaml.yaml import load_yaml_files

pytestmark = pytest.mark.unit


def test_load_yaml_files_returns_plain_types_compatible_with_get(tmp_path: Path) -> None:
    input_path = tmp_path / "input.yaml"
    input_path.write_text(
        """---
root:
  feature_profiles:
    - - name: profile1
"""
    )

    data = load_yaml_files([input_path])

    # This used to fail when ruamel types leaked out (inner list item is a list/CommentedSeq)
    # and downstream code did `p.get(...)`.
    feature_profiles = data["root"]["feature_profiles"]
    assert isinstance(feature_profiles, list)
    assert isinstance(feature_profiles[0], list)
    assert isinstance(feature_profiles[0][0], dict)

    # Simulate downstream code doing `.get()` on each element.
    found = next((p for p in feature_profiles[0] if p.get("name") == "profile1"), None)
    assert found == {"name": "profile1"}
