# Unreleased

- BREAKING CHANGE: Rework list merge logic — within-file duplicates are now preserved; if any file contains duplicate dict items in a list, merging is disabled for that entire list and items are concatenated instead
- BREAKING CHANGE: Two dict items in a list now merge when all shared primitive fields match, even if both sides have additional unique primitive fields (previously this was blocked)
- Add support for Python 3.14
- Add `typ` parameter to `load_yaml_files()` to select ruamel.yaml loader mode (e.g. `"safe"` for native dict/list)

# 1.1.1

- Reverted changes to `ansible_vault.py` to maintain backwards compatibility

# 1.1.0

- Migrate to `uv` package manager
- Update license references

# 1.0.0

- Dependency updates

# 0.1.3

- BREAKING CHANGE: Update YAML merge logic to not merge list items with matching attributes and primitive values, if both have additional attributes the other does not have

# 0.1.2

- When merging dictionaries, always replace attributes with "null" values

# 0.1.1

- When merging dictionaries, never replace values with "null"

# 0.1.0

- Initial release
