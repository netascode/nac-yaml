## Unreleased

- Fix `!vault` decryption ignoring `ANSIBLE_VAULT_PASSWORD`; values are now decrypted with the configured vault password. Values encrypted with the content of `ansible_vault.py` as password (e.g. copied from test fixtures) are no longer decrypted
- Decrypt all `!vault` values of all files in a single batch in a separate process; ansible-core is used from the current environment or from the `ansible-vault` command on `PATH`
- Raise `VaultError` if vault values of an explicitly provided file cannot be decrypted; files found in directories are skipped with a warning that now includes the underlying error
- Add optional `vault` extra (`pip install nac-yaml[vault]`)
- Remove the unused `nac_yaml/ansible_vault.py` vault password script
- Fix `load_yaml_files()` failing on empty or comment-only YAML files; these are now skipped, and files with a non-mapping root (e.g. a list) are skipped with a warning
- Improve `load_yaml_files()` performance for data models split across many files by caching the duplicate status of merged lists instead of rescanning them for every file; the cache is disabled once a file with YAML aliases is merged
- Fix list merging failing with `IndexError` or comparing source items against the wrong destination item after a file appended non-dict items or dict items without primitive values to the same list

## 2.0.0

- BREAKING CHANGE: Rework list merge logic — within-file duplicates are now preserved; if any file contains duplicate dict items in a list, merging is disabled for that entire list and items are concatenated instead
- BREAKING CHANGE: Two dict items in a list now merge when all shared primitive fields match, even if both sides have additional unique primitive fields (previously this was blocked)
- Add support for Python 3.14
- Add `typ` parameter to `load_yaml_files()` to select ruamel.yaml loader mode (e.g. `"safe"` for native dict/list)

## 1.1.1

- Reverted changes to `ansible_vault.py` to maintain backwards compatibility

## 1.1.0

- Migrate to `uv` package manager
- Update license references

## 1.0.0

- Dependency updates

## 0.1.3

- BREAKING CHANGE: Update YAML merge logic to not merge list items with matching attributes and primitive values, if both have additional attributes the other does not have

## 0.1.2

- When merging dictionaries, always replace attributes with "null" values

## 0.1.1

- When merging dictionaries, never replace values with "null"

## 0.1.0

- Initial release
