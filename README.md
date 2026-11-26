[![Tests](https://github.com/netascode/nac-yaml/actions/workflows/test.yml/badge.svg)](https://github.com/netascode/nac-yaml/actions/workflows/test.yml)
![Python Support](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-informational "Python Support: 3.11, 3.12, 3.13, 3.14")

# nac-yaml

A Python library with common YAML utility functions supporting `Network as Code`.

## Usage

### Load and merge YAML files

```python
from pathlib import Path
from nac_yaml.yaml import load_yaml_files

# Default is ruamel's round-trip loader (preserves formatting internally).
data = load_yaml_files([
    Path("path/to/file1.yaml"),
    Path("path/to/file2.yaml"),
])

# Use the safe loader to get native dict/list containers.
data_safe = load_yaml_files([
    Path("path/to/file1.yaml"),
    Path("path/to/file2.yaml"),
], typ="safe")
```

Note: when `typ` is not round-trip (e.g. `"safe"`), formatting features (quotes/comments/style) are not preserved.

Files are merged in the given order; the files of a directory are sorted by relative path, like Terraform's `fileset()` (e.g. `a.yaml`, `a/b.yaml`, `b.yaml`). The order decides the order of list items and, for a value defined in several files, which one is kept (the last).

### Merge parsed documents and find where values come from

For tools that keep documents in memory, such as editors, `merge_documents()` merges parsed documents like `load_yaml_files()` without modifying them, and records where each merged value comes from. `parse_yaml()` parses a document like `load_yaml_files()` does, `find_yaml_files()` returns the files it reads in merge order, and `node_position()` turns a path into a line and column. See the docstrings for details.

```python
from nac_yaml.yaml import merge_documents, parse_yaml

result = merge_documents([("a.yaml", parse_yaml(text_a)), ("b.yaml", parse_yaml(text_b))])
result.origins.get(("nxos", "devices", 0, "name"))  # [Origin(source='a.yaml', path=('nxos', 'devices', 0, 'name'))]
```

### Ansible Vault encrypted values

Values encrypted with [Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html) can be used with the `!vault` tag:

```yaml
password: !vault |
  $ANSIBLE_VAULT;1.2;AES256;dev
  6535616566336139303566666336633163663431306337356461633439626638...
```

The vault password is read from the `ANSIBLE_VAULT_PASSWORD` environment variable, `ANSIBLE_VAULT_ID` is optional:

```bash
export ANSIBLE_VAULT_ID=dev
export ANSIBLE_VAULT_PASSWORD=Password123
```

Decryption requires `ansible-core`, either installed in the same Python environment (`pip install nac-yaml[vault]`) or available as `ansible-vault` command on `PATH`. All values are decrypted in a single batch after parsing. If a value cannot be decrypted, `load_yaml_files()` raises a `VaultError` for explicitly provided files, while files found in directories are skipped with a warning. Ansible Vault is not supported on Windows.

## Installation

### Using uv (recommended)

```bash
uv add nac-yaml
```

### Using pip

```bash
pip install nac-yaml
```
