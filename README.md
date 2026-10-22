[![Tests](https://github.com/netascode/nac-yaml/actions/workflows/test.yml/badge.svg)](https://github.com/netascode/nac-yaml/actions/workflows/test.yml)
![Python Support](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-informational "Python Support: 3.10, 3.11, 3.12, 3.13, 3.14")

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
