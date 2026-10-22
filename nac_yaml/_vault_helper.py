# SPDX-License-Identifier: MPL-2.0
# Copyright (c) 2025 Daniel Schmidt

"""Batch-decrypt Ansible Vault values in a separate process.

This script is executed by ``nac_yaml.yaml`` with a Python interpreter that has
ansible-core installed, which is not necessarily the interpreter running
nac-yaml. It must therefore only depend on the standard library and ansible-core,
and must not import anything from ``nac_yaml``.

Protocol (JSON over stdin/stdout):
    Request:  {"vault_id": str, "password": str, "values": [str, ...]}
    Response: {"results": [{"ok": true, "value": str} | {"ok": false, "error": str}]}
              or {"error": str} if decryption is not possible at all
"""

import json
import sys
from typing import Any


def decrypt_values(request: dict[str, Any]) -> dict[str, Any]:
    """Decrypt all values of a request.

    Args:
        request: Request dictionary as described in the module docstring

    Returns:
        Response dictionary as described in the module docstring
    """
    try:
        from ansible.parsing.vault import VaultLib, VaultSecret
    except ImportError:
        return {"error": f"ansible-core is not installed for {sys.executable}"}

    secret = VaultSecret(request["password"].encode())
    vault = VaultLib([(request["vault_id"], secret)])
    results: list[dict[str, Any]] = []
    for value in request["values"]:
        try:
            results.append({"ok": True, "value": vault.decrypt(value).decode()})
        except Exception as e:
            # Never include the vault content or plaintext in error messages
            results.append({"ok": False, "error": f"{type(e).__name__}: {e}"})
    return {"results": results}


def main() -> None:
    """Read a request from stdin and write the response to stdout."""
    request = json.load(sys.stdin)
    json.dump(decrypt_values(request), sys.stdout)


if __name__ == "__main__":
    main()
