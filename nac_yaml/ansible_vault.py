# Copyright: (c) 2022, Daniel Schmidt <danischm@cisco.com>

import os
import sys

if "ANSIBLE_VAULT_PASSWORD" in os.environ:
    sys.stdout.write("{}\n".format(os.environ["ANSIBLE_VAULT_PASSWORD"]))
