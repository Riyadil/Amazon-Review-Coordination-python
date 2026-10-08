#!/usr/bin/env bash
set -euo pipefail

# EMR bootstrap actions run on every node as root.
python3 -m pip install --no-cache-dir numpy==1.26.4 setuptools graphframes

