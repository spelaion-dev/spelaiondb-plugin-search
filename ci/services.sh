#!/usr/bin/env bash
set -euo pipefail
docker compose -p search-ci -f ci/compose.yaml up -d --wait
