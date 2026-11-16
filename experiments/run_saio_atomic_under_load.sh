#!/usr/bin/env bash

# Launcher for the EXISTING prepared Lightsail SAIO, not a provision/reset script.

# Usage: bash experiments/run_saio_atomic_under_load.sh [runner options]

# Defaults: five cases, one round each, 300 seconds of traffic per case.

# Report exit codes: 0 clean pass; 1 experiment failure; 2 incomplete;

# 3 atomic checks pass with traffic/cleanup warnings; 77 preflight refused.

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

if [[ "${EUID}" -eq 0 ]]; then

    echo "STOP: run as the normal ubuntu user, not sudo. Only scratch setup may need sudo." >&2

    exit 77

fi

if [[ ! -x "$ROOT/.venv/bin/python" ]]; then

    echo "STOP: missing .venv/bin/python. Use the prepared Lightsail checkout." >&2

    exit 77

fi

if [[ ! -f "$ROOT/experiments/saio_atomic_under_load.py" ]]; then

    echo "STOP: integrated Python runner is missing." >&2

    exit 77

fi

# Help must work without requiring storage setup or sudo.

for arg in "$@"; do

    if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then

        exec "$ROOT/.venv/bin/python" "$ROOT/experiments/saio_atomic_under_load.py" --help

    fi

done

if ! mountpoint -q /mnt/sdb1; then

    echo "STOP: /mnt/sdb1 is not mounted. Check existing SAIO storage; do not reset it." >&2

    exit 77

fi

SCRATCH=/mnt/sdb1/atomic-save-scratch

if [[ -L "$SCRATCH" ]]; then

    echo "STOP: scratch directory must not be a symlink." >&2

    exit 77

fi

if [[ ! -d "$SCRATCH" || ! -w "$SCRATCH" || ! -x "$SCRATCH" ]]; then

    echo "Preparing only $SCRATCH; sudo may prompt for authorization."

    if ! sudo install -d -o "$(id -un)" -g "$(id -gn)" -m 0700 "$SCRATCH"; then

        echo "STOP: scratch setup failed. No tests started." >&2

        exit 77

    fi

fi

if [[ ! -w "$SCRATCH" || ! -x "$SCRATCH" ]]; then

    echo "STOP: scratch directory is still not writable/searchable." >&2

    exit 77

fi

echo "Starting atomic-save validation: default five cases, 50 required executions."

echo "Default traffic target: about 25 minutes total, plus seeding/verification/cleanup."

echo "No live rings are modified; object errors remain visible separately."

exec "$ROOT/.venv/bin/python" "$ROOT/experiments/saio_atomic_under_load.py" --scratch-parent "$SCRATCH" "$@"


