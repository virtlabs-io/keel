#!/bin/bash
# Initialize MySQL primary — runs via docker-entrypoint-initdb.d
set -e
echo "[KEEL] Primary initialized for GTID-based replication."
