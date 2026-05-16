#!/usr/bin/env bash
# =============================================================================
# host-network-setup.sh
#
# Applies kernel-level network settings that CANNOT be set per-container
# because they are not network-namespace scoped.
#
# Must be run on the Docker HOST (the Linux VM) as root, BEFORE starting
# the compose stack.
#
# On macOS + Docker Desktop run via:
#   docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -i -n \
#     /bin/sh -c "$(cat docker/host-network-setup.sh)"
# =============================================================================

set -euo pipefail

# ── Kernel module ─────────────────────────────────────────────────────────────
# BBR congestion control (required before setting tcp_congestion_control=bbr)
if ! lsmod | grep -q tcp_bbr 2>/dev/null; then
  echo "[host-setup] Loading tcp_bbr module..."
  modprobe tcp_bbr || echo "[host-setup] WARN: tcp_bbr not available, skipping"
fi

# ── Socket buffer sizes (not namespace-scoped) ────────────────────────────────
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.core.rmem_default=31457280
sysctl -w net.core.wmem_default=31457280
sysctl -w net.core.optmem_max=25165824
sysctl -w net.core.netdev_max_backlog=65536

# ── TCP buffer sizes (not namespace-scoped) ───────────────────────────────────
sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"

# ── TCP features (not namespace-scoped) ───────────────────────────────────────
sysctl -w net.ipv4.tcp_fastopen=3
sysctl -w net.ipv4.tcp_congestion_control=bbr
sysctl -w net.ipv4.tcp_window_scaling=1
sysctl -w net.ipv4.tcp_sack=1

# ── conntrack (not namespace-scoped) ──────────────────────────────────────────
sysctl -w net.netfilter.nf_conntrack_max=1000000
sysctl -w net.netfilter.nf_conntrack_tcp_timeout_established=86400
sysctl -w net.netfilter.nf_conntrack_tcp_timeout_time_wait=1

echo "[host-setup] Host network tuning applied."
