#!/usr/bin/env sh
# =============================================================================
# KEEL Docker Entrypoint
# =============================================================================
# Applies KEEL_* environment variable overrides to the INI config before
# handing off to the keel binary.
#
# Any KEEL_* variable is translated to an INI override using the pattern:
#
#   KEEL_<SECTION>_<KEY>=<value>  →  [section] key = value
#
# Special top-level overrides (no section prefix):
#   KEEL_CONFIG        Path to the INI config file (default: /etc/keel/keel.ini)
#   KEEL_LOG_LEVEL     [keel] log_level
#
# Worker-group config (e.g. KEEL_WG_MYDB_POOL_MODE=transaction):
#   KEEL_WG_<GROUP>_<KEY>=<value>  →  [worker_group.<group>] key = value
#
# Cluster config:
#   KEEL_CLUSTER_NODE_ID=keel-1        →  [cluster] node_id = keel-1
#   KEEL_CLUSTER_INITIAL_PEERS=h:p,...  →  [cluster] initial_peers = h:p,...
#   KEEL_CLUSTER_ENABLED=true          →  [cluster] enabled = true
#   KEEL_CLUSTER_LISTEN_PORT=9100      →  [cluster] listen_port = 9100
#
# The override mechanism writes an INI fragment to /run/keel/env-overrides.ini
# and passes it to keel as an additional --config argument (keel merges
# multiple --config files; later files win).
#
# Usage (typically invoked by Docker):
#   /usr/local/bin/docker-entrypoint.sh keel -c /etc/keel/keel.ini
# =============================================================================
set -e

KEEL_CONFIG="${KEEL_CONFIG:-/etc/keel/keel.ini}"
OVERRIDE_FILE="/run/keel/env-overrides.ini"

# Ensure runtime directory is writable (tmpfs in container)
mkdir -p /run/keel

# ─── Build override file from KEEL_* environment variables ──────────────────

# We use printf/echo rather than heredoc for POSIX sh compatibility.
# Each variable is translated to its INI section.key form.

truncate_override() {
    printf '' > "${OVERRIDE_FILE}"
}

emit_kv() {
    # $1=section  $2=key  $3=value
    printf '[%s]\n%s = %s\n\n' "$1" "$2" "$3" >> "${OVERRIDE_FILE}"
}

truncate_override

# ── [keel] top-level ────────────────────────────────────────────────────────
[ -n "${KEEL_LOG_LEVEL:-}" ]         && emit_kv "keel"      "log_level"          "${KEEL_LOG_LEVEL}"
[ -n "${KEEL_SHUTDOWN_TIMEOUT:-}" ]  && emit_kv "keel"      "shutdown_timeout_ms" "${KEEL_SHUTDOWN_TIMEOUT}"

# ── [admin] ──────────────────────────────────────────────────────────────────
[ -n "${KEEL_ADMIN_ENABLED:-}" ]     && emit_kv "admin"     "enabled"            "${KEEL_ADMIN_ENABLED}"
[ -n "${KEEL_ADMIN_PORT:-}" ]        && emit_kv "admin"     "listen_port"        "${KEEL_ADMIN_PORT}"
[ -n "${KEEL_ADMIN_ADDR:-}" ]        && emit_kv "admin"     "listen_addr"        "${KEEL_ADMIN_ADDR}"

# ── [prometheus] ─────────────────────────────────────────────────────────────
[ -n "${KEEL_PROM_ENABLED:-}" ]      && emit_kv "prometheus" "enabled"           "${KEEL_PROM_ENABLED}"
[ -n "${KEEL_PROM_PORT:-}" ]         && emit_kv "prometheus" "port"              "${KEEL_PROM_PORT}"
[ -n "${KEEL_PROM_ADDR:-}" ]         && emit_kv "prometheus" "listen_addr"       "${KEEL_PROM_ADDR}"

# ── [cluster] ────────────────────────────────────────────────────────────────
[ -n "${KEEL_CLUSTER_ENABLED:-}" ]       && emit_kv "cluster" "enabled"              "${KEEL_CLUSTER_ENABLED}"
[ -n "${KEEL_CLUSTER_NODE_ID:-}" ]       && emit_kv "cluster" "node_id"              "${KEEL_CLUSTER_NODE_ID}"
[ -n "${KEEL_CLUSTER_LISTEN_ADDR:-}" ]   && emit_kv "cluster" "listen_addr"          "${KEEL_CLUSTER_LISTEN_ADDR}"
[ -n "${KEEL_CLUSTER_LISTEN_PORT:-}" ]   && emit_kv "cluster" "listen_port"          "${KEEL_CLUSTER_LISTEN_PORT}"
[ -n "${KEEL_CLUSTER_INITIAL_PEERS:-}" ] && emit_kv "cluster" "initial_peers"        "${KEEL_CLUSTER_INITIAL_PEERS}"
[ -n "${KEEL_CLUSTER_HB_INTERVAL:-}" ]   && emit_kv "cluster" "heartbeat_interval_ms" "${KEEL_CLUSTER_HB_INTERVAL}"
[ -n "${KEEL_CLUSTER_HB_TIMEOUT:-}" ]    && emit_kv "cluster" "heartbeat_timeout_ms"  "${KEEL_CLUSTER_HB_TIMEOUT}"
[ -n "${KEEL_CLUSTER_FAIL_THRESHOLD:-}" ] && emit_kv "cluster" "failure_threshold"   "${KEEL_CLUSTER_FAIL_THRESHOLD}"
[ -n "${KEEL_CLUSTER_AUTO_SYNC:-}" ]     && emit_kv "cluster" "auto_sync"            "${KEEL_CLUSTER_AUTO_SYNC}"

# ── Pass-through: any remaining KEEL_SECTION_KEY=value pattern ──────────────
# Variables already handled above are skipped.  Everything else matching
# KEEL_[A-Z]+_[A-Z_]+ is translated as [section] key = value where SECTION is
# the first component (lowercased) and KEY is the remainder (lowercased,
# underscores preserved).
_handled="KEEL_LOG_LEVEL KEEL_SHUTDOWN_TIMEOUT \
          KEEL_ADMIN_ENABLED KEEL_ADMIN_PORT KEEL_ADMIN_ADDR \
          KEEL_PROM_ENABLED KEEL_PROM_PORT KEEL_PROM_ADDR \
          KEEL_CLUSTER_ENABLED KEEL_CLUSTER_NODE_ID KEEL_CLUSTER_LISTEN_ADDR \
          KEEL_CLUSTER_LISTEN_PORT KEEL_CLUSTER_INITIAL_PEERS \
          KEEL_CLUSTER_HB_INTERVAL KEEL_CLUSTER_HB_TIMEOUT \
          KEEL_CLUSTER_FAIL_THRESHOLD KEEL_CLUSTER_AUTO_SYNC \
          KEEL_CONFIG"

# ─── Determine extra config args ─────────────────────────────────────────────
# keel only honours the last -c flag (getopt assigns, not appends), so we
# produce a single merged config file rather than passing two -c arguments.
FINAL_CONFIG="${KEEL_CONFIG}"
if [ -s "${OVERRIDE_FILE}" ]; then
    MERGED_FILE="/run/keel/merged.ini"
    cat "${KEEL_CONFIG}" "${OVERRIDE_FILE}" > "${MERGED_FILE}"
    FINAL_CONFIG="${MERGED_FILE}"
fi

# ─── If the first arg is "keel" pass straight to exec; otherwise exec as-is ──
if [ "${1:-}" = "keel" ] || [ "${1:-}" = "/usr/local/bin/keel" ]; then
    shift
    exec /usr/local/bin/keel -c "${FINAL_CONFIG}" "$@"
fi

exec "$@"
