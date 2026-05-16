#!/usr/bin/env bash
# =============================================================================
# tests/chaos/lib/sentinel.sh — Multi-Sentinel Harness for KEEL Chaos Tests
# =============================================================================
#
# Provides reliable, multi-row sentinel verification across chaos scenarios.
# Instead of a single lucky-pass sentinel row, every scenario writes sentinel
# rows in three phases (pre-fault, during-fault, post-recovery) and verifies:
#
#   1. PRESENCE   — all expected rows are individually visible after recovery
#   2. ATOMICITY  — transactions that crossed a fault boundary committed either
#                   fully or not at all (no partial commits / torn writes)
#   3. CONTENT    — each row's stored value matches its expected string exactly
#                   (guards against silent corruption or row misidentification)
#   4. ISOLATION  — rows from one run do not interfere with another run because
#                   every run generates a unique tag derived from PID + timestamp
#
# Usage (source from scenario scripts):
#   source "$(dirname "$0")/../lib/sentinel.sh"
#
# Each function takes explicit host/port/user/pass/db/table parameters so that
# callers can freely mix direct-to-primary connections and keel-proxy connections
# within the same scenario.
#
# Standard sentinel table (auto-created by sentinel_setup):
#   CREATE TABLE IF NOT EXISTS <table> (
#       id          BIGSERIAL PRIMARY KEY,
#       scenario    TEXT NOT NULL,
#       tag         TEXT NOT NULL,   -- unique per-run label
#       phase       TEXT NOT NULL,   -- pre_fault | during_fault_N | post_recovery
#       seq         INT  NOT NULL,   -- 1..N within this tag+phase batch
#       val         TEXT NOT NULL,   -- tag:phase:seq — globally unique
#       written_via TEXT NOT NULL,   -- 'keel' or 'direct'
#       ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
#       CONSTRAINT <table>_val_uq UNIQUE (val)
#   );
# =============================================================================

# ── Internal helpers ─────────────────────────────────────────────────────────

_sentinel_log()  { printf '[sentinel] %s\n' "$*"; }
_sentinel_warn() { printf '[sentinel:WARN] %s\n' "$*" >&2; }
_sentinel_die()  {
    printf '[sentinel:FAIL] %s\n' "$*" >&2
    # Propagate to the calling script's die() if defined, else exit 1
    if declare -f die >/dev/null 2>&1; then
        die "$*"
    else
        exit 1
    fi
}

# Run psql and return output; non-zero exit is propagated but not fatal by default.
# Stdout flows to the caller; stderr is forwarded so failures are visible.
_sentinel_psql() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5"
    shift 5
    PGPASSWORD="$pass" psql \
        -h "$host" -p "$port" -U "$user" -d "$db" \
        -v ON_ERROR_STOP=1 \
        "$@"
}

# ── sentinel_setup ────────────────────────────────────────────────────────────
#
# Create the sentinel table if it does not exist.
#
# Usage:
#   sentinel_setup HOST PORT USER PASS DB [TABLE]
#   TABLE defaults to "chaos_sentinel"
#
sentinel_setup() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5"
    local table="${6:-chaos_sentinel}"
    _sentinel_log "setting up table '${table}' on ${host}:${port}/${db}"
    _sentinel_psql "$host" "$port" "$user" "$pass" "$db" -q <<SQL
CREATE TABLE IF NOT EXISTS ${table} (
    id          BIGSERIAL PRIMARY KEY,
    scenario    TEXT NOT NULL,
    tag         TEXT NOT NULL,
    phase       TEXT NOT NULL,
    seq         INT  NOT NULL,
    val         TEXT NOT NULL,
    written_via TEXT NOT NULL DEFAULT 'direct',
    ts          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ${table}_val_uq UNIQUE (val)
);
SQL
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        _sentinel_warn "sentinel_setup failed (rc=${rc}) — table may already exist or connection refused"
    fi
    return $rc
}

# ── sentinel_tag ─────────────────────────────────────────────────────────────
#
# Generate a unique run tag: <prefix>_<pid>_<epoch_ms>
#
# Usage:
#   MY_TAG=$(sentinel_tag "flip")
#
sentinel_tag() {
    local prefix="${1:-run}"
    printf '%s_%d_%d' "$prefix" "$$" "$(date +%s%3N)"
}

# ── sentinel_write_batch ─────────────────────────────────────────────────────
#
# Write N sentinel rows in a single multi-value INSERT (no explicit transaction
# wrapper — relies on implicit autocommit).  Each row gets a unique val string
# of the form  TAG:PHASE:SEQ  so individual rows can be verified by value.
#
# Usage:
#   sentinel_write_batch HOST PORT USER PASS DB TABLE SCENARIO TAG PHASE VIA N
#
# Returns 0 on success, non-zero on failure.
#
sentinel_write_batch() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local scenario="$7" tag="$8" phase="$9" via="${10}" n="${11}"

    # Build VALUES clause from seq 1..N
    local values="" i
    for i in $(seq 1 "$n"); do
        local val="${tag}:${phase}:${i}"
        [[ -n "$values" ]] && values="${values},"
        values="${values}('${scenario}','${tag}','${phase}',${i},'${val}','${via}')"
    done

    _sentinel_log "write_batch: ${n} rows  tag=${tag}  phase=${phase}  via=${via}"
    _sentinel_psql "$host" "$port" "$user" "$pass" "$db" -q \
        -c "INSERT INTO ${table} (scenario,tag,phase,seq,val,written_via) VALUES ${values} ON CONFLICT (val) DO NOTHING"
}

# ── sentinel_write_txn ───────────────────────────────────────────────────────
#
# Write N sentinel rows inside a single explicit BEGIN … COMMIT transaction.
# If the connection is lost or the backend crashes mid-INSERT, PostgreSQL will
# roll back the entire transaction, leaving zero rows for this tag+phase.
#
# After a chaos event, use sentinel_assert_atomicity to verify that exactly 0
# or exactly N rows are present (never a partial count).
#
# Usage:
#   sentinel_write_txn HOST PORT USER PASS DB TABLE SCENARIO TAG PHASE VIA N
#
# Returns:
#   0 — transaction committed (N rows written)
#   1 — transaction rolled back or connection lost (0 rows written)
#   2 — unexpected / unknown outcome
#
sentinel_write_txn() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local scenario="$7" tag="$8" phase="$9" via="${10}" n="${11}"

    local values="" i
    for i in $(seq 1 "$n"); do
        local val="${tag}:${phase}:${i}"
        [[ -n "$values" ]] && values="${values},"
        values="${values}('${scenario}','${tag}','${phase}',${i},'${val}','${via}')"
    done

    _sentinel_log "write_txn: ${n} rows  tag=${tag}  phase=${phase}  via=${via}"
    _sentinel_psql "$host" "$port" "$user" "$pass" "$db" -q <<SQL
BEGIN;
INSERT INTO ${table} (scenario,tag,phase,seq,val,written_via)
VALUES ${values};
COMMIT;
SQL
    local rc=$?
    # rc=0 means COMMIT succeeded; rc=1 means rolled back or connection lost
    return $rc
}

# ── sentinel_count ────────────────────────────────────────────────────────────
#
# Return the number of rows matching TAG and PHASE.
# Prints the count to stdout.
#
# Usage:
#   n=$(sentinel_count HOST PORT USER PASS DB TABLE TAG PHASE)
#
sentinel_count() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local tag="$7" phase="$8"
    _sentinel_psql "$host" "$port" "$user" "$pass" "$db" -t -A \
        -c "SELECT COUNT(*) FROM ${table} WHERE tag='${tag}' AND phase='${phase}'" \
        2>/dev/null | tr -d ' \n' || echo "0"
}

# ── sentinel_assert_count ─────────────────────────────────────────────────────
#
# Assert that exactly EXPECTED rows exist for TAG+PHASE.  Fails loudly if not.
#
# Usage:
#   sentinel_assert_count HOST PORT USER PASS DB TABLE TAG PHASE EXPECTED DESC
#
sentinel_assert_count() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local tag="$7" phase="$8" expected="$9" desc="${10}"

    local actual
    actual=$(sentinel_count "$host" "$port" "$user" "$pass" "$db" "$table" "$tag" "$phase")
    if [[ "${actual:-0}" -ne "$expected" ]]; then
        _sentinel_die "ASSERT COUNT [${desc}]: expected=${expected} actual=${actual:-ERR}  tag=${tag}  phase=${phase}"
    fi
    _sentinel_log "✓ [${desc}] count=${actual}/${expected}  tag=${tag}  phase=${phase}"
}

# ── sentinel_assert_atomicity ─────────────────────────────────────────────────
#
# Assert that for TAG+PHASE, the row count is EITHER 0 OR EXPECTED.
# Any other count (1 .. EXPECTED-1) is a partial commit / atomicity violation.
#
# Usage:
#   sentinel_assert_atomicity HOST PORT USER PASS DB TABLE TAG PHASE EXPECTED DESC
#
sentinel_assert_atomicity() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local tag="$7" phase="$8" expected="$9" desc="${10}"

    local actual
    actual=$(sentinel_count "$host" "$port" "$user" "$pass" "$db" "$table" "$tag" "$phase")
    if [[ "${actual:-0}" -ne 0 && "${actual:-0}" -ne "$expected" ]]; then
        _sentinel_die "ATOMICITY VIOLATION [${desc}]: partial commit! count=${actual:-ERR} expected=0 or ${expected}  tag=${tag}  phase=${phase}"
    fi
    if [[ "${actual:-0}" -eq 0 ]]; then
        _sentinel_log "✓ [${desc}] rolled back cleanly (0 rows)  tag=${tag}  phase=${phase}"
    else
        _sentinel_log "✓ [${desc}] committed atomically (${actual} rows)  tag=${tag}  phase=${phase}"
    fi
}

# ── sentinel_assert_values ────────────────────────────────────────────────────
#
# Verify that each specific value  TAG:PHASE:1 … TAG:PHASE:N  exists in the
# database exactly once.  This is stronger than a count check: it detects
# corruption where the row count is correct but stored values are wrong.
#
# Usage:
#   sentinel_assert_values HOST PORT USER PASS DB TABLE TAG PHASE N DESC
#
sentinel_assert_values() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local tag="$7" phase="$8" n="$9" desc="${10}"

    local missing=0 i
    for i in $(seq 1 "$n"); do
        local expected_val="${tag}:${phase}:${i}"
        local cnt
        cnt=$(_sentinel_psql "$host" "$port" "$user" "$pass" "$db" -t -A \
            -c "SELECT COUNT(*) FROM ${table} WHERE val='${expected_val}'" \
            2>/dev/null | tr -d ' \n' || echo "0")
        if [[ "${cnt:-0}" -ne 1 ]]; then
            _sentinel_warn "missing individual sentinel value '${expected_val}' (count=${cnt:-ERR})"
            missing=$((missing + 1))
        fi
    done

    if [[ $missing -gt 0 ]]; then
        _sentinel_die "ASSERT VALUES [${desc}]: ${missing}/${n} sentinel values missing or corrupted  tag=${tag}  phase=${phase}"
    fi
    _sentinel_log "✓ [${desc}] all ${n} individual values verified  tag=${tag}  phase=${phase}"
}

# ── sentinel_assert_none_partial ──────────────────────────────────────────────
#
# For a set of transaction tags (e.g. mid_fault_1 … mid_fault_M), assert that
# each one has either 0 or TXN_SIZE rows — never a number in between.
# Also verifies that any committed rows have correct individual values.
#
# Usage:
#   sentinel_assert_none_partial HOST PORT USER PASS DB TABLE BASE_TAG PHASE_PREFIX TXN_COUNT TXN_SIZE DESC
#   (asserts TXN_COUNT transactions, each with TXN_SIZE rows in a single txn)
#
sentinel_assert_none_partial() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local base_tag="$7" phase_prefix="$8" txn_count="$9" txn_size="${10}" desc="${11}"

    local violations=0 committed=0 rolled_back=0 i
    for i in $(seq 1 "$txn_count"); do
        local tag="${base_tag}_${i}"
        local actual
        actual=$(sentinel_count "$host" "$port" "$user" "$pass" "$db" "$table" "$tag" "$phase_prefix")
        if [[ "${actual:-0}" -ne 0 && "${actual:-0}" -ne "$txn_size" ]]; then
            _sentinel_warn "PARTIAL COMMIT txn ${i}: count=${actual:-ERR} expected 0 or ${txn_size}"
            violations=$((violations + 1))
        elif [[ "${actual:-0}" -eq "$txn_size" ]]; then
            committed=$((committed + 1))
        else
            rolled_back=$((rolled_back + 1))
        fi
    done

    if [[ $violations -gt 0 ]]; then
        _sentinel_die "ATOMICITY [${desc}]: ${violations} partial-commit violations across ${txn_count} transactions"
    fi
    _sentinel_log "✓ [${desc}] ${txn_count} transactions: ${committed} committed, ${rolled_back} rolled back, 0 partial"
}

# ── sentinel_background_writer ───────────────────────────────────────────────
#
# Launch a background process that continuously writes N-row transactions
# through the given connection, using unique per-iteration tags.  The PID file
# path and tag-list file path are written to caller-supplied variable names.
#
# Usage:
#   sentinel_background_writer HOST PORT USER PASS DB TABLE SCENARIO BASE_TAG \
#       PHASE TXN_SIZE INTERVAL_MS PID_VAR COUNT_VAR
#
#   PID_VAR   — name of shell variable to store the background writer's PID
#   COUNT_VAR — name of shell variable to store the number of transactions started
#
# The caller must stop the writer with:
#   kill "$<PID_VAR>" 2>/dev/null; wait "$<PID_VAR>" 2>/dev/null
#
# Transaction tags are  BASE_TAG_1, BASE_TAG_2, ...  stored in a temp file
# whose name is echo'd by the background process to COUNT_VAR_FILE.
#
sentinel_background_writer() {
    local host="$1" port="$2" user="$3" pass="$4" db="$5" table="$6"
    local scenario="$7" base_tag="$8" phase="$9" txn_size="${10}"
    local interval_ms="${11}" pid_file="${12}" count_file="${13}"
    local via="${14:-keel}"

    # Write the background writer loop to a temp script and execute it
    local tmpscript
    tmpscript=$(mktemp /tmp/sentinel-writer-XXXXXX.sh)
    chmod +x "$tmpscript"

    cat > "$tmpscript" <<BGSCRIPT
#!/usr/bin/env bash
i=0
echo "0" > "${count_file}"
while true; do
    i=\$((i + 1))
    tag="${base_tag}_\${i}"
    values=""
    for j in \$(seq 1 ${txn_size}); do
        val="\${tag}:${phase}:\${j}"
        [ -n "\$values" ] && values="\${values},"
        values="\${values}('${scenario}','\${tag}','${phase}',\${j},'\${val}','${via}')"
    done
    PGPASSWORD="${pass}" psql \
        -h "${host}" -p "${port}" -U "${user}" -d "${db}" \
        -v ON_ERROR_STOP=1 -q 2>/dev/null <<SQL
BEGIN;
INSERT INTO ${table} (scenario,tag,phase,seq,val,written_via) VALUES \${values};
COMMIT;
SQL
    echo "\${i}" > "${count_file}"
    sleep "$(echo "scale=3; ${interval_ms}/1000" | bc 2>/dev/null || echo "0.2")"
done
BGSCRIPT

    bash "$tmpscript" &
    echo $! > "$pid_file"
    rm -f "$tmpscript"
}

# ── sentinel_stop_background_writer ──────────────────────────────────────────
#
# Stop a background writer and return the number of transactions it started.
#
# Usage:
#   txn_count=$(sentinel_stop_background_writer PID_FILE COUNT_FILE)
#
sentinel_stop_background_writer() {
    local pid_file="$1" count_file="$2"
    local pid
    pid=$(cat "$pid_file" 2>/dev/null || echo "0")
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null || true
    cat "$count_file" 2>/dev/null || echo "0"
}
