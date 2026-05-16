-- =============================================================================
-- query_logger.lua — Log every query to a file
-- =============================================================================
--
-- Hook point: after_query_parse  (SQL text + classification available)
--
-- Writes one line per query to /tmp/keel_queries.log:
--   timestamp | session | user@database | R/W | query_type | SQL
--
-- Configuration (inside a worker group):
--   [worker_group.<group>.hooks]
--   hook.lua.after_query_parse.query_logger = \
--       script=examples/hooks/lua/query_logger.lua \
--       func=log_query priority=200
-- =============================================================================

-- Query type enum → human-readable name
local QUERY_NAMES = {
    [0]  = "UNKNOWN",
    [1]  = "SELECT",   [2]  = "SHOW",      [3]  = "EXPLAIN",
    [4]  = "INSERT",   [5]  = "UPDATE",     [6]  = "DELETE",
    [7]  = "TRUNCATE",
    [8]  = "CREATE",   [9]  = "ALTER",      [10] = "DROP",
    [11] = "BEGIN",    [12] = "COMMIT",     [13] = "ROLLBACK",
    [14] = "SAVEPOINT",
    [15] = "SET",      [16] = "RESET",      [17] = "DISCARD",
    [18] = "PREPARE",  [19] = "EXECUTE",    [20] = "DEALLOCATE",
    [21] = "COPY",
}

-- Determine read vs write from needs_primary flag
local function rw_label(ctx)
    if ctx.needs_primary then
        return "WRITE"
    else
        return "READ"
    end
end

--- Main hook function.
-- @param ctx  Table with session/query context (see keel_hook_ctx_t)
-- @return true, ctx   — always continue (this is a logging-only hook)
function log_query(ctx)
    local logfile = os.getenv("KEEL_QUERY_LOG") or "/tmp/keel_queries.log"
    local f = io.open(logfile, "a")
    if not f then
        return true, ctx   -- silently skip if file can't be opened
    end

    local ts       = os.date("%Y-%m-%d %H:%M:%S")
    local sid      = ctx.session_id or 0
    local user     = ctx.username or "?"
    local db       = ctx.database or "?"
    local rw       = rw_label(ctx)
    local qtype    = QUERY_NAMES[ctx.query_type] or "UNKNOWN"
    local sql      = (ctx.sql_text or ""):gsub("\n", " "):sub(1, 512)
    local in_tx    = ctx.in_transaction and "TX" or "--"
    local effects  = string.format("0x%04X", ctx.effect_flags or 0)
    local qcount   = ctx.query_count or 0

    f:write(string.format(
        "%s | sid=%-6d | %s@%s | %-5s | %-10s | tx=%s | effects=%s | #%d | %s\n",
        ts, sid, user, db, rw, qtype, in_tx, effects, qcount, sql
    ))
    f:close()

    -- Never abort — always continue processing
    return true, ctx
end
