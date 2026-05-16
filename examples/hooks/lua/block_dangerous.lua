-- =============================================================================
-- block_dangerous.lua — Abort dangerous queries
-- =============================================================================
--
-- Hook point: after_query_parse
--
-- Blocks:
--   - DROP TABLE / DROP DATABASE
--   - TRUNCATE
--   - DELETE without WHERE (heuristic: checks for "WHERE" keyword)
--   - Any query from user "readonly"
--
-- Configuration (inside a worker group):
--   [worker_group.<group>.hooks]
--   hook.lua.after_query_parse.block_dangerous = \
--       script=examples/hooks/lua/block_dangerous.lua \
--       func=check_query priority=50
-- =============================================================================

-- Query type constants (must match keel_query_type_t enum)
local QT_DROP     = 10
local QT_TRUNCATE = 7
local QT_DELETE   = 6
local QT_INSERT   = 4
local QT_UPDATE   = 5

--- Check and potentially block dangerous queries.
-- @param ctx  Hook context table
-- @return bool, ctx — false to abort, true to continue
function check_query(ctx)
    local qtype = ctx.query_type or 0
    local sql   = (ctx.sql_text or ""):upper()
    local user  = ctx.username or ""

    -- Block all writes from "readonly" user
    if user == "readonly" and ctx.needs_primary then
        ctx.error_msg = "User 'readonly' is not allowed to execute write queries"
        return false, ctx
    end

    -- Block DROP TABLE / DROP DATABASE
    if qtype == QT_DROP then
        ctx.error_msg = "DROP statements are blocked by security policy"
        return false, ctx
    end

    -- Block TRUNCATE
    if qtype == QT_TRUNCATE then
        ctx.error_msg = "TRUNCATE statements are blocked by security policy"
        return false, ctx
    end

    -- Block DELETE without WHERE clause (dangerous!)
    if qtype == QT_DELETE then
        if not sql:find("WHERE") then
            ctx.error_msg = "DELETE without WHERE clause is blocked"
            return false, ctx
        end
    end

    -- All checks passed
    return true, ctx
end
