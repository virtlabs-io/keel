-- =============================================================================
-- latency_sampler.lua — Tag queries for latency sampling
-- =============================================================================
--
-- Hook point: before_send
--
-- Logs the final query payload size and routing decision just before
-- the query is forwarded to the backend.  Useful for debugging routing
-- and as a template for performance sampling hooks.
--
-- Configuration (inside a worker group):
--   [worker_group.<group>.hooks]
--   hook.lua.before_send.latency_sampler = \
--       script=examples/hooks/lua/latency_sampler.lua \
--       func=before_send priority=500
-- =============================================================================

local ROUTE_NAMES = {
    [0] = "PRIMARY",
    [1] = "REPLICA",
    [2] = "ANY",
}

--- Log payload info just before sending to backend.
-- @param ctx  Hook context table
-- @return true, ctx
function before_send(ctx)
    local logfile = os.getenv("KEEL_SEND_LOG") or "/tmp/keel_send.log"
    local f = io.open(logfile, "a")
    if not f then
        return true, ctx
    end

    local ts     = os.date("%Y-%m-%d %H:%M:%S")
    local sid    = ctx.session_id or 0
    local route  = ROUTE_NAMES[ctx.route_hint] or "UNKNOWN"
    local plen   = ctx.be_payload_len or 0
    local sql    = (ctx.sql_text or ""):gsub("\n", " "):sub(1, 200)

    f:write(string.format(
        "%s | sid=%-6d | \226\134\146 %s | %d bytes | %s\n",
        ts, sid, route, plen, sql
    ))
    f:close()

    return true, ctx
end
