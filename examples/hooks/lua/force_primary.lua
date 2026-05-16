-- =============================================================================
-- force_primary.lua — Force specific queries to primary
-- =============================================================================
--
-- Hook point: before_route
--
-- Routes all queries from users in a "primary_users" list to the primary,
-- regardless of the router's read/write classification.  Useful for apps
-- that need strong consistency (e.g., after a write, read your own writes).
--
-- Configuration (inside a worker group):
--   [worker_group.<group>.hooks]
--   hook.lua.before_route.force_primary = \
--       script=examples/hooks/lua/force_primary.lua \
--       func=route_check priority=100
-- =============================================================================

-- Users who always go to primary (strong consistency)
local PRIMARY_USERS = {
    ["admin"]     = true,
    ["migration"] = true,
    ["replicator"] = true,
}

-- Databases that always go to primary
local PRIMARY_DATABASES = {
    ["config_db"]   = true,
    ["system"]      = true,
}

--- Route check — override routing for specific users/databases.
-- @param ctx  Hook context table
-- @return true, ctx — always continue (just mutate route_hint)
function route_check(ctx)
    local user = ctx.username or ""
    local db   = ctx.database or ""

    if PRIMARY_USERS[user] then
        ctx.route_hint    = ctx.ROUTE_PRIMARY
        ctx.needs_primary = true
        return true, ctx
    end

    if PRIMARY_DATABASES[db] then
        ctx.route_hint    = ctx.ROUTE_PRIMARY
        ctx.needs_primary = true
        return true, ctx
    end

    -- No override — let the normal router decide
    return true, ctx
end
