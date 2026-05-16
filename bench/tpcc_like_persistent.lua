-- tpcc_like.lua
-- TPC-C inspired mixed workload using the pgbench schema.
-- PERSISTENT CONNECTION VERSION: each sysbench thread opens one backend
-- connection in thread_init() and reuses it for the entire run. Simulates an
-- application server with a connection pool (one socket per thread).
--
-- Connection mode is controlled via READ_TXN_MODE (autocommit|readonly).
--
-- READ transactions  (default 70%): plain autocommit SELECTs — no BEGIN, no COMMIT.
-- WRITE transactions (default 30%): single explicit BEGIN ... COMMIT with DML.
--
-- Read/Write split behaviour:
--   Plain autocommit SELECTs → proxy cannot distinguish → routes to PRIMARY.
--   Set READ_TXN_MODE="readonly" to wrap reads in BEGIN READ ONLY ... COMMIT,
--   which signals the proxy to route them to REPLICAS.
--
--   READ_TXN_MODE="autocommit"  — bare SELECTs, no transaction wrapper
--   READ_TXN_MODE="readonly"    — BEGIN READ ONLY ... COMMIT → routes to replicas (default)
--
-- NOTE: "readonly" is the default so that keel's R/W split is exercised by
-- default.  Set READ_TXN_MODE=autocommit to simulate clients that do not
-- use explicit read-only transactions and therefore all hit the primary.
--
-- Operations:
--   Reads  (5 types, uniform weight):
--     account_lookup   — SELECT abalance by single aid               (point read)
--     account_range    — SELECT avg(abalance) over 100-row range     (range scan)
--     branch_summary   — SELECT bbalance for a branch                (hot row read)
--     teller_status    — SELECT tbalance for a teller                (hot row read)
--     account_history  — SELECT last 10 history rows for an account  (index scan)
--
--   Writes (2 types, weighted 2:1):
--     payment          — UPDATE accounts + tellers + branches + INSERT history  (TPC-C Payment)
--     transfer         — UPDATE two accounts (debit + credit) + INSERT history  (TPC-C New-Order-like)
--
-- Usage:
--   sysbench /keel/bench/tpcc_like_persistent.lua \
--     --db-driver=pgsql --pgsql-host=127.0.0.1 --pgsql-port=7432 \
--     --pgsql-user=postgres --pgsql-password=postgres --pgsql-db=testdb \
--     --threads=100 --time=60 run
--
--   READ_TXN_MODE=readonly sysbench /keel/bench/tpcc_like_persistent.lua ...

sysbench.cmdline.options = {
    scale       = {"pgbench scale factor (accounts = scale × 100000)", 10},
    read_pct    = {"Percentage of events that are reads (0-100)",       70},
}

-- ============================================================================
-- Initialisation
-- ============================================================================

local READ_MODE = os.getenv("READ_TXN_MODE") or "readonly"

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
end

function thread_done()
    con:disconnect()
end

-- ============================================================================
-- Helpers
-- ============================================================================

local function rand_aid()
    return sysbench.rand.uniform(1, sysbench.opt.scale * 100000)
end

local function rand_bid()
    return sysbench.rand.uniform(1, sysbench.opt.scale * 1)
end

local function rand_tid()
    return sysbench.rand.uniform(1, sysbench.opt.scale * 10)
end

local function rand_delta()
    -- sysbench.rand.uniform doesn't handle negative bounds in LuaJIT
    return sysbench.rand.uniform(0, 10000) - 5000
end

-- ============================================================================
-- Read operations — no transaction wrapper by default.
-- Wrapped in BEGIN READ ONLY when READ_TXN_MODE=readonly.
-- ============================================================================

local function read_account_lookup()
    -- Point read: single account balance (most common read pattern)
    local aid = rand_aid()
    con:query(string.format(
        "SELECT aid, bid, abalance FROM pgbench_accounts WHERE aid = %d", aid))
end

local function read_account_range()
    -- Range scan: average balance over 100 accounts (reporting query)
    local aid = rand_aid()
    con:query(string.format(
        "SELECT avg(abalance), count(*) FROM pgbench_accounts"..
        " WHERE aid BETWEEN %d AND %d", aid, aid + 99))
end

local function read_branch_summary()
    -- Hot-row read: branch balance (heavily contended in writes too)
    local bid = rand_bid()
    con:query(string.format(
        "SELECT bid, bbalance FROM pgbench_branches WHERE bid = %d", bid))
end

local function read_teller_status()
    -- Hot-row read: teller balance
    local tid = rand_tid()
    con:query(string.format(
        "SELECT tid, bid, tbalance FROM pgbench_tellers WHERE tid = %d", tid))
end

local function read_account_history()
    -- Index scan: last 10 transactions for an account (order status query)
    local aid = rand_aid()
    con:query(string.format(
        "SELECT tid, bid, delta, mtime FROM pgbench_history"..
        " WHERE aid = %d ORDER BY mtime DESC LIMIT 10", aid))
end

local READ_OPS = {
    read_account_lookup,
    read_account_range,
    read_branch_summary,
    read_teller_status,
    read_account_history,
}

local function do_read()
    local op = READ_OPS[sysbench.rand.uniform(1, #READ_OPS)]
    if READ_MODE == "readonly" then
        con:query("BEGIN READ ONLY")
        op()
        con:query("COMMIT")
    else
        op()   -- plain autocommit SELECT
    end
end

-- ============================================================================
-- Write operations — always wrapped in explicit BEGIN ... COMMIT.
-- ============================================================================

local function write_payment()
    -- TPC-C Payment: debit/credit account, update teller + branch, log history.
    local aid   = rand_aid()
    local bid   = rand_bid()
    local tid   = rand_tid()
    local delta = rand_delta()

    con:query("BEGIN")
    con:query(string.format(
        "UPDATE pgbench_accounts SET abalance = abalance + %d WHERE aid = %d",
        delta, aid))
    con:query(string.format(
        "UPDATE pgbench_tellers SET tbalance = tbalance + %d WHERE tid = %d",
        delta, tid))
    con:query(string.format(
        "UPDATE pgbench_branches SET bbalance = bbalance + %d WHERE bid = %d",
        delta, bid))
    con:query(string.format(
        "INSERT INTO pgbench_history(tid, bid, aid, delta, mtime)"..
        " VALUES(%d, %d, %d, %d, CURRENT_TIMESTAMP)",
        tid, bid, aid, delta))
    con:query("COMMIT")
end

local function write_transfer()
    -- Transfer: debit one account, credit another, log both history entries.
    local aid_from = rand_aid()
    local aid_to   = rand_aid()
    -- Avoid self-transfer and ensure consistent lock order (lower aid first)
    if aid_from == aid_to then aid_to = (aid_to % (sysbench.opt.scale * 100000)) + 1 end
    if aid_from > aid_to then aid_from, aid_to = aid_to, aid_from end
    local amount = sysbench.rand.uniform(1, 1000)
    local bid    = rand_bid()
    local tid    = rand_tid()

    con:query("BEGIN")
    con:query(string.format(
        "UPDATE pgbench_accounts SET abalance = abalance - %d WHERE aid = %d",
        amount, aid_from))
    con:query(string.format(
        "UPDATE pgbench_accounts SET abalance = abalance + %d WHERE aid = %d",
        amount, aid_to))
    con:query(string.format(
        "INSERT INTO pgbench_history(tid, bid, aid, delta, mtime)"..
        " VALUES(%d, %d, %d, %d, CURRENT_TIMESTAMP)",
        tid, bid, aid_from, -amount))
    con:query(string.format(
        "INSERT INTO pgbench_history(tid, bid, aid, delta, mtime)"..
        " VALUES(%d, %d, %d, %d, CURRENT_TIMESTAMP)",
        tid, bid, aid_to, amount))
    con:query("COMMIT")
end

-- Weighted: 2 payments for every 1 transfer (mirrors TPC-C transaction mix)
local WRITE_OPS = {
    write_payment, write_payment,
    write_transfer,
}

local function do_write()
    local op = WRITE_OPS[sysbench.rand.uniform(1, #WRITE_OPS)]
    op()
end

-- ============================================================================
-- Event dispatcher
-- ============================================================================

function event()
    if sysbench.rand.uniform(1, 100) <= sysbench.opt.read_pct then
        do_read()
    else
        do_write()
    end
end
