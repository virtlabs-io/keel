# Admin SQL Query Language

Keel exposes an SQL-based admin interface through its admin console port.
Clients connect with `psql` (or any PostgreSQL client) and execute DML
statements against virtual tables that map to proxy state.

This is an alternative to the legacy imperative commands (`SHOW STATS`,
`ADD SERVER`, etc.). Both interfaces are available simultaneously — the
SQL parser is a fallback for commands that don't match a legacy keyword.

## Connection

```sh
psql -h 127.0.0.1 -p 6432 -U admin keel
```

The admin database name and credentials are configured in the keel
configuration file.

## SELECT — Reading Proxy State

```sql
SELECT * FROM <table>;
```

All SELECT queries return the full virtual table. Column projection and
WHERE filtering are parsed but currently ignored by the dispatcher — the
full result set is always returned.

### Virtual Tables

| Table            | Description                              | Legacy Equivalent     |
|------------------|------------------------------------------|-----------------------|
| `stats`          | Aggregate per-database statistics        | `SHOW STATS`          |
| `stats_detail`   | Per-pool detailed statistics             | `SHOW STATS_DETAIL`   |
| `servers`        | Backend server list with status          | `SHOW SERVERS`        |
| `pools`          | Connection pool state and counts         | `SHOW POOLS`          |
| `clients`        | Connected client sessions                | `SHOW CLIENTS`        |
| `config`         | Current configuration key-value pairs    | `SHOW CONFIG`         |
| `latency`        | Per-backend latency percentiles          | `SHOW LATENCY`        |
| `system`         | System resource usage (memory, FDs)      | `SHOW SYSTEM`         |
| `rebalance`      | Load-balancing statistics                | `SHOW REBALANCE`      |
| `cluster`        | Cluster peer membership                  | `SHOW CLUSTER`        |
| `cluster_stats`  | Cluster replication statistics           | `SHOW CLUSTER STATS`  |
| `tracing`        | OpenTelemetry tracing configuration      | `SHOW TRACING`        |
| `shard_rules`    | Registered shard routing rules           | `SHOW SHARD RULES`    |
| `help`           | Available admin commands                 | `SHOW HELP`           |
| `version`        | Keel version and build info              | `SHOW VERSION`        |

Table names are **case-insensitive**: `SELECT * FROM STATS`, `select * from Stats`,
and `SELECT * FROM stats` all work.

### Examples

```sql
-- View all backend servers
SELECT * FROM servers;

-- View connection pools
SELECT * FROM pools;

-- View current config
SELECT * FROM config;

-- View registered shard rules
SELECT * FROM shard_rules;

-- Explain where a query will be routed
EXPLAIN SHARD PLAN FOR 'SELECT * FROM users WHERE id = 42';

-- (WHERE clause is parsed but not filtered server-side)
SELECT * FROM servers WHERE role = 'primary';
```

## UPDATE — Modifying Configuration

### Update Configuration

```sql
UPDATE config SET value = '<new_value>' WHERE key = '<setting>';
```

Equivalent to the legacy `SET <setting> = <value>` command.

**Examples:**

```sql
-- Change max client connections
UPDATE config SET value = '200' WHERE key = 'max_client_conn';

-- Change default pool size
UPDATE config SET value = '25' WHERE key = 'default_pool_size';
```

### Enable/Disable Servers

```sql
-- Disable a server
UPDATE servers SET enabled = false WHERE index = 0;

-- Re-enable it
UPDATE servers SET enabled = true WHERE index = 0;
```

The `enabled` column accepts: `true`, `false`, `'on'`, `'off'`, `'true'`,
`'false'`, `1`, `0`, `'1'`, `'0'`.

## INSERT — Adding Resources

### Add a Backend Server

```sql
INSERT INTO servers (host, port, role, weight)
VALUES ('10.0.1.50', 5432, 'primary', 100);

-- With all columns
INSERT INTO servers (host, port, role, weight, enabled)
VALUES ('10.0.1.51', 5432, 'replica', 50, true);
```

Equivalent to: `ADD SERVER host=10.0.1.50 port=5432 role=primary weight=100`

Column names and value positions are paired. The number of columns must
equal the number of values.

### Add a Cluster Peer

```sql
INSERT INTO peers (host) VALUES ('10.0.2.10:6432');

-- IPv6
INSERT INTO peers (host) VALUES ('[::1]:6432');
```

Equivalent to: `ADD PEER 10.0.2.10:6432`

## DELETE — Removing Resources

### Remove a Server

```sql
DELETE FROM servers WHERE index = 0;
```

Equivalent to: `REMOVE SERVER 0`

### Kill a Client Connection

```sql
DELETE FROM clients WHERE id = 42;
```

Equivalent to: `KILL CLIENT 42`

### Remove a Cluster Peer

```sql
DELETE FROM peers WHERE host = '10.0.2.10:6432';
```

Equivalent to: `REMOVE PEER 10.0.2.10:6432`

## JSON Output

Any query (SQL or legacy) can be suffixed with `FORMAT JSON` to receive the
result as a JSON document instead of the standard PostgreSQL wire-format
row set:

```sql
SELECT * FROM stats FORMAT JSON;
SELECT * FROM servers FORMAT JSON;
```

## Legacy Commands

The following imperative commands are still available and are matched
before the SQL parser:

| Command                     | Description                          |
|-----------------------------|--------------------------------------|
| `SHOW SHARD RULES`           | List registered shard routing rules      |
| `EXPLAIN SHARD PLAN FOR '<sql>'` | Show routing plan for a given query  |
| `SHOW <table>`              | Display virtual table                |
| `SET <key> = <value>`       | Update a configuration setting       |
| `ADD SERVER <key=val ...>`  | Add a backend server                 |
| `ADD PEER <host:port>`      | Add a cluster peer                   |
| `REMOVE SERVER <index>`     | Remove a backend server              |
| `REMOVE PEER <host:port>`   | Remove a cluster peer                |
| `KILL CLIENT <id>`          | Disconnect a client                  |
| `ENABLE SERVER <index>`     | Enable a disabled server             |
| `DISABLE SERVER <index>`    | Temporarily disable a server         |
| `PAUSE`                     | Pause all client traffic             |
| `RESUME`                    | Resume paused traffic                |
| `DRAIN [args]`              | Drain connections gracefully         |
| `RELOAD`                    | Reload configuration from disk       |
| `RESTART WORKERS [args]`    | Restart worker processes             |

## Error Handling

- Unknown table names: `"Unknown admin table: \"<name>\""`
- SQL parse errors: returned as PostgreSQL ErrorResponse messages
- Column/value count mismatch in INSERT: error before dispatch
- Missing WHERE clause on DELETE: error (WHERE is required)
