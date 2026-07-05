# Quick Start Guide

This guide will help you get KEEL up and running as a PostgreSQL transaction pooler in under 5 minutes.

## Prerequisites

Before you begin, ensure your environment meets the following requirements:

- **Operating System**: Linux (recommended) or macOS.
- **Build Tools**: CMake 3.25+, GCC 13+ or Clang 17+.
- **Dependencies**: OpenSSL headers, `liburing-dev` (for Linux).
- **Database**: A running PostgreSQL 14+ instance.

## 1. Installation

### Build from Source

```bash
git clone https://github.com/virtlabs-io/keel.git
cd keel
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

The resulting binary will be located at `build/src/main/keel`.

## 2. Configuration

KEEL uses a simple `.ini` format for configuration. Create a file named `keel-quickstart.ini`:

```ini
[keel]
config_version = 2
log_level = info

[worker_group.primary_pool]
protocol = postgres
bind_addr = 127.0.0.1
bind_port = 7432
mode = pool
prepared_statement = virtualize

# Backend Database Credentials
server_user = your_pg_user
server_password = your_pg_password

# Frontend Client Auth
auth_method = scram-sha-256 # Recommended
userlist_file = etc/userlist.txt

[worker_group.primary_pool.servers]
primary = host=127.0.0.1 port=5432 dbname=postgres role=RW weight=100
```

> **Note**: Ensure the `userlist_file` exists. You can create a simple one for testing:
> `echo '"your_pg_user" "your_pg_password"' > etc/userlist.txt`

## 3. Running KEEL

Start the proxy by pointing it to your configuration file:

```bash
./build/src/main/keel -c keel-quickstart.ini
```

You should see logs indicating that the workers have started and the backend database has been detected.

## 4. Verify the Connection

Connect to KEEL using standard PostgreSQL tools like `psql`. Note that you connect to KEEL's `bind_port` (7432), not the direct PostgreSQL port (5432).

```bash
PGPASSWORD=your_pg_password psql -h 127.0.0.1 -p 7432 -U your_pg_user postgres
```

Try running a query to confirm it works:

```sql
SELECT version();
SELECT pg_backend_pid(); -- Run this multiple times to see pool reuse in action
```

## Next Steps

Now that you have a basic pooler running, explore these topics:

- **Read/Write Splitting**: Learn how to configure Smart Routing.
- **Prepared Statements**: Understand how KEEL handles Virtualization.
- **Production Hardening**: Review the Production Readiness checklist.
- **Testing**: See how to run the full Integration Suite.

---
*For issues or questions, please visit our GitHub Discussions.*