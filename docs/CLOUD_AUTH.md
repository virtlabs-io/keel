# Cloud-Native Authentication

Keel supports transparent, short-lived token authentication for PostgreSQL
backends hosted on AWS, GCP, and Azure. Tokens are fetched automatically,
cached per backend pool, and refreshed before expiry — the application sees
only a normal PostgreSQL connection.

## Architecture

```
client  ─►  keel  ─►  [cloud auth provider]  ─►  backend
                 │
                 └──  token cache (per pool)
                      ├─ cached_token
                      ├─ expires_at
                      └─ refresh_margin_s
```

Each backend pool embeds a `keel_cloud_token_cache_t`.  When a new backend
connection is opened, `keel_cloud_auth_get_password()` checks the cache:

1. **Cache valid** → return cached token immediately (zero latency).
2. **Cache expired / within margin** → call `provider->ops->fetch_token()`,
   update cache, return fresh token.
3. **No provider** → return the static password from pool config.

## Providers

### AWS RDS IAM

Generates a SigV4 pre-signed URL that serves as the database password for
IAM-authenticated RDS and Aurora instances.

**Token format:** `<host>:<port>/?Action=connect&DBUser=<user>&X-Amz-Algorithm=...&X-Amz-Signature=...`

**Token lifetime:** 15 minutes (refreshed at 14 minutes).

**Configuration:**

| Field                | Type     | Description                                      |
|----------------------|----------|--------------------------------------------------|
| `region`             | string   | AWS region (e.g. `us-east-1`). **Required.**     |
| `access_key_id`      | string   | AWS access key ID. `NULL` = read from env.       |
| `secret_access_key`  | string   | AWS secret access key. `NULL` = read from env.   |
| `session_token`      | string   | STS session token. `NULL` = none.                |

**Environment variables (fallback):**

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`

**Example:**

```c
keel_cloud_aws_config_t cfg = {
    .region            = "us-west-2",
    .access_key_id     = NULL,   /* from env */
    .secret_access_key = NULL,
};
keel_cloud_auth_provider_t* prov = NULL;
keel_cloud_auth_aws_create(&prov, &cfg);
```

### GCP Cloud SQL IAM

Obtains an OAuth2 access token for IAM-authenticated Cloud SQL instances.
Three acquisition methods, tried in order:

1. **Service account JSON key file** → JWT signing → OAuth2 token exchange.
2. **GCP metadata server** (on GCE/GKE VMs) → automatic.
3. **`CLOUDSQL_ACCESS_TOKEN_FILE` or `GOOGLE_APPLICATION_CREDENTIALS` env** →
   static token read from file.

**Configuration:**

| Field                   | Type   | Description                                  |
|-------------------------|--------|----------------------------------------------|
| `service_account_file`  | string | Path to service account JSON. `NULL` = metadata/env. |

**JWT flow:** The provider parses `client_email` and `private_key` from the
JSON key file, builds a JWT with scope
`https://www.googleapis.com/auth/sqlservice.login`, signs with RS256, and
exchanges it for an access token via `https://oauth2.googleapis.com/token`.

**Example:**

```c
keel_cloud_gcp_config_t cfg = {
    .service_account_file = "/secrets/gcp-sa.json",
};
keel_cloud_auth_gcp_create(&prov, &cfg);
```

### Azure AD / Entra Managed Identity

Fetches a managed-identity token from the Azure IMDS endpoint for
Azure Database for PostgreSQL/MySQL.

**Configuration:**

| Field       | Type   | Description                                               |
|-------------|--------|-----------------------------------------------------------|
| `client_id` | string | User-assigned identity client ID. `NULL` = system identity. |
| `resource`  | string | Resource URI. Default: `https://ossrdbms-aad.database.windows.net` |

**Token source (in priority order):**

1. `AZURE_AD_TOKEN` environment variable.
2. `AZURE_AD_TOKEN_FILE` → read token from file.
3. Azure IMDS endpoint (`http://169.254.169.254/metadata/identity/oauth2/token`).

**Example:**

```c
keel_cloud_azure_config_t cfg = {
    .client_id = NULL,  /* system-assigned identity */
    .resource  = NULL,  /* default Azure DB resource */
};
keel_cloud_auth_azure_create(&prov, &cfg);
```

### Static (File / Environment Variable)

Reads a password from a file or environment variable at each refresh interval.
Useful for external credential rotators (Vault, Kubernetes secrets, etc.).

| Field                 | Type   | Description                           |
|-----------------------|--------|---------------------------------------|
| `path`                | string | File path or env var name. **Required.** |
| `refresh_interval_s`  | int    | Re-read interval in seconds. `0` = once. |

**Example (file):**

```c
keel_cloud_static_config_t cfg = {
    .path = "/run/secrets/db_password",
    .refresh_interval_s = 300,  /* re-read every 5 min */
};
keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_FILE, &cfg);
```

**Example (env var):**

```c
keel_cloud_static_config_t cfg = {
    .path = "DB_PASSWORD",
    .refresh_interval_s = 0,
};
keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);
```

## Token Cache

Every backend pool embeds a `keel_cloud_token_cache_t`:

```c
keel_cloud_token_cache_t cache;
keel_cloud_token_cache_init(&cache, provider, /*margin_s=*/60);

/* On each backend connection: */
const char* pw = keel_cloud_auth_get_password(
    &cache, "mydb.rds.amazonaws.com", 5432, "iam_user", "fallback_pw");

/* Cleanup: */
keel_cloud_token_cache_destroy(&cache);
```

The `refresh_margin_s` parameter controls how many seconds before token expiry
the cache triggers a refresh. Default is 60 seconds. Zero or negative values
are clamped to 60.

## Security Notes

- Tokens are stored in heap-allocated memory and zeroed on free.
- File-based static tokens are read into a stack buffer that is explicitly
  `memset(0)` after copying.
- No credentials are logged. Provider log messages include only type and
  non-sensitive metadata (region, client ID prefix).
- SigV4 signing uses HMAC-SHA-256 via OpenSSL EVP (no deprecated API calls).
- RSA JWT signing uses EVP_DigestSign (OpenSSL 1.1.1+).

## Build Requirements

Cloud auth requires OpenSSL. Pass `-DKEEL_HAS_OPENSSL=1` at build time
(enabled by default when OpenSSL is found by CMake).

Without OpenSSL, the AWS and GCP providers return `KEEL_ERR_AUTH` at creation
time. Static and Azure (env/file) providers work without OpenSSL.
