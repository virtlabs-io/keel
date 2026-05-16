# Cloud Auth Performance & Tuning Guide

This guide provides performance characteristics, tuning recommendations, and operational best practices for cloud-native authentication in Keel.

## Performance Characteristics

### Token Cache Efficiency

The token cache is **extremely efficient** for the common case (repeated password requests from the same backend pool):

| Scenario | Latency | Notes |
|----------|---------|-------|
| **Cache hit** (token valid) | ~0.02 µs | Pure pointer dereference + time comparison |
| **Cache miss + provider fetch** | ~1-50 ms | Depends on provider (env/file: ~1 µs, network: ~10-50 ms) |
| **Token refresh** | ~1-50 ms | Network latency (AWS SigV4, GCP OAuth2) |
| **Throughput (cached)** | ~22M ops/sec | Per cache instance on typical hardware |

### Provider-Specific Latencies

| Provider | Latency | Varies By | Notes |
|----------|---------|-----------|-------|
| **Static Env** | ~1 µs | Env var size | Reading `getenv()` |
| **Static File** | ~10-100 µs | File size, I/O cache | `fopen()` + `fread()` |
| **AWS SigV4** | ~1-5 ms | Clock skew, string size | HMAC-SHA-256 hashing |
| **GCP OAuth2** | ~50-200 ms | Network latency, JWT signing | HTTP round-trip + RSA signing |
| **Azure IMDS** | ~50-200 ms | VM metadata cache, network | HTTP to `169.254.169.254` |

### Memory Overhead

| Component | Size | Notes |
|-----------|------|-------|
| **Cache instance** | ~512 bytes | Per backend pool |
| **Provider struct (AWS)** | ~256 bytes | Region + credentials |
| **Provider struct (GCP)** | ~128 bytes | Key file path |
| **Provider struct (Azure)** | ~128 bytes | Client ID + resource URI |
| **Cached token** | ~200-1000 bytes | Depends on token size |
| **Provider-specific data** | ~1-2 KB | Keys, internal state |

**Total per pool:** ~2-3 KB for typical configurations.

---

## Tuning Recommendations

### Token Refresh Margin

The `refresh_margin_s` parameter controls when to refresh the token **before** it expires:

```c
keel_cloud_token_cache_init(&cache, provider, 60);  /* 60-second margin */
```

**Guidance:**

| Margin | Use Case | Trade-off |
|--------|----------|-----------|
| **30 seconds** | High-throughput, predictable | Small risk if clock skew > 30s |
| **60 seconds** | Recommended (default) | Balance between freshness & overhead |
| **120 seconds** | Long-lived tokens, rare refresh | More tolerant of clock skew |
| **≤ 0** | Force-clamped to 60 | Never set manually |

**Why margin exists:**
- Protects against clock skew between proxy and cloud services
- Prevents edge-case "token just expired" failures
- Reduces contention if multiple connections check expiry simultaneously

**Tuning formula:**
```
margin_s = 60 + max_clock_skew_s + (token_lifetime_s * 0.05)
```

For example:
- AWS SigV4: 15 min lifetime → `60 + 5 + 45 = 110s` → use 120s
- GCP: 1 hour lifetime → `60 + 5 + 180 = 245s` → use 300s
- Azure: 15 min lifetime → `60 + 5 + 45 = 110s` → use 120s

### Backend Pool Sizing

Each backend pool has its own token cache. Pool size affects token refresh patterns:

```
pools = max(1, backend_servers / 10)
```

**Guideline:**

| # Backends | # Pools | Token Refresh Frequency |
|-----------|--------|------------------------|
| 1-10 | 1 | Once per 15 min (AWS) |
| 11-50 | 5 | 1 refresh per 3 min (round-robin) |
| 51-100 | 10 | 1 refresh per 1.5 min |
| 100+ | backend_count / 10 | Staggered refreshes |

**Benefit:** Staggered refresh reduces burst load on cloud services.

---

## Operational Best Practices

### 1. Monitor Token Acquisition Latency

Track percentiles to detect network issues:

```
# Prometheus metric (example)
cloud_auth_token_acquisition_seconds_bucket{provider="gcp",le="0.050"}
cloud_auth_token_acquisition_seconds_bucket{provider="aws",le="0.010"}
```

**Alert thresholds:**
- AWS SigV4: p99 > 10 ms → clock skew or high CPU
- GCP OAuth2: p99 > 300 ms → network latency
- Azure IMDS: p99 > 300 ms → IMDS endpoint issues

### 2. Validate Token Format

For AWS RDS IAM, verify SigV4 token structure:

```bash
# Token format: <host>:<port>/?Action=connect&DBUser=user&X-Amz-Algorithm=...
echo "$KEEL_TOKEN" | grep -E "^[^:]+:[0-9]+/\?Action=connect"
```

### 3. Prevent Clock Skew

Cloud auth relies on accurate system time. Validate:

```bash
# Check NTP sync (all proxy nodes + cloud services)
chronyc waitsync
timedatectl status

# Maximum acceptable skew: 30 seconds
```

### 4. Handle Token Expiry Gracefully

If a token expires **before** refresh (clock skew, network latency):

```c
const char* pw = keel_cloud_auth_get_password(&cache,
    "backend.example.com", 5432, "user", "fallback_static_pw");

if (pw == NULL) {
    /* Fallback to static password if available */
    /* Log to troubleshoot token acquisition failure */
}
```

### 5. Test Provider Initialization

Validate provider configuration **at startup**, before accepting connections:

```c
keel_cloud_auth_provider_t* prov = NULL;
keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
if (err != KEEL_OK) {
    fprintf(stderr, "Cloud auth provider init failed: %d\n", err);
    exit(1);
}
```

### 6. Rotate Credentials Safely

**For static providers (file/env):**
1. Update the source (file or env var) with new credential
2. Token cache automatically picks up on next refresh
3. No proxy restart required

**For AWS SigV4:**
1. Update credentials in AWS account
2. Keel will use new credentials on next SigV4 signature
3. Session tokens expire at configured time

**For GCP/Azure:**
1. Rotate keys/identities at cloud provider
2. Keel caches access tokens (not keys)
3. Cache TTL determines when new token is fetched

---

## Troubleshooting

### High Token Acquisition Latency

**Symptom:** Connections slow, `cloud_auth_token_acquisition_seconds` p99 > expected

**Root causes & fixes:**

| Cause | Evidence | Fix |
|-------|----------|-----|
| **Network latency** | Ping latency > 100ms | Use regional endpoints, check ISP |
| **Clock skew** | AWS/GCP rejects token | `ntpdate` sync, increase margin |
| **CPU contention** | High CPU% during refresh | Reduce pool concurrency, scale workers |
| **DNS resolution** | DNS query takes >100ms | Cache DNS, use IP addresses |
| **Rate limiting** | HTTP 429 from cloud service | Stagger refreshes (more pools), backoff |

### Token Cache Not Refreshing

**Symptom:** "Token expired" errors despite cache configured

**Root causes:**

1. **Cache expiry time in past:**
   ```c
   if (cache.expires_at < time(NULL)) {
       fprintf(stderr, "Token expired %ld seconds ago\n",
           time(NULL) - cache.expires_at);
   }
   ```

2. **Margin too small:**
   ```c
   if (cache.refresh_margin_s < 30) {
       fprintf(stderr, "Warning: margin %d < recommended 60\n",
           cache.refresh_margin_s);
   }
   ```

3. **Provider fetch fails silently:**
   - Check provider-specific error logs (AWS SigV4 signature, GCP key file, etc.)
   - Verify credentials have not expired

### Memory Leaks on Token Refresh

**Symptom:** Memory growth over time during token refreshes

**Check:**
- Run with ASan: `ASAN_OPTIONS=detect_leaks=1 ./keel ...`
- Verify `keel_cloud_token_cache_destroy()` is called at pool cleanup
- Confirm provider `destroy()` is called (implementations must zero sensitive data)

---

## Performance Benchmarks

Run the included benchmarks to validate your environment:

```bash
cd /keel/bench
gcc -o bench_cloud_auth bench_cloud_auth.c \
    -I../include -I../build/include \
    ../build/src/core/libkeelcore.a \
    ../build/src/mem/libkeelmem.a \
    ../build/src/util/libkeelutil.a \
    ../build/src/util/libkeellog.a \
    ../build/src/sql/libkeelsql.a \
    -lssl -lcrypto -lpthread -lm

./bench_cloud_auth --iterations=100000 --margin=60
```

**Output interpretation:**

```
First fetch:      1.14 us (min: 1.14, max: 1.14)  ← Static provider latency
Cache hit:        0.02 us (min: 0.02, max: 1.56)  ← Cached access (expect <1 µs)
Hit rate:         99.9% (999 / 999)                ← Should be >99%
Throughput:       22165576 ops/sec                 ← Expected >10M ops/sec
```

**If throughput < 1M ops/sec or cache hit > 1 µs:**
- Check CPU frequency scaling: `cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`
- Verify no contention (run on idle system)
- Check for memory pressure (swap, OOM killer)

---

## Security Considerations

### Token Sensitive Data

- Tokens are zeroed in memory when cache is destroyed
- Fallback static passwords are **not** zeroed (application's responsibility)
- Log messages do not include token content (only metadata)

### Clock Skew Attacks

Large clock skew can enable token replay or allow expired tokens to be accepted. Enforce:

```bash
# Maximum clock skew to accept
MAX_SKEW=30  # seconds

# Validate on all proxy nodes
for node in $PROXY_NODES; do
    ssh $node "timedatectl show --property=MaximumTimeSynced"
done
```

### Credential Rotation

Always rotate credentials safely:
1. Deploy new credentials to all proxies **simultaneously**
2. Verify old credentials rejected by backend
3. Remove old credentials after grace period
4. Monitor for "auth failed" errors during transition

---

## Next Steps

- [CLOUD_AUTH.md](CLOUD_AUTH.md) — Configuration reference
- [TESTING.md](TESTING.md#cloud-auth-tests) — Integration test setup
- [docs/](../docs/) — Full documentation index
