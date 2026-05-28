# KEEL Configuration

KEEL accepts its runtime configuration in two **fully interchangeable** formats:

| Format | Extension(s)        | Detection         |
| ------ | ------------------- | ----------------- |
| INI    | `.ini` (or anything not matching YAML) | Default            |
| YAML   | `.yaml`, `.yml`     | By file extension |

The format is auto-detected from the file extension you pass to `-c`. Both
formats deserialize into the **same in-memory `keel_config_t`** — there are no
features available in one form but not the other.

```bash
keel -c /etc/keel/keel.ini      # INI form
keel -c /etc/keel/keel.yaml     # YAML form (identical config)
```

The current schema is `config_version = 2`. Files at version 1 are auto-rejected
with a hint to run `keel --migrate-config <in> -o <out>`; see
[Migration](#migration-from-v1-to-v2).

---

## Lossless conversion

`keel --convert-config <in> -o <out>` converts between the two formats. The
output format is decided by the destination extension. The conversion is
semantically lossless — round-tripping `ini → yaml → ini` (or the reverse)
produces a structurally equivalent configuration. Only cosmetic differences
(alignment, ordering, comments) may change.

```bash
keel --convert-config keel.ini  -o keel.yaml   # INI → YAML
keel --convert-config keel.yaml -o keel.ini    # YAML → INI
```

The same flag is rejected when both sides resolve to the same format, to make
mistakes obvious in scripts.

---

## Mapping rules (INI ↔ YAML)

The YAML form is a thin restructuring of the INI form. The translation rules
are:

### 1. Sections ↔ top-level mappings

Every INI `[section]` is a top-level YAML mapping with the same name.

```ini
[logging]
plugin = stdout
log_level = info
```

```yaml
logging:
  plugin: stdout
  log_level: info
```

### 2. Nested mappings flatten with `_`

YAML may group related keys under a sub-mapping; at runtime the keys are
flattened by joining parent and child names with an underscore. This is purely
cosmetic — both forms produce identical config.

```yaml
keel:
  log_level: 2
tls:
  handshake_timeout: 5s
  prefer_server_ciphers: true
```

is equivalent to:

```ini
[keel]
log_level = 2

[tls]
handshake_timeout = 5s
prefer_server_ciphers = true
```

Nesting can be arbitrarily deep; the joiner is always `_`.

### 3. Worker groups are a sequence

INI uses one `[worker_group.<name>]` section per backend group plus a
companion `[worker_group.<name>.servers]` section. YAML collapses both into a
single `worker_groups:` sequence:

```yaml
worker_groups:
  - name: default
    protocol: postgres
    bind_addr: 0.0.0.0
    bind_port: 7432
    min_pool_size: 10
    max_pool_size: 50
    servers:
      - { name: primary, host: pg-primary, port: 5432, role: RW, weight: 100 }
      - { name: replica, host: pg-replica, port: 5432, role: RO, weight: 200 }
```

is equivalent to:

```ini
[worker_group.default]
name = default
protocol = postgres
bind_addr = 0.0.0.0
bind_port = 7432
min_pool_size = 10
max_pool_size = 50

[worker_group.default.servers]
primary = host=pg-primary port=5432 role=RW weight=100
replica = host=pg-replica port=5432 role=RO weight=200
```

Each entry **must** have a `name:` key — it becomes the worker-group
identifier and the server-table key.

### 4. Environment-variable interpolation

Every scalar value supports `${VAR}` substitution at parse time:

```yaml
worker_groups:
  - name: default
    password: ${DB_PASSWORD}
```

Use `$$` for a literal dollar sign. Missing variables resolve to an empty
string (matches the INI loader's behaviour).

---

## Choosing a format

Both formats are first-class; the choice is operational, not technical.

- **INI** is recommended for small single-tenant deployments, when most
  configuration is static, and for `bash`/`grep`-based tooling.
- **YAML** is recommended for Kubernetes/Helm deployments (structured
  ConfigMaps render naturally), GitOps repositories, and configurations that
  share keys with other systems already authored in YAML.

`etc/keel.ini.example` and `etc/keel.yaml.example` ship the same canonical
PostgreSQL pool in each form.

---

## Helm

The `helm/keel` chart accepts either format:

```yaml
# values.yaml — INI form (default, raw text block)
config: |
  [keel]
  log_level = 2
  ...

# values.yaml — YAML form (structured map; auto-renders as keel.yaml)
configFormat: yaml
configYaml:
  keel:
    log_level: 2
  worker_groups:
    - name: default
      protocol: postgres
      bind_addr: 0.0.0.0
      bind_port: 7432
      servers:
        - { name: primary, host: postgres, port: 5432, role: RW, weight: 100 }
```

The chart writes the right basename into the ConfigMap (`keel.ini` or
`keel.yaml`) and passes the matching `-c /etc/keel/<basename>` to the
container. Switching between formats is a single `helm upgrade` away.

---

## Docker

The published images keep the historical default of mounting `keel.ini` at
`/etc/keel/keel.ini`. Mounting a `.yaml` file works the same way — the
entrypoint auto-detects the format. `KEEL_*` environment-variable overrides
are still expressed as INI fragments and are merged on top of the (possibly
converted) base configuration before the daemon is exec'd.

```bash
# INI (historical):
docker run -v $PWD/keel.ini:/etc/keel/keel.ini ghcr.io/virtlabs-io/keel

# YAML (new):
docker run -v $PWD/keel.yaml:/etc/keel/keel.yaml \
           -e KEEL_CONFIG=/etc/keel/keel.yaml \
           ghcr.io/virtlabs-io/keel
```

---

## Migration from v1 to v2

`config_version = 2` introduces a handful of renames (notably the OTLP
exporter keys and the dropped `scatter_merge_multiplier`) plus the explicit
version stamp. v1 files now fail to load; convert them once with:

```bash
keel --migrate-config /etc/keel/keel.ini -o /etc/keel/keel.ini.new
mv /etc/keel/keel.ini.new /etc/keel/keel.ini
```

The migrator is idempotent (it refuses to rewrite an already-v2 file) and
operates purely on text, so it can be run against unloadable files as well.
After migrating, you can optionally switch to YAML with
`keel --convert-config keel.ini -o keel.yaml`.

---

## Key reference

The full key reference lives in the heavily-commented example files:

- [`etc/keel.ini.example`](../etc/keel.ini.example) — every section and key
  documented inline (canonical INI form).
- [`etc/keel.yaml.example`](../etc/keel.yaml.example) — equivalent YAML form
  generated by `keel --convert-config`.

Both files render to the same `keel_config_t`. When in doubt about a key's
INI-side name, run:

```bash
keel --convert-config keel.yaml -o /tmp/keel.ini && diff -u etc/keel.ini.example /tmp/keel.ini
```
