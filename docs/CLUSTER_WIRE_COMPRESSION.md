# Cluster Wire-Protocol Compression

KEEL's multi-proxy cluster gossip protocol supports transparent payload
compression for WAN and cross-region deployments.  Both **zlib** (always
available) and **zstd** (optional, preferred for low-latency WAN links) are
supported.  Compression is disabled by default and is enabled per node with a
single INI key.

---

## Overview

The cluster gossip wire protocol connects KEEL nodes over TCP using a fixed
72-byte message header followed by a variable-length payload.  In
high-bandwidth WAN environments the gossip traffic — topology updates, config
sync payloads, election messages — can be meaningfully reduced by compressing
each message payload before transmission.

Compression is **transparent**: `send_msg()` compresses the payload when the
codec is enabled and the compressed output is smaller than the original;
`recv_msg()` detects the codec from the wire header and decompresses
automatically.  Application code and every call site are unaffected.

---

## Protocol Details

### Wire Header (72 bytes, packed)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     magic  (0x4B454C43)                       |  4 bytes
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    version    |   msg_type    |C C|   payload_len (14 bits)   |  4 bytes
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       node_id  (64 bytes, NUL-padded)         |
|                                ...                            | 64 bytes
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The `payload_len` field is a **16-bit network-order** integer whose top 2 bits
encode the compression codec and whose bottom 14 bits encode the wire (possibly
compressed) payload length:

| Bits 15–14 | Constant | Meaning |
|-----------|----------|---------|
| `00` | `KEEL_CLUSTER_WIRE_NONE` (`0x0000`) | No compression |
| `01` | `KEEL_CLUSTER_WIRE_ZLIB` (`0x4000`) | zlib gzip |
| `10` | `KEEL_CLUSTER_WIRE_ZSTD` (`0x8000`) | zstd |
| `11` | *(reserved)* | — |

The maximum encodable payload length is **16 383 bytes** (0x3FFF).  All
current cluster messages fit well within this limit.

### Protocol Version

This encoding was introduced with **protocol version 4** (`KEEL_CLUSTER_PROTO_VERSION = 4`).
Nodes on version 3 will reject version-4 headers at the `validate_header()`
call and drop the connection; always upgrade all nodes together when enabling
compression.

---

## Configuration

### INI File

```ini
[cluster]
enabled       = true
node_id       = keel-0
listen_port   = 7000
initial_peers = 10.0.0.2:7000,10.0.0.3:7000

# Wire-protocol compression (optional)
compress                = zstd   ; none | zlib | zstd  (default: none)
compress_threshold_bytes = 256   ; only compress payloads >= this size (default: 256)
```

| Key | Values | Default | Notes |
|-----|--------|---------|-------|
| `compress` | `none`, `zlib`, `zstd` | `none` | Codec for outbound messages |
| `compress_threshold_bytes` | uint32 | `256` | Payloads smaller than this are sent uncompressed regardless of codec |

### Environment Variables

```bash
KEEL_CLUSTER_COMPRESS=zstd          # none | zlib | zstd
```

Environment variables take precedence over INI values.

### Per-Node Independence

Each node configures compression independently.  A node that sends with zstd
encodes `KEEL_CLUSTER_WIRE_ZSTD` in the header; the receiving node reads the
flag and decompresses regardless of its own configured codec.  Mixed-codec
clusters are fully supported — nodes A and B can use different codecs and still
communicate correctly.

---

## Codec Selection Guide

| Codec | Library | WAN latency | CPU cost | Notes |
|-------|---------|-------------|----------|-------|
| `none` | built-in (memcpy) | — | zero | Default; best for same-datacenter |
| `zlib` | `libz` (always present) | good | moderate | RFC 1952 gzip; universal |
| `zstd` | `libzstd` (optional) | best | low | Preferred for cross-region; 3–5× faster than zlib at comparable ratios |

**Recommendation for WAN/cross-region:** use `compress = zstd` when libzstd is
available on all nodes; fall back to `compress = zlib` otherwise.

---

## Build-Time Detection

CMake detects libzstd automatically:

```cmake
find_library(ZSTD_LIBRARY NAMES zstd)
find_path(ZSTD_INCLUDE_DIR NAMES zstd.h)
```

If found, `KEEL_HAS_ZSTD=1` is defined as a compile-time feature flag (also
reflected in `cmake/keel_config.h.in`).  The `keel_compress_zstd()` family of
functions is compiled to full implementations; when absent they return `-1`
immediately so that the codec gracefully falls back to uncompressed.

Check at runtime:

```c
#if KEEL_HAS_ZSTD
    // zstd available
#endif
```

### Docker / Container Images

The official KEEL Dockerfiles install the required packages:

| Stage | Package |
|-------|---------|
| `builder` | `libzstd-dev`, `zlib1g-dev` |
| `runner`  | `libzstd1`, `zlib1g` |

Both `Dockerfile` (production) and `docker/Dockerfile.linux` (dev environment)
have been updated accordingly.

---

## Implementation Reference

### Public API (`include/keel/core/compress.h`)

```c
/* Codec selection */
typedef enum keel_compress_codec {
    KEEL_COMPRESS_NONE = 0,
    KEEL_COMPRESS_GZIP,
    KEEL_COMPRESS_ZSTD,   /* only functional when KEEL_HAS_ZSTD=1 */
} keel_compress_codec_t;

/* Unified multi-codec API */
ssize_t keel_compress(keel_compress_codec_t codec,
                      const void *src, size_t src_len,
                      void *dst, size_t dst_len);

ssize_t keel_decompress(keel_compress_codec_t codec,
                        const void *src, size_t src_len,
                        void *dst, size_t dst_len);

size_t  keel_compress_bound(keel_compress_codec_t codec, size_t src_len);
```

### Cluster Config API (`include/keel/core/cluster.h`)

```c
/* Compact codec IDs stored in keel_cluster_config_t.compress_codec */
#define KEEL_CLUSTER_COMPRESS_NONE  ((uint8_t)0)
#define KEEL_CLUSTER_COMPRESS_ZLIB  ((uint8_t)1)
#define KEEL_CLUSTER_COMPRESS_ZSTD  ((uint8_t)2)

/* Wire-encoding flags (top 2 bits of payload_len) */
#define KEEL_CLUSTER_COMPRESS_MASK    UINT16_C(0xC000)
#define KEEL_CLUSTER_PAYLOAD_LEN_MASK UINT16_C(0x3FFF)
#define KEEL_CLUSTER_MAX_PAYLOAD      UINT16_C(0x3FFF)
#define KEEL_CLUSTER_WIRE_NONE        UINT16_C(0x0000)
#define KEEL_CLUSTER_WIRE_ZLIB        UINT16_C(0x4000)
#define KEEL_CLUSTER_WIRE_ZSTD        UINT16_C(0x8000)

/* Config fields added to keel_cluster_config_t */
uint8_t  compress_codec;            /* KEEL_CLUSTER_COMPRESS_NONE/ZLIB/ZSTD */
uint32_t compress_threshold_bytes;  /* Only compress payloads >= this size   */

/* Accessor */
const keel_cluster_config_t* keel_cluster_get_config(const keel_cluster_t* cluster);
```

### Internal Flow (`src/core/cluster.c`)

```
send_msg(fd, msg_type, payload, len, node_id, codec)
  │
  ├─ if codec != NONE && len >= threshold
  │    compress(payload → cbuf)
  │    if cbuf_len < len:
  │      wire_payload = cbuf, codec_flag = WIRE_ZLIB/ZSTD
  │    else:
  │      wire_payload = payload, codec_flag = WIRE_NONE   ← graceful fallback
  │
  ├─ encode_header(hdr, msg_type, wire_len,
  │                (wire_len & 0x3FFF) | codec_flag)
  └─ send(hdr + wire_payload)

recv_msg(fd, hdr, payload, cap, timeout)
  │
  ├─ recv header, validate_header()
  ├─ raw_field = ntohs(hdr->payload_len)
  ├─ codec_flag = raw_field & 0xC000
  ├─ wire_len  = raw_field & 0x3FFF
  ├─ recv wire_len bytes
  │
  ├─ if codec_flag == WIRE_NONE:
  │    memcpy → payload
  └─ else:
       decompress(wire_buf → payload)
       hdr->payload_len = decompressed_len  ← callers see plain length
```

---

## Testing

Fourteen dedicated tests in `tests/test_cluster_election.c` cover the
compression subsystem:

| Test | What it verifies |
|------|-----------------|
| `test_compress_constants` | Bitmask values and round-trip encoding of wire flags |
| `test_compress_config_default` | Default config: codec=NONE, threshold=256 |
| `test_compress_config_zlib` | ZLIB codec stored and read back via `keel_cluster_get_config()` |
| `test_compress_config_zstd` | ZSTD codec stored and read back |
| `test_compress_bound` | `keel_compress_bound()` ≥ src_len for all codecs |
| `test_compress_none_passthrough` | NONE codec is identity (memcpy semantics) |
| `test_compress_roundtrip_gzip` | Compress + decompress with gzip yields original data |
| `test_compress_roundtrip_zstd` | Compress + decompress with zstd yields original data (SKIP if !KEEL_HAS_ZSTD) |
| `test_compress_corrupt_input` | Corrupt compressed data → `-1`, no crash |
| `test_compress_dst_too_small` | 1-byte output buffer → `-1`, no crash |
| `test_compress_null_inputs` | NULL src/dst pointers → `-1`, no crash |
| `test_compress_e2e_zlib` | Two-node cluster with ZLIB compression: full gossip round-trip |
| `test_compress_e2e_zstd` | Two-node cluster with ZSTD compression (SKIP if !KEEL_HAS_ZSTD) |
| `test_compress_asymmetric_none_vs_zstd` | Mixed codecs: node A=NONE, node B=ZSTD — both directions succeed |

Run them with:

```bash
./build/tests/test_cluster_election
# === Results: 51 passed, 0 failed, 51 total ===
```

---

## Frequently Asked Questions

**Can I enable compression on only one node in the cluster?**
Yes.  Each node configures its outbound compression independently.  A node with
`compress = none` sends uncompressed; it still decompresses messages it
receives that were compressed by a peer.

**What happens if zstd is configured but the library is missing?**
`keel_compress_zstd()` returns `-1` immediately.  `send_msg()` treats any
compression failure as "not beneficial" and falls back to sending the payload
uncompressed with `KEEL_CLUSTER_WIRE_NONE`.  The peer receives and handles it
correctly.  A `KEEL_HAS_ZSTD=0` build will always take this path.

**Does compression affect the cluster protocol ABI?**
The header format is unchanged (still 72 bytes).  The codec is encoded in the
top 2 bits of the existing `payload_len` field — bits that were previously
zero-filled.  Protocol version was bumped from 3 to 4; nodes on different
versions reject each other's messages at `validate_header()`.

**Is there a performance overhead with `compress = none`?**
Zero.  `KEEL_COMPRESS_NONE` in `keel_compress()` is a plain `memcpy` and is
skipped entirely in `send_msg()` when `codec == KEEL_COMPRESS_NONE`.

**What is the maximum payload size?**
16 383 bytes (14-bit length field).  All current cluster messages are well
within this limit.  Messages at or above this limit are rejected with `-1` from
`send_msg()`.
