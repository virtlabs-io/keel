/**
 * @file keel_prom_format.c
 * @brief Implementation of the Prometheus compatibility formatter (§23.2).
 */

#include "keel_prom_format.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

/* ============================================================================
 * Metric metadata table — shared with the OTLP converter.
 * Order does not need to match the snapshot; lookup is by name.
 * ============================================================================ */

typedef struct prom_meta {
    const char* name;
    const char* help;
} prom_meta_t;

static const prom_meta_t k_meta[] = {
    /* Sessions */
    { "keel_sessions_created_total",   "Total frontend sessions created"                 },
    { "keel_sessions_closed_total",    "Total frontend sessions closed"                  },
    { "keel_sessions_active",          "Currently active frontend sessions"              },
    { "keel_sessions_pinned",          "Sessions with any active pin reason"             },

    /* Queries */
    { "keel_queries_total",            "Total queries routed"                            },
    { "keel_queries_read_total",       "Read-only queries routed"                        },
    { "keel_queries_write_total",      "Write queries routed"                            },
    { "keel_queries_tx_total",         "Explicit transactions routed"                    },

    /* Errors */
    { "keel_errors_total",             "Total errors observed"                           },
    { "keel_errors_auth_total",        "Authentication errors"                           },
    { "keel_errors_proto_total",       "Protocol errors"                                 },
    { "keel_errors_backend_total",     "Backend errors"                                  },
    { "keel_errors_timeout_total",     "Timeout errors"                                  },

    /* Bytes */
    { "keel_bytes_recv_total",         "Bytes received from frontends"                   },
    { "keel_bytes_sent_total",         "Bytes sent to frontends"                         },
    { "keel_bytes_backend_recv_total", "Bytes received from backends"                    },
    { "keel_bytes_backend_sent_total", "Bytes sent to backends"                          },
    { "keel_bytes_spliced_total",      "Bytes moved via zero-copy splice"                },

    /* Pool */
    { "keel_pool_borrows_total",       "Backend connections borrowed from the pool"      },
    { "keel_pool_returns_total",       "Backend connections returned to the pool"        },
    { "keel_pool_creates_total",       "Backend connections opened"                      },
    { "keel_pool_destroys_total",      "Backend connections destroyed"                   },
    { "keel_pool_hits_total",          "Pool borrows satisfied by an idle backend"       },
    { "keel_pool_misses_total",        "Pool borrows that required opening a new backend"},

    /* Reactor */
    { "keel_loop_iterations_total",    "Reactor loop iterations"                         },
    { "keel_ops_submitted_total",      "io_uring SQEs submitted"                         },
    { "keel_ops_completed_total",      "io_uring CQEs reaped"                            },

    /* Multiplexing safety */
    { "keel_discard_all_total",        "Full backend cleanup commands issued"            },
    { "keel_state_sync_total",         "Session-state sync replays issued"               },
    { "keel_backends_cleaning",        "Backends currently in cleanup state machine"     },

    /* Reason-coded backend close — single-writer point in
       src/worker/backend_pool.c (proposal §28 R1). */
    { "keel_backend_close_dead_idle_total",
      "Idle backends closed after liveness check failure"                                },
    { "keel_backend_close_cleanup_error_total",
      "Backends closed after cleanup/protocol error"                                     },
    { "keel_backend_close_cleanup_timeout_total",
      "Backends closed after cleanup timeout"                                            },
    { "keel_backend_close_client_disconnect_total",
      "Backends closed because owning client disconnected"                               },
    { "keel_backend_close_io_error_total",
      "Backends closed after socket-level I/O error"                                     },
    { "keel_backend_close_prune_idle_total",
      "Idle backends pruned by pool size policy"                                         },
    { "keel_backend_close_prune_aged_total",
      "Backends pruned for exceeding max-age"                                            },
    { "keel_backend_close_drain_idle_total",
      "Idle backends closed during pool drain"                                           },
    { "keel_backend_close_backend_eof_total",
      "Backends closed on unexpected EOF/RST outside cleanup"                            },
    { "keel_backend_close_connect_failed_total",
      "Backends closed after connect/handshake socket failure"                           },
    { "keel_backend_close_auth_failed_total",
      "Backends closed after authentication denial"                                      },
    { "keel_backend_close_protocol_error_total",
      "Backends closed after steady-state protocol violation"                            },
    { "keel_backend_close_sync_error_total",
      "Backends closed after extended-protocol Sync mismatch"                            },
    { "keel_backend_close_stmt_replay_error_total",
      "Backends closed after prepared-statement replay failure"                          },
    { "keel_backend_close_shutdown_total",
      "Backends closed during process shutdown"                                          },
    { "keel_backend_close_pool_eviction_total",
      "Backends closed due to pool resize/policy eviction"                               },

    /* Commit-in-doubt resolution — single-writer point in
       engine_flow.c / state_machine.c / session.c (proposal §28 R2). */
    { "keel_commit_in_doubt_started_total",
      "Commit-in-doubt recovery sessions started"                                        },
    { "keel_commit_in_doubt_resolved_total",
      "Commit-in-doubt recovery sessions resolved"                                       },
    { "keel_commit_in_doubt_failed_total",
      "Commit-in-doubt recovery sessions that could not resolve"                         },
    { "keel_sessions_commit_in_doubt",
      "Sessions currently resolving commit outcome"                                      },

    /* Process meta */
    { "keel_uptime_seconds",           "Process uptime in seconds"                       },
    { "keel_workers",                  "Configured worker count"                         },
};

static const char* lookup_help(const char* name) {
    for (size_t i = 0; i < sizeof(k_meta) / sizeof(k_meta[0]); i++) {
        if (strcmp(name, k_meta[i].name) == 0) return k_meta[i].help;
    }
    return NULL;
}

/* Counter convention: Prometheus cumulative counters carry a `_total`
 * suffix. The OTLP converter already names KEEL counters with that suffix,
 * so we use the suffix as the discriminator. */
static int is_counter(const char* name) {
    size_t n = strlen(name);
    return n >= 6 && memcmp(name + n - 6, "_total", 6) == 0;
}

/* ============================================================================
 * Formatter
 * ============================================================================ */

int keel_prom_format_snapshot(const keel_otlp_snapshot_t* snap,
                              char* out,
                              size_t out_cap)
{
    if (!snap || !out || out_cap == 0) return -1;

    size_t off = 0;
    for (size_t i = 0; i < snap->metric_count; i++) {
        const keel_otlp_metric_sample_t* m = &snap->metrics[i];
        const char* help = lookup_help(m->name);
        const char* type = is_counter(m->name) ? "counter" : "gauge";

        int n = snprintf(out + off, out_cap - off,
                         "# HELP %s %s\n# TYPE %s %s\n%s %" PRIu64 "\n",
                         m->name, help ? help : m->name,
                         m->name, type,
                         m->name, m->value);
        if (n < 0) return -1;
        if ((size_t)n >= out_cap - off) return -2;
        off += (size_t)n;
    }

    if (off >= out_cap) return -2;
    out[off] = '\0';
    return (int)off;
}
