/**
 * @file test_ssv_atom.c
 * @brief Unit tests for SSV consistency atoms and helpers.
 *
 * The SSV (Semantic State Virtualization) consistency subsystem
 * tracks per-session replication state — write-LSN, timestamps,
 * and staleness windows — so the router can steer reads to
 * replicas that are "caught up" to the client's causal snapshot.
 *
 * Test families:
 *   §1 — Atom init / clear: keel_ssv_consistency_init() zeros
 *         values while preserving structural metadata (domain,
 *         key, virt_class, cost_class).
 *   §2 — Token set / get: round-trip a consistency_token through
 *         the atom array; verify LSN string, timestamp, and
 *         has_write_lsn predicate.
 *   §3 — No-token state: freshly-initialised atoms report no
 *         write LSN and return an empty string.
 *   §4 — Unknown state: exercise any fallback for unrecognised
 *         consistency keys.
 *   §5 — TTL window: keel_ssv_consistency_within_ttl() returns
 *         true when the token age is below the staleness budget
 *         and false after the budget expires.
 *   §6 — Primary routing: keel_ssv_requires_primary() gates
 *         on the consistency token presence.
 *   §7 — Discard predicate: keel_ssv_needs_discard() determines
 *         whether a backend reassignment requires DISCARD ALL.
 *   §8 — Opaque atoms: init and legacy wrappers for the opaque
 *         storage path.
 *   §9 — Config atoms: init, set/get, clear for the per-session
 *         configuration consistency domain.
 */


#include "test_utils.h"

#include "keel/session/ssv_atom.h"
#include "keel/session/ssv.h"

/* ---------- atom init / clear ---------- */

static void test_atom_init(void)
{
    TEST_BEGIN("consistency atom init zeroes all");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    for (int i = 0; i < KEEL_SSV_CK__COUNT; i++) {
        TEST_ASSERT_EQ(atoms[i].domain, KEEL_SSV_DOMAIN_CONSISTENCY);
        TEST_ASSERT_EQ(atoms[i].value.u64, (uint64_t)0);
    }
    /* Per-atom class assignments */
    TEST_ASSERT_EQ(atoms[0].virt_class, KEEL_SSV_VIRT_CONDITIONAL);
    TEST_ASSERT_EQ(atoms[0].cost_class, KEEL_SSV_COST_EXPENSIVE);
    TEST_ASSERT_EQ(atoms[1].virt_class, KEEL_SSV_VIRT_FULL);
    TEST_ASSERT_EQ(atoms[1].cost_class, KEEL_SSV_COST_CHEAP);
    TEST_ASSERT_EQ(atoms[0].key, (uint16_t)KEEL_SSV_CK_WRITE_LSN);
    TEST_ASSERT_EQ(atoms[1].key, (uint16_t)KEEL_SSV_CK_WRITE_LSN_TS);

    TEST_END();
}

static void test_atom_clear(void)
{
    TEST_BEGIN("consistency atom clear resets values");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    /* Set some state, then clear. */
    atoms[0].value.u64 = 42;
    atoms[1].value.u64 = 999;

    keel_ssv_consistency_clear(atoms);

    TEST_ASSERT_EQ(atoms[0].value.u64, (uint64_t)0);
    TEST_ASSERT_EQ(atoms[1].value.u64, (uint64_t)0);
    /* Structural fields should remain. */
    TEST_ASSERT_EQ(atoms[0].domain, KEEL_SSV_DOMAIN_CONSISTENCY);
    TEST_ASSERT_EQ(atoms[0].key, (uint16_t)KEEL_SSV_CK_WRITE_LSN);

    TEST_END();
}

/* ---------- set / get token ---------- */

static void test_set_get_token(void)
{
    TEST_BEGIN("set/get consistency token");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    keel_consistency_token_t tok;
    memset(&tok, 0, sizeof(tok));
    snprintf(tok.value, sizeof(tok.value), "0/16B3748");
    tok.captured_at_ns = 123456789ULL;

    keel_ssv_consistency_set_token(atoms, &tok);

    /* LSN string should be stored. */
    const char *lsn = keel_ssv_consistency_get_lsn(atoms);
    TEST_ASSERT_NOT_NULL(lsn);
    TEST_ASSERT_STR_EQ(lsn, "0/16B3748");

    /* Timestamp should match. */
    uint64_t ts = keel_ssv_consistency_get_ts(atoms);
    TEST_ASSERT_EQ(ts, 123456789ULL);

    /* has_write_lsn should be true. */
    TEST_ASSERT(keel_ssv_consistency_has_write_lsn(atoms));

    TEST_END();
}

static void test_no_token(void)
{
    TEST_BEGIN("no token => no write LSN");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    TEST_ASSERT(!keel_ssv_consistency_has_write_lsn(atoms));

    const char *lsn = keel_ssv_consistency_get_lsn(atoms);
    TEST_ASSERT_EQ(lsn[0], '\0');

    TEST_END();
}

/* ---------- unknown state ---------- */

static void test_unknown_state(void)
{
    TEST_BEGIN("opaque unknown state flag");

    keel_ssv_atom_t opaque[KEEL_SSV_OK__COUNT];
    keel_ssv_opaque_init(opaque);

    TEST_ASSERT(!keel_ssv_opaque_has_unknown(opaque));

    keel_ssv_opaque_set_unknown(opaque);
    TEST_ASSERT(keel_ssv_opaque_has_unknown(opaque));

    /* Clear resets it. */
    keel_ssv_opaque_clear(opaque);
    TEST_ASSERT(!keel_ssv_opaque_has_unknown(opaque));

    TEST_END();
}

/* ---------- TTL ---------- */

static void test_ttl_within(void)
{
    TEST_BEGIN("TTL within window");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    keel_consistency_token_t tok;
    memset(&tok, 0, sizeof(tok));
    snprintf(tok.value, sizeof(tok.value), "0/1");
    tok.captured_at_ns = 1000000000ULL; /* 1s */

    keel_ssv_consistency_set_token(atoms, &tok);

    /* now = 1.05s, ttl = 100ms => within window => replica NOT ok (need primary) */
    uint64_t now = 1050000000ULL;
    TEST_ASSERT(!keel_ssv_consistency_ttl_ok(atoms, now, 100));

    TEST_END();
}

static void test_ttl_expired(void)
{
    TEST_BEGIN("TTL expired");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    keel_consistency_token_t tok;
    memset(&tok, 0, sizeof(tok));
    snprintf(tok.value, sizeof(tok.value), "0/1");
    tok.captured_at_ns = 1000000000ULL; /* 1s */

    keel_ssv_consistency_set_token(atoms, &tok);

    /* now = 1.2s, ttl = 100ms => expired => replica IS ok */
    uint64_t now = 1200000000ULL;
    TEST_ASSERT(keel_ssv_consistency_ttl_ok(atoms, now, 100));

    TEST_END();
}

static void test_ttl_no_token(void)
{
    TEST_BEGIN("TTL with no write timestamp => true");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    uint64_t now = 1000000000ULL;
    /* No write => replica is fine (ttl_ok returns true) */
    TEST_ASSERT(keel_ssv_consistency_ttl_ok(atoms, now, 100));

    TEST_END();
}

static void test_ttl_timestamp_only(void)
{
    TEST_BEGIN("TTL timestamp-only sticky fallback");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    uint64_t write_ns = 1000000000ULL;
    keel_ssv_consistency_set_write_ts(atoms, write_ns);

    TEST_ASSERT(!keel_ssv_consistency_has_write_lsn(atoms));
    TEST_ASSERT(!keel_ssv_consistency_ttl_ok(atoms,
                                             write_ns + 50000000ULL,
                                             100));
    TEST_ASSERT(keel_ssv_consistency_ttl_ok(atoms,
                                            write_ns + 200000000ULL,
                                            100));

    TEST_END();
}

/* ---------- SSV helper wrappers ---------- */

static void test_ssv_requires_primary(void)
{
    TEST_BEGIN("ssv requires_primary");

    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT];
    keel_ssv_consistency_init(atoms);

    /* No write => no primary required. */
    uint64_t now = 1000000000ULL;
    TEST_ASSERT(!keel_ssv_requires_primary(atoms, now, 100));

    /* Set a recent write. */
    keel_consistency_token_t tok;
    memset(&tok, 0, sizeof(tok));
    snprintf(tok.value, sizeof(tok.value), "0/ABC");
    tok.captured_at_ns = now - 50000000ULL; /* 50ms ago */
    keel_ssv_consistency_set_token(atoms, &tok);

    TEST_ASSERT(keel_ssv_requires_primary(atoms, now, 100));

    /* Advance time past TTL. Exact tokens do not expire without a
     * reactor-owned replica catch-up proof. */
    uint64_t later = now + 200000000ULL; /* 200ms later */
    TEST_ASSERT(keel_ssv_requires_primary(atoms, later, 100));

    TEST_END();
}

static void test_ssv_needs_discard(void)
{
    TEST_BEGIN("ssv needs_discard with opaque atoms");

    keel_ssv_atom_t opaque[KEEL_SSV_OK__COUNT];
    keel_ssv_opaque_init(opaque);

    TEST_ASSERT(!keel_ssv_needs_discard(opaque));

    keel_ssv_opaque_set_unknown(opaque);
    TEST_ASSERT(keel_ssv_needs_discard(opaque));

    TEST_END();
}

/* ---------- opaque domain ---------- */

static void test_opaque_init(void)
{
    TEST_BEGIN("opaque atom init");

    keel_ssv_atom_t opaque[KEEL_SSV_OK__COUNT];
    keel_ssv_opaque_init(opaque);

    TEST_ASSERT_EQ(opaque[0].domain, KEEL_SSV_DOMAIN_OPAQUE);
    TEST_ASSERT_EQ(opaque[0].virt_class, KEEL_SSV_VIRT_NONE);
    TEST_ASSERT_EQ(opaque[0].cost_class, KEEL_SSV_COST_CHEAP);
    TEST_ASSERT_EQ(opaque[0].key, (uint16_t)KEEL_SSV_OK_UNKNOWN_STATE);
    TEST_ASSERT_EQ(opaque[0].value.flag, false);

    TEST_END();
}

static void test_opaque_legacy_wrappers(void)
{
    TEST_BEGIN("opaque legacy wrappers");

    keel_ssv_atom_t opaque[KEEL_SSV_OK__COUNT];
    keel_ssv_opaque_init(opaque);

    /* Legacy wrappers should forward to opaque functions. */
    TEST_ASSERT(!keel_ssv_consistency_has_unknown(opaque));

    keel_ssv_consistency_set_unknown(opaque);
    TEST_ASSERT(keel_ssv_consistency_has_unknown(opaque));
    TEST_ASSERT(keel_ssv_opaque_has_unknown(opaque));

    keel_ssv_opaque_clear(opaque);
    TEST_ASSERT(!keel_ssv_consistency_has_unknown(opaque));

    TEST_END();
}

/* ---------- config domain ---------- */

static void test_config_init(void)
{
    TEST_BEGIN("config atom init");

    keel_ssv_atom_t atoms[KEEL_SSV_CFG__COUNT];
    keel_ssv_config_init(atoms);

    TEST_ASSERT_EQ(atoms[0].domain, KEEL_SSV_DOMAIN_CONFIG);
    TEST_ASSERT_EQ(atoms[0].virt_class, KEEL_SSV_VIRT_FULL);
    TEST_ASSERT_EQ(atoms[0].cost_class, KEEL_SSV_COST_MODERATE);
    TEST_ASSERT_EQ(atoms[0].key, (uint16_t)KEEL_SSV_CFG_PROFILE_HASH);
    TEST_ASSERT_EQ(keel_ssv_config_get_profile_hash(atoms), (uint64_t)0);

    TEST_END();
}

static void test_config_set_get(void)
{
    TEST_BEGIN("config atom set/get profile hash");

    keel_ssv_atom_t atoms[KEEL_SSV_CFG__COUNT];
    keel_ssv_config_init(atoms);

    keel_ssv_config_set_profile_hash(atoms, 0xDEADBEEFCAFE0001ULL);
    TEST_ASSERT_EQ(keel_ssv_config_get_profile_hash(atoms), 0xDEADBEEFCAFE0001ULL);

    keel_ssv_config_set_profile_hash(atoms, 0);
    TEST_ASSERT_EQ(keel_ssv_config_get_profile_hash(atoms), (uint64_t)0);

    TEST_END();
}

static void test_config_clear(void)
{
    TEST_BEGIN("config atom clear resets hash");

    keel_ssv_atom_t atoms[KEEL_SSV_CFG__COUNT];
    keel_ssv_config_init(atoms);

    keel_ssv_config_set_profile_hash(atoms, 12345ULL);
    TEST_ASSERT_EQ(keel_ssv_config_get_profile_hash(atoms), 12345ULL);

    keel_ssv_config_clear(atoms);
    TEST_ASSERT_EQ(keel_ssv_config_get_profile_hash(atoms), (uint64_t)0);
    /* Structural fields preserved after clear. */
    TEST_ASSERT_EQ(atoms[0].domain, KEEL_SSV_DOMAIN_CONFIG);

    TEST_END();
}

/* ---------- main ---------- */

int main(void)
{
    test_atom_init();
    test_atom_clear();
    test_set_get_token();
    test_no_token();
    test_unknown_state();
    test_ttl_within();
    test_ttl_expired();
    test_ttl_no_token();
    test_ttl_timestamp_only();
    test_ssv_requires_primary();
    test_ssv_needs_discard();
    test_opaque_init();
    test_opaque_legacy_wrappers();
    test_config_init();
    test_config_set_get();
    test_config_clear();
    return test_summary();
}
