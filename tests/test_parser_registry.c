/**
 * @file test_parser_registry.c
 * @brief Parser registry, semantic-plan, and builtin PostgreSQL parser tests.
 */

#include "test_utils.h"
#include "keel/parser/parser.h"
#include "keel/mem/mem.h"

#include <string.h>

int g_tests_run = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) { return g_tests_failed ? 1 : 0; }

static int dummy_init_calls = 0;
static int dummy_shutdown_calls = 0;

static int dummy_init(const void* config)
{
    (void)config;
    dummy_init_calls++;
    return 0;
}

static keel_parse_status_t dummy_parse(const keel_parse_input_t* input,
                                       keel_parse_result_t* result)
{
    if (!input || !result) return KEEL_PARSE_INTERNAL_ERROR;
    keel_parse_result_init(result);
    result->status = KEEL_PARSE_OK;
    keel_semantic_plan_init(&result->plan, input->language, input->dialect);
    result->plan.semantic_class = KEEL_SEM_READ_ONLY;
    result->plan.safety = KEEL_SAFETY_SAFE_REPLICA;
    result->plan.is_read_only = true;
    result->plan.safe_for_replica = true;
    result->plan.requires_primary = false;
    result->plan.parser_confident = true;
    snprintf(result->plan.reason, sizeof result->plan.reason, "dummy readonly");
    return result->status;
}

static void dummy_shutdown(void)
{
    dummy_shutdown_calls++;
}

static bool dummy_supports(const char* feature)
{
    return feature && strcmp(feature, "semantic_plan") == 0;
}

static const keel_parser_plugin_ops_t dummy_ops = {
    .name = "sql.dummy_readonly",
    .version = "test",
    .language = KEEL_LANG_SQL,
    .dialect = KEEL_DIALECT_SQL_ANSI,
    .init = dummy_init,
    .parse = dummy_parse,
    .free_result = NULL,
    .shutdown = dummy_shutdown,
    .supports_feature = dummy_supports,
};

static keel_parse_input_t pg_input(const char* sql)
{
    return (keel_parse_input_t){
        .data = (const uint8_t*)sql,
        .len = strlen(sql),
        .language = KEEL_LANG_SQL,
        .dialect = KEEL_DIALECT_SQL_POSTGRESQL,
    };
}

static void test_register_lookup_builtin(void)
{
    TEST_BEGIN("parser_registry_register_lookup_builtin");
    keel_parser_registry_t* reg = keel_parser_registry_create();
    TEST_ASSERT_NOT_NULL(reg);
    TEST_ASSERT_EQ(keel_parser_registry_register_builtins(reg), KEEL_OK);
    const keel_parser_plugin_ops_t* ops =
        keel_parser_registry_lookup(reg, "sql.postgresql");
    TEST_ASSERT_NOT_NULL(ops);
    TEST_ASSERT_STR_EQ(ops->name, "sql.postgresql");
    TEST_ASSERT(ops->supports_feature("semantic_plan"));
    keel_parser_registry_destroy(reg);
    TEST_END();
}

static void test_duplicate_rejected(void)
{
    TEST_BEGIN("parser_registry_duplicate_rejected");
    keel_parser_registry_t* reg = keel_parser_registry_create();
    TEST_ASSERT_NOT_NULL(reg);
    TEST_ASSERT_EQ(keel_parser_registry_register(reg, &dummy_ops, true), KEEL_OK);
    TEST_ASSERT_EQ(keel_parser_registry_register(reg, &dummy_ops, true),
                   KEEL_ERR_ALREADY_EXISTS);
    keel_parser_registry_destroy(reg);
    TEST_END();
}

static void test_disabled_lookup_and_parse_rejected(void)
{
    TEST_BEGIN("parser_registry_disabled_rejected");
    keel_parser_registry_t* reg = keel_parser_registry_create();
    TEST_ASSERT_NOT_NULL(reg);
    TEST_ASSERT_EQ(keel_parser_registry_register(reg, &dummy_ops, false), KEEL_OK);
    TEST_ASSERT_NULL(keel_parser_registry_lookup(reg, "sql.dummy_readonly"));

    keel_parse_result_t result;
    keel_parse_input_t input = {
        .data = (const uint8_t*)"SELECT 1",
        .len = 8,
        .language = KEEL_LANG_SQL,
        .dialect = KEEL_DIALECT_SQL_ANSI,
    };
    TEST_ASSERT_EQ(keel_parser_registry_parse(
                       reg, "sql.dummy_readonly", &input, &result),
                   KEEL_PARSE_UNSUPPORTED);
    keel_parse_result_free(NULL, &result);
    keel_parser_registry_destroy(reg);
    TEST_END();
}

static void test_enable_initializes_and_shutdown_called(void)
{
    TEST_BEGIN("parser_registry_enable_lifecycle");
    dummy_init_calls = 0;
    dummy_shutdown_calls = 0;

    keel_parser_registry_t* reg = keel_parser_registry_create();
    TEST_ASSERT_NOT_NULL(reg);
    TEST_ASSERT_EQ(keel_parser_registry_register(reg, &dummy_ops, false), KEEL_OK);
    TEST_ASSERT_EQ(dummy_init_calls, 0);
    TEST_ASSERT_EQ(keel_parser_registry_set_enabled(
                       reg, "sql.dummy_readonly", true),
                   KEEL_OK);
    TEST_ASSERT_EQ(dummy_init_calls, 1);
    TEST_ASSERT_NOT_NULL(keel_parser_registry_lookup(reg, "sql.dummy_readonly"));
    keel_parser_registry_destroy(reg);
    TEST_ASSERT_EQ(dummy_shutdown_calls, 1);
    TEST_END();
}

static void test_postgresql_semantic_read_write_ddl(void)
{
    TEST_BEGIN("postgresql_semantic_read_write_ddl");
    const keel_parser_plugin_ops_t* ops = keel_parser_builtin_postgresql_sql();
    keel_parse_result_t result;

    keel_parse_input_t sel = pg_input("SELECT id FROM users WHERE id = 1");
    TEST_ASSERT_EQ(ops->parse(&sel, &result), KEEL_PARSE_OK);
    TEST_ASSERT_EQ(result.plan.semantic_class, KEEL_SEM_READ_ONLY);
    TEST_ASSERT_EQ(result.plan.safety, KEEL_SAFETY_SAFE_REPLICA);
    TEST_ASSERT(result.plan.safe_for_replica);
    TEST_ASSERT(keel_semantic_plan_valid(&result.plan));
    keel_parse_result_free(ops, &result);

    keel_parse_input_t ins = pg_input("INSERT INTO users(id) VALUES (1)");
    TEST_ASSERT_EQ(ops->parse(&ins, &result), KEEL_PARSE_OK);
    TEST_ASSERT_EQ(result.plan.semantic_class, KEEL_SEM_WRITE);
    TEST_ASSERT(result.plan.may_write);
    TEST_ASSERT(result.plan.requires_primary);
    TEST_ASSERT(!result.plan.safe_for_replica);
    TEST_ASSERT(keel_semantic_plan_valid(&result.plan));
    keel_parse_result_free(ops, &result);

    keel_parse_input_t ddl = pg_input("CREATE TABLE t(id int)");
    TEST_ASSERT_EQ(ops->parse(&ddl, &result), KEEL_PARSE_OK);
    TEST_ASSERT_EQ(result.plan.semantic_class, KEEL_SEM_DDL);
    TEST_ASSERT(result.plan.changes_schema);
    TEST_ASSERT(result.plan.requires_pinned_backend);
    TEST_ASSERT(keel_semantic_plan_valid(&result.plan));
    keel_parse_result_free(ops, &result);
    TEST_END();
}

static void test_postgresql_function_fails_to_primary(void)
{
    TEST_BEGIN("postgresql_function_fails_to_primary");
    const keel_parser_plugin_ops_t* ops = keel_parser_builtin_postgresql_sql();
    keel_parse_result_t result;
    keel_parse_input_t input = pg_input("SELECT refresh_customer_score(10)");
    TEST_ASSERT_EQ(ops->parse(&input, &result), KEEL_PARSE_OK);
    TEST_ASSERT(result.plan.calls_function);
    TEST_ASSERT(result.plan.external_side_effects_possible);
    TEST_ASSERT(result.plan.requires_primary);
    TEST_ASSERT(!result.plan.safe_for_replica);
    TEST_ASSERT_EQ(result.plan.safety, KEEL_SAFETY_PRIMARY_REQUIRED);
    keel_parse_result_free(ops, &result);
    TEST_END();
}

static void test_postgresql_session_and_txn_pin(void)
{
    TEST_BEGIN("postgresql_session_and_txn_pin");
    const keel_parser_plugin_ops_t* ops = keel_parser_builtin_postgresql_sql();
    keel_parse_result_t result;

    keel_parse_input_t set = pg_input("SET search_path = tenant_42, public");
    TEST_ASSERT_EQ(ops->parse(&set, &result), KEEL_PARSE_OK);
    TEST_ASSERT_EQ(result.plan.semantic_class, KEEL_SEM_SESSION_STATE);
    TEST_ASSERT(result.plan.changes_session_state);
    TEST_ASSERT(result.plan.requires_pinned_backend);
    keel_parse_result_free(ops, &result);

    keel_parse_input_t begin = pg_input("BEGIN");
    TEST_ASSERT_EQ(ops->parse(&begin, &result), KEEL_PARSE_OK);
    TEST_ASSERT_EQ(result.plan.semantic_class, KEEL_SEM_TRANSACTION_CONTROL);
    TEST_ASSERT(result.plan.transaction_control);
    TEST_ASSERT(result.plan.requires_pinned_backend);
    keel_parse_result_free(ops, &result);
    TEST_END();
}

static void test_postgresql_invalid_and_empty_fail_closed(void)
{
    TEST_BEGIN("postgresql_invalid_and_empty_fail_closed");
    const keel_parser_plugin_ops_t* ops = keel_parser_builtin_postgresql_sql();
    keel_parse_result_t result;

    keel_parse_input_t empty = pg_input("");
    TEST_ASSERT_EQ(ops->parse(&empty, &result), KEEL_PARSE_INCOMPLETE);
    TEST_ASSERT_EQ(result.plan.safety, KEEL_SAFETY_UNKNOWN_FAIL_CLOSED);
    keel_parse_result_free(ops, &result);

    keel_parse_input_t bad = pg_input("SELECT (");
    TEST_ASSERT_EQ(ops->parse(&bad, &result), KEEL_PARSE_ERROR);
    TEST_ASSERT_EQ(result.plan.safety, KEEL_SAFETY_UNKNOWN_FAIL_CLOSED);
    keel_parse_result_free(ops, &result);
    TEST_END();
}

static void assert_postgresql_not_replica_safe(const char* sql)
{
    const keel_parser_plugin_ops_t* ops = keel_parser_builtin_postgresql_sql();
    keel_parse_result_t result;
    keel_parse_input_t input = pg_input(sql);
    keel_parse_status_t st = ops->parse(&input, &result);

    TEST_ASSERT(st == KEEL_PARSE_OK ||
                st == KEEL_PARSE_ERROR ||
                st == KEEL_PARSE_UNSUPPORTED ||
                st == KEEL_PARSE_RESOURCE_LIMIT);
    TEST_ASSERT(!result.plan.safe_for_replica);
    TEST_ASSERT(result.plan.safety != KEEL_SAFETY_SAFE_REPLICA);
    TEST_ASSERT(result.plan.requires_primary ||
                result.plan.requires_pinned_backend ||
                result.plan.safety == KEEL_SAFETY_UNKNOWN_FAIL_CLOSED ||
                result.plan.safety == KEEL_SAFETY_REJECT_REQUIRED);
    keel_parse_result_free(ops, &result);
}

static void test_postgresql_semantic_hazards_fail_closed(void)
{
    TEST_BEGIN("postgresql_semantic_hazards_fail_closed");
    assert_postgresql_not_replica_safe("SELECT nextval('order_id_seq')");
    assert_postgresql_not_replica_safe("SELECT pg_catalog.nextval('order_id_seq')");
    assert_postgresql_not_replica_safe("SELECT setval('order_id_seq', 10)");
    assert_postgresql_not_replica_safe("SELECT pg_advisory_lock(42)");
    assert_postgresql_not_replica_safe("SELECT function_that_writes()");
    assert_postgresql_not_replica_safe("COPY users FROM STDIN");
    assert_postgresql_not_replica_safe("CREATE TEMP TABLE t(id int)");
    assert_postgresql_not_replica_safe("LISTEN events");
    assert_postgresql_not_replica_safe("NOTIFY events");
    assert_postgresql_not_replica_safe("DO $$ BEGIN PERFORM 1; END $$");
    TEST_END();
}

int main(void)
{
    test_register_lookup_builtin();
    test_duplicate_rejected();
    test_disabled_lookup_and_parse_rejected();
    test_enable_initializes_and_shutdown_called();
    test_postgresql_semantic_read_write_ddl();
    test_postgresql_function_fails_to_primary();
    test_postgresql_session_and_txn_pin();
    test_postgresql_invalid_and_empty_fail_closed();
    test_postgresql_semantic_hazards_fail_closed();
    return test_summary();
}
