/**
 * @file semantic_plan.c
 * @brief Parser-neutral semantic plan helpers.
 */

#include "keel/parser/semantic_plan.h"

#include <stdio.h>
#include <string.h>

void keel_semantic_plan_init(keel_semantic_plan_t* plan,
                             keel_language_id_t language,
                             keel_dialect_id_t dialect)
{
    if (!plan) return;
    memset(plan, 0, sizeof *plan);
    plan->language = language;
    plan->dialect = dialect;
    plan->semantic_class = KEEL_SEM_UNKNOWN;
    plan->safety = KEEL_SAFETY_UNKNOWN_FAIL_CLOSED;
    plan->requires_primary = true;
    plan->parser_confident = false;
    snprintf(plan->reason, sizeof plan->reason, "semantic plan not classified");
}

bool keel_semantic_plan_valid(const keel_semantic_plan_t* plan)
{
    if (!plan) return false;
    if (plan->language == 0 || plan->dialect == KEEL_DIALECT_UNKNOWN) return false;
    if (plan->safety == KEEL_SAFETY_SAFE_REPLICA) {
        return plan->is_read_only && plan->safe_for_replica &&
               !plan->may_write && !plan->requires_primary &&
               !plan->requires_pinned_backend;
    }
    if (plan->safe_for_replica &&
        (plan->may_write || plan->requires_primary ||
         plan->requires_pinned_backend || plan->changes_schema ||
         plan->changes_session_state || plan->external_side_effects_possible)) {
        return false;
    }
    return true;
}

const char* keel_semantic_class_name(keel_semantic_class_t cls)
{
    switch (cls) {
    case KEEL_SEM_UNKNOWN: return "unknown";
    case KEEL_SEM_READ_ONLY: return "read_only";
    case KEEL_SEM_WRITE: return "write";
    case KEEL_SEM_DDL: return "ddl";
    case KEEL_SEM_TRANSACTION_CONTROL: return "transaction_control";
    case KEEL_SEM_SESSION_STATE: return "session_state";
    case KEEL_SEM_SECURITY: return "security";
    case KEEL_SEM_ADMIN: return "admin";
    case KEEL_SEM_EXTERNAL_EFFECT: return "external_effect";
    case KEEL_SEM_MIXED: return "mixed";
    default: return "invalid";
    }
}

const char* keel_safety_level_name(keel_safety_level_t safety)
{
    switch (safety) {
    case KEEL_SAFETY_SAFE_REPLICA: return "safe_replica";
    case KEEL_SAFETY_PRIMARY_REQUIRED: return "primary_required";
    case KEEL_SAFETY_PINNED_BACKEND_REQUIRED: return "pinned_backend_required";
    case KEEL_SAFETY_REJECT_REQUIRED: return "reject_required";
    case KEEL_SAFETY_UNKNOWN_FAIL_CLOSED: return "unknown_fail_closed";
    default: return "invalid";
    }
}
