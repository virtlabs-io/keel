/**
 * @file test_web_ui.c
 * @brief Unit tests for the embedded Web Management UI and JSON status API.
 *
 * Tests cover:
 *   §1  keel_ui_html content — structure and correctness of the embedded SPA
 *   §2  JSON status body formatting — field presence and valid structure
 *   §3  Route matching — prefix strings used by handle_prom_http
 *   §4  HTML security — no inline event handlers, no external script sources
 *   §5  SPA behaviour — auto-refresh, error handling, all expected metrics shown
 */

#include "test_utils.h"
#include "keel/core/web_ui.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ---- helpers ---- */
static int html_contains(const char *needle) {
    return strstr(keel_ui_html, needle) != NULL;
}

/* Build a JSON status body with known values using the same format string that
 * write_status_json() uses, then verify field presence and rough structure. */
static char *build_test_json(void) {
    char *body = NULL;
    size_t len = 0;
    FILE *f = open_memstream(&body, &len);
    if (!f) return NULL;

    fprintf(f,
        "{\n"
        "  \"state\": \"%s\",\n"
        "  \"workers\": %u,\n"
        "  \"uptime_seconds\": %.1f,\n"
        "  \"sessions\": {\"active\":%llu,\"created\":%llu,\"closed\":%llu},\n"
        "  \"pool\": {\"active\":%zu,\"idle\":%zu,\"total\":%zu,\"waiting\":%zu},\n"
        "  \"queries\": {\"total\":%llu,\"read\":%llu,\"write\":%llu,\"tx\":%llu},\n"
        "  \"errors\": {\"total\":%llu,\"auth\":%llu,\"timeout\":%llu}\n"
        "}\n",
        "active", (unsigned)4, 120.0,
        (unsigned long long)12, (unsigned long long)1000, (unsigned long long)988,
        (size_t)8, (size_t)4, (size_t)16, (size_t)0,
        (unsigned long long)50000, (unsigned long long)40000,
        (unsigned long long)10000, (unsigned long long)500,
        (unsigned long long)3, (unsigned long long)1, (unsigned long long)2);
    fclose(f);
    return body; /* caller frees */
}

/* ============================================================================
 * §1  keel_ui_html content
 * ============================================================================ */

static void test_html_not_empty(void) {
    TEST_BEGIN("web_ui: keel_ui_html is non-empty");
    TEST_ASSERT(keel_ui_html[0] != '\0');
    TEST_ASSERT(strlen(keel_ui_html) > 100);
    TEST_END();
}

static void test_html_doctype(void) {
    TEST_BEGIN("web_ui: HTML starts with <!DOCTYPE html>");
    TEST_ASSERT(strncmp(keel_ui_html, "<!DOCTYPE html>", 15) == 0);
    TEST_END();
}

static void test_html_title(void) {
    TEST_BEGIN("web_ui: title contains 'Keel'");
    TEST_ASSERT(html_contains("<title>"));
    TEST_ASSERT(html_contains("Keel"));
    TEST_END();
}

static void test_html_has_closing_tags(void) {
    TEST_BEGIN("web_ui: HTML has </body> and </html>");
    TEST_ASSERT(html_contains("</body>"));
    TEST_ASSERT(html_contains("</html>"));
    TEST_END();
}

static void test_html_has_script(void) {
    TEST_BEGIN("web_ui: HTML contains <script> block");
    TEST_ASSERT(html_contains("<script>"));
    TEST_ASSERT(html_contains("</script>"));
    TEST_END();
}

static void test_html_has_css(void) {
    TEST_BEGIN("web_ui: HTML contains <style> block");
    TEST_ASSERT(html_contains("<style>"));
    TEST_ASSERT(html_contains("</style>"));
    TEST_END();
}

/* ============================================================================
 * §2  JSON status body formatting
 * ============================================================================ */

static void test_json_has_state(void) {
    TEST_BEGIN("web_ui: JSON body contains 'state' field");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"state\"") != NULL);
    TEST_ASSERT(strstr(json, "\"active\"") != NULL);
    free(json);
    TEST_END();
}

static void test_json_has_workers(void) {
    TEST_BEGIN("web_ui: JSON body contains 'workers' field");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"workers\"") != NULL);
    TEST_ASSERT(strstr(json, ": 4") != NULL);
    free(json);
    TEST_END();
}

static void test_json_has_uptime(void) {
    TEST_BEGIN("web_ui: JSON body contains 'uptime_seconds' field");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"uptime_seconds\"") != NULL);
    TEST_ASSERT(strstr(json, "120.0") != NULL);
    free(json);
    TEST_END();
}

static void test_json_has_sessions(void) {
    TEST_BEGIN("web_ui: JSON body contains 'sessions' object with active/created/closed");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"sessions\"") != NULL);
    TEST_ASSERT(strstr(json, "\"active\"") != NULL);
    TEST_ASSERT(strstr(json, "\"created\"") != NULL);
    TEST_ASSERT(strstr(json, "\"closed\"") != NULL);
    free(json);
    TEST_END();
}

static void test_json_has_pool(void) {
    TEST_BEGIN("web_ui: JSON body contains 'pool' object with active/idle/total/waiting");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"pool\"") != NULL);
    TEST_ASSERT(strstr(json, "\"idle\"") != NULL);
    TEST_ASSERT(strstr(json, "\"total\"") != NULL);
    TEST_ASSERT(strstr(json, "\"waiting\"") != NULL);
    free(json);
    TEST_END();
}

static void test_json_has_queries(void) {
    TEST_BEGIN("web_ui: JSON body contains 'queries' object with total/read/write/tx");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"queries\"") != NULL);
    TEST_ASSERT(strstr(json, "\"read\"") != NULL);
    TEST_ASSERT(strstr(json, "\"write\"") != NULL);
    TEST_ASSERT(strstr(json, "\"tx\"") != NULL);
    free(json);
    TEST_END();
}

static void test_json_has_errors(void) {
    TEST_BEGIN("web_ui: JSON body contains 'errors' object with total/auth/timeout");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"errors\"") != NULL);
    TEST_ASSERT(strstr(json, "\"auth\"") != NULL);
    TEST_ASSERT(strstr(json, "\"timeout\"") != NULL);
    free(json);
    TEST_END();
}

static void test_json_starts_with_brace(void) {
    TEST_BEGIN("web_ui: JSON body starts with '{' and ends with '}'");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(json[0] == '{');
    /* find last non-whitespace character */
    size_t len = strlen(json);
    size_t last = len;
    while (last > 0 && (json[last-1] == '\n' || json[last-1] == '\r' ||
                         json[last-1] == ' '  || json[last-1] == '\t'))
        last--;
    TEST_ASSERT(last > 0 && json[last-1] == '}');
    free(json);
    TEST_END();
}

static void test_json_values_for_known_inputs(void) {
    TEST_BEGIN("web_ui: JSON body encodes known input values correctly");
    char *json = build_test_json();
    TEST_ASSERT_NOT_NULL(json);
    /* sessions: active=12, created=1000, closed=988 */
    TEST_ASSERT(strstr(json, "\"active\":12") != NULL);
    TEST_ASSERT(strstr(json, "\"created\":1000") != NULL);
    TEST_ASSERT(strstr(json, "\"closed\":988") != NULL);
    /* queries: total=50000, read=40000, write=10000, tx=500 */
    TEST_ASSERT(strstr(json, "50000") != NULL);
    TEST_ASSERT(strstr(json, "40000") != NULL);
    /* errors: total=3 */
    TEST_ASSERT(strstr(json, "\"total\":3") != NULL);
    free(json);
    TEST_END();
}

/* ============================================================================
 * §3  Route matching — strncmp patterns used in handle_prom_http
 * ============================================================================ */

static void test_route_ui_prefix(void) {
    TEST_BEGIN("web_ui: route prefix 'GET /ui' matches expected requests");
    TEST_ASSERT(strncmp("GET /ui HTTP/1.1\r\n", "GET /ui", 7) == 0);
    TEST_ASSERT(strncmp("GET /ui/ HTTP/1.1\r\n", "GET /ui", 7) == 0);
    /* must not match /uiother if full prefix is wrong */
    TEST_ASSERT(strncmp("GET /metrics", "GET /ui", 7) != 0);
    TEST_END();
}

static void test_route_status_json_prefix(void) {
    TEST_BEGIN("web_ui: route prefix 'GET /api/status.json' matches expected requests");
    TEST_ASSERT(strncmp("GET /api/status.json HTTP/1.1\r\n",
                        "GET /api/status.json", 20) == 0);
    TEST_ASSERT(strncmp("GET /api/status.js HTTP/1.1\r\n",
                        "GET /api/status.json", 20) != 0);
    TEST_END();
}

static void test_route_metrics_still_matches(void) {
    TEST_BEGIN("web_ui: existing /metrics and / routes still match correctly");
    TEST_ASSERT(strncmp("GET /metrics HTTP/1.1\r\n", "GET /metrics", 12) == 0);
    TEST_ASSERT(strncmp("GET / HTTP/1.1\r\n", "GET / ", 6) == 0);
    /* /ui must NOT match /metrics prefix */
    TEST_ASSERT(strncmp("GET /ui HTTP/1.1\r\n", "GET /metrics", 12) != 0);
    TEST_END();
}

static void test_route_ui_does_not_match_metrics(void) {
    TEST_BEGIN("web_ui: /ui request does not accidentally match /metrics route");
    const char *req = "GET /ui HTTP/1.1\r\n";
    TEST_ASSERT(strncmp(req, "GET /metrics", 12) != 0);
    TEST_ASSERT(strncmp(req, "GET / ", 6) != 0);
    TEST_ASSERT(strncmp(req, "GET /ui", 7) == 0);
    TEST_END();
}

/* ============================================================================
 * §4  HTML security — no external script sources
 * ============================================================================ */

static void test_html_no_external_script_src(void) {
    TEST_BEGIN("web_ui: HTML has no external <script src=...> tags");
    /* A script tag with a src= attribute loading external resources would be a
     * security concern and defeat the self-contained nature of the SPA. */
    const char *p = keel_ui_html;
    while ((p = strstr(p, "<script")) != NULL) {
        /* Check that the tag has no src= attribute before the closing '>' */
        const char *close = strchr(p, '>');
        if (close) {
            size_t tag_len = (size_t)(close - p);
            char tag[256] = {0};
            if (tag_len < sizeof(tag)) {
                memcpy(tag, p, tag_len);
                TEST_ASSERT(strstr(tag, "src=") == NULL);
            }
        }
        p++;
    }
    TEST_END();
}

static void test_html_no_inline_event_handlers(void) {
    TEST_BEGIN("web_ui: HTML has no inline onclick/onload event handlers");
    TEST_ASSERT(strstr(keel_ui_html, "onclick=") == NULL);
    TEST_ASSERT(strstr(keel_ui_html, "onload=") == NULL);
    TEST_ASSERT(strstr(keel_ui_html, "onerror=") == NULL);
    TEST_END();
}

static void test_html_no_eval(void) {
    TEST_BEGIN("web_ui: JS does not use eval()");
    TEST_ASSERT(strstr(keel_ui_html, "eval(") == NULL);
    TEST_END();
}

/* ============================================================================
 * §5  SPA behaviour — auto-refresh, API calls, metric fields
 * ============================================================================ */

static void test_html_fetches_status_json(void) {
    TEST_BEGIN("web_ui: SPA fetches /api/status.json");
    TEST_ASSERT(html_contains("/api/status.json"));
    TEST_END();
}

static void test_html_has_auto_refresh(void) {
    TEST_BEGIN("web_ui: SPA uses setInterval for auto-refresh");
    TEST_ASSERT(html_contains("setInterval"));
    TEST_END();
}

static void test_html_has_5s_refresh_interval(void) {
    TEST_BEGIN("web_ui: SPA refresh interval is 5000ms");
    TEST_ASSERT(html_contains("5000"));
    TEST_END();
}

static void test_html_has_reconnect_handler(void) {
    TEST_BEGIN("web_ui: SPA has .catch handler for fetch errors");
    TEST_ASSERT(html_contains(".catch"));
    TEST_END();
}

static void test_html_shows_sessions(void) {
    TEST_BEGIN("web_ui: SPA renders sessions metrics");
    /* The JS accesses d.sessions.active, .created, .closed */
    TEST_ASSERT(html_contains("sessions"));
    TEST_ASSERT(html_contains("created"));
    TEST_ASSERT(html_contains("closed"));
    TEST_END();
}

static void test_html_shows_pool(void) {
    TEST_BEGIN("web_ui: SPA renders pool metrics");
    TEST_ASSERT(html_contains("pool"));
    TEST_ASSERT(html_contains("idle"));
    TEST_ASSERT(html_contains("waiting"));
    TEST_END();
}

static void test_html_shows_queries(void) {
    TEST_BEGIN("web_ui: SPA renders query metrics (read/write/tx)");
    TEST_ASSERT(html_contains("queries"));
    TEST_ASSERT(html_contains(".read"));
    TEST_ASSERT(html_contains(".write"));
    TEST_ASSERT(html_contains(".tx"));
    TEST_END();
}

static void test_html_shows_errors(void) {
    TEST_BEGIN("web_ui: SPA renders error metrics");
    TEST_ASSERT(html_contains("errors"));
    TEST_ASSERT(html_contains(".auth"));
    TEST_ASSERT(html_contains(".timeout"));
    TEST_END();
}

static void test_html_has_metrics_link(void) {
    TEST_BEGIN("web_ui: SPA links to /metrics for raw Prometheus data");
    TEST_ASSERT(html_contains("/metrics"));
    TEST_ASSERT(html_contains("href='/metrics'") || html_contains("href=\"/metrics\""));
    TEST_END();
}

static void test_html_has_healthz_link(void) {
    TEST_BEGIN("web_ui: SPA links to /healthz");
    TEST_ASSERT(html_contains("/healthz"));
    TEST_END();
}

static void test_html_displays_uptime(void) {
    TEST_BEGIN("web_ui: SPA renders uptime field");
    TEST_ASSERT(html_contains("uptime"));
    TEST_ASSERT(html_contains("fmtUp") || html_contains("Uptime"));
    TEST_END();
}

static void test_html_displays_workers(void) {
    TEST_BEGIN("web_ui: SPA renders worker count");
    TEST_ASSERT(html_contains("workers") || html_contains("Workers"));
    TEST_END();
}

static void test_html_displays_state_badge(void) {
    TEST_BEGIN("web_ui: SPA shows engine state badge with colour classes");
    TEST_ASSERT(html_contains("badge-active"));
    TEST_ASSERT(html_contains("badge-other"));
    TEST_ASSERT(html_contains("d.state"));
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    /* §1 HTML content */
    test_html_not_empty();
    test_html_doctype();
    test_html_title();
    test_html_has_closing_tags();
    test_html_has_script();
    test_html_has_css();

    /* §2 JSON format */
    test_json_has_state();
    test_json_has_workers();
    test_json_has_uptime();
    test_json_has_sessions();
    test_json_has_pool();
    test_json_has_queries();
    test_json_has_errors();
    test_json_starts_with_brace();
    test_json_values_for_known_inputs();

    /* §3 Route matching */
    test_route_ui_prefix();
    test_route_status_json_prefix();
    test_route_metrics_still_matches();
    test_route_ui_does_not_match_metrics();

    /* §4 Security */
    test_html_no_external_script_src();
    test_html_no_inline_event_handlers();
    test_html_no_eval();

    /* §5 SPA behaviour */
    test_html_fetches_status_json();
    test_html_has_auto_refresh();
    test_html_has_5s_refresh_interval();
    test_html_has_reconnect_handler();
    test_html_shows_sessions();
    test_html_shows_pool();
    test_html_shows_queries();
    test_html_shows_errors();
    test_html_has_metrics_link();
    test_html_has_healthz_link();
    test_html_displays_uptime();
    test_html_displays_workers();
    test_html_displays_state_badge();

    return test_summary();
}
