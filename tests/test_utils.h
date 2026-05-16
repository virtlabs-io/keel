/**
 * @file test_utils.h
 * @brief Minimal assertion and reporting helpers shared by C unit tests.
 *
 * KEEL's small unit tests intentionally avoid pulling in a heavyweight testing
 * framework. The helpers in this header provide just enough structure to make
 * failures readable and to keep the individual test files focused on scenario
 * setup instead of boilerplate counting and reporting.
 *
 * The tradeoff is simplicity over rich diagnostics: these macros count pass/fail
 * outcomes and print the failing source location, but they do not isolate test
 * cases in subprocesses or provide fixtures beyond what each file builds
 * manually.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

/* Global counters shared by the simple assertion macros. */
extern int g_tests_run;
extern int g_tests_passed;
extern int g_tests_failed;

/*
 * Assertion macros increment the global counters directly so the surrounding
 * test file only needs to call `test_summary()` once from `main()`.
 */
#define TEST_ASSERT(cond) \
    do { \
        g_tests_run++; \
        if (!(cond)) { \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            g_tests_failed++; \
        } else { \
            g_tests_passed++; \
        } \
    } while (0)

#define TEST_ASSERT_EQ(a, b) \
    do { \
        g_tests_run++; \
        if ((a) != (b)) { \
            fprintf(stderr, "FAIL: %s:%d: %s != %s\n", __FILE__, __LINE__, #a, #b); \
            g_tests_failed++; \
        } else { \
            g_tests_passed++; \
        } \
    } while (0)

#define TEST_ASSERT_STR_EQ(a, b) \
    do { \
        g_tests_run++; \
        if (strcmp((a), (b)) != 0) { \
            fprintf(stderr, "FAIL: %s:%d: \"%s\" != \"%s\"\n", __FILE__, __LINE__, (a), (b)); \
            g_tests_failed++; \
        } else { \
            g_tests_passed++; \
        } \
    } while (0)

#define TEST_ASSERT_NULL(p) TEST_ASSERT((p) == NULL)
#define TEST_ASSERT_NOT_NULL(p) TEST_ASSERT((p) != NULL)

#define TEST_BEGIN(name) \
    printf("Testing %s...\n", name)

#define TEST_END() \
    printf("  %d/%d passed\n", g_tests_passed, g_tests_run)

/* Test result */
/**
 * @brief Print a one-line summary block for the current test process.
 *
 * @return Process exit code style status: `0` when no assertions failed,
 *         otherwise `1`.
 */
int test_summary(void);

#endif /* TEST_UTILS_H */
