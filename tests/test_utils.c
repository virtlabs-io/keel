/**
 * @file test_utils.c
 * @brief Shared counter and summary implementation for lightweight C tests.
 *
 * The implementation deliberately stays tiny: a few process-global counters and
 * a consistent summary printout. That makes it easy for every standalone test
 * binary to behave the same way without depending on an external framework.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"

int g_tests_run = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

/**
 * @brief Print aggregate assertion counts and return a shell-friendly status.
 *
 * @return `0` when the test binary observed no failures, otherwise `1`.
 */
int test_summary(void) {
    printf("\n");
    printf("========================================\n");
    printf("Tests run:    %d\n", g_tests_run);
    printf("Tests passed: %d\n", g_tests_passed);
    printf("Tests failed: %d\n", g_tests_failed);
    printf("========================================\n");
    
    return (g_tests_failed == 0) ? 0 : 1;
}
