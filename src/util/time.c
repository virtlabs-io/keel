/**
 * @file time.c
 * @brief Monotonic/realtime clock helpers, duration formatting, and blocking sleeps.
 *
 * Time handling in KEEL splits into two families with intentionally different
 * semantics:
 *
 * - monotonic timestamps for elapsed-time measurement, deadlines, and latency
 *   calculations that must ignore wall-clock adjustments;
 * - realtime timestamps for log output and any value that must align with human
 *   calendar time.
 *
 * The helpers in this file keep that distinction explicit while providing a
 * small convenience layer for parsing and formatting duration values from
 * configuration and diagnostics.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

/* Feature test macros must come before any includes.
 * On macOS, _POSIX_C_SOURCE alone may hide some functions,
 * so we also define _DARWIN_C_SOURCE for full compatibility. */
#if defined(__APPLE__)
    #define _DARWIN_C_SOURCE 1
#endif

#ifndef _POSIX_C_SOURCE
    #define _POSIX_C_SOURCE 199309L
#endif

#include "keel_types.h"
#include "keel/util/util.h"

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

/* ============================================================================
 * Time Getting
 * ============================================================================ */

/**
 * @brief Get current monotonic time
 *
 * Returns nanoseconds from an arbitrary epoch. Guaranteed to be
 * monotonically increasing (won't go backward). Use for measuring
 * elapsed time.
 *
 * @return Current monotonic time in nanoseconds
 */
/**
 * @brief Read the monotonic clock in nanoseconds.
 *
 * `CLOCK_MONOTONIC` is used here because latency and timeout calculations must
 * not jump backward or forward when the system wall clock changes.
 *
 * @return Monotonic nanosecond timestamp, or `0` on clock failure.
 */
keel_time_t keel_time_now(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (keel_time_t)ts.tv_sec * KEEL_NS_PER_SEC + (keel_time_t)ts.tv_nsec;
}

uint64_t keel_time_now_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC_COARSE, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/**
 * @brief Get current wall-clock time
 *
 * Returns nanoseconds since Unix epoch (1970-01-01 00:00:00 UTC).
 * May be adjusted by NTP. Use for logging and timestamps.
 *
 * @return Current real time in nanoseconds since epoch
 */
/**
 * @brief Read the realtime clock in nanoseconds since the Unix epoch.
 *
 * @return Realtime nanosecond timestamp, or `0` on clock failure.
 */
keel_time_t keel_time_realtime(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return 0;
    }
    return (keel_time_t)ts.tv_sec * KEEL_NS_PER_SEC + (keel_time_t)ts.tv_nsec;
}

/* ============================================================================
 * Time Arithmetic
 * ============================================================================ */

/**
 * @brief Calculate the difference between two times
 * @param start  Start time
 * @param end    End time
 * @return Duration (end - start), may be negative
 */
keel_duration_t keel_time_diff(keel_time_t start, keel_time_t end) {
    return (keel_duration_t)(end - start);
}

/**
 * @brief Add a duration to a time
 * @param t  Base time
 * @param d  Duration to add
 * @return Time + duration
 */
keel_time_t keel_time_add(keel_time_t t, keel_duration_t d) {
    return t + (keel_time_t)d;
}

/**
 * @brief Check if time a is before time b
 */
bool keel_time_before(keel_time_t a, keel_time_t b) {
    return a < b;
}

/**
 * @brief Check if time a is after time b
 */
bool keel_time_after(keel_time_t a, keel_time_t b) {
    return a > b;
}

/* ============================================================================
 * Duration Helpers
 *
 * Factory functions to create durations from common units.
 * Prefer the macros KEEL_NSEC, KEEL_USEC, KEEL_MSEC, KEEL_SEC for constants.
 * ============================================================================ */

/** @brief Create duration from nanoseconds */
keel_duration_t keel_duration_ns(int64_t ns) {
    return (keel_duration_t)ns;
}

/** @brief Create duration from microseconds */
keel_duration_t keel_duration_us(int64_t us) {
    return (keel_duration_t)(us * KEEL_NS_PER_US);
}

/** @brief Create duration from milliseconds */
keel_duration_t keel_duration_ms(int64_t ms) {
    return (keel_duration_t)(ms * KEEL_NS_PER_MS);
}

/** @brief Create duration from seconds */
keel_duration_t keel_duration_sec(int64_t sec) {
    return (keel_duration_t)(sec * KEEL_NS_PER_SEC);
}

/** @brief Create duration from minutes */
keel_duration_t keel_duration_min(int64_t min) {
    return (keel_duration_t)(min * 60 * KEEL_NS_PER_SEC);
}

/** @brief Convert duration to nanoseconds */
int64_t keel_duration_to_ns(keel_duration_t d) {
    return (int64_t)d;
}

/** @brief Convert duration to microseconds (truncated) */
int64_t keel_duration_to_us(keel_duration_t d) {
    return (int64_t)(d / KEEL_NS_PER_US);
}

/** @brief Convert duration to milliseconds (truncated) */
int64_t keel_duration_to_ms(keel_duration_t d) {
    return (int64_t)(d / KEEL_NS_PER_MS);
}

/** @brief Convert duration to seconds (floating point) */
double keel_duration_to_sec(keel_duration_t d) {
    return (double)d / (double)KEEL_NS_PER_SEC;
}

/* ============================================================================
 * Time Formatting
 * ============================================================================ */

/**
 * @brief Format time as ISO 8601 string
 *
 * Produces format: 2024-01-15T12:34:56.123456789Z (UTC)
 *
 * @param t    Time to format (from keel_time_realtime)
 * @param buf  Output buffer
 * @param len  Buffer size
 * @return Number of characters written (excluding null)
 */
size_t keel_time_format_iso8601(keel_time_t t, char* buf, size_t len) {
    time_t secs = (time_t)(t / KEEL_NS_PER_SEC);
    long nanos = (long)(t % KEEL_NS_PER_SEC);
    
    struct tm tm;
    if (gmtime_r(&secs, &tm) == NULL) {
        if (len > 0) buf[0] = '\0';
        return 0;
    }
    
    /* Format: 2024-01-15T12:34:56.123456789Z */
    int written = snprintf(buf, len, "%04d-%02d-%02dT%02d:%02d:%02d.%09ldZ",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                           tm.tm_hour, tm.tm_min, tm.tm_sec, nanos);
    
    return written > 0 ? (size_t)written : 0;
}

/**
 * @brief Format time as local date-time string
 *
 * Produces format: 2024-01-15 12:34:56 (local timezone)
 *
 * @param t    Time to format
 * @param buf  Output buffer
 * @param len  Buffer size
 * @return Number of characters written (excluding null)
 */
size_t keel_time_format_local(keel_time_t t, char* buf, size_t len) {
    time_t secs = (time_t)(t / KEEL_NS_PER_SEC);
    
    struct tm tm;
    if (localtime_r(&secs, &tm) == NULL) {
        if (len > 0) buf[0] = '\0';
        return 0;
    }
    
    int written = snprintf(buf, len, "%04d-%02d-%02d %02d:%02d:%02d",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                           tm.tm_hour, tm.tm_min, tm.tm_sec);
    
    return written > 0 ? (size_t)written : 0;
}

/**
 * @brief Format duration as human-readable string
 *
 * Automatically selects appropriate unit:
 *   - 500ns      (nanoseconds)
 *   - 1.23µs     (microseconds)
 *   - 45.67ms    (milliseconds)
 *   - 1.23s      (seconds)
 *   - 5m30s      (minutes and seconds)
 *   - 2h15m      (hours and minutes)
 *
 * @param d    Duration to format
 * @param buf  Output buffer
 * @param len  Buffer size
 * @return Number of characters written
 */
size_t keel_duration_format(keel_duration_t d, char* buf, size_t len) {
    if (d < KEEL_NS_PER_US) {
        return (size_t)snprintf(buf, len, "%ldns", (long)d);
    } else if (d < KEEL_NS_PER_MS) {
        return (size_t)snprintf(buf, len, "%.2fµs", (double)d / KEEL_NS_PER_US);
    } else if (d < KEEL_NS_PER_SEC) {
        return (size_t)snprintf(buf, len, "%.2fms", (double)d / KEEL_NS_PER_MS);
    } else if (d < 60 * KEEL_NS_PER_SEC) {
        return (size_t)snprintf(buf, len, "%.2fs", (double)d / KEEL_NS_PER_SEC);
    } else if (d < 3600 * KEEL_NS_PER_SEC) {
        int64_t mins = d / (60 * KEEL_NS_PER_SEC);
        int64_t secs = (d / KEEL_NS_PER_SEC) % 60;
        return (size_t)snprintf(buf, len, "%ldm%lds", (long)mins, (long)secs);
    } else {
        int64_t hours = d / (3600 * KEEL_NS_PER_SEC);
        int64_t mins = (d / (60 * KEEL_NS_PER_SEC)) % 60;
        return (size_t)snprintf(buf, len, "%ldh%ldm", (long)hours, (long)mins);
    }
}

/* ============================================================================
 * Time Parsing
 * ============================================================================ */

/**
 * @brief Parse a duration string
 *
 * Supported formats:
 *   100ns, 50us, 50µs, 100ms, 5s, 10m, 10min, 2h, 2hr
 *   Plain number defaults to seconds: "30" = 30s
 *
 * @code
 * keel_duration_t timeout;
 * if (keel_duration_parse("100ms", &timeout)) {
 *     // timeout == 100,000,000 ns
 * }
 * @endcode
 *
 * @param str  Duration string
 * @param out  [out] Parsed duration
 * @return true on success, false on parse error
 */
/**
 * @brief Parse a textual duration into nanoseconds.
 *
 * The parser intentionally accepts a small, configuration-friendly vocabulary
 * instead of a full natural-language grammar. A bare number defaults to
 * seconds because that is the least surprising choice for timeout-style values.
 *
 * @param str Input string.
 * @param[out] out Parsed duration.
 * @return true on successful parse, false on invalid input.
 */
bool keel_duration_parse(const char* str, keel_duration_t* out) {
    if (!str || !out) {
        return false;
    }
    
    char* endptr;
    double value = strtod(str, &endptr);
    
    if (endptr == str) {
        return false;
    }
    
    /* Skip whitespace */
    while (*endptr == ' ' || *endptr == '\t') {
        endptr++;
    }
    
    keel_duration_t multiplier = KEEL_NS_PER_SEC; /* default to seconds */
    
    if (*endptr == '\0' || strcmp(endptr, "s") == 0) {
        multiplier = KEEL_NS_PER_SEC;
    } else if (strcmp(endptr, "ms") == 0) {
        multiplier = KEEL_NS_PER_MS;
    } else if (strcmp(endptr, "us") == 0 || strcmp(endptr, "µs") == 0) {
        multiplier = KEEL_NS_PER_US;
    } else if (strcmp(endptr, "ns") == 0) {
        multiplier = 1;
    } else if (strcmp(endptr, "m") == 0 || strcmp(endptr, "min") == 0) {
        multiplier = 60 * KEEL_NS_PER_SEC;
    } else if (strcmp(endptr, "h") == 0 || strcmp(endptr, "hr") == 0) {
        multiplier = 3600 * KEEL_NS_PER_SEC;
    } else {
        return false;
    }
    
    *out = (keel_duration_t)(value * (double)multiplier);
    return true;
}

/* ============================================================================
 * High-Resolution Timer (Stopwatch)
 *
 * For measuring elapsed time with start/stop/resume capability.
 * ============================================================================ */

/* keel_stopwatch_t is defined in util.h */

/** @brief Start or resume the stopwatch */
/**
 * @brief Start a stopwatch from zero.
 *
 * The current implementation resets previously accumulated elapsed time when
 * starting. That makes it a restart operation rather than a pause/resume API.
 *
 * @param sw Stopwatch to start.
 * @return
 */
void keel_stopwatch_start(keel_stopwatch_t* sw) {
    if (!sw) return;
    sw->elapsed = 0;
    sw->start = keel_time_now();
    sw->running = true;
}

/** @brief Stop the stopwatch, accumulating elapsed time */
/**
 * @brief Stop a running stopwatch and preserve the accumulated elapsed time.
 *
 * @param sw Stopwatch to stop.
 * @return
 */
void keel_stopwatch_stop(keel_stopwatch_t* sw) {
    if (!sw || !sw->running) return;
    sw->elapsed += keel_time_now() - sw->start;
    sw->running = false;
}

/** @brief Reset the stopwatch to zero */
void keel_stopwatch_reset(keel_stopwatch_t* sw) {
    if (!sw) return;
    sw->start = 0;
    sw->elapsed = 0;
    sw->running = false;
}

/** @brief Get elapsed time (works while running or stopped) */
/**
 * @brief Read the elapsed duration from a stopwatch.
 *
 * When the stopwatch is running, the function folds in the live monotonic
 * delta so callers do not need separate code paths for running versus stopped
 * measurements.
 *
 * @param sw Stopwatch to inspect.
 * @return Elapsed duration, or zero when `sw` is `NULL`.
 */
keel_duration_t keel_stopwatch_elapsed(const keel_stopwatch_t* sw) {
    if (!sw) return 0;
    if (sw->running) {
        return (keel_duration_t)(sw->elapsed + (keel_time_now() - sw->start));
    }
    return (keel_duration_t)sw->elapsed;
}

/* ============================================================================
 * Sleep
 *
 * Blocking sleep functions. For async waiting, use timers instead.
 * ============================================================================ */

/** @brief Sleep for nanoseconds */
/**
 * @brief Block the calling thread for a nanosecond interval.
 *
 * This is a thin wrapper over `nanosleep()`. Interrupt handling is left simple
 * on purpose because the helper is mainly for tests, tooling, and coarse
 * control-plane waits rather than robust signal-aware scheduling.
 *
 * @param ns Number of nanoseconds to sleep.
 * @return
 */
void keel_sleep_ns(int64_t ns) {
    struct timespec ts = {
        .tv_sec = ns / KEEL_NS_PER_SEC,
        .tv_nsec = ns % KEEL_NS_PER_SEC
    };
    nanosleep(&ts, NULL);
}

/** @brief Sleep for milliseconds */
void keel_sleep_ms(int64_t ms) {
    keel_sleep_ns(ms * KEEL_NS_PER_MS);
}

/** @brief Sleep for microseconds */
void keel_sleep_us(int64_t us) {
    keel_sleep_ns(us * KEEL_NS_PER_US);
}

/** @brief Sleep for a duration */
void keel_sleep(keel_duration_t d) {
    if (d <= 0) return;
    keel_sleep_ns((int64_t)d);
}
