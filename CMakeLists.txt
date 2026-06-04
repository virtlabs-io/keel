# =============================================================================
# CMakeLists.txt — Top-level build configuration for the Keel proxy
# =============================================================================
#
# Keel is a high-performance database connection pooler / proxy supporting
# PostgreSQL and MySQL wire protocols.  This file defines the entire build
# graph for the project:
#
#   1. Build options and feature flags
#   2. Compiler settings (C23, strict warnings, -Werror)
#   3. Platform detection (Linux / macOS / Windows) and reactor selection
#   4. Sanitizer and coverage instrumentation
#   5. Binary hardening flags (FORTIFY_SOURCE, PIE, RELRO, stack protector)
#   6. Dependency discovery (threads, io_uring, OpenSSL)
#   7. Subsystem libraries (one add_subdirectory per module)
#   8. Test suite registration
#   9. Install layout and CPack packaging (DEB, RPM)
#
# Reactor priority:
#   Linux:  io_uring (if liburing found) > epoll (fallback)
#   macOS:  kqueue (always)
#
# Required build tools:
#   cmake >= 3.25, a C23-capable compiler (GCC 14+ or Clang 17+),
#   OpenSSL development headers, pthreads.
#
# Optional dependencies:
#   liburing (for io_uring backend on Linux 5.6+)
#   Lua 5.4 / LuaJIT (for Lua hook scripting)
#   Python 3.x development (for Python hook scripting)
# =============================================================================

cmake_minimum_required(VERSION 3.25)

project(keel
    VERSION 0.5.3
    DESCRIPTION "High-performance database connection pooler"
    LANGUAGES C
)

include(GNUInstallDirs)

# ============================================================================
# Build Options
# ============================================================================
# Feature toggles exposed as CMake cache variables.  Each option can be
# overridden on the command line:
#   cmake -DKEEL_USE_IOURING=OFF -DKEEL_ENABLE_ASAN=ON ..
#
# I/O backend options:
#   KEEL_USE_IOURING  — Linux io_uring (requires liburing + kernel 5.6+)
#   KEEL_USE_KQUEUE   — macOS/BSD kqueue (auto-detected on Darwin)
#   KEEL_USE_EPOLL    — Linux epoll (fallback when io_uring unavailable)
#
# Sanitizer options (mutually exclusive with MSan; ASan+UBSan can combine):
#   KEEL_ENABLE_ASAN  — AddressSanitizer (heap/stack/global buffer overflows)
#   KEEL_ENABLE_TSAN  — ThreadSanitizer (data races)
#   KEEL_ENABLE_UBSAN — UndefinedBehaviorSanitizer
#   KEEL_ENABLE_MSAN  — MemorySanitizer (uninitialised reads)
#
# Stats tiers (cumulative — higher tiers include all lower ones):
#   KEEL_ENABLE_SYSTEM_STATS  — L3: /proc sampling (CPU, memory, FD counts)
#   KEEL_ENABLE_TRACE         — L4: USDT probe points for perf/bpftrace
#   KEEL_ENABLE_FULL_STATS    — L5: per-query SQL fingerprint aggregation
# ============================================================================
option(KEEL_USE_IOURING "Use io_uring for async I/O (Linux 5.6+)" ON)
option(KEEL_USE_KQUEUE "Use kqueue for async I/O (macOS/BSD)" ON)
option(KEEL_USE_EPOLL "Use epoll for async I/O (Linux fallback)" ON)
option(KEEL_ENABLE_TESTS "Build tests" ON)
option(KEEL_ENABLE_BENCHMARKS "Build benchmarks" OFF)
option(KEEL_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(KEEL_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(KEEL_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(KEEL_ENABLE_MSAN "Enable MemorySanitizer" OFF)
option(KEEL_ENABLE_FUZZ "Build fuzz targets with -fsanitize=fuzzer (requires Clang)" OFF)
option(KEEL_ENABLE_COVERAGE "Enable code coverage" OFF)
option(KEEL_ENABLE_HARDENING "Enable compiler/linker exploit mitigations" ON)
option(KEEL_BUILD_DOCS "Build documentation" OFF)
option(KEEL_ENABLE_SYSTEM_STATS "Enable L3 system-level stats (/proc sampling)" ON)
option(KEEL_ENABLE_TRACE "Enable L4 USDT probe points" OFF)
option(KEEL_ENABLE_FULL_STATS "Enable L5 SQL fingerprint aggregation" OFF)
option(KEEL_ENABLE_LUA "Enable embedded Lua interpreter for hooks" OFF)
option(KEEL_ENABLE_PYTHON "Enable embedded Python interpreter for hooks" OFF)
# Default OFF: scripting interpreters are an expanded attack surface
# and runtime cost. The CI matrix builds two variants: 'core' (both OFF) and
# 'full' (both ON). Distributors should ship the 'core' package by default and
# offer 'full' as a separately-installed superset. See docs/BUILD_VARIANTS.md.
option(KEEL_ENABLE_OTLP "Build OTLP metrics exporter (requires nanopb; off by default)" OFF)

# Inline buffer size (bytes) used to stash the post-deferred-BEGIN FE payload
# while the BEGIN's ReadyForQuery is being absorbed asynchronously.  Sessions
# that exceed this buffer return a hard error; bump if you see
# pre_query_overflow incrementing in production.
set(KEEL_PRE_QUERY_REPLAY_BUFSZ "8192" CACHE STRING
    "Inline byte buffer per session for async deferred-BEGIN payload stash (default 8192)")

# ============================================================================
# C Standard and Compiler Settings
# ============================================================================
# C23 is required for:
#   - typeof (used in container_of macros)
#   - nullptr (replaces NULL in new code)
#   - Improved _Static_assert with optional message
#   - Unnamed struct/union improvements
#
# Extensions are OFF: the codebase relies only on the C standard plus
# _GNU_SOURCE (added per-platform below for POSIX extensions like
# pthread_rwlock_t and io_uring).
# ============================================================================
set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

# Export compile commands for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release build if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ============================================================================
# Platform Detection
# ============================================================================
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(KEEL_PLATFORM_LINUX ON)
    set(KEEL_PLATFORM "linux")
    # Enable GNU extensions for pthread_rwlock_t, io_uring, etc.
    add_compile_definitions(_GNU_SOURCE)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(KEEL_PLATFORM_MACOS ON)
    set(KEEL_PLATFORM "macos")
    set(KEEL_USE_IOURING OFF) # io_uring not available on macOS
    # Apple ld warns when the same static library is listed more than once in the
    # link command (a common side-effect of CMake transitive deps + explicit
    # target_link_libraries listings).  The duplicates are harmless, so silence
    # the noise.  The flag is available since Xcode 15 / Apple LLVM 15.
    add_link_options(-Wl,-no_warn_duplicate_libraries)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(KEEL_PLATFORM_WINDOWS ON)
    set(KEEL_PLATFORM "windows")
    set(KEEL_USE_IOURING OFF) # io_uring not available on Windows
else()
    set(KEEL_PLATFORM "unknown")
endif()

message(STATUS "KEEL: Building for platform: ${KEEL_PLATFORM}")
message(STATUS "KEEL: io_uring support: ${KEEL_USE_IOURING}")
if(KEEL_PLATFORM STREQUAL "macos")
    message(STATUS "KEEL: kqueue support: ${KEEL_USE_KQUEUE}")
else()
    message(STATUS "KEEL: epoll support: ${KEEL_USE_EPOLL}")
endif()

# ============================================================================
# Compiler Flags
# ============================================================================
# Strict warning set — every build is -Werror.  This catches issues at
# compile time rather than in production.  The flag list targets both
# GCC and Clang; GCC-only flags are appended conditionally below.
#
# Notable inclusions:
#   -Wconversion / -Wsign-conversion  — catch implicit narrowing
#   -Wvla / -Walloca                  — ban variable-length arrays
#   -Wdouble-promotion                — catch float→double in printf
#   -Wnull-dereference                — static NULL-deref analysis
# ============================================================================
set(KEEL_WARNING_FLAGS
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    -Wconversion
    -Wsign-conversion
    -Wcast-qual
    -Wformat=2
    -Wundef
    -Wshadow
    -Wwrite-strings
    -Wcast-align
    -Wstrict-prototypes
    -Wmissing-prototypes
    -Wmissing-declarations
    -Wredundant-decls
    -Wnested-externs
    -Winline
    -Wdouble-promotion
    -Wnull-dereference
    -Wvla
    -Walloca
)

# GCC-specific warnings
if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    list(APPEND KEEL_WARNING_FLAGS
        -Wlogical-op
        -Wduplicated-cond
        -Wduplicated-branches
        -Wrestrict
        -Wjump-misses-init
    )
endif()

# ============================================================================
# Sanitizers
# ============================================================================
# Sanitizer flags are applied globally (compile + link) so that every
# translation unit and the final link carry the instrumentation.  Without
# link flags, the sanitizer runtime is not linked, causing unresolved
# symbols or missing interceptors at execution time.
#
# ASan + UBSan can be combined; TSan and MSan are exclusive of each other
# and of ASan.  CMake does not enforce this — the user is responsible for
# not enabling incompatible combinations.
# ============================================================================
set(KEEL_SANITIZER_FLAGS "")

if(KEEL_ENABLE_ASAN)
    list(APPEND KEEL_SANITIZER_FLAGS -fsanitize=address -fno-omit-frame-pointer)
    message(STATUS "KEEL: AddressSanitizer enabled")
endif()

if(KEEL_ENABLE_TSAN)
    list(APPEND KEEL_SANITIZER_FLAGS -fsanitize=thread)
    # Point CTest at the TSAN suppression file
    set(ENV{TSAN_OPTIONS} "suppressions=${CMAKE_SOURCE_DIR}/cmake/tsan_suppressions.txt")
    message(STATUS "KEEL: ThreadSanitizer enabled (suppressions: cmake/tsan_suppressions.txt)")
endif()

if(KEEL_ENABLE_UBSAN)
    list(APPEND KEEL_SANITIZER_FLAGS -fsanitize=undefined)
    message(STATUS "KEEL: UndefinedBehaviorSanitizer enabled")
endif()

if(KEEL_ENABLE_MSAN)
    list(APPEND KEEL_SANITIZER_FLAGS -fsanitize=memory -fno-omit-frame-pointer
        -fsanitize-ignorelist=${CMAKE_SOURCE_DIR}/cmake/msan_ignorelist.txt)
    message(STATUS "KEEL: MemorySanitizer enabled (ignorelist: cmake/msan_ignorelist.txt)")
endif()

if(KEEL_ENABLE_UBSAN)
    # Abort on UB rather than continuing to run with undefined behaviour
    list(APPEND KEEL_SANITIZER_FLAGS -fno-sanitize-recover=all)
endif()

# Default: no fuzz link flags; set to -fsanitize=fuzzer when KEEL_ENABLE_FUZZ=ON
set(KEEL_FUZZ_LINK_FLAGS "")
if(KEEL_ENABLE_FUZZ)
    if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "KEEL_ENABLE_FUZZ requires Clang (libFuzzer is Clang-only)")
    endif()
    # -fsanitize=fuzzer-no-link instruments every TU for coverage without pulling
    # in the fuzzer main().  Each fuzz executable gets -fsanitize=fuzzer at link
    # time to bring in the libFuzzer driver (see tests/CMakeLists.txt).
    # Pair with -DKEEL_ENABLE_ASAN=ON for maximum signal.
    list(APPEND KEEL_SANITIZER_FLAGS -fsanitize=fuzzer-no-link)
    set(KEEL_FUZZ_LINK_FLAGS -fsanitize=fuzzer)
    message(STATUS "KEEL: libFuzzer instrumentation enabled")
endif()

# Apply sanitizer flags globally so every library AND the linker carry them.
# Without the link flags the sanitizer runtime will not be linked in, causing
# unresolved symbols at link time or missing runtime support at execution time.
if(KEEL_SANITIZER_FLAGS)
    add_compile_options(${KEEL_SANITIZER_FLAGS})
    add_link_options(${KEEL_SANITIZER_FLAGS})
endif()

# ============================================================================
# Coverage
# ============================================================================
if(KEEL_ENABLE_COVERAGE)
    # -fprofile-update=atomic prevents negative counter wrap-arounds when
    # multiple threads update the same gcov counter concurrently (the default
    # non-atomic single-word write can race and produce values that wrap below
    # zero, causing lcov geninfo to abort with "Unexpected negative count").
    set(KEEL_COVERAGE_FLAGS --coverage -fprofile-arcs -ftest-coverage -fprofile-update=atomic)
    # Apply globally so every static library and executable is instrumented.
    # Per-target blocks in subdirectories are redundant but harmless.
    add_compile_options(${KEEL_COVERAGE_FLAGS})
    add_link_options(${KEEL_COVERAGE_FLAGS})
    message(STATUS "KEEL: Code coverage enabled")
endif()

# ============================================================================
# Binary Hardening
# ============================================================================
# Enabled by default (KEEL_ENABLE_HARDENING=ON).  Adds compile-time and
# link-time exploit mitigations:
#
#   Compile:
#     -fstack-protector-strong  — stack canaries on functions with buffers
#     -fPIE                     — position-independent executable code
#     -D_FORTIFY_SOURCE=3       — compile-time + run-time buffer overflow checks
#
#   Link:
#     -pie                      — ASLR for the executable
#     -Wl,-z,relro              — read-only relocations (partial RELRO)
#     -Wl,-z,now                — full RELRO (resolve all symbols at load time)
#     -Wl,-z,noexecstack        — non-executable stack
#
# These flags have near-zero performance cost and significantly raise the
# bar for memory-corruption exploits.  Disable with -DKEEL_ENABLE_HARDENING=OFF
# only for profiling builds where PIE overhead matters.
# ============================================================================
if(KEEL_ENABLE_HARDENING)
    set(KEEL_HARDENING_COMPILE_FLAGS
        -fstack-protector-strong
        -fPIE
        -D_FORTIFY_SOURCE=3
    )
    set(KEEL_HARDENING_LINK_FLAGS
        -pie
        -Wl,-z,relro
        -Wl,-z,now
        -Wl,-z,noexecstack
    )
    add_compile_options(${KEEL_HARDENING_COMPILE_FLAGS})
    add_link_options(${KEEL_HARDENING_LINK_FLAGS})
    message(STATUS "KEEL: exploit-mitigation hardening flags enabled")
endif()

# ============================================================================
# Find Dependencies
# ============================================================================
# Required:
#   Threads (pthreads) — worker threads, mutexes, condition variables
#   OpenSSL            — TLS termination, SCRAM-SHA-256 auth, MD5
#
# Optional:
#   liburing — io_uring backend for zero-copy I/O on Linux 5.6+.
#              Discovered via pkg-config first, then manual find_library
#              fallback for systems without a .pc file.
# ============================================================================
find_package(Threads REQUIRED)

# io_uring
if(KEEL_USE_IOURING)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(URING liburing)
    endif()
    
    if(NOT URING_FOUND)
        find_library(URING_LIBRARIES NAMES uring)
        find_path(URING_INCLUDE_DIRS NAMES liburing.h)
        if(URING_LIBRARIES AND URING_INCLUDE_DIRS)
            set(URING_FOUND TRUE)
        endif()
    endif()
    
    if(URING_FOUND)
        message(STATUS "KEEL: Found liburing: ${URING_LIBRARIES}")
    else()
        message(WARNING "KEEL: liburing not found, disabling io_uring backend")
        set(KEEL_USE_IOURING OFF)
    endif()
endif()

# Platform-specific reactor backend detection
# New architecture uses native APIs: io_uring > kqueue > epoll
if(KEEL_PLATFORM STREQUAL "macos")
    # macOS uses kqueue
    set(KEEL_USE_KQUEUE ON)
    set(KEEL_USE_EPOLL OFF)
    set(KEEL_USE_IOURING OFF)
    message(STATUS "KEEL: Using kqueue reactor (macOS)")
elseif(KEEL_PLATFORM STREQUAL "linux")
    # Linux: prefer io_uring if available, fallback to epoll
    if(KEEL_USE_IOURING AND URING_FOUND)
        message(STATUS "KEEL: Using io_uring reactor (Linux 5.6+)")
    else()
        set(KEEL_USE_EPOLL ON)
        message(STATUS "KEEL: Using epoll reactor (Linux fallback)")
    endif()
else()
    message(WARNING "KEEL: Unknown platform, defaulting to epoll")
    set(KEEL_USE_EPOLL ON)
endif()

# OpenSSL (for crypto and TLS)
find_package(OpenSSL REQUIRED)

# libyaml — YAML configuration loader (hard dep since config v2).
# Discovered via pkg-config first (preferred — gives PkgConfig::YAML imported
# target with include dirs + libs), then manual find_library fallback for
# systems without a .pc file.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(YAML IMPORTED_TARGET yaml-0.1)
endif()
if(NOT YAML_FOUND)
    find_library(YAML_LIBRARIES NAMES yaml)
    find_path(YAML_INCLUDE_DIRS NAMES yaml.h)
    if(YAML_LIBRARIES AND YAML_INCLUDE_DIRS)
        set(YAML_FOUND TRUE)
    endif()
endif()
if(NOT YAML_FOUND)
    message(FATAL_ERROR "KEEL: libyaml (yaml-0.1) is required since config v2. "
                        "Install with: apt install libyaml-dev  /  dnf install libyaml-devel  /  brew install libyaml")
endif()
message(STATUS "KEEL: Found libyaml: ${YAML_LIBRARIES}")

# zstd — wire-protocol compression for cluster gossip (WAN / cross-region)
find_library(ZSTD_LIBRARY NAMES zstd)
find_path(ZSTD_INCLUDE_DIR NAMES zstd.h)
if(ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
    set(KEEL_HAS_ZSTD ON)
    message(STATUS "KEEL: libzstd found (${ZSTD_LIBRARY}) — cluster wire compression enabled")
else()
    set(KEEL_HAS_ZSTD OFF)
    message(STATUS "KEEL: libzstd not found — zstd cluster compression disabled (zlib always available)")
endif()

# ─── Optional enterprise authentication dependencies ──────────────────────────
# libldap — LDAP authentication provider
find_library(LDAP_LIBRARY NAMES ldap ldap_r)
find_path(LDAP_INCLUDE_DIR NAMES ldap.h)
if(LDAP_LIBRARY AND LDAP_INCLUDE_DIR)
    set(KEEL_HAS_LDAP ON)
    message(STATUS "KEEL: libldap found (${LDAP_LIBRARY}) — LDAP auth enabled")
else()
    set(KEEL_HAS_LDAP OFF)
    message(STATUS "KEEL: libldap not found — LDAP auth will reject at runtime")
endif()

# libpam — PAM authentication provider
find_library(PAM_LIBRARY NAMES pam)
find_path(PAM_INCLUDE_DIR NAMES security/pam_appl.h)
if(PAM_LIBRARY AND PAM_INCLUDE_DIR)
    set(KEEL_HAS_PAM ON)
    message(STATUS "KEEL: libpam found (${PAM_LIBRARY}) — PAM auth enabled")
else()
    set(KEEL_HAS_PAM OFF)
    message(STATUS "KEEL: libpam not found — PAM auth will reject at runtime")
endif()

# libpq — auth_query provider (password lookup via SQL)
find_library(PQ_LIBRARY NAMES pq)
find_path(PQ_INCLUDE_DIR NAMES libpq-fe.h PATH_SUFFIXES postgresql)
if(PQ_LIBRARY AND PQ_INCLUDE_DIR)
    set(KEEL_HAS_LIBPQ ON)
    message(STATUS "KEEL: libpq found (${PQ_LIBRARY}) — auth_query enabled")
else()
    set(KEEL_HAS_LIBPQ OFF)
    message(STATUS "KEEL: libpq not found — auth_query will reject at runtime")
endif()

# ============================================================================
# Configure Header
# ============================================================================
# Generates include/keel/core/config.h from the cmake/keel_config.h.in
# template.  This header exposes version numbers, platform flags, feature
# toggles, and compiler metadata as C preprocessor macros.  Every source
# file includes it transitively via the public headers.
# ============================================================================
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/keel_config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/include/keel/core/config.h
    @ONLY
)

# ============================================================================
# Include Directories
# ============================================================================
set(KEEL_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
)

# ============================================================================
# Subdirectories
# ============================================================================
# Each add_subdirectory() builds one static library (or the main executable).
# The order matters for CMake target resolution but NOT for link order —
# link order is controlled by each target's target_link_libraries().
#
# Dependency DAG (simplified):
#   mem → util → stats → core → sql → protocol → session → engine → worker
#                                   → log → hook → probe → admin → main
#
# The architecture modules (arch, protocol, session, worker, engine) form
# the new reactor-based pipeline that replaced the legacy libuv layer.
# ============================================================================

# Memory management library
add_subdirectory(src/mem)

# Stats / Instrumentation framework
add_subdirectory(src/stats)

# New architecture: reactor + worker + session + engine
add_subdirectory(src/arch)
add_subdirectory(src/protocol)
add_subdirectory(src/session)
add_subdirectory(src/worker)
add_subdirectory(src/engine)

# Utility library
add_subdirectory(src/util)

# Log plugin framework and query logging
add_subdirectory(src/log)

# SQL parser library
add_subdirectory(src/sql)

# Parser plugin registry and builtin parser plugins
add_subdirectory(src/parser)

# Core library (auth, config, router)
add_subdirectory(src/core)

# Probe system (health checks + failover)
add_subdirectory(src/probe)

# Failover manager (active detector loop + router-epoch reconciliation)
add_subdirectory(src/failover)

# Hook/trigger system (Lua, Python, native plugins)
add_subdirectory(src/hook)

# Admin console + Prometheus metrics
add_subdirectory(src/admin)

# Distributed tracing (W3C context, OTLP export)
add_subdirectory(src/trace)

# OTLP metrics exporter (opt-in; off by default)
if(KEEL_ENABLE_OTLP)
    add_subdirectory(src/observability)
endif()

# Main executable
add_subdirectory(src/main)

# CLI client
add_subdirectory(src/cli)

# Tests
if(KEEL_ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Documentation
if(KEEL_BUILD_DOCS)
    add_subdirectory(docs)
endif()

# ============================================================================
# Install layout for Linux packages
# ============================================================================
# Targets the Filesystem Hierarchy Standard (FHS):
#   /etc/keel/             — configuration files (conffiles)
#   /etc/logrotate.d/      — logrotate config
#   /lib/systemd/system/   — systemd service unit
#   /usr/share/man/        — manual pages (keel.1, keel.ini.5)
#   /usr/share/keel/       — example hooks, documentation
#   /usr/share/doc/keel/   — README, LICENSE, Markdown docs
#
# The keel binary itself is installed by src/main/CMakeLists.txt into
# ${CMAKE_INSTALL_BINDIR} (typically /usr/bin).
# ============================================================================

# --- Configuration ---
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/etc/keel.ini.example
    ${CMAKE_CURRENT_SOURCE_DIR}/etc/keel.yaml.example
    DESTINATION /etc/keel
)

# --- systemd service ---
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/packaging/systemd/keel.service
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system
)

# --- Logrotate ---
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/packaging/logrotate/keel
    DESTINATION /etc/logrotate.d
)

# --- Man pages ---
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/man/keel.1
    DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
)
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/man/keel.ini.5
    DESTINATION ${CMAKE_INSTALL_MANDIR}/man5
)

# --- Example hooks ---
install(DIRECTORY
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/hooks/lua
    DESTINATION ${CMAKE_INSTALL_DATADIR}/keel/examples/hooks
)
install(DIRECTORY
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/hooks/python
    DESTINATION ${CMAKE_INSTALL_DATADIR}/keel/examples/hooks
)

# --- Documentation ---
install(DIRECTORY
    ${CMAKE_CURRENT_SOURCE_DIR}/docs/
    DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/keel
    FILES_MATCHING PATTERN "*.md"
)
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/README.md
    ${CMAKE_CURRENT_SOURCE_DIR}/LICENSE
    DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/keel
)

# ============================================================================
# CPack package configuration (TGZ, DEB, RPM)
# ============================================================================
# Generates distributable packages via `cmake --build . --target package`.
#
# DEB: Debian/Ubuntu .deb with shared-library dependency detection,
#      proper dependency declarations, and maintainer scripts (postinst,
#      prerm, postrm) that manage the keel system user, directories,
#      self-signed certificates, and systemd activation.
# RPM: Red Hat/CentOS/Fedora .rpm with equivalent scriptlets.
# TGZ: Portable tarball for manual deployment (built by package-linux.sh).
#
# The output filename includes the platform and architecture:
#   keel_0.1.0_amd64.deb
#   keel-0.1.0-1.x86_64.rpm
# ============================================================================
set(CPACK_PACKAGE_NAME "keel")
set(CPACK_PACKAGE_VENDOR "virtlabs-io")
set(CPACK_PACKAGE_CONTACT "maintainers@virtlabs.io")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "High-performance database connection pooler")
set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
# Build-variant suffix: 'core' = no scripting; 'full' = Lua and/or Python on.
# Distributors and operators can tell at a glance which package they hold.
if(KEEL_ENABLE_LUA OR KEEL_ENABLE_PYTHON)
    set(KEEL_PACKAGE_VARIANT "full")
else()
    set(KEEL_PACKAGE_VARIANT "core")
endif()
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${KEEL_PACKAGE_VARIANT}-${CPACK_PACKAGE_VERSION}-${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")

set(CPACK_PACKAGING_INSTALL_PREFIX "/usr")
set(CPACK_STRIP_FILES "bin/keel;bin/keel-cli")
set(CPACK_GENERATOR "DEB;RPM")

# --- Debian / Ubuntu (.deb) ------------------------------------------------
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "virtlabs-io <maintainers@virtlabs.io>")
set(CPACK_DEBIAN_PACKAGE_SECTION "database")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)

set(CPACK_DEBIAN_PACKAGE_DEPENDS
    "adduser, openssl, ca-certificates, libyaml-0-2")
set(CPACK_DEBIAN_PACKAGE_RECOMMENDS
    "logrotate")
set(CPACK_DEBIAN_PACKAGE_SUGGESTS
    "lua5.4, python3")

# Maintainer scripts for DEB (CPack uses the file basename as the control name)
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/packaging/scripts/postinst;${CMAKE_CURRENT_SOURCE_DIR}/packaging/scripts/prerm;${CMAKE_CURRENT_SOURCE_DIR}/packaging/scripts/postrm")

# Mark configuration files so dpkg prompts on upgrade conflicts
set(CPACK_DEBIAN_PACKAGE_CONFFILES
    "/etc/keel/keel.ini.example\n/etc/keel/keel.yaml.example\n/etc/logrotate.d/keel")

# --- Red Hat / Fedora (.rpm) ------------------------------------------------
set(CPACK_RPM_PACKAGE_LICENSE "AGPL-3.0-only")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
set(CPACK_RPM_PACKAGE_AUTOREQ ON)

set(CPACK_RPM_PACKAGE_REQUIRES
    "openssl, ca-certificates, libyaml")
set(CPACK_RPM_PACKAGE_SUGGESTS
    "logrotate, lua, python3")

# RPM scriptlets
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/packaging/scripts/postinst")
set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/packaging/scripts/prerm")
set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/packaging/scripts/postrm")

# Do not strip or auto-provide the config and logrotate files
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    "/etc/keel"
    "/etc/logrotate.d")

# Mark config files so RPM preserves user edits
set(CPACK_RPM_USER_FILELIST
    "%config(noreplace) /etc/keel/keel.ini.example"
    "%config(noreplace) /etc/keel/keel.yaml.example"
    "%config(noreplace) /etc/logrotate.d/keel")

include(CPack)

# ============================================================================
# Summary
# ============================================================================
# Printed at the end of cmake configuration to give the developer a quick
# overview of what was detected and enabled.  Useful for CI logs and for
# diagnosing "why isn't feature X working" issues.
# ============================================================================
message(STATUS "")
message(STATUS "=== KEEL Build Configuration ===")
message(STATUS "Version:        ${PROJECT_VERSION}")
message(STATUS "Platform:       ${KEEL_PLATFORM}")
message(STATUS "Build type:     ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler:     ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "C Standard:     C${CMAKE_C_STANDARD}")
message(STATUS "")
message(STATUS "Features:")
message(STATUS "  io_uring:     ${KEEL_USE_IOURING}")
message(STATUS "  kqueue:       ${KEEL_USE_KQUEUE}")
message(STATUS "  epoll:        ${KEEL_USE_EPOLL}")
message(STATUS "  Tests:        ${KEEL_ENABLE_TESTS}")
message(STATUS "  Benchmarks:   ${KEEL_ENABLE_BENCHMARKS}")
message(STATUS "")
message(STATUS "Stats:")
message(STATUS "  System (L3):  ${KEEL_ENABLE_SYSTEM_STATS}")
message(STATUS "  Trace (L4):   ${KEEL_ENABLE_TRACE}")
message(STATUS "  Full (L5):    ${KEEL_ENABLE_FULL_STATS}")
message(STATUS "")
message(STATUS "Sanitizers:")
message(STATUS "  ASan:         ${KEEL_ENABLE_ASAN}")
message(STATUS "  TSan:         ${KEEL_ENABLE_TSAN}")
message(STATUS "  UBSan:        ${KEEL_ENABLE_UBSAN}")
message(STATUS "  MSan:         ${KEEL_ENABLE_MSAN}")
message(STATUS "  Fuzz:         ${KEEL_ENABLE_FUZZ}")
message(STATUS "================================")
message(STATUS "")
