---
applyTo: "**/*.c,**/*.h,**/CMakeLists.txt"
---

# KEEL C Codebase Conventions

## Header File Placement — MANDATORY

**All `.h` files belong under `include/keel/<subsystem>/`, never inside `src/`.**

```
include/
  keel/
    core/        ← config, auth, admin, cluster, routing, …
    engine/      ← engine, worker, backend_pool, runtime_mode, …
    main/        ← cli, process, security, stats_display, worker_group, config_load
    log/         ← log, log_plugin, query_log, audit_log
    mem/         ← mem
    probe/       ← probe
    protocol/    ← tls_context, tls_auto
    trace/       ← trace
    …
src/             ← .c translation units ONLY; zero .h files permitted here
```

**Rules:**
- When adding a new source module to `src/<subsystem>/`, create its public header at
  `include/keel/<subsystem>/<module>.h`.
- Include the header from `.c` files using the rooted path:
  `#include "keel/<subsystem>/<module>.h"` — never a relative `"../…"` path.
- The CMake include-directory for the `keel` executable and all libraries is
  `${CMAKE_SOURCE_DIR}/include`, so `keel/main/foo.h` resolves correctly from
  any `.c` file in the tree.
- The only exception is the auto-generated `build*/include/keel/core/config.h`
  (build flags / feature detection). Do **not** confuse it with the config-parsing
  API, which lives in `include/keel/core/ini.h`.

## Configuration / INI API

`keel_config_t` and all `keel_config_*` functions are declared in
`keel/core/ini.h`, not in `keel/core/config.h` (which is a CMake-generated
build-flags header). Always include `keel/core/ini.h` when you need the
config-loading or config-querying API.

## CMakeLists.txt for Executables

Add new `.c` files to the `target_sources(keel PRIVATE …)` list in
`src/main/CMakeLists.txt`. Do not create new CMake targets for files that are
part of the same executable.

## Coding Style Reminders

- Functions extracted to a new module must lose the `static` qualifier if they
  are declared in the corresponding header (i.e., called from other translation
  units).
- Internal helpers that are **not** declared in the header must keep `static`.
- Raw `free()` calls need a trailing `/* NOLINT(keel-syscall) */` annotation or
  the `check_forbidden_syscalls` CTest gate will fail.
