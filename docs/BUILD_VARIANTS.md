# Build variants

KEEL ships two CMake build variants. Pick one based on whether you need
the embedded Lua / Python hook interpreters.

| Variant | `KEEL_ENABLE_LUA` | `KEEL_ENABLE_PYTHON` | Notes                              |
|---------|-------------------|----------------------|------------------------------------|
| core    | `OFF` (default)   | `OFF` (default)      | Smaller binary, fewer dynamic deps, smaller attack surface. This is the package shipped on releases. |
| full    | `ON`              | `ON`                 | Lua 5.4 + Python 3 embedded for hook scripting. Use for plugin development. |

The hook subsystem (`src/hook/lua_bridge.c`, `src/hook/python_bridge.c`)
provides no-op stubs in `core` builds, so the public hook ABI is
unchanged — registering a Lua or Python hook in a `core` build is a
no-op that returns success and never fires.

## Building locally

```bash
# core (default)
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j$(nproc)

# full
cmake -S . -B build-full -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DKEEL_ENABLE_LUA=ON -DKEEL_ENABLE_PYTHON=ON
cmake --build build-full -j$(nproc)
```

## Building via Docker

```bash
# core
./docker/build-linux.sh test

# full
KEEL_VARIANT=full ./docker/build-linux.sh test
```

## Packaging

`CPACK_PACKAGE_FILE_NAME` includes the variant suffix, and release packaging
can also append the target distro when `KEEL_PACKAGE_DISTRO` is set:

- `keel-core-<version>-Linux-<arch>.deb`
- `keel-full-<version>-Linux-<arch>.deb`
- `keel-core-<version>-debian12-Linux-<arch>.deb`
- `keel-core-<version>-ubuntu24-Linux-<arch>.deb`
- `keel-core-<version>-rocky9-Linux-<arch>.rpm`

Distributors can ship both side by side; the `core` package is the
recommended default.
