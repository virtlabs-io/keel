#
# KEEL Hook Subsystem Build Definition
# Copyright (c) Virtlabs, https://virtlabs.io
# License: GNU Affero General Public License v3.0
# Author: Charly Batista
#
# Builds the registry core and the optional scripting bridges used to execute
# Lua and Python hooks. The base hook library is always compiled; optional
# interpreters are wired in only when explicitly enabled and discovered.

# Core hook library (always built)
add_library(keel_hook STATIC
    hook.c
    lua_bridge.c
    python_bridge.c
)

target_include_directories(keel_hook
    PUBLIC  ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_BINARY_DIR}/include
)

target_link_libraries(keel_hook
    PUBLIC  keelmem
    PUBLIC  keellog
    PRIVATE keelutil
    PRIVATE ${CMAKE_DL_LIBS}   # dlopen/dlsym
)

# ============================================================================
# Optional: Lua support
# ============================================================================
if(KEEL_ENABLE_LUA)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        # Try LuaJIT first, then Lua 5.4.
        # IMPORTED_TARGET creates PkgConfig::<name> which carries include dirs,
        # library dirs (-L), and link libraries (-l) as a single target — this
        # correctly handles distros that install Lua in non-standard paths
        # (e.g. Alpine puts liblua.so in /usr/lib/lua5.4/).
        pkg_search_module(LUAJIT QUIET IMPORTED_TARGET luajit)
        if(LUAJIT_FOUND)
            target_compile_definitions(keel_hook PRIVATE KEEL_ENABLE_LUA)
            target_link_libraries(keel_hook PRIVATE PkgConfig::LUAJIT)
            message(STATUS "Lua hooks: enabled (LuaJIT ${LUAJIT_VERSION})")
        else()
            pkg_search_module(LUA54 QUIET IMPORTED_TARGET lua5.4 lua-5.4 lua54 lua)
            if(LUA54_FOUND)
                target_compile_definitions(keel_hook PRIVATE KEEL_ENABLE_LUA)
                target_link_libraries(keel_hook PRIVATE PkgConfig::LUA54)
                message(STATUS "Lua hooks: enabled (Lua ${LUA54_VERSION})")
            else()
                message(WARNING "KEEL_ENABLE_LUA=ON but Lua not found — disabling")
            endif()
        endif()
    else()
        # Fallback: try find_package
        find_package(Lua QUIET)
        if(LUA_FOUND)
            target_compile_definitions(keel_hook PRIVATE KEEL_ENABLE_LUA)
            target_include_directories(keel_hook PRIVATE ${LUA_INCLUDE_DIR})
            target_link_libraries(keel_hook PRIVATE ${LUA_LIBRARIES})
            message(STATUS "Lua hooks: enabled (Lua ${LUA_VERSION_STRING})")
        else()
            message(WARNING "KEEL_ENABLE_LUA=ON but Lua not found — disabling")
        endif()
    endif()
else()
    message(STATUS "Lua hooks: disabled")
endif()

# ============================================================================
# Optional: Python support
# ============================================================================
if(KEEL_ENABLE_PYTHON)
    find_package(Python3 COMPONENTS Development QUIET)
    if(Python3_FOUND)
        target_compile_definitions(keel_hook PRIVATE KEEL_ENABLE_PYTHON)
        target_include_directories(keel_hook PRIVATE ${Python3_INCLUDE_DIRS})
        target_link_libraries(keel_hook PRIVATE ${Python3_LIBRARIES})
        message(STATUS "Python hooks: enabled (Python ${Python3_VERSION})")
    else()
        message(WARNING "KEEL_ENABLE_PYTHON=ON but Python3 dev not found — disabling")
    endif()
else()
    message(STATUS "Python hooks: disabled")
endif()

# Compiler settings
target_compile_options(keel_hook PRIVATE
    -Wall -Wextra
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O2>
)

set_target_properties(keel_hook PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
)
