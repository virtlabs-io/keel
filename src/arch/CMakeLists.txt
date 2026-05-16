# =============================================================================
# src/arch/CMakeLists.txt — Platform-specific reactor backends
# =============================================================================
#
# Builds the I/O reactor abstraction layer that sits between the operating
# system’s async I/O API and the Keel worker loop.  The architecture has
# two layers:
#
#   keel_arch_common    — Platform-independent reactor interface, factory,
#                         and shared utilities (reactor_common.c).
#
#   keel_arch_platform  — Platform-specific implementations:
#     Linux:  io_uring (preferred, requires liburing + kernel 5.6+)
#             epoll (fallback, always available)
#             splice(2) for zero-copy data transfer
#     macOS:  kqueue
#     BSD:    kqueue (same source as macOS)
#
# Feature detection:
#   The CMake file probes for progressively newer io_uring capabilities:
#     KEEL_HAS_IOURING             — basic io_uring support (liburing found)
#     KEEL_HAS_URING_BUF_RING     — provided-buffer rings (liburing 2.2+, Linux 5.19+)
#     KEEL_HAS_URING_RECV_MULTISHOT — multishot recv (liburing 2.3+, Linux 6.0+)
#   These flags are defined PUBLIC so all downstream targets see them.
#
# The two targets are combined into a single INTERFACE target (keel_arch)
# for convenient linking by the engine and worker layers.
# =============================================================================

# Common reactor code
add_library(keel_arch_common STATIC
    common/reactor_common.c
)

target_include_directories(keel_arch_common
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(keel_arch_common
    PUBLIC keelutil
)

# Platform-specific code
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Linux: io_uring and epoll reactors, and splice operations
    add_library(keel_arch_platform STATIC
        linux/reactor_iouring.c
        linux/reactor_epoll.c
        linux/io_splice.c
    )
    
    target_include_directories(keel_arch_platform
        PUBLIC ${PROJECT_SOURCE_DIR}/include
    )
    
    # Find liburing
    find_library(LIBURING_LIBRARY uring)
    if(LIBURING_LIBRARY)
        # KEEL_HAS_IOURING must be visible to keel_arch_common (reactor_common.c
        # uses it for the factory switch and detection) as well as to all
        # downstream consumers, so we define it PUBLIC on both targets.
        target_compile_definitions(keel_arch_common PUBLIC KEEL_HAS_IOURING)
        target_link_libraries(keel_arch_platform
            PUBLIC keel_arch_common
            PUBLIC ${LIBURING_LIBRARY}
        )
        target_compile_definitions(keel_arch_platform PUBLIC KEEL_HAS_IOURING)

        # Detect buf ring support (io_uring_register_buf_ring, liburing 2.2+ / Linux 5.19+)
        # These are static inline functions in liburing.h, so check_symbol_exists
        # (which looks for linker symbols) won't find them.  Use compile checks.
        include(CheckCSourceCompiles)
        set(CMAKE_REQUIRED_LIBRARIES ${LIBURING_LIBRARY})
        set(CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
        check_c_source_compiles("
            #include <signal.h>
            #include <fcntl.h>
            #include <sys/types.h>
            #include <liburing.h>
            int main(void) {
                struct io_uring ring;
                struct io_uring_buf_reg reg = {0};
                io_uring_register_buf_ring(&ring, &reg, 0);
                return 0;
            }
        " HAVE_URING_BUF_RING)
        if(HAVE_URING_BUF_RING)
            target_compile_definitions(keel_arch_platform PUBLIC KEEL_HAS_URING_BUF_RING)
            target_compile_definitions(keel_arch_common   PUBLIC KEEL_HAS_URING_BUF_RING)
            message(STATUS "io_uring buf ring support detected (liburing 2.2+)")

            # Detect multishot recv (io_uring_prep_recv_multishot, liburing 2.3+ / Linux 6.0+)
            check_c_source_compiles("
                #include <signal.h>
                #include <fcntl.h>
                #include <sys/types.h>
                #include <liburing.h>
                int main(void) {
                    struct io_uring_sqe sqe = {0};
                    io_uring_prep_recv_multishot(&sqe, 0, 0, 0, 0);
                    return 0;
                }
            " HAVE_URING_RECV_MULTISHOT)
            if(HAVE_URING_RECV_MULTISHOT)
                target_compile_definitions(keel_arch_platform PUBLIC KEEL_HAS_URING_RECV_MULTISHOT)
                target_compile_definitions(keel_arch_common   PUBLIC KEEL_HAS_URING_RECV_MULTISHOT)
                message(STATUS "io_uring multishot recv support detected (liburing 2.3+)")
            else()
                message(STATUS "io_uring multishot recv NOT detected -- "
                               "upgrade liburing to 2.3+ for KEEL_HAS_URING_RECV_MULTISHOT")
            endif()
        else()
            message(STATUS "io_uring buf ring support NOT detected -- "
                           "upgrade liburing to 2.2+ for KEEL_HAS_URING_BUF_RING")
        endif()
        unset(CMAKE_REQUIRED_DEFINITIONS)
        unset(CMAKE_REQUIRED_LIBRARIES)
    else()
        message(WARNING "liburing not found - io_uring support disabled")
        target_link_libraries(keel_arch_platform
            PUBLIC keel_arch_common
        )
    endif()
    
    # Epoll is always available on Linux
    target_compile_definitions(keel_arch_platform PUBLIC KEEL_HAS_EPOLL)
    
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # macOS: kqueue reactor
    add_library(keel_arch_platform STATIC
        macos/reactor_kqueue.c
    )
    
    target_include_directories(keel_arch_platform
        PUBLIC ${PROJECT_SOURCE_DIR}/include
    )
    
    target_link_libraries(keel_arch_platform
        PUBLIC keel_arch_common
    )
    
    target_compile_definitions(keel_arch_platform PUBLIC KEEL_HAS_KQUEUE)
    
elseif(CMAKE_SYSTEM_NAME MATCHES "BSD")
    # BSD: kqueue reactor
    add_library(keel_arch_platform STATIC
        macos/reactor_kqueue.c  # Same implementation works on BSD
    )
    
    target_include_directories(keel_arch_platform
        PUBLIC ${PROJECT_SOURCE_DIR}/include
    )
    
    target_link_libraries(keel_arch_platform
        PUBLIC keel_arch_common
    )
    
    target_compile_definitions(keel_arch_platform PUBLIC KEEL_HAS_KQUEUE)
    
else()
    message(FATAL_ERROR "Unsupported platform: ${CMAKE_SYSTEM_NAME}")
endif()

# Combine into single arch library
add_library(keel_arch INTERFACE)
target_link_libraries(keel_arch
    INTERFACE keel_arch_common
    INTERFACE keel_arch_platform
)
