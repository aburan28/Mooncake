# Build entry points own these targets and call each provider at most once.
# Consumer directories only link the resulting Mooncake::* targets.

function(mooncake_provide_zstd)
  find_path(MOONCAKE_ZSTD_INCLUDE_DIR zstd.h)
  find_library(MOONCAKE_ZSTD_LIBRARY zstd)
  if(NOT MOONCAKE_ZSTD_INCLUDE_DIR OR NOT MOONCAKE_ZSTD_LIBRARY)
    message(FATAL_ERROR "zstd development files not found")
  endif()
  add_library(Mooncake::zstd UNKNOWN IMPORTED)
  set_target_properties(
    Mooncake::zstd
    PROPERTIES IMPORTED_LOCATION "${MOONCAKE_ZSTD_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${MOONCAKE_ZSTD_INCLUDE_DIR}")
endfunction()

function(mooncake_provide_xxhash)
  find_path(MOONCAKE_XXHASH_INCLUDE_DIR xxhash.h)
  find_library(MOONCAKE_XXHASH_LIBRARY NAMES xxhash libxxhash)
  if(NOT MOONCAKE_XXHASH_INCLUDE_DIR OR NOT MOONCAKE_XXHASH_LIBRARY)
    message(FATAL_ERROR "xxHash development files not found")
  endif()
  add_library(Mooncake::xxhash UNKNOWN IMPORTED)
  set_target_properties(
    Mooncake::xxhash
    PROPERTIES IMPORTED_LOCATION "${MOONCAKE_XXHASH_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${MOONCAKE_XXHASH_INCLUDE_DIR}")
endfunction()

function(mooncake_provide_dpdk)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(MOONCAKE_DPDK REQUIRED IMPORTED_TARGET libdpdk)
  add_library(Mooncake::dpdk INTERFACE IMPORTED)
  target_link_libraries(Mooncake::dpdk INTERFACE PkgConfig::MOONCAKE_DPDK)
  # Link-only view for targets that do not compile DPDK headers, so the
  # pkg-config compile flags (-march, -include rte_config.h) stay with the
  # transport objects.
  add_library(Mooncake::dpdk_link INTERFACE IMPORTED)
  set_target_properties(
    Mooncake::dpdk_link
    PROPERTIES INTERFACE_LINK_LIBRARIES "${MOONCAKE_DPDK_LINK_LIBRARIES}"
               INTERFACE_LINK_OPTIONS "${MOONCAKE_DPDK_LDFLAGS_OTHER}")
  # Shared DPDK builds load PMDs as plugins and do not list them in
  # libdpdk.pc; the ring PMD backs the in-process ringpair test ports.
  find_library(MOONCAKE_DPDK_NET_RING rte_net_ring
               HINTS ${MOONCAKE_DPDK_LIBRARY_DIRS})
  if(MOONCAKE_DPDK_NET_RING)
    target_link_libraries(Mooncake::dpdk INTERFACE ${MOONCAKE_DPDK_NET_RING})
    target_compile_definitions(Mooncake::dpdk
                               INTERFACE MOONCAKE_DPDK_HAVE_NET_RING)
    set_property(
      TARGET Mooncake::dpdk_link
      APPEND
      PROPERTY INTERFACE_LINK_LIBRARIES ${MOONCAKE_DPDK_NET_RING})
  endif()
  message(STATUS "DPDK: ${MOONCAKE_DPDK_VERSION}")
endfunction()

function(mooncake_provide_liburing)
  # Several entry points want liburing and any of them may run first, so
  # re-defining the imported target has to be a no-op rather than an error.
  if(TARGET Mooncake::liburing)
    return()
  endif()
  find_path(MOONCAKE_LIBURING_INCLUDE_DIR liburing.h)
  find_library(MOONCAKE_LIBURING_LIBRARY uring)
  if(MOONCAKE_LIBURING_INCLUDE_DIR AND MOONCAKE_LIBURING_LIBRARY)
    add_library(Mooncake::liburing UNKNOWN IMPORTED)
    set_target_properties(
      Mooncake::liburing
      PROPERTIES IMPORTED_LOCATION "${MOONCAKE_LIBURING_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES
                 "${MOONCAKE_LIBURING_INCLUDE_DIR}")
  endif()
endfunction()

# Decides whether the io_uring TCP data plane can be built. The transfer engine
# is configured either from the top-level project or on its own, and both need
# the same answer, so the decision lives here rather than in one of them: it
# provides the imported target the tcp_transport sources link against, turns
# USE_IOURING_TCP off in the caller's scope when the backend cannot be built,
# and defines USE_IOURING_TCP for the caller's directory when it can.
function(mooncake_configure_iouring_tcp)
  if(NOT USE_IOURING_TCP)
    return()
  endif()
  mooncake_provide_liburing()
  if(NOT TARGET Mooncake::liburing)
    set(USE_IOURING_TCP
        OFF
        PARENT_SCOPE)
    message(STATUS "io_uring TCP backend: Disabled (liburing not found)")
    return()
  endif()
  # The backend needs the submission and zero-copy API that liburing grew in
  # 2.3-2.4 (registered rings and sparse tables, 64-bit user data, multishot
  # accept, send_zc, cancel_fd). Build images that ship an older liburing --
  # several wheel builders do -- still find the library, so probe for the API
  # rather than its presence and fall back to the asio backend, which is the
  # same thing that happens at run time when the kernel refuses a ring.
  include(CheckCXXSourceCompiles)
  get_target_property(MOONCAKE_LIBURING_INCLUDE_DIRS Mooncake::liburing
                      INTERFACE_INCLUDE_DIRECTORIES)
  get_target_property(MOONCAKE_LIBURING_LOCATION Mooncake::liburing
                      IMPORTED_LOCATION)
  set(CMAKE_REQUIRED_INCLUDES ${MOONCAKE_LIBURING_INCLUDE_DIRS})
  set(CMAKE_REQUIRED_LIBRARIES ${MOONCAKE_LIBURING_LOCATION})
  check_cxx_source_compiles(
    "
#include <liburing.h>
int main() {
    struct io_uring ring{};
    unsigned flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
                     IORING_SETUP_COOP_TASKRUN;
    (void)flags;
    (void)(IORING_CQE_F_NOTIF | IORING_ASYNC_CANCEL_ALL);
    (void)io_uring_register_ring_fd(&ring);
    (void)io_uring_register_files_sparse(&ring, 1);
    (void)io_uring_register_buffers_sparse(&ring, 1);
    (void)io_uring_register(0, 0, nullptr, 0);
    struct io_uring_sqe sqe{};
    io_uring_sqe_set_data64(&sqe, 0);
    struct io_uring_cqe cqe{};
    (void)io_uring_cqe_get_data64(&cqe);
    io_uring_prep_multishot_accept(&sqe, 0, nullptr, nullptr, 0);
    io_uring_prep_send_zc(&sqe, 0, nullptr, 0, 0, 0);
    io_uring_prep_send_zc_fixed(&sqe, 0, nullptr, 0, 0, 0, 0);
    io_uring_prep_cancel_fd(&sqe, 0, IORING_ASYNC_CANCEL_ALL);
    return 0;
}
"
    MOONCAKE_IOURING_TCP_API_COMPILES)
  if(MOONCAKE_IOURING_TCP_API_COMPILES)
    add_compile_definitions(USE_IOURING_TCP)
    message(STATUS "io_uring TCP backend: Enabled")
  else()
    set(USE_IOURING_TCP
        OFF
        PARENT_SCOPE)
    message(
      STATUS
        "io_uring TCP backend: Disabled (liburing is too old; needs the 2.3+ registered-ring and send_zc API)"
    )
  endif()
endfunction()

function(mooncake_provide_libzmq)
  find_path(MOONCAKE_LIBZMQ_INCLUDE_DIR zmq.h)
  find_library(MOONCAKE_LIBZMQ_LIBRARY NAMES zmq libzmq)
  if(NOT MOONCAKE_LIBZMQ_INCLUDE_DIR OR NOT MOONCAKE_LIBZMQ_LIBRARY)
    message(FATAL_ERROR "libzmq development files not found")
  endif()
  add_library(Mooncake::libzmq UNKNOWN IMPORTED)
  set_target_properties(
    Mooncake::libzmq
    PROPERTIES IMPORTED_LOCATION "${MOONCAKE_LIBZMQ_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${MOONCAKE_LIBZMQ_INCLUDE_DIR}")
endfunction()

function(mooncake_provide_hiredis)
  cmake_parse_arguments(ARG "REQUIRED" "" "" ${ARGN})
  find_path(MOONCAKE_HIREDIS_INCLUDE_DIR hiredis/hiredis.h)
  find_library(MOONCAKE_HIREDIS_LIBRARY hiredis)
  if(ARG_REQUIRED)
    if(NOT MOONCAKE_HIREDIS_INCLUDE_DIR OR NOT MOONCAKE_HIREDIS_LIBRARY)
      message(FATAL_ERROR "hiredis development files not found")
    endif()
  endif()

  if(MOONCAKE_HIREDIS_INCLUDE_DIR AND MOONCAKE_HIREDIS_LIBRARY)
    add_library(Mooncake::hiredis UNKNOWN IMPORTED)
    set_target_properties(
      Mooncake::hiredis
      PROPERTIES IMPORTED_LOCATION "${MOONCAKE_HIREDIS_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES
                 "${MOONCAKE_HIREDIS_INCLUDE_DIR}")
  endif()
endfunction()
