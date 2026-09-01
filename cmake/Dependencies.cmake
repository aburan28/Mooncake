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
