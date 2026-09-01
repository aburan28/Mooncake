file(GLOB TCP_SOURCES "*.cpp")
file(GLOB TCP_IMPL_HEADERS "*_impl.h")

# The io_uring data plane needs liburing; without it only the asio backend is
# compiled and MC_TCP_IO_BACKEND=io_uring degrades with a warning.
if(NOT USE_IOURING_TCP)
  list(FILTER TCP_SOURCES EXCLUDE REGEX "tcp_uring_.*\\.cpp$")
  list(FILTER TCP_SOURCES EXCLUDE REGEX "tcp_zero_copy\\.cpp$")
endif()

if (USE_HIP)
  list(APPEND TCP_SOURCES ${TCP_IMPL_HEADERS})
  hipify_files(TCP_SOURCES)
endif()

add_library(tcp_transport OBJECT ${TCP_SOURCES})
target_link_libraries(
  tcp_transport
  PUBLIC asio_shared JsonCpp::JsonCpp yalantinglibs::yalantinglibs
  PRIVATE glog::glog)

if(BUILD_UNIT_TESTS)
  target_compile_definitions(tcp_transport
                             PRIVATE MOONCAKE_TCP_TRANSPORT_TEST_HOOKS)
endif()

if(USE_IOURING_TCP)
  target_link_libraries(tcp_transport PUBLIC Mooncake::liburing)
endif()
