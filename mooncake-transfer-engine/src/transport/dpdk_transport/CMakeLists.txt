file(GLOB DPDK_SOURCES "*.cpp")

add_library(dpdk_transport OBJECT ${DPDK_SOURCES})
target_link_libraries(
  dpdk_transport
  PUBLIC Mooncake::dpdk JsonCpp::JsonCpp yalantinglibs::yalantinglibs
  PRIVATE glog::glog)
# DPDK headers increment volatile counters, which C++20 deprecates.
target_compile_options(dpdk_transport PRIVATE -Wno-volatile)

if(BUILD_UNIT_TESTS)
  target_compile_definitions(dpdk_transport
                             PRIVATE MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS)
endif()
