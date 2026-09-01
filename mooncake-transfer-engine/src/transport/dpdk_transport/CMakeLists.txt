file(GLOB DPDK_SOURCES "*.cpp")

add_library(dpdk_transport OBJECT ${DPDK_SOURCES})
target_link_libraries(
  dpdk_transport
  PUBLIC Mooncake::dpdk JsonCpp::JsonCpp yalantinglibs::yalantinglibs
  PRIVATE glog::glog)
