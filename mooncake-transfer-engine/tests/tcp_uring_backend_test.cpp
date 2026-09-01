// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstdlib>

#include "transport/tcp_transport/tcp_transport.h"
#include "transport/tcp_transport/tcp_wire.h"

namespace mooncake {

TEST(TcpIoBackendSelection, DefaultsToAsio) {
    unsetenv("MC_TCP_IO_BACKEND");
    EXPECT_EQ(TcpTransport::parseIoBackendEnv(),
              TcpTransport::IoBackend::ASIO);
}

TEST(TcpIoBackendSelection, AcceptsIoUringSpellings) {
    for (const char* spelling : {"io_uring", "IO_URING", "uring", "iouring"}) {
        setenv("MC_TCP_IO_BACKEND", spelling, 1);
        EXPECT_EQ(TcpTransport::parseIoBackendEnv(),
                  TcpTransport::IoBackend::IO_URING)
            << spelling;
    }
    unsetenv("MC_TCP_IO_BACKEND");
}

TEST(TcpIoBackendSelection, RejectsUnknownValue) {
    setenv("MC_TCP_IO_BACKEND", "dpdk", 1);
    EXPECT_EQ(TcpTransport::parseIoBackendEnv(),
              TcpTransport::IoBackend::ASIO);
    unsetenv("MC_TCP_IO_BACKEND");
}

TEST(TcpWire, HeaderAndStatusFrameSizes) {
    // Both backends send the header struct raw; the wire size must never
    // change without a protocol version bump.
    EXPECT_EQ(tcp_wire::kSessionHeaderWireSize, sizeof(uint64_t) * 3);
    EXPECT_EQ(tcp_wire::kStatusFrameSize, 8u);
    EXPECT_TRUE(tcp_wire::statusFrameValid(tcp_wire::kStatusOk));
    EXPECT_TRUE(tcp_wire::statusFrameValid(tcp_wire::kStatusAddrRejected));
    EXPECT_FALSE(tcp_wire::statusFrameValid(0));
}

}  // namespace mooncake
