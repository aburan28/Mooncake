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

// End-to-end tests for the dpdk transport on software ports. Two
// TransferEngine instances in this process each own one end of an in-process
// ring PMD pair (MC_DPDK_PORTS=ringpair:0:a / ringpair:0:b, created with
// rte_eth_from_rings), so the whole MKTP path (credits, ACK/NACK,
// retransmission, DONE, range validation) runs without a NIC or hugepages:
// the EAL starts with --no-huge -m 512 --no-pci. Loss recovery is exercised
// through the test-only drop hook. Override MC_DPDK_TEST_PORT_{A,B} and
// MC_DPDK_TEST_IP_{A,B} (and MC_DPDK_EAL_ARGS) to run the same tests on real
// ports.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"

namespace mooncake {
void dpdkTransportSetLossInjectionForTest(uint32_t data_drop_percent,
                                          uint32_t ack_drop_percent) noexcept;
uint64_t dpdkTransportInjectedDropCountForTest() noexcept;
uint64_t dpdkTransportRetransmitCountForTest() noexcept;
void dpdkTransportResetStatsForTest() noexcept;
}  // namespace mooncake

using namespace mooncake;

namespace {

constexpr size_t kPoolSize = 64ull << 20;
constexpr size_t kMiB = 1ull << 20;

const char *envOr(const char *name, const char *fallback) {
    const char *value = std::getenv(name);
    return value && *value ? value : fallback;
}

std::string metadataServer() {
    return envOr("MC_METADATA_SERVER", "P2PHANDSHAKE");
}

void fillPattern(void *buffer, size_t length, uint64_t seed) {
    auto *p = static_cast<uint8_t *>(buffer);
    uint64_t state = seed * 0x9E3779B97F4A7C15ull + 0x632BE59BD9B4E019ull;
    for (size_t i = 0; i < length; ++i) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        p[i] = static_cast<uint8_t>(state >> 24);
    }
}

bool checkPattern(const void *buffer, size_t length, uint64_t seed,
                  size_t *first_bad = nullptr) {
    std::vector<uint8_t> expected(length);
    fillPattern(expected.data(), length, seed);
    const auto *p = static_cast<const uint8_t *>(buffer);
    for (size_t i = 0; i < length; ++i) {
        if (p[i] != expected[i]) {
            if (first_bad) *first_bad = i;
            return false;
        }
    }
    return true;
}

struct Engine {
    std::unique_ptr<TransferEngine> engine;
    void *pool = nullptr;
    Transport::SegmentID peer_segment = 0;
    uint64_t peer_base = 0;
    bool ok = false;

    ~Engine() { reset(); }

    void reset() {
        engine.reset();
        free(pool);
        pool = nullptr;
        ok = false;
    }

    // Installs the dpdk transport from the environment set by the caller.
    void init(const std::string &server_name, const std::string &port,
              const std::string &ip, uint16_t udp_port) {
        setenv("MC_DPDK_PORTS", port.c_str(), 1);
        setenv("MC_DPDK_IP", ip.c_str(), 1);
        setenv("MC_DPDK_UDP_PORT", std::to_string(udp_port).c_str(), 1);
        engine = std::make_unique<TransferEngine>(false);
        auto hp = parseHostNameWithPort(server_name);
        ASSERT_EQ(engine->init(metadataServer(), server_name, hp.first.c_str(),
                               hp.second),
                  0);
        ASSERT_NE(engine->installTransport("dpdk", nullptr), nullptr);
        pool = aligned_alloc(4096, kPoolSize);
        ASSERT_NE(pool, nullptr);
        memset(pool, 0, kPoolSize);
        ASSERT_EQ(engine->registerLocalMemory(pool, kPoolSize, "cpu:0"), 0);
        ok = true;
    }

    void openPeer(const Engine &peer) {
        const std::string name = metadataServer() == P2PHANDSHAKE
                                     ? peer.engine->getLocalIpAndPort()
                                     : peer.name;
        peer_segment = engine->openSegment(name);
        auto desc = engine->getMetadata()->getSegmentDescByID(peer_segment);
        ASSERT_NE(desc, nullptr);
        ASSERT_NE(desc->protocol.find("dpdk"), std::string::npos);
        ASSERT_FALSE(desc->dpdk_ip.empty());
        ASSERT_EQ(desc->buffers.size(), 1u);
        peer_base = desc->buffers[0].addr;
    }

    uint8_t *bytes(size_t offset) {
        return static_cast<uint8_t *>(pool) + offset;
    }
    std::string name;
};

TransferRequest makeRequest(TransferRequest::OpCode op, Engine &from,
                            size_t local_offset, size_t remote_offset,
                            size_t length) {
    TransferRequest request;
    request.opcode = op;
    request.source = from.bytes(local_offset);
    request.target_id = from.peer_segment;
    request.target_offset = from.peer_base + remote_offset;
    request.length = length;
    return request;
}

bool waitBatch(TransferEngine *engine, Transport::BatchID batch_id,
               size_t task_count, std::vector<TransferStatusEnum> &final,
               std::chrono::seconds timeout = std::chrono::seconds(60)) {
    final.assign(task_count, TransferStatusEnum::WAITING);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        bool all = true;
        for (size_t i = 0; i < task_count; ++i) {
            if (final[i] == TransferStatusEnum::COMPLETED ||
                final[i] == TransferStatusEnum::FAILED)
                continue;
            TransferStatus status;
            if (!engine->getTransferStatus(batch_id, i, status).ok())
                return false;
            final[i] = status.s;
            if (status.s != TransferStatusEnum::COMPLETED &&
                status.s != TransferStatusEnum::FAILED)
                all = false;
        }
        if (all) return true;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return false;
}

TransferStatusEnum runOne(Engine &initiator, const TransferRequest &request) {
    auto *engine = initiator.engine.get();
    auto batch_id = engine->allocateBatchID(1);
    if (!engine->submitTransfer(batch_id, {request}).ok()) {
        (void)engine->freeBatchID(batch_id);
        return TransferStatusEnum::FAILED;
    }
    std::vector<TransferStatusEnum> final;
    const bool terminal = waitBatch(engine, batch_id, 1, final);
    if (terminal) (void)engine->freeBatchID(batch_id);
    return terminal ? final[0] : TransferStatusEnum::TIMEOUT;
}

// Both engines live for the whole process: the EAL initializes once, and
// the ring pair is created once and reused across engine restarts.
struct Fixture {
    Engine a, b;

    Fixture() {
        setenv("MC_DPDK_EAL_ARGS", "--no-huge -m 512 --no-pci", 0);
        setenv("MC_DPDK_RTO_US", "200", 0);
        start(a, "A", 0);
        start(b, "B", 1);
        if (a.ok && b.ok) {
            a.openPeer(b);
            b.openPeer(a);
        }
    }

    void start(Engine &engine, const char *tag, int index) {
        const std::string port_env = std::string("MC_DPDK_TEST_PORT_") + tag;
        const std::string ip_env = std::string("MC_DPDK_TEST_IP_") + tag;
        const std::string port = envOr(
            port_env.c_str(), index == 0 ? "ringpair:0:a" : "ringpair:0:b");
        const std::string ip =
            envOr(ip_env.c_str(), index == 0 ? "10.77.0.1" : "10.77.0.2");
        engine.name = index == 0 ? "127.0.0.1:18801" : "127.0.0.1:18802";
        engine.init(engine.name, port, ip, 5555);
    }

    void restart(Engine &engine, const char *tag, int index) {
        engine.reset();
        start(engine, tag, index);
    }
};

Fixture &fx() {
    static Fixture fixture;
    return fixture;
}

class DpdkTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(fx().a.ok && fx().b.ok) << "engine setup failed";
        dpdkTransportSetLossInjectionForTest(0, 0);
    }
    void TearDown() override { dpdkTransportSetLossInjectionForTest(0, 0); }
};

}  // namespace

TEST_F(DpdkTransportTest, WriteAndReadLengthsWithVerification) {
    const size_t lengths[] = {1, 1400, 65536, 2 * kMiB + 1, 16 * kMiB};
    uint64_t seed = 1;
    for (size_t length : lengths) {
        SCOPED_TRACE("length " + std::to_string(length));
        const size_t remote_offset = 4096;
        // WRITE a -> b
        fillPattern(fx().a.bytes(0), length, seed);
        memset(fx().b.bytes(remote_offset), 0, length);
        ASSERT_EQ(runOne(fx().a, makeRequest(TransferRequest::WRITE, fx().a, 0,
                                             remote_offset, length)),
                  TransferStatusEnum::COMPLETED);
        size_t bad = 0;
        ASSERT_TRUE(
            checkPattern(fx().b.bytes(remote_offset), length, seed, &bad))
            << "WRITE corrupted at byte " << bad;

        // READ a <- b into a different local region
        const size_t local_offset = 32 * kMiB;
        memset(fx().a.bytes(local_offset), 0, length);
        ASSERT_EQ(
            runOne(fx().a, makeRequest(TransferRequest::READ, fx().a,
                                       local_offset, remote_offset, length)),
            TransferStatusEnum::COMPLETED);
        ASSERT_TRUE(
            checkPattern(fx().a.bytes(local_offset), length, seed, &bad))
            << "READ corrupted at byte " << bad;

        // WRITE b -> a with b as the initiator
        seed++;
        fillPattern(fx().b.bytes(8 * kMiB), length, seed);
        memset(fx().a.bytes(48 * kMiB), 0, length);
        ASSERT_EQ(runOne(fx().b, makeRequest(TransferRequest::WRITE, fx().b,
                                             8 * kMiB, 48 * kMiB, length)),
                  TransferStatusEnum::COMPLETED);
        ASSERT_TRUE(checkPattern(fx().a.bytes(48 * kMiB), length, seed, &bad))
            << "reverse WRITE corrupted at byte " << bad;
        seed++;
    }
}

TEST_F(DpdkTransportTest, ZeroLengthRequestCompletes) {
    ASSERT_EQ(
        runOne(fx().a, makeRequest(TransferRequest::WRITE, fx().a, 0, 0, 0)),
        TransferStatusEnum::COMPLETED);
}

TEST_F(DpdkTransportTest, ConcurrentTasksInOneBatch) {
    constexpr size_t kTasks = 16;
    constexpr size_t kLength = kMiB;
    std::vector<TransferRequest> writes;
    for (size_t i = 0; i < kTasks; ++i) {
        fillPattern(fx().a.bytes(i * kLength), kLength, 100 + i);
        memset(fx().b.bytes(16 * kMiB + i * kLength), 0, kLength);
        writes.push_back(makeRequest(TransferRequest::WRITE, fx().a,
                                     i * kLength, 16 * kMiB + i * kLength,
                                     kLength));
    }
    auto *engine = fx().a.engine.get();
    auto batch_id = engine->allocateBatchID(kTasks);
    ASSERT_TRUE(engine->submitTransfer(batch_id, writes).ok());
    std::vector<TransferStatusEnum> final;
    ASSERT_TRUE(waitBatch(engine, batch_id, kTasks, final));
    ASSERT_TRUE(engine->freeBatchID(batch_id).ok());
    for (size_t i = 0; i < kTasks; ++i) {
        EXPECT_EQ(final[i], TransferStatusEnum::COMPLETED) << "task " << i;
        EXPECT_TRUE(checkPattern(fx().b.bytes(16 * kMiB + i * kLength), kLength,
                                 100 + i))
            << "task " << i;
    }

    std::vector<TransferRequest> reads;
    for (size_t i = 0; i < kTasks; ++i) {
        memset(fx().a.bytes(32 * kMiB + i * kLength), 0, kLength);
        reads.push_back(makeRequest(TransferRequest::READ, fx().a,
                                    32 * kMiB + i * kLength,
                                    16 * kMiB + i * kLength, kLength));
    }
    batch_id = engine->allocateBatchID(kTasks);
    ASSERT_TRUE(engine->submitTransfer(batch_id, reads).ok());
    ASSERT_TRUE(waitBatch(engine, batch_id, kTasks, final));
    ASSERT_TRUE(engine->freeBatchID(batch_id).ok());
    for (size_t i = 0; i < kTasks; ++i) {
        EXPECT_EQ(final[i], TransferStatusEnum::COMPLETED) << "task " << i;
        EXPECT_TRUE(checkPattern(fx().a.bytes(32 * kMiB + i * kLength), kLength,
                                 100 + i))
            << "task " << i;
    }
}

TEST_F(DpdkTransportTest, BatchOfManySmallSlices) {
    constexpr size_t kTasks = 512;
    constexpr size_t kLength = 4096;
    std::vector<TransferRequest> requests;
    for (size_t i = 0; i < kTasks; ++i) {
        const size_t local = i * kLength;
        const size_t remote = 24 * kMiB + i * kLength;
        // Alternate WRITE and READ so both directions share the batch.
        if (i % 2 == 0) {
            fillPattern(fx().a.bytes(local), kLength, 1000 + i);
            memset(fx().b.bytes(remote), 0, kLength);
            requests.push_back(makeRequest(TransferRequest::WRITE, fx().a,
                                           local, remote, kLength));
        } else {
            fillPattern(fx().b.bytes(remote), kLength, 1000 + i);
            memset(fx().a.bytes(local), 0, kLength);
            requests.push_back(makeRequest(TransferRequest::READ, fx().a, local,
                                           remote, kLength));
        }
    }
    auto *engine = fx().a.engine.get();
    auto batch_id = engine->allocateBatchID(kTasks);
    ASSERT_TRUE(engine->submitTransfer(batch_id, requests).ok());
    std::vector<TransferStatusEnum> final;
    ASSERT_TRUE(waitBatch(engine, batch_id, kTasks, final));
    ASSERT_TRUE(engine->freeBatchID(batch_id).ok());
    for (size_t i = 0; i < kTasks; ++i) {
        EXPECT_EQ(final[i], TransferStatusEnum::COMPLETED) << "task " << i;
        const uint8_t *data = i % 2 == 0 ? fx().b.bytes(24 * kMiB + i * kLength)
                                         : fx().a.bytes(i * kLength);
        EXPECT_TRUE(checkPattern(data, kLength, 1000 + i)) << "task " << i;
    }
}

TEST_F(DpdkTransportTest, RejectsRangesOutsideRegisteredBuffers) {
    // Entirely past the end of the peer's registered pool.
    EXPECT_EQ(runOne(fx().a, makeRequest(TransferRequest::WRITE, fx().a, 0,
                                         kPoolSize, 4096)),
              TransferStatusEnum::FAILED);
    // Straddling the end.
    EXPECT_EQ(runOne(fx().a, makeRequest(TransferRequest::WRITE, fx().a, 0,
                                         kPoolSize - 100, 200)),
              TransferStatusEnum::FAILED);
    // READ from an unregistered remote range.
    EXPECT_EQ(runOne(fx().a, makeRequest(TransferRequest::READ, fx().a, 0,
                                         kPoolSize + 4096, 4096)),
              TransferStatusEnum::FAILED);
    // An address that belongs to no buffer at all.
    TransferRequest bogus =
        makeRequest(TransferRequest::WRITE, fx().a, 0, 0, 64);
    bogus.target_offset = 0x1000;
    EXPECT_EQ(runOne(fx().a, bogus), TransferStatusEnum::FAILED);
    // The transport keeps working afterwards.
    fillPattern(fx().a.bytes(0), 8192, 77);
    ASSERT_EQ(runOne(fx().a, makeRequest(TransferRequest::WRITE, fx().a, 0,
                                         kPoolSize - 8192, 8192)),
              TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(checkPattern(fx().b.bytes(kPoolSize - 8192), 8192, 77));
}

TEST_F(DpdkTransportTest, RecoversFromInjectedLoss) {
    struct Scenario {
        uint32_t data_drop, ack_drop;
        size_t length;
    };
    // The last scenario is larger than the 4 MiB credit window, so lost
    // ACKs also cost credit refreshes and force RTO probes.
    const Scenario scenarios[] = {{5, 5, 8 * kMiB},
                                  {20, 20, 2 * kMiB},
                                  {50, 10, 256 * 1024},
                                  {0, 60, 512 * 1024},
                                  {0, 60, 8 * kMiB}};
    uint64_t seed = 500;
    for (const auto &s : scenarios) {
        SCOPED_TRACE("drop data " + std::to_string(s.data_drop) + "% ack " +
                     std::to_string(s.ack_drop) + "% length " +
                     std::to_string(s.length));
        dpdkTransportResetStatsForTest();
        dpdkTransportSetLossInjectionForTest(s.data_drop, s.ack_drop);
        fillPattern(fx().a.bytes(0), s.length, seed);
        memset(fx().b.bytes(kMiB), 0, s.length);
        ASSERT_EQ(runOne(fx().a, makeRequest(TransferRequest::WRITE, fx().a, 0,
                                             kMiB, s.length)),
                  TransferStatusEnum::COMPLETED);
        size_t bad = 0;
        EXPECT_TRUE(checkPattern(fx().b.bytes(kMiB), s.length, seed, &bad))
            << "WRITE corrupted at byte " << bad;
        memset(fx().a.bytes(32 * kMiB), 0, s.length);
        ASSERT_EQ(runOne(fx().a, makeRequest(TransferRequest::READ, fx().a,
                                             32 * kMiB, kMiB, s.length)),
                  TransferStatusEnum::COMPLETED);
        EXPECT_TRUE(checkPattern(fx().a.bytes(32 * kMiB), s.length, seed, &bad))
            << "READ corrupted at byte " << bad;
        dpdkTransportSetLossInjectionForTest(0, 0);
        const uint64_t dropped = dpdkTransportInjectedDropCountForTest();
        const uint64_t retransmits = dpdkTransportRetransmitCountForTest();
        LOG(INFO) << "loss scenario data " << s.data_drop << "% ack "
                  << s.ack_drop << "% length " << s.length << ": dropped "
                  << dropped << " packets, retransmitted " << retransmits;
        EXPECT_GT(dropped, 0u);
        if (s.data_drop > 0) {
            EXPECT_GT(retransmits, 0u);
        }
        seed++;
    }

    // Concurrent tasks under loss.
    dpdkTransportResetStatsForTest();
    dpdkTransportSetLossInjectionForTest(5, 5);
    constexpr size_t kTasks = 16;
    constexpr size_t kLength = 256 * 1024;
    std::vector<TransferRequest> requests;
    for (size_t i = 0; i < kTasks; ++i) {
        fillPattern(fx().a.bytes(i * kLength), kLength, 900 + i);
        memset(fx().b.bytes(40 * kMiB + i * kLength), 0, kLength);
        requests.push_back(makeRequest(TransferRequest::WRITE, fx().a,
                                       i * kLength, 40 * kMiB + i * kLength,
                                       kLength));
    }
    auto *engine = fx().a.engine.get();
    auto batch_id = engine->allocateBatchID(kTasks);
    ASSERT_TRUE(engine->submitTransfer(batch_id, requests).ok());
    std::vector<TransferStatusEnum> final;
    ASSERT_TRUE(waitBatch(engine, batch_id, kTasks, final));
    ASSERT_TRUE(engine->freeBatchID(batch_id).ok());
    dpdkTransportSetLossInjectionForTest(0, 0);
    for (size_t i = 0; i < kTasks; ++i) {
        EXPECT_EQ(final[i], TransferStatusEnum::COMPLETED) << "task " << i;
        EXPECT_TRUE(checkPattern(fx().b.bytes(40 * kMiB + i * kLength), kLength,
                                 900 + i))
            << "task " << i;
    }
    LOG(INFO) << "loss scenario concurrent: dropped "
              << dpdkTransportInjectedDropCountForTest()
              << " packets, retransmitted "
              << dpdkTransportRetransmitCountForTest();
    EXPECT_GT(dpdkTransportRetransmitCountForTest(), 0u);
}

TEST_F(DpdkTransportTest, BidirectionalConcurrentWrites) {
    constexpr int kIterations = 20;
    constexpr size_t kLength = 512 * 1024;
    std::atomic<int> failures{0};
    auto run = [&](Engine &initiator, size_t local, size_t remote,
                   uint64_t seed) {
        for (int i = 0; i < kIterations; ++i) {
            fillPattern(initiator.bytes(local), kLength, seed + i);
            if (runOne(initiator, makeRequest(TransferRequest::WRITE, initiator,
                                              local, remote, kLength)) !=
                TransferStatusEnum::COMPLETED)
                failures++;
        }
    };
    std::thread t1(run, std::ref(fx().a), 2 * kMiB, 50 * kMiB, 3000);
    std::thread t2(run, std::ref(fx().b), 2 * kMiB, 50 * kMiB, 4000);
    t1.join();
    t2.join();
    EXPECT_EQ(failures.load(), 0);
    EXPECT_TRUE(
        checkPattern(fx().b.bytes(50 * kMiB), kLength, 3000 + kIterations - 1));
    EXPECT_TRUE(
        checkPattern(fx().a.bytes(50 * kMiB), kLength, 4000 + kIterations - 1));
}

TEST_F(DpdkTransportTest, InstallFailsCleanlyOnBadPorts) {
    struct Case {
        const char *ports;
        const char *ips;
    };
    // A PCI address under --no-pci, an already-owned ring end, malformed
    // pseudo-port syntax, and an IP list that does not match the port list.
    // One engine serves every case: a failed install must leave it usable.
    const Case cases[] = {{"0000:ff:1f.7", "10.77.9.1"},
                          {"ringpair:0:a", "10.77.9.2"},
                          {"ringpair:0:c", "10.77.9.3"},
                          {"ringpair:7:a", ""}};
    auto engine = std::make_unique<TransferEngine>(false);
    auto hp = parseHostNameWithPort("127.0.0.1:18803");
    ASSERT_EQ(engine->init(metadataServer(), "127.0.0.1:18803",
                           hp.first.c_str(), hp.second),
              0);
    for (const auto &c : cases) {
        SCOPED_TRACE(c.ports);
        setenv("MC_DPDK_PORTS", c.ports, 1);
        setenv("MC_DPDK_IP", c.ips, 1);
        EXPECT_EQ(engine->installTransport("dpdk", nullptr), nullptr);
    }
}

// Keep last: restarts engine b so its ring end is closed and reopened, then
// checks that transfers still flow.
TEST_F(DpdkTransportTest, EngineRestartReopensPort) {
    fx().restart(fx().b, "B", 1);
    ASSERT_TRUE(fx().b.ok);
    fx().a.engine->syncSegmentCache(fx().b.engine->getLocalIpAndPort());
    fx().a.openPeer(fx().b);
    fx().b.openPeer(fx().a);
    const size_t length = 3 * kMiB;
    fillPattern(fx().a.bytes(0), length, 4242);
    memset(fx().b.bytes(kMiB), 0, length);
    ASSERT_EQ(runOne(fx().a, makeRequest(TransferRequest::WRITE, fx().a, 0,
                                         kMiB, length)),
              TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(checkPattern(fx().b.bytes(kMiB), length, 4242));
    fillPattern(fx().b.bytes(0), length, 4343);
    ASSERT_EQ(runOne(fx().b, makeRequest(TransferRequest::WRITE, fx().b, 0,
                                         kMiB, length)),
              TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(checkPattern(fx().a.bytes(kMiB), length, 4343));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    return RUN_ALL_TESTS();
}
