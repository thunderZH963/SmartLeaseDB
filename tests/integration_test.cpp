#include "smartlease/broker.hpp"
#include "smartlease/execution.hpp"
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <unistd.h>

using namespace smartlease;
namespace {
void check(bool value, const char* why) { if (!value) throw std::runtime_error(why); }
struct Temp {
  std::string dir;
  Temp() { char p[] = "/tmp/smartlease-integration-XXXXXX"; auto* v = ::mkdtemp(p); if (!v) throw std::runtime_error("mkdtemp"); dir = v; }
  ~Temp() { std::error_code ignored; std::filesystem::remove_all(dir, ignored); }
};
void executeCase(OperatorKind kind, bool kill_replay) {
  std::vector<Row> input, build;
  for (int i = 0; i < 200; ++i) input.push_back({i % 7, i - 50});
  for (int i = 0; i < 21; ++i) build.push_back({i % 7, i * 3});
  OperatorSpec spec;
  spec.kind = kind;
  spec.identity = {"test-query", "snapshot-7", "i64x2", "op", "expr", "morsel-0"};
  spec.minimum_value = -10;
  PortableOperator uninterrupted(spec, input, kind == OperatorKind::PartitionedHashJoin ? build : std::vector<Row>{});
  while (!uninterrupted.finished()) uninterrupted.step(11);
  Temp temp;
  std::uint64_t committed_generation = 0;
  {
    DurableCoordinator coordinator(temp.dir);
    const MorselKey key{"test-query", "snapshot-7", "morsel-0"};
    const auto initial = coordinator.begin(key);
    std::atomic<Time> clock{1}; Enforcer enforcer([&] { return clock.load(); });
    Broker broker(enforcer, {4000, 65536, 1000000, 1000000});
    broker.configureTenant(1, 1); broker.setForeground({});
    Request r; r.tenant = 1; r.demand = {1000, 32768, 100000, 100000}; r.checkpoint_bytes = 16384;
    auto lease = broker.admitRanked({r}).at(0);
    auto source = std::make_unique<PortableOperator>(spec, input, kind == OperatorKind::PartitionedHashJoin ? build : std::vector<Row>{});
    auto network_buffer = enforcer.beginSend(lease.key, 256);
    auto memory = enforcer.allocate(lease.key, 1024);
    auto cpu = enforcer.acquireCpu(lease.key, 100);
    source->step(73); cpu = {};
    const auto image = kill_replay ? std::vector<std::uint8_t>{} : source->checkpoint(r.checkpoint_bytes);
    // Expiry is independent of the coordinator and leaves in-flight bytes held.
    clock.store(lease.expires_at); enforcer.tick();
    check(!enforcer.canExecute(lease.key), "expired storage cannot execute");
    auto handoff = coordinator.handoff(key, initial.generation, image);
    check(handoff.result == HandoffResult::HandedOff, "placement generation advanced");
    check(coordinator.commit(key, initial.generation, "late", uninterrupted.output()) == CommitResult::Fenced, "late output fenced");
    auto resumed = image.empty() ? PortableOperator(spec, input, kind == OperatorKind::PartitionedHashJoin ? build : std::vector<Row>{}) :
        PortableOperator::resume(spec, input, kind == OperatorKind::PartitionedHashJoin ? build : std::vector<Row>{}, image);
    source.reset();
    while (!resumed.finished()) resumed.step(9);
    check(resumed.output() == uninterrupted.output(), "whole result, including duplicate matches, must agree");
    check(coordinator.commit(key, handoff.generation, "computed", resumed.output()) == CommitResult::Committed, "compute may commit before cleanup");
    check(!enforcer.pollRelease(lease.key) && broker.usage() == lease.quota, "query completion is not release evidence");
    memory = {}; check(!enforcer.pollRelease(lease.key), "queued sends alone keep resource charged");
    network_buffer = {};
    auto receipt = enforcer.pollRelease(lease.key);
    check(receipt && broker.acknowledgeRelease(*receipt) && broker.usage().empty(), "actual cleanup + ACK permit reuse");
    committed_generation = handoff.generation;
  }
  DurableCoordinator recovered(temp.dir);
  const MorselKey key{"test-query", "snapshot-7", "morsel-0"};
  check(recovered.state(key).complete && recovered.state(key).output == uninterrupted.output(), "committed result survives coordinator restart");
  check(recovered.commit(key, committed_generation, "computed", uninterrupted.output()) == CommitResult::AlreadyCommitted, "commit retry after restart is idempotent");
}
}
int main() {
  try {
    for (auto kind : {OperatorKind::ScanFilter, OperatorKind::GroupedAggregate, OperatorKind::PartitionedHashJoin}) {
      executeCase(kind, false); executeCase(kind, true);
    }
    std::cout << "integration: 3 operator families x checkpoint/replay, expiry, late-output fencing, cleanup and durable results passed\n";
    return 0;
  } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
