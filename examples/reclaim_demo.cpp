#include "smartlease/broker.hpp"
#include "smartlease/execution.hpp"
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <unistd.h>

using namespace smartlease;
int main(int argc, char** argv) {
  try {
    std::string directory;
    if (argc > 1) directory = argv[1];
    else {
      char path[] = "/tmp/smartlease-demo-XXXXXX";
      auto* result = ::mkdtemp(path);
      if (!result) throw std::runtime_error("mkdtemp failed");
      directory = result;
    }
    std::vector<Row> input;
    for (int i = 0; i < 4096; ++i) input.push_back({i % 17, i % 53 - 10});
    OperatorSpec spec;
    spec.identity = {"example", "immutable-snapshot-1", "int64-pair-v1", "group", "sum-count-min-max", "partition-0"};
    spec.kind = OperatorKind::GroupedAggregate;
    PortableOperator baseline(spec, input);
    while (!baseline.finished()) baseline.step(128);

    std::atomic<Time> clock{1};
    Enforcer enforcer([&] { return clock.load(); });
    Broker broker(enforcer, {4000, 65536, 1000000, 1000000});
    broker.configureTenant(1, 1); broker.setForeground({});
    Request request;
    request.tenant = 1; request.demand = {1000, 32768, 100000, 100000};
    request.checkpoint_bytes = 8192; request.estimated_release_ms = 5;
    const auto lease = broker.admitRanked({request}).at(0);
    DurableCoordinator coordinator(directory);
    MorselKey key{"example", "immutable-snapshot-1", "partition-0"};
    auto state = coordinator.begin(key);
    if (state.complete) throw std::runtime_error("use a fresh demo directory");
    auto storage = std::make_unique<PortableOperator>(spec, input);
    // These are real tracked transport buffers. The reference operator's STL
    // allocations are separate host memory; they are not a native DB quota test.
    auto buffer = enforcer.allocate(lease.key, 4096);
    auto io = enforcer.beginIo(lease.key, 128);
    auto permit = enforcer.acquireCpu(lease.key, 100);
    storage->step(1536); permit = {};
    broker.recall(lease.key);
    const auto checkpoint = storage->checkpoint(request.checkpoint_bytes);
    const auto decision = coordinator.handoff(key, state.generation, checkpoint);
    if (decision.result != HandoffResult::HandedOff) throw std::runtime_error("handoff failed");
    auto compute = PortableOperator::resume(spec, input, {}, checkpoint);
    storage.reset();
    while (!compute.finished()) compute.step(128);
    const bool fenced = coordinator.commit(key, state.generation, "old-attempt", baseline.output()) == CommitResult::Fenced;
    if (coordinator.commit(key, decision.generation, "compute-result", compute.output()) != CommitResult::Committed)
      throw std::runtime_error("compute commit failed");
    const bool equal = coordinator.state(key).output == baseline.output();
    const auto charged_before = broker.usage().memory_bytes;
    const bool cleanup_blocked = !enforcer.pollRelease(lease.key).has_value();
    buffer = {}; io = {};
    const auto ack = enforcer.pollRelease(lease.key);
    if (!ack || !broker.acknowledgeRelease(*ack)) throw std::runtime_error("release not verified");
    if (!equal || !fenced || !cleanup_blocked || !broker.usage().empty())
      throw std::runtime_error("integration invariant failed");
    std::cout << "{\n"
              << "  \"input_rows\": " << input.size() << ",\n"
              << "  \"output_groups\": " << compute.output().size() << ",\n"
              << "  \"checkpoint_bytes\": " << checkpoint.size() << ",\n"
              << "  \"whole_result_equal\": true,\n"
              << "  \"old_generation_fenced\": true,\n"
              << "  \"compute_committed_before_cleanup\": true,\n"
              << "  \"memory_charged_before_release_ack\": " << charged_before << ",\n"
              << "  \"memory_charged_after_release_ack\": " << broker.usage().memory_bytes << "\n}\n";
    std::cout << "Durable coordinator files: " << directory << '\n';
    return 0;
  } catch (const std::exception& e) { std::cerr << "smartlease-demo: " << e.what() << '\n'; return 1; }
}
