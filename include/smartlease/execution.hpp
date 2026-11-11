#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace smartlease {

// Portable reference semantics: signed 64-bit keys/values, no NULLs, exact SUM
// with overflow reported as an exception; filter is value >= minimum_value.
struct Row {
    std::int64_t key = 0, value = 0;
    bool operator==(const Row& r) const { return key == r.key && value == r.value; }
};
struct OutputRow {
    std::int64_t key = 0, value = 0, build_value = 0;
    std::uint64_t count = 0;
    std::int64_t minimum = 0, maximum = 0;
    bool operator==(const OutputRow& r) const;
};
struct ExecutionIdentity {
    std::string query, snapshot, schema, operator_id, expression_id, partition;
    bool operator==(const ExecutionIdentity& r) const;
};
enum class OperatorKind : std::uint8_t { ScanFilter = 1, GroupedAggregate = 2, PartitionedHashJoin = 3 };
struct OperatorLimits {
    std::size_t max_input_rows = 1000000;
    std::size_t max_output_rows = 1000000;
    std::size_t max_groups = 100000;
    std::size_t max_checkpoint_bytes = 16 * 1024 * 1024;
};
struct OperatorSpec {
    ExecutionIdentity identity;
    OperatorKind kind = OperatorKind::ScanFilter;
    std::int64_t minimum_value = std::numeric_limits<std::int64_t>::min();
    std::uint64_t begin = 0;
    // UINT64_MAX means input.size(). Partitioning uses uint64(key) % partitions.
    std::uint64_t end = std::numeric_limits<std::uint64_t>::max();
    std::uint32_t partitions = 1, partition_index = 0;
    OperatorLimits limits;
};
class PortableOperator {
public:
    PortableOperator(OperatorSpec spec, std::vector<Row> input,
                     std::vector<Row> immutable_build = {});
    ~PortableOperator();
    PortableOperator(PortableOperator&&) noexcept;
    PortableOperator& operator=(PortableOperator&&) noexcept;
    PortableOperator(const PortableOperator&) = delete;
    PortableOperator& operator=(const PortableOperator&) = delete;
    // At most budget input inspections, duplicate matches, or aggregate emits.
    std::size_t step(std::size_t budget);
    bool finished() const;
    const std::vector<OutputRow>& output() const; // staged, never published here
    const OperatorSpec& spec() const;
    std::vector<std::uint8_t> checkpoint(std::size_t max_bytes = 0) const;
    static PortableOperator resume(OperatorSpec spec, std::vector<Row> input,
                                   std::vector<Row> immutable_build,
                                   const std::vector<std::uint8_t>& checkpoint,
                                   std::size_t max_bytes = 0);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

struct MorselKey {
    std::string query, snapshot, morsel;
    bool operator==(const MorselKey& r) const;
    bool operator<(const MorselKey& r) const;
};
struct MorselState {
    MorselKey key;
    std::uint64_t generation = 1;
    bool complete = false;
    std::string commit_id;
    std::vector<std::uint8_t> checkpoint;
    std::vector<OutputRow> output; // empty until complete
};
enum class CommitResult { Committed, AlreadyCommitted, Fenced, Conflict };
enum class HandoffResult { HandedOff, AlreadyCommitted, Fenced };
struct HandoffDecision {
    HandoffResult result;
    std::uint64_t generation;
};
struct CoordinatorLimits {
    std::size_t max_morsels = 100000;
    std::size_t max_output_rows = 1000000;
    std::size_t max_checkpoint_bytes = 16 * 1024 * 1024;
    std::size_t max_store_bytes = 256 * 1024 * 1024;
};
// Single-process writer (thread safe) plus POSIX flock, atomic rename, file and
// directory fsync. Throws on unsupported OS, corruption, or durability errors.
// Reopening fences all unfinished attempts durably before exposing any state.
class DurableCoordinator {
public:
    explicit DurableCoordinator(std::string directory, CoordinatorLimits limits = {});
    ~DurableCoordinator();
    DurableCoordinator(const DurableCoordinator&) = delete;
    DurableCoordinator& operator=(const DurableCoordinator&) = delete;
    MorselState begin(const MorselKey& key); // durable, idempotent registration
    MorselState state(const MorselKey& key) const;
    std::vector<MorselState> states() const;
    CommitResult commit(const MorselKey& key, std::uint64_t generation,
                        const std::string& commit_id, const std::vector<OutputRow>& output);
    // Empty checkpoint means replay from the original morsel boundary. Nonempty
    // images must be valid PortableOperator checkpoints bound to this key.
    HandoffDecision handoff(const MorselKey& key, std::uint64_t generation,
                            const std::vector<std::uint8_t>& checkpoint = {});
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace smartlease
