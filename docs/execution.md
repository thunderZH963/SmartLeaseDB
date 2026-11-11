# Portable execution and durable commitment

This module is a dependency-free C++17 reference implementation of the logical execution contract. It does not implement SQL, TapDB internals, device execution, physical memory release, or a timing guarantee. The durable coordinator requires a local POSIX filesystem that honors atomic rename, `flock`, and `fsync`. Unsupported platforms fail explicitly.

## Reference operator semantics

`Row` contains two signed 64-bit fields (`key`, `value`), with no NULL representation. Every operator applies `value >= minimum_value` and the selected partition before processing the row. Partition membership is `uint64_t(key) % partitions == partition_index`, including negative keys. `[begin,end)` selects a bounded probe/input range; an omitted `end` uses the input length.

- `ScanFilter` emits `{key,value,0,0,0,0}` in input order.
- `GroupedAggregate` emits one `OutputRow` per key, in ascending signed-key order. `value` is exact SUM, `count` is COUNT, and `minimum`/`maximum` are MIN/MAX. `build_value` is zero. SUM overflow throws before advancing that row or altering its group; it does not wrap or saturate. Aggregates emit only after consuming the input range.
- `PartitionedHashJoin` is an inner equijoin on `key`. `value` contains the probe value and `build_value` the build value; aggregate fields are zero. It preserves probe order and build-input order within duplicate keys, including identical duplicate rows. Build input is copied into an immutable snapshot at construction; resume receives the same immutable build snapshot and reconstructs the selected partition's hash index.

`step(budget)` performs at most `budget` work units. A unit inspects an input row, emits a single duplicate join match, or emits one aggregate group. Duplicate-heavy joins therefore stop in the middle of one probe row. This bounds logical work per call; it is not a wall-clock bound. Allocation, hashing, serialization, and rebuilding a build partition still have costs. Input, output, groups, and checkpoint bytes have explicit configurable limits. Exceeding the output or group envelope requires caller-directed fallback or smaller morsels, and never silently drops rows.

## Portable checkpoints

`checkpoint()` returns an explicit little-endian binary format, rather than a C++ object dump. The image contains:

1. Eight-byte `SLCKPT01` magic and a 32-bit format version.
2. Length-prefixed query, snapshot, schema, operator, expression, and input-partition identities.
3. Operator kind, predicate value, input range, partition count and selected partition.
4. Input/build row counts and content fingerprints; first unfinished input cursor; next duplicate-match index.
5. Sorted logical aggregate state and the complete staged output sequence.
6. A trailing 64-bit FNV-1a checksum over all preceding bytes.

Integers use fixed-width encodings. Signed integers use a defined two's-complement wire representation, decoded without out-of-range unsigned-to-signed casts. IDs are nonempty, NUL-free byte strings of at most 4096 bytes. Collection counts are checked against both configured limits and remaining encoded bytes before allocation. The decoder rejects unknown versions/kinds, bad checksums, truncation, trailing bytes, invalid ranges/partitions/cursors, duplicate group keys, inconsistent aggregate counts/output, inappropriate operator state, and invalid partial join progress. Resume additionally compares every identity and semantic parameter, validates immutable input/build fingerprints, and checks partially emitted duplicate matches against the rebuilt index.

Checksums/fingerprints detect accidental corruption; they are **not cryptographic authentication**. This reference assumes trusted workers and immutable snapshot storage. It does not provide a defense against an actor deliberately rewriting a valid image and recomputing its checksum. Operators do not recompute the consumed prefix merely to resume. Identity strings must identify the intended semantics; passing new input under an old snapshot identity is rejected when its content fingerprint differs.

All output remains staged until the entire morsel commits. A resumed instance inherits staged rows as well as progress, so a cursor never discards uncommitted results. `output()` is available for inspection but is not publication. The caller should call `commit()` only after `finished()` is true. Pipelined downstream publication would require an additional batch commitment protocol and is not implemented.

## Coordinator protocol

`DurableCoordinator(directory)` owns an exclusive process lock and serializes calls across threads. The directory's parent must already exist. The directory itself is created if needed. A `MorselKey` is `(query,snapshot,morsel)`; checkpoint `identity.partition` must match `morsel`.

- `begin(key)` durably registers generation 1, or returns the existing state without changing it.
- `handoff(key,g,checkpoint)` validates the image, advances placement generation, and stores the image atomically. An empty image explicitly requests replay from the original morsel boundary. Stale attempts are fenced. Retrying with the old generation returns `Fenced`; the caller can inspect the recorded current state. This does not allocate another generation.
- `commit(key,g,id,rows)` persists both output rows and completion in one snapshot. It exposes the rows only after persistence succeeds. A retry with the same generation, commit ID, and identical rows returns `AlreadyCommitted`; conflicting IDs or payloads return `Conflict`. Any generation mismatch returns `Fenced`.
- Commit and handoff have one serialized order. If commit wins, handoff reports `AlreadyCommitted`. If handoff wins, the old commit is fenced.
- Opening an existing coordinator validates the whole store, advances **every unfinished** morsel's generation, and persists those fences before returning. Completed morsels retain their generation, commit ID, and output; a lost successful reply can be retried after restart. Unfinished morsels preserve their last accepted checkpoint for resume.

Each update encodes a bounded complete store into `state.tmp`, fsyncs that file, atomically renames it to `state.bin`, and fsyncs the containing directory. The initial creation also fsyncs the directory's parent. The output and completion bit cannot appear in different committed versions. A crash can expose the preceding or succeeding whole version. Incomplete temporary files are ignored on recovery. A durable initialization marker prevents silently creating an empty store when an initialized directory has lost `state.bin`. Invalid or missing initialized state fails closed; there is no automatic rollback to an older snapshot, because rollback could reuse a stale generation or forget committed output.

An I/O error during persistence makes the live coordinator unusable until it is closed and recovered. This covers uncertainty after rename but before directory fsync. A caller must not infer a failed commit was absent; recovery and idempotent retry determine the outcome. The lock is scoped to one local store, not a replicated distributed coordinator. Filesystem rollback, loss of the whole directory, malicious store modification, and storage replication are outside this reference's guarantees.

The coordinator stores full outputs and rewrites its bounded state on each operation. This intentionally simple durability boundary is suitable for a reference executable, not a claim of production throughput. Record retirement, distributed consensus, authenticated snapshots, and engine transaction-log integration remain future adapter work.

## Example

```cpp
OperatorSpec spec;
spec.identity = {"q1", "snapshot1", "i64-pair-v1", "scan1", "value-ge-5", "m1"};
spec.minimum_value = 5;
std::vector<Row> input = {{1, 8}, {2, 3}, {3, 9}};
DurableCoordinator coordinator("/existing/parent/coordinator");
MorselKey key{"q1", "snapshot1", "m1"};
auto record = coordinator.begin(key);
if (!record.complete) {
    auto worker = record.checkpoint.empty()
        ? PortableOperator(spec, input)
        : PortableOperator::resume(spec, input, {}, record.checkpoint);
    worker.step(1);
    auto transfer = coordinator.handoff(key, record.generation, worker.checkpoint());
    if (transfer.result == HandoffResult::HandedOff) {
        auto replacement = PortableOperator::resume(spec, input, {}, coordinator.state(key).checkpoint);
        while (!replacement.finished()) replacement.step(128);
        coordinator.commit(key, transfer.generation, "attempt-result-1", replacement.output());
    }
}
// Consumers enumerate only records with complete == true.
```

A real adapter must separately obtain execution authority from the lease layer and confirm physical cleanup through its enforcer. Neither a checkpoint nor a committed result proves resources are free.

## Validation

`tests/execution_test.cpp` uses exception-based checks that remain active with `NDEBUG`. It compares exact rows and order across every interruption cutoff and repeated one-unit migrations; verifies aggregate and nested-loop join oracles, duplicate keys, negative keys, ranges, overflow, limits, identity mismatches, truncation, and checksum damage; and checks durable restart, stale and repeated commits, lock exclusion, lost/corrupt stores, real concurrent commit/handoff races, and child-process exit without destructor cleanup. These tests do not simulate power failure or establish hardware reclamation latency.
