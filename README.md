# SmartLeaseDB runtime

C++17 implementation of the SmartLeaseDB execution contract for reclaimable
query pushdown. It is engine-independent: resource leases and physical-release
accounting are separate from database progress and output commitment.

## Build and run

Requirements: a C++17 compiler, CMake 3.16+, and POSIX macOS or Linux. No Arrow,
CAF, Python packages, or database installation is needed for the reference suite.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4
(cd build && ctest --output-on-failure)
./build/smartlease-demo
```

The demo runs grouped aggregation over 4,096 rows, recalls the storage lease
after partial execution, exports a checkpoint, fences the old attempt, resumes
on the logical compute side, and compares every output group with uninterrupted
execution. It commits the recovered result while old transport buffers remain
charged, then releases those buffers and consumes the verified release receipt.
It prints actual correctness outcomes and byte counts, not benchmark timings.
Its durable coordinator files are retained at the printed temporary directory.
An optional first argument selects a fresh coordinator directory.

Sanitizers:

```sh
cmake -S . -B build-sanitize -DCMAKE_BUILD_TYPE=Debug -DSMARTLEASE_SANITIZE=ON
cmake --build build-sanitize -j 4
(cd build-sanitize && ctest --output-on-failure)
```

## Mechanisms implemented

| Mechanism | Implementation | Behavior |
|---|---|---|
| Multidimensional resource contract | `types.hpp`, `broker.*` | CPU, memory, network, I/O; pending installation and pending release both consume budget |
| Forecast and request selection | `controller.*` | Time-scaled EWMA, stale-data rejection, bounded recovery eligibility, configured weights, capped credits, deterministic greedy ranking |
| Install before grant | `Broker::reserve/install/acknowledgeInstall` | Reservation precedes installation; lost replies cannot publish a grant |
| Storage recall and local expiry | `enforcer.*` | Independent expiry thread, explicit renewal, CPU/network/I/O permits, owned byte buffers, tracked outstanding work |
| Dimensional shrink | `Enforcer::requestRelease` | Drain old work; retain permitted bytes; issue a new lease generation only after verification |
| Physical release before reuse | `pollRelease`, `acknowledgeRelease`, `reconcile` | Generation-matched replayable receipts; no resource returned merely because recall was sent or the query completed |
| Portable state | `execution.*` | Scan/filter, grouped SUM/count/min/max, duplicate-preserving partitioned inner hash join; versioned bounded checkpoints |
| Query progress and result visibility | `DurableCoordinator` | Atomic durable result/completion records, serialized handoff vs commit, stale-attempt rejection, restart fencing |
| Recovery controls | Tests and demo | Checkpoint/resume and kill/replay from the same morsel boundary, result-equivalence checks, lost ACKs and crash windows |

## Runtime sequence

1. Construct an `Enforcer`; construct a `Broker` with the domain's physical
   capacity and guard. Construction takes over a new broker incarnation and
   reconstructs still-charged allocations from authoritative inventory.
2. Configure tenant weights. Supply a fresh infrastructure-demand forecast using
   `setForeground`. If `DemandEstimator::forecast` is absent or stale, call
   `failClosed` and `recallToBudget`; never reuse an old forecast indefinitely.
3. Submit a bounded `Request`. `admitRanked` ranks eligible requests, reserves
   budget, installs quotas, and publishes only acknowledged grants. The separate
   reserve/install/ACK methods expose transport fault windows to tests.
4. Before each bounded work step, retain an execution permit. Keep actual memory
   buffers and I/O/send permits alive for their full physical lifetime.
5. On recall, stop input, reach a safe boundary, and export a checkpoint.
   `DurableCoordinator::handoff` serializes with output commitment and advances
   the placement generation. Resume unfinished work on compute with that
   generation. A checkpoint that exceeds its envelope is rejected; replay from
   the original morsel boundary is the conservative alternative.
6. Drain/cancel outstanding activity and destroy old state. Only a verified
   `ReleaseReceipt` or authoritative inventory reconciliation returns budget.
   Query commitment may happen earlier; it never substitutes for cleanup proof.

CPU units are milli-cores, memory is bytes, and network/I/O rates are bytes/s.
Timestamps are enforcer-local monotonic milliseconds. The CPU permit argument
is microseconds of declared cooperative work. Lease generations authorize
resource use; placement generations authorize result publication.

## Database integration and validation scope

The executable implementation is a **process-local reference runtime**. It has
real owned buffers, actual filesystem persistence and threaded correctness tests.
The reference operators use signed 64-bit keys/values and do not implement a SQL
parser, Arrow types/null semantics, or complete SSB/TPC-H queries.

The enforcer controls resources acquired through its cooperative APIs. It does
not install Linux cgroups, police RDMA queues, or kill an uncooperative process.
Its owned bytes are not a measurement of total process RSS. In particular, the
reference operators' STL state resides in host memory separately from the
tracked transport buffers in the demo. A native adapter must integrate the
engine's real allocator and physical completion callbacks; allocating dummy
buffers alongside untracked Arrow memory would not establish enforcement.

There is no remote RPC transport in this version. An install exception followed
by authoritative absence is a definitive negative result only because this
adapter is synchronous and local. A network deployment needs explicit negative
ACKs or cancellation tombstones to handle reordered installs safely. The enforcer
survives broker reconstruction in these tests; independent enforcer-process
restart requires a persistent incarnation/session gate and worker cleanup before
reopening admission.

Native TapDB, FlexPushdownDB, openGauss and Taurus executions and hardware
performance have not been validated by this code. See:

- [TapDB integration locations and required hooks](docs/tapdb-integration.md)
- [Enforcer protocol, resource lifetimes and rate gates](docs/enforcer.md)
- [Operator codecs and durable coordinator](docs/execution.md)
- [Validation record](docs/validation.md)

To link another executable, add this directory with `add_subdirectory` and link
the `smartlease` CMake target. `include/smartlease/` is the public API.

## Tests

The five CTest suites use throwing checks, so Release/NDEBUG builds retain all
assertions. They cover arithmetic/eligibility, rate gates and expiry, receipt
replay, shrink generations, broker takeover, concurrent budget admission,
corrupt/oversize checkpoints, output equivalence, commit/handoff races, durable
restart, and fork/exit recovery. `integration_test` exercises all three operator
families under both checkpoint/resume and replay.

These tests cover the standalone runtime and supported operator semantics.
