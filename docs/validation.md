# Validation record

Local validation date: 2026-11-06 (Asia/Shanghai).

Results: **5/5 Release suites passed; 5/5 AddressSanitizer/UndefinedBehaviorSanitizer
suites passed.** The integrated demo also completed successfully.

## Coverage

| Suite | Checked behavior |
|---|---|
| `controller` | Checked resource arithmetic; no cold/stale forecast admission; EWMA; eligibility bounds; unequal configured weights and capped credit |
| `enforcer` | Autonomous expiry, exact install retries, explicit renewal, takeover fencing, outstanding resource lifetimes, partial shrink, stale receipts, quarantine, integral token buckets |
| `execution` | Every checkpoint cutoff, duplicate join matches, aggregation overflow, corrupt/truncated/oversize state, identity mismatch, durable reopen, exclusive writer locking, commit/handoff races, child-process exit recovery |
| `broker` | Pending grants charged; lost/duplicate/spoofed ACKs; shrink successor fencing; local expiry vs renewed install; broker takeover; 32-thread admission; changing pressure; expiry between inventory and recall; definitive local install rejection |
| `integration` | Three operator families under checkpoint/resume and kill/replay; whole-result comparison; stale output rejection; compute commitment before storage cleanup; retained sends; durable committed output |

The release build uses AppleClang 15, C++17 and `-Wall -Wextra -Wpedantic
-Werror` for the library and tests. All checks remain active under `NDEBUG`.
CTest results are recorded under `build/Testing/Temporary/LastTest.log`.
Sanitizer checks use AddressSanitizer and UndefinedBehaviorSanitizer in a
separate build. See `build-sanitize/Testing/Temporary/LastTest.log` for results.

## Demo evidence

The demo processed 4,096 rows into 17 aggregate groups. Its checkpoint was
894 bytes. Whole-result comparison succeeded; old-generation output was fenced.
The broker retained its 32,768-byte reservation through compute commitment and
returned it only after the tracked buffers drained and a release receipt arrived.
The bytes describe the configured reservation and reference checkpoint, not
native TapDB resident memory or a hardware measurement.

## Deployment boundary

Validated here: standalone C++ runtime, process-local cooperative enforcer,
reference integer operators, local POSIX durable coordinator and fault tests.

Not executed by these tests: native SQL engine
adapters, remote lease RPC, device process restart, Linux/RDMA physical isolation,
or BlueField SSB/TPC-H performance. Protocol tests do not substitute for those
integration and hardware checks.
