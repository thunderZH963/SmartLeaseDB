# Local enforcement adapter

`Enforcer` is a dependency-free C++17 **cooperative reference adapter**, not a
BlueField driver or an OS process sandbox. It owns real byte buffers, tracks
scoped CPU, send, and I/O work, and serializes authority with a mutex. It does
not install Linux cgroups, terminate processes, intercept unregistered memory,
or validate DMA completion. An integration must keep each activity alive until
its real operation has completed or cancellation has drained it. Destroying a
handle while external work still uses its buffer is an integration error.

## Installation and ownership

Call `acquireBroker()` to acquire a fresh incarnation. Every takeover fences all
prior leases and preserves their complete reservations until cleanup is
verified. The incarnation is monotonic for this enforcer object's lifetime.
There is no persistent device identity: after enforcer restart, all old workers
must be disabled and drained outside this process before a new enforcer admits
work. A process-local counter reset does not safely fence surviving workers.

`install()` creates an installed lease and returns its acknowledgment. The
broker publishes grants only after an active acknowledgment. Retrying the exact
original key and payload is idempotent; changing quota, expiry, or grace is an
error. The returned lease reports the current explicit expiry if a successful
renewal occurred meanwhile. Retrying an expired or retired installation never
reactivates it. IDs cannot be reused, including under later incarnations.
Tombstones intentionally remain in memory; production systems need a durable
epoch/compaction protocol before reclaiming this metadata.

`renew(key, expiry)` accepts only an exact active key in the current incarnation
and a nondecreasing future expiry. No status read, install retry, activity,
heartbeat, missing RPC, or cleanup extends authority. A private thread checks
expiry every 2 ms, independently of broker calls; every authority operation also
checks expiry synchronously. This is an ordinary OS thread, not a hard real-time
deadline guarantee. The injectable test clock must be monotonic, thread-safe,
and nonblocking. Clock regression or exceptions fail closed.

## Tracked resource use

`allocate()` returns a move-only `Buffer` backed by an actual byte vector.
Payload bytes count against memory quota. `beginIo()` and `beginSend()` return
move-only `Activity` handles with owned staging buffers that also count against
memory. `acquireCpu()` registers a scoped CPU activity without allocating a
payload buffer. Payload accounting excludes allocator overhead and control
metadata. All query state, staging, and recovery allocations must use this
interface to be covered by its checks.

CPU, send, and I/O admissions use token buckets with one second of burst
capacity. CPU units are microseconds; a quota of 1000 CPU millis represents one
core and refills 1,000,000 permitted microseconds per second. Send and I/O units
are bytes. Unused tokens refill with elapsed local time; permit completion does
not refund consumed tokens. Work must split large requests into bounded units.
These are cooperative admission budgets, not measured CPU accounting or kernel
network/I/O shaping. The adapter cannot constrain code that ignores the API or
continues beyond its declared CPU allowance.

Destruction deallocates a handle's owned payload before reducing residency and
in-flight counters. No caller-supplied `released` flag can retire quota. New work
is denied immediately when recall, takeover, or expiry removes authority; any
already granted activity remains tracked until it is destroyed.

## Release, shrink, and receipts

`requestRelease(key)` requests full revocation; supplying a componentwise smaller
quota requests dimensional shrink. The old generation becomes pending release
and retains its **full quota charge**. No memory is forcibly erased on deadline.
A missed deadline enters quarantine; successful later cleanup still resolves it.

Full release completes only with zero resident payload bytes and zero CPU,
I/O, and send activities. Shrink completes only when residency fits the target
and every old-generation activity has drained. It creates a successor at lease
generation + 1, preserves the expiry and grace, and transfers retained buffers
to that successor's accounting. Old activity tokens are clamped to the smaller
quota, rather than granting a fresh burst. Expiry or takeover while shrink is
pending upgrades it to full revocation, so an expired lease cannot create a
successor. This implementation conservatively drains all activity types even
when only one resource dimension changes.

`pollRelease()` returns no receipt while evidence is incomplete. Once complete,
its stable receipt contains the exact retired key, exact quota reduction, and
optional successor. This receipt acknowledges local physical cleanup; the
broker must consume it before returning capacity to its pool or publishing a
successor grant. Repeated polls return the same receipt and cannot modify a
successor. Repeating the original release target is idempotent; a full revoke
can supersede an unfinished shrink, but a different shrink target is rejected.

`inventory()` includes current and pending leases, quarantine, zero-charge
retired tombstones, retained occupancy, in-flight counts, and every completed
receipt. It enables recovery after lost install/release responses and broker
restart. The broker must reconcile all entries and receipts rather than treating
an empty local ledger as free capacity. `canExecute()` only checks this resource
authority; it does not establish database placement authority or result commit.

## Validation

`tests/enforcer_test.cpp` checks exact install retries, changed-payload rejection,
explicit/duplicate renewal, lost release ACK recovery, shrink transfer and stale
keys, independent local expiry, takeover inventory, held memory and in-flight
activity blocking, quarantine, rate budgets, clock regression, and concurrent
allocation/recall. Tests use throwing checks, including in release builds.
