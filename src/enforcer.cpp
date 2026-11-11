#include "smartlease/enforcer.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>

namespace smartlease {
namespace {
bool sameLease(const Lease& a, const Lease& b) {
  return a.key == b.key && a.quota == b.quota &&
         a.expires_at == b.expires_at && a.release_grace == b.release_grace;
}
Time saturatingDeadline(Time now, Time grace) {
  return grace > std::numeric_limits<Time>::max() - now
             ? std::numeric_limits<Time>::max() : now + grace;
}
enum Kind : unsigned { Memory = 0, Cpu = 1, Io = 2, Send = 3 };
} // namespace

struct Enforcer::Impl {
  struct Usage {
    std::uint64_t memory = 0;
    std::uint64_t cpu = 0, io = 0, send = 0;
    // Token buckets measured in CPU microseconds, I/O bytes, and send bytes.
    std::array<std::uint64_t, 3> tokens{};
    std::array<std::uint64_t, 3> fractional{}; // Thousandths of a token.
    Time refill_at = 0;
  };
  struct Record {
    Lease installed; // Immutable install request, for exact retry validation.
    Lease lease;
    EnforcementState state = EnforcementState::Active;
    std::optional<Resources> target;
    Time deadline = 0;
    std::shared_ptr<Usage> usage;
    std::optional<ReleaseReceipt> receipt;
  };

  mutable std::mutex mutex;
  std::condition_variable wake;
  Clock clock;
  std::thread expiry_thread;
  bool stopped = false;
  bool clock_failed = false;
  Time last_time = 0;
  std::uint64_t incarnation = 0;
  std::map<LeaseKey, Record> records;
  std::set<std::uint64_t> used_ids;

  explicit Impl(Clock input) : clock(std::move(input)) {
    if (!clock) {
      clock = [] {
        return static_cast<Time>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
      };
    }
    last_time = clock();
  }

  static std::array<std::uint64_t, 3> rates(const Resources& q) {
    const auto max = std::numeric_limits<std::uint64_t>::max();
    // Saturating an unrepresentably large CPU burst is conservative.
    return {q.cpu_millis > max / 1000 ? max : q.cpu_millis * 1000,
            q.io_bytes_per_sec, q.network_bytes_per_sec};
  }

  void start() {
    expiry_thread = std::thread([this] {
      std::unique_lock<std::mutex> lock(mutex);
      while (!stopped) {
        try { refresh(); }
        catch (...) {
          // Metadata allocation failure cannot leave execution authorized.
          clock_failed = true;
          fenceAll(last_time, true);
        }
        wake.wait_for(lock, std::chrono::milliseconds(2), [this] { return stopped; });
      }
    });
  }

  void shutdown() noexcept {
    {
      std::lock_guard<std::mutex> lock(mutex);
      stopped = true;
      fenceAll(last_time, true);
    }
    wake.notify_all();
    if (expiry_thread.joinable()) expiry_thread.join();
  }

  ~Impl() { shutdown(); }

  Time readTime() {
    if (!clock_failed) {
      try {
        const Time n = clock();
        if (n < last_time) throw std::runtime_error("enforcer clock regressed");
        last_time = n;
      } catch (...) {
        clock_failed = true;
        fenceAll(last_time, true);
      }
    }
    return last_time;
  }

  void healthy() const {
    if (clock_failed || stopped)
      throw std::runtime_error("enforcer is fenced: unavailable monotonic clock or shutdown");
  }

  void fenceAll(Time n, bool quarantine) noexcept {
    for (auto& item : records) {
      auto& r = item.second;
      if (r.state == EnforcementState::Retired) continue;
      r.target = Resources{};
      if (r.state == EnforcementState::Active)
        r.deadline = saturatingDeadline(n, r.lease.release_grace);
      r.state = quarantine ? EnforcementState::Quarantined
                           : EnforcementState::PendingRelease;
    }
  }

  Record& find(const LeaseKey& key) {
    auto it = records.find(key);
    if (it == records.end()) throw std::invalid_argument("unknown exact lease key");
    return it->second;
  }

  Record& active(const LeaseKey& key) {
    healthy();
    auto& r = find(key);
    if (key.incarnation != incarnation || r.state != EnforcementState::Active)
      throw std::runtime_error("lease authority is not active");
    return r;
  }

  void refill(Record& r, Time n) {
    const auto limits = rates(r.lease.quota);
    const Time elapsed = n - r.usage->refill_at;
    for (std::size_t i = 0; i < limits.size(); ++i) {
      if (elapsed >= 1000) {
        r.usage->tokens[i] = limits[i];
        r.usage->fractional[i] = 0;
        continue;
      }
      // Split the multiplication to preserve exact integer budgets without
      // overflow, including rates close to UINT64_MAX.
      const auto partial = (limits[i] % 1000) * elapsed + r.usage->fractional[i];
      const auto refill = (limits[i] / 1000) * elapsed + partial / 1000;
      if (refill >= limits[i] - r.usage->tokens[i]) {
        r.usage->tokens[i] = limits[i];
        r.usage->fractional[i] = 0;
      } else {
        r.usage->tokens[i] += refill;
        r.usage->fractional[i] = partial % 1000;
      }
    }
    r.usage->refill_at = n;
  }

  void complete(Record& r, Time n) {
    if (r.state == EnforcementState::Active || r.state == EnforcementState::Retired)
      return;
    const Resources target = *r.target;
    const auto& u = *r.usage;
    // Every old-generation operation drains, even when only one dimension is
    // shrinking. This is conservative and makes the successor handoff exact.
    if (u.memory > target.memory_bytes || u.cpu || u.io || u.send) {
      if (n >= r.deadline) r.state = EnforcementState::Quarantined;
      return;
    }
    ReleaseReceipt receipt{r.lease.key, r.lease.quota - target, std::nullopt};
    if (!target.empty()) {
      Lease successor = r.lease;
      ++successor.key.generation; // Overflow is rejected before shrink begins.
      successor.quota = target;
      Record next;
      next.installed = next.lease = successor;
      next.usage = r.usage;
      // No fresh burst is granted by changing generation.
      const auto limits = rates(target);
      for (std::size_t i = 0; i < limits.size(); ++i)
        next.usage->tokens[i] = std::min(next.usage->tokens[i], limits[i]);
      next.usage->fractional = {};
      next.usage->refill_at = n;
      const auto inserted = records.emplace(successor.key, std::move(next));
      if (!inserted.second) throw std::logic_error("successor generation already exists");
      receipt.successor = successor;
    }
    r.receipt = receipt;
    r.state = EnforcementState::Retired;
    r.usage.reset();
  }

  Time refresh() {
    const Time n = readTime();
    // std::map insertion does not invalidate iterators. Newly installed
    // successors cannot expire here because expired shrinks become revokes.
    for (auto& item : records) {
      auto& r = item.second;
      if (r.state == EnforcementState::Retired) continue;
      if (n >= r.lease.expires_at) {
        if (r.state == EnforcementState::Active) {
          r.state = EnforcementState::PendingRelease;
          r.deadline = saturatingDeadline(r.lease.expires_at, r.lease.release_grace);
        }
        r.target = Resources{};
      }
      complete(r, n);
    }
    return n;
  }

  Resources totalCharged() const {
    Resources total;
    for (const auto& item : records)
      if (item.second.state != EnforcementState::Retired)
        total = total + item.second.lease.quota;
    return total;
  }
};

struct Enforcer::OwnedResource {
  std::shared_ptr<Impl> owner;
  std::shared_ptr<Impl::Usage> usage;
  std::vector<std::byte> bytes;
  unsigned kind = Memory;
  bool registered = false;

  ~OwnedResource() {
    if (!registered) return;
    const auto size = bytes.size();
    // Deallocate first; decrementing metadata alone is not cleanup evidence.
    std::vector<std::byte>().swap(bytes);
    std::lock_guard<std::mutex> lock(owner->mutex);
    usage->memory -= size;
    if (kind == Cpu) --usage->cpu;
    if (kind == Io) --usage->io;
    if (kind == Send) --usage->send;
    // Expiry thread/poll produces receipts; destructors never allocate metadata.
  }
};

Enforcer::Buffer::Buffer() noexcept = default;
Enforcer::Buffer::~Buffer() = default;
Enforcer::Buffer::Buffer(Buffer&&) noexcept = default;
Enforcer::Buffer& Enforcer::Buffer::operator=(Buffer&&) noexcept = default;
Enforcer::Buffer::Buffer(std::unique_ptr<OwnedResource> r) : resource_(std::move(r)) {}
std::byte* Enforcer::Buffer::data() noexcept { return resource_ ? resource_->bytes.data() : nullptr; }
const std::byte* Enforcer::Buffer::data() const noexcept { return resource_ ? resource_->bytes.data() : nullptr; }
std::size_t Enforcer::Buffer::size() const noexcept { return resource_ ? resource_->bytes.size() : 0; }
Enforcer::Buffer::operator bool() const noexcept { return static_cast<bool>(resource_); }
Enforcer::Activity::Activity() noexcept = default;
Enforcer::Activity::~Activity() = default;
Enforcer::Activity::Activity(Activity&&) noexcept = default;
Enforcer::Activity& Enforcer::Activity::operator=(Activity&&) noexcept = default;
Enforcer::Activity::Activity(std::unique_ptr<OwnedResource> r) : resource_(std::move(r)) {}
std::byte* Enforcer::Activity::data() noexcept { return resource_ ? resource_->bytes.data() : nullptr; }
std::size_t Enforcer::Activity::size() const noexcept { return resource_ ? resource_->bytes.size() : 0; }
Enforcer::Activity::operator bool() const noexcept { return static_cast<bool>(resource_); }

Enforcer::Enforcer(Clock clock) : impl_(std::make_shared<Impl>(std::move(clock))) { impl_->start(); }
Enforcer::~Enforcer() { impl_->shutdown(); }

Time Enforcer::now() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto n = impl_->readTime();
  impl_->healthy();
  return n;
}

std::uint64_t Enforcer::acquireBroker() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto n = impl_->refresh();
  impl_->healthy();
  if (impl_->incarnation == std::numeric_limits<std::uint64_t>::max())
    throw std::overflow_error("broker incarnation exhausted");
  ++impl_->incarnation;
  impl_->fenceAll(n, false);
  impl_->refresh();
  return impl_->incarnation;
}

InstallAck Enforcer::install(const Lease& lease) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto n = impl_->refresh();
  impl_->healthy();
  if (!lease.key.id || !lease.key.generation || !impl_->incarnation ||
      lease.key.incarnation != impl_->incarnation)
    throw std::invalid_argument("invalid lease key or stale broker incarnation");
  auto found = impl_->records.find(lease.key);
  if (found != impl_->records.end()) {
    if (!sameLease(found->second.installed, lease))
      throw std::invalid_argument("install retry changed the exact-key request");
    return {found->second.lease, found->second.state == EnforcementState::Active};
  }
  if (impl_->used_ids.count(lease.key.id))
    throw std::invalid_argument("lease ID already has an install tombstone");
  if (lease.expires_at <= n) throw std::invalid_argument("installation already expired");
  (void)(impl_->totalCharged() + lease.quota); // Fail before accounting overflow.
  Impl::Record r;
  r.installed = r.lease = lease;
  r.usage = std::make_shared<Impl::Usage>();
  r.usage->tokens = Impl::rates(lease.quota);
  r.usage->refill_at = n;
  impl_->used_ids.insert(lease.key.id);
  try { impl_->records.emplace(lease.key, std::move(r)); }
  catch (...) { impl_->used_ids.erase(lease.key.id); throw; }
  return {lease, true};
}

Lease Enforcer::renew(const LeaseKey& key, Time explicit_expiry) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto n = impl_->refresh();
  auto& r = impl_->active(key);
  if (explicit_expiry <= n || explicit_expiry < r.lease.expires_at)
    throw std::invalid_argument("renewal must preserve or extend explicit expiry");
  r.lease.expires_at = explicit_expiry;
  return r.lease;
}

void Enforcer::requestRelease(const LeaseKey& key, Resources target) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto n = impl_->refresh();
  auto& r = impl_->find(key);
  if (!target.fits(r.lease.quota)) throw std::invalid_argument("shrink target exceeds quota");
  if (r.state == EnforcementState::Retired) {
    if (!r.target || target != *r.target)
      throw std::invalid_argument("release retry changed target");
    return;
  }
  if (r.state != EnforcementState::Active) {
    if (r.target == target) return;
    // Revoke may supersede an unfinished shrink, never the reverse.
    if (!target.empty()) throw std::invalid_argument("release already pending with another target");
    r.target = target;
    impl_->complete(r, n);
    return;
  }
  impl_->active(key);
  if (!target.empty() && key.generation == std::numeric_limits<std::uint64_t>::max())
    throw std::overflow_error("lease generation exhausted");
  r.target = target;
  r.state = EnforcementState::PendingRelease;
  r.deadline = saturatingDeadline(n, r.lease.release_grace);
  impl_->complete(r, n);
}

std::optional<ReleaseReceipt> Enforcer::pollRelease(const LeaseKey& key) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->refresh();
  return impl_->find(key).receipt;
}

EnforcerInventory Enforcer::inventory() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->refresh();
  EnforcerInventory out;
  out.incarnation = impl_->incarnation;
  out.charged = impl_->totalCharged();
  for (const auto& item : impl_->records) {
    const auto& r = item.second;
    EnforcementEntry entry;
    entry.lease = r.lease;
    entry.state = r.state;
    entry.charged = r.state == EnforcementState::Retired ? Resources{} : r.lease.quota;
    entry.target = r.target;
    entry.release_deadline = r.deadline;
    if (r.usage) {
      entry.resident_bytes = r.usage->memory;
      entry.cpu_inflight = r.usage->cpu;
      entry.io_inflight = r.usage->io;
      entry.send_inflight = r.usage->send;
    }
    out.entries.push_back(entry);
    if (r.receipt) out.receipts.push_back(*r.receipt);
  }
  return out;
}

bool Enforcer::canExecute(const LeaseKey& key) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->refresh();
  auto found = impl_->records.find(key);
  return !impl_->clock_failed && !impl_->stopped && key.incarnation == impl_->incarnation &&
         found != impl_->records.end() && found->second.state == EnforcementState::Active;
}

void Enforcer::tick() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->refresh();
}

std::unique_ptr<Enforcer::OwnedResource> Enforcer::acquire(
    const LeaseKey& key, std::size_t bytes, unsigned kind, std::uint64_t units) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto n = impl_->refresh();
  auto& r = impl_->active(key);
  if (bytes > r.lease.quota.memory_bytes - r.usage->memory)
    throw std::runtime_error("lease memory quota exhausted");
  impl_->refill(r, n);
  if (kind != Memory && (!units || units > r.usage->tokens[kind - 1]))
    throw std::runtime_error("cooperative rate budget exhausted or empty operation");
  auto resource = std::make_unique<OwnedResource>();
  resource->owner = impl_;
  resource->usage = r.usage;
  resource->kind = kind;
  resource->bytes.resize(bytes); // Actual allocation, before metadata charge.
  r.usage->memory += bytes;
  if (kind == Cpu) ++r.usage->cpu;
  if (kind == Io) ++r.usage->io;
  if (kind == Send) ++r.usage->send;
  if (kind != Memory) r.usage->tokens[kind - 1] -= units;
  resource->registered = true;
  return resource;
}

Enforcer::Buffer Enforcer::allocate(const LeaseKey& key, std::size_t bytes) {
  return Buffer(acquire(key, bytes, Memory, 0));
}
Enforcer::Activity Enforcer::beginIo(const LeaseKey& key, std::size_t bytes) {
  return Activity(acquire(key, bytes, Io, bytes));
}
Enforcer::Activity Enforcer::beginSend(const LeaseKey& key, std::size_t bytes) {
  return Activity(acquire(key, bytes, Send, bytes));
}
Enforcer::Activity Enforcer::acquireCpu(const LeaseKey& key, std::uint64_t microseconds) {
  return Activity(acquire(key, 0, Cpu, microseconds));
}

} // namespace smartlease
