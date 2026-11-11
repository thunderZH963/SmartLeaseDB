#pragma once

#include "smartlease/types.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace smartlease {

enum class EnforcementState { Active, PendingRelease, Quarantined, Retired };

struct InstallAck {
  Lease lease;
  bool active = false; // A replay after expiry never grants execution again.
};

struct ReleaseReceipt {
  LeaseKey key;
  Resources freed;
  std::optional<Lease> successor;
};

struct EnforcementEntry {
  Lease lease;
  EnforcementState state = EnforcementState::Active;
  Resources charged;
  std::optional<Resources> target;
  std::uint64_t resident_bytes = 0;
  std::uint64_t cpu_inflight = 0;
  std::uint64_t io_inflight = 0;
  std::uint64_t send_inflight = 0;
  Time release_deadline = 0;
};

struct EnforcerInventory {
  std::uint64_t incarnation = 0;
  Resources charged;
  std::vector<EnforcementEntry> entries;
  std::vector<ReleaseReceipt> receipts;
};

// A process-local, cooperative reference adapter. No cgroup, DMA, or driver
// isolation is implied. The enforcer must outlive every worker using its API.
class Enforcer {
  struct Impl;
  struct OwnedResource;
 public:
  using Clock = std::function<Time()>;

  // A real owned allocation. Move-only; destruction frees storage before
  // removing its charge. Retained buffers survive a dimensional shrink.
  class Buffer {
   public:
    Buffer() noexcept;
    ~Buffer();
    Buffer(Buffer&&) noexcept;
    Buffer& operator=(Buffer&&) noexcept;
    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;
    std::byte* data() noexcept;
    const std::byte* data() const noexcept;
    std::size_t size() const noexcept;
    explicit operator bool() const noexcept;
   private:
    friend class Enforcer;
    explicit Buffer(std::unique_ptr<OwnedResource> resource);
    std::unique_ptr<OwnedResource> resource_;
  };

  // A scoped cooperative work permit. IO/send permits also own their staging
  // buffer. Keep the permit alive until the real operation has drained. Only
  // destruction releases its owned buffer and in-flight registration.
  class Activity {
   public:
    Activity() noexcept;
    ~Activity();
    Activity(Activity&&) noexcept;
    Activity& operator=(Activity&&) noexcept;
    Activity(const Activity&) = delete;
    Activity& operator=(const Activity&) = delete;
    std::byte* data() noexcept;
    std::size_t size() const noexcept;
    explicit operator bool() const noexcept;
   private:
    friend class Enforcer;
    explicit Activity(std::unique_ptr<OwnedResource> resource);
    std::unique_ptr<OwnedResource> resource_;
  };

  explicit Enforcer(Clock clock = {});
  ~Enforcer();
  Enforcer(const Enforcer&) = delete;
  Enforcer& operator=(const Enforcer&) = delete;

  Time now() const;
  std::uint64_t acquireBroker();
  InstallAck install(const Lease& lease);
  Lease renew(const LeaseKey& key, Time explicit_expiry);
  void requestRelease(const LeaseKey& key, Resources target = {});
  std::optional<ReleaseReceipt> pollRelease(const LeaseKey& key);
  EnforcerInventory inventory();
  bool canExecute(const LeaseKey& key);
  void tick(); // Expiry also runs on a private thread, independently of RPCs.

  Buffer allocate(const LeaseKey& key, std::size_t bytes);
  Activity beginIo(const LeaseKey& key, std::size_t bytes);
  Activity beginSend(const LeaseKey& key, std::size_t bytes);
  Activity acquireCpu(const LeaseKey& key, std::uint64_t microseconds);

 private:
  std::shared_ptr<Impl> impl_;
  std::unique_ptr<OwnedResource> acquire(const LeaseKey&, std::size_t,
                                       unsigned kind, std::uint64_t units);
};

} // namespace smartlease
