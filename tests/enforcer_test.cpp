#include "smartlease/enforcer.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace smartlease;
namespace {
void check(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}
template <class F> void rejected(F&& fn, const char* message) {
  try { fn(); } catch (const std::exception&) { return; }
  throw std::runtime_error(message);
}
Lease makeLease(std::uint64_t id, std::uint64_t incarnation, Time expiry = 1000) {
  return {{id, incarnation, 1}, {100, 1024, 1000, 2000}, expiry, 20};
}
EnforcementEntry entry(Enforcer& e, const LeaseKey& key) {
  for (const auto& item : e.inventory().entries) if (item.lease.key == key) return item;
  throw std::runtime_error("missing inventory entry");
}

void idempotencyAndRenewal() {
  std::atomic<Time> clock{100};
  Enforcer e([&] { return clock.load(); });
  const auto incarnation = e.acquireBroker();
  Lease lease = makeLease(1, incarnation);
  (void)e.install(lease); // Installed ACK lost.
  auto ack = e.install(lease);
  check(ack.active && ack.lease.key == lease.key, "lost install ACK not recoverable");
  check(e.inventory().charged == lease.quota, "duplicate install double charged");
  auto mismatch = lease;
  mismatch.quota.memory_bytes++;
  rejected([&] { e.install(mismatch); }, "mismatching quota install accepted");
  mismatch = lease;
  mismatch.expires_at++;
  rejected([&] { e.install(mismatch); }, "mismatching expiry install accepted");
  check(e.renew(lease.key, 2000).expires_at == 2000, "explicit renewal failed");
  check(e.renew(lease.key, 2000).expires_at == 2000, "duplicate renewal failed");
  rejected([&] { e.renew(lease.key, 1500); }, "stale renewal shortened lease");
  check(e.install(lease).lease.expires_at == 2000, "install retry reverted renewal");
  e.requestRelease(lease.key);
  (void)e.pollRelease(lease.key); // Release ACK lost.
  e.requestRelease(lease.key);
  const auto receipt = e.pollRelease(lease.key);
  check(receipt && receipt->key == lease.key && receipt->freed == lease.quota &&
        !receipt->successor, "lost release ACK not recoverable");
  check(e.inventory().receipts.size() == 1 && e.inventory().charged.empty(),
        "receipt inventory or retirement incorrect");
  check(!e.install(lease).active, "retired install replay reactivated lease");
  auto reused = lease;
  reused.key.generation++;
  rejected([&] { e.install(reused); }, "stable lease ID tombstone was ignored");
}

void shrinkAndEvidence() {
  std::atomic<Time> clock{100};
  Enforcer e([&] { return clock.load(); });
  Lease lease = makeLease(2, e.acquireBroker());
  e.install(lease);
  auto retained = e.allocate(lease.key, 200);
  auto excess = e.allocate(lease.key, 400);
  retained.data()[0] = std::byte{42};
  auto io = e.beginIo(lease.key, 50);
  auto send = e.beginSend(lease.key, 20);
  auto cpu = e.acquireCpu(lease.key, 100);
  const Resources target{50, 256, 500, 1000};
  e.requestRelease(lease.key, target);
  check(!e.canExecute(lease.key), "recall retained old execution authority");
  rejected([&] { e.allocate(lease.key, 1); }, "pending lease accepted allocation");
  rejected([&] { e.renew(lease.key, 3000); }, "pending lease was renewed");
  check(!e.pollRelease(lease.key), "ACK preceded physical memory release");
  excess = {};
  check(!e.pollRelease(lease.key), "ACK preceded old operation drain");
  clock.store(121);
  e.tick();
  auto pending = entry(e, lease.key);
  check(pending.state == EnforcementState::Quarantined && pending.charged == lease.quota,
        "deadline did not retain full quarantined charge");
  io = {};
  send = {};
  check(!e.pollRelease(lease.key), "CPU in-flight work did not block ACK");
  cpu = {};
  auto receipt = e.pollRelease(lease.key);
  check(receipt && receipt->successor && receipt->freed == lease.quota - target,
        "dimensional shrink receipt incorrect");
  const Lease successor = *receipt->successor;
  check(successor.key.generation == lease.key.generation + 1 &&
        successor.quota == target && successor.expires_at == lease.expires_at,
        "successor key, quota, or expiry incorrect");
  check(e.inventory().charged == target && e.canExecute(successor.key),
        "successor accounting or authority incorrect");
  check(entry(e, successor.key).resident_bytes == 200 && retained.data()[0] == std::byte{42},
        "retained allocation did not transfer to successor");
  e.requestRelease(lease.key, target);
  const auto stale = e.pollRelease(lease.key);
  check(stale && stale->successor->key == successor.key && e.inventory().charged == target,
        "stale release retired successor");
  rejected([&] { e.renew(lease.key, 4000); }, "old generation renewed after shrink");
  e.requestRelease(successor.key);
  check(!e.pollRelease(successor.key), "successor released retained buffer early");
  retained = {};
  check(e.pollRelease(successor.key).has_value() && e.inventory().charged.empty(),
        "successor failed to release transferred buffer");
}

void expiryAndTakeover() {
  std::atomic<Time> clock{100};
  Enforcer e([&] { return clock.load(); });
  Lease lease = makeLease(3, e.acquireBroker(), 150);
  e.install(lease);
  auto buffer = e.allocate(lease.key, 128);
  clock.store(151);
  check(!e.canExecute(lease.key), "local expiry failed without broker RPC");
  rejected([&] { e.renew(lease.key, 300); }, "expired pending lease renewed");
  check(!e.pollRelease(lease.key) && e.inventory().charged == lease.quota,
        "expired buffers were prematurely freed");
  clock.store(171);
  check(entry(e, lease.key).state == EnforcementState::Quarantined,
        "expired allocation did not quarantine");
  const auto replacement = e.acquireBroker();
  check(replacement == lease.key.incarnation + 1 && e.inventory().charged == lease.quota,
        "takeover reset occupied ledger");
  rejected([&] { e.install(lease); }, "old broker install accepted");
  auto next = makeLease(4, replacement, 400);
  e.install(next);
  auto inFlight = e.beginIo(next.key, 32);
  const auto third = e.acquireBroker();
  check(third > replacement && !e.canExecute(next.key), "takeover did not fence old lease");
  rejected([&] { e.renew(next.key, 500); }, "old broker renewed after takeover");
  check(entry(e, next.key).io_inflight == 1, "takeover erased live IO");
  buffer = {};
  inFlight = {};
  check(e.pollRelease(lease.key) && e.pollRelease(next.key) && e.inventory().charged.empty(),
        "takeover cleanup receipts missing");
}

void tokenBudgets() {
  std::atomic<Time> clock{100};
  Enforcer e([&] { return clock.load(); });
  Lease lease = makeLease(5, e.acquireBroker(), 3000);
  lease.quota.memory_bytes = 4000;
  e.install(lease);
  { auto activity = e.acquireCpu(lease.key, 100000); }
  rejected([&] { e.acquireCpu(lease.key, 1); }, "CPU bucket exceeded");
  { auto activity = e.beginIo(lease.key, 2000); }
  rejected([&] { e.beginIo(lease.key, 1); }, "IO bucket exceeded");
  { auto activity = e.beginSend(lease.key, 1000); }
  rejected([&] { e.beginSend(lease.key, 1); }, "send bucket exceeded");
  clock.store(600);
  { auto activity = e.acquireCpu(lease.key, 50000); }
  { auto activity = e.beginIo(lease.key, 1000); }
  { auto activity = e.beginSend(lease.key, 500); }
  auto retained = e.allocate(lease.key, 4000);
  rejected([&] { e.allocate(lease.key, 1); }, "memory quota exceeded");
  check(entry(e, lease.key).resident_bytes == 4000, "failed allocation changed accounting");
  auto fractional = makeLease(10, lease.key.incarnation, 3000);
  fractional.quota = {0, 10, 3, 0};
  e.install(fractional);
  { auto send = e.beginSend(fractional.key, 3); }
  for (Time t = 700; t <= 900; t += 100) {
    clock.store(t);
    rejected([&] { e.beginSend(fractional.key, 1); }, "fractional refill rounded up");
  }
  clock.store(933);
  rejected([&] { e.beginSend(fractional.key, 1); }, "fractional refill granted early");
  clock.store(934);
  { auto send = e.beginSend(fractional.key, 1); }
  rejected([&] { e.beginSend(fractional.key, 1); }, "fractional refill double counted");
}

void shrinkingExpiryAndClockFailure() {
  std::atomic<Time> clock{100};
  Enforcer e([&] { return clock.load(); });
  Lease lease = makeLease(6, e.acquireBroker(), 150);
  e.install(lease);
  auto retained = e.allocate(lease.key, 200);
  auto io = e.beginIo(lease.key, 10);
  e.requestRelease(lease.key, {50, 256, 100, 100});
  clock.store(151);
  io = {};
  check(!e.pollRelease(lease.key), "expired shrink minted a successor");
  retained = {};
  const auto receipt = e.pollRelease(lease.key);
  check(receipt && !receipt->successor && receipt->freed == lease.quota,
        "expiry during shrink did not force full release");
  auto next = makeLease(7, lease.key.incarnation, 1000);
  e.install(next);
  auto buffer = e.allocate(next.key, 20);
  clock.store(99);
  check(!e.canExecute(next.key), "clock regression did not fail closed");
  rejected([&] { e.renew(next.key, 2000); }, "clock failure allowed renewal");
  check(entry(e, next.key).state == EnforcementState::Quarantined &&
        e.inventory().charged == next.quota, "clock failure erased occupancy");
}

void concurrentRecall() {
  std::atomic<Time> clock{100};
  Enforcer e([&] { return clock.load(); });
  Lease lease = makeLease(8, e.acquireBroker());
  e.install(lease);
  auto retained = e.allocate(lease.key, 20);
  std::atomic<bool> start{false};
  std::atomic<unsigned> successful{0};
  std::vector<std::thread> workers;
  for (unsigned i = 0; i < 6; ++i) {
    workers.emplace_back([&] {
      while (!start.load()) std::this_thread::yield();
      for (unsigned j = 0; j < 300; ++j) {
        try { auto buffer = e.allocate(lease.key, 4); ++successful; }
        catch (const std::runtime_error&) {}
      }
    });
  }
  start.store(true);
  e.requestRelease(lease.key);
  for (auto& worker : workers) worker.join();
  check(!e.canExecute(lease.key) && !e.pollRelease(lease.key),
        "concurrent recall lost retained allocation");
  check(entry(e, lease.key).resident_bytes == 20, "race corrupted resource accounting");
  retained = {};
  check(e.pollRelease(lease.key) && e.inventory().charged.empty(),
        "concurrent recall failed cleanup");
}

void realClockExpiry() {
  Enforcer e;
  const auto incarnation = e.acquireBroker();
  Lease lease = makeLease(9, incarnation, checked_deadline(e.now(), 25));
  e.install(lease);
  auto retained = e.allocate(lease.key, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(55));
  check(!e.canExecute(lease.key) && entry(e, lease.key).state == EnforcementState::Quarantined,
        "real monotonic expiry did not revoke authority");
  retained = {};
  check(e.pollRelease(lease.key).has_value(), "real clock release failed");
}

void autonomousExpiryThread() {
  std::atomic<Time> clock{100};
  std::atomic<unsigned> reads{0};
  Enforcer e([&] { ++reads; return clock.load(); });
  Lease lease = makeLease(11, e.acquireBroker(), 150);
  e.install(lease);
  auto buffer = e.allocate(lease.key, 1);
  const auto before = reads.load();
  clock.store(180);
  // No enforcer API call occurs while the private thread advances expiry.
  for (unsigned i = 0; i < 100 && reads.load() <= before; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  check(reads.load() > before, "expiry clock was never read without a broker call");
  check(entry(e, lease.key).state == EnforcementState::Quarantined,
        "autonomous expiry failed to quarantine held memory");
}
} // namespace

int main() {
  try {
    idempotencyAndRenewal();
    shrinkAndEvidence();
    expiryAndTakeover();
    tokenBudgets();
    shrinkingExpiryAndClockFailure();
    concurrentRecall();
    realClockExpiry();
    autonomousExpiryThread();
    std::cout << "enforcer tests passed\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "enforcer test failed: " << e.what() << '\n';
    return 1;
  }
}
