#include "smartlease/broker.hpp"
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <thread>

using namespace smartlease;
namespace {
void check(bool value, const char* why) { if (!value) throw std::runtime_error(why); }
template<class Fn> void rejects(Fn fn) {
  bool rejected = false;
  try { fn(); } catch (const std::exception&) { rejected = true; }
  check(rejected, "expected rejection");
}
Request request() {
  Request r; r.tenant = 1; r.demand = {1000, 4096, 100000, 100000};
  r.checkpoint_bytes = 1024; r.estimated_release_ms = 5; return r;
}
void releaseAndShrink() {
  std::atomic<Time> clock{1}; Enforcer enforcer([&] { return clock.load(); });
  Broker broker(enforcer, {2000, 8192, 200000, 200000});
  broker.configureTenant(1, 1);
  check(!broker.reserve(request()), "cold broker has no telemetry authority");
  broker.setForeground({});
  auto lease = *broker.reserve(request());
  check(broker.usage() == lease.quota && !broker.grant(lease.key), "pending reservation is charged but unpublished");
  auto ack = broker.install(lease.key);
  check(!broker.grant(lease.key), "lost install ACK cannot publish a grant");
  check(!broker.cancelUninstalled(lease.key), "sent install cannot be canceled as absent");
  check(broker.acknowledgeInstall(broker.install(lease.key)), "retry discovers exact install");
  check(broker.acknowledgeInstall(ack), "duplicate ACK idempotent");
  auto forged = ack; forged.lease.quota.memory_bytes++;
  check(!broker.acknowledgeInstall(forged), "forged install ACK rejected");
  auto buffer = enforcer.allocate(lease.key, 1024);
  auto io = enforcer.beginIo(lease.key, 128);
  Resources target{500, 2048, 50000, 50000};
  broker.recall(lease.key, target);
  check(!broker.grant(lease.key) && !enforcer.pollRelease(lease.key), "shrink fences work and awaits in-flight IO");
  check(broker.usage() == lease.quota, "projected shrink is not reusable");
  io = {};
  auto receipt = enforcer.pollRelease(lease.key);
  check(receipt && receipt->successor.has_value(), "drain permits shrink successor");
  check(broker.usage() == lease.quota, "lost release ACK conservatively charged");
  auto bad = *receipt; bad.freed.memory_bytes++;
  check(!broker.acknowledgeRelease(bad), "mismatched release evidence rejected");
  check(broker.acknowledgeRelease(*receipt), "verified shrink consumed");
  check(broker.usage() == target, "only acknowledged difference retired");
  auto successor = *receipt->successor;
  check(broker.grant(successor.key).has_value() && !broker.grant(lease.key), "new generation alone executable");
  broker.acknowledgeRelease(*receipt);
  check(broker.usage() == target, "duplicate old ACK cannot retire successor");
  broker.recall(successor.key);
  check(!enforcer.pollRelease(successor.key), "retained state prevents full release");
  clock.store(30); enforcer.tick(); broker.reconcile();
  check(broker.usage() == target, "quarantine still occupies budget");
  buffer = {};
  check(enforcer.pollRelease(successor.key).has_value(), "actual buffer destruction verifies cleanup");
  broker.reconcile();
  check(broker.usage().empty(), "inventory repairs missing release ACK");
  broker.acknowledgeRelease(*receipt);
  check(broker.usage().empty(), "old shrink receipt cannot resurrect a retired successor");
}
void restartAndExpiry() {
  std::atomic<Time> clock{10}; Enforcer enforcer([&] { return clock.load(); });
  Resources h{1000,4096,100000,100000};
  Broker first(enforcer, h); first.configureTenant(1, 1); first.setForeground({});
  auto lease = *first.reserve(request());
  auto lostAck = first.install(lease.key);
  (void)lostAck;
  auto memory = enforcer.allocate(lease.key, 64);
  Broker second(enforcer, h); second.configureTenant(1, 1); second.setForeground({});
  check(second.usage() == h && !second.reserve(request()), "restart reconstructs pending physical occupancy");
  rejects([&] { first.reserve(request()); });
  rejects([&] { first.renew(lease.key, 100); });
  check(!enforcer.canExecute(lease.key), "takeover fences old worker");
  memory = {}; second.reconcile();
  check(second.usage().empty(), "takeover cleanup eventually reusable");
  auto next = second.admitRanked({request()}).at(0);
  auto hold = enforcer.allocate(next.key, 8);
  clock.store(next.expires_at); enforcer.tick();
  check(!second.grant(next.key), "local expiry needs no broker message");
  rejects([&] { second.renew(next.key, 100); });
  second.reconcile(); check(second.usage() == h, "expired state remains charged");
  hold = {}; second.reconcile(); check(second.usage().empty(), "expired cleanup retires usage");
}
void pressureAndConcurrency() {
  std::atomic<Time> clock{1}; Enforcer enforcer([&] { return clock.load(); });
  Resources h{8000,32768,800000,800000};
  Broker broker(enforcer, h); broker.configureTenant(1, 1); broker.setForeground({});
  auto notSent = *broker.reserve(request());
  broker.failClosed(); rejects([&] { broker.install(notSent.key); });
  broker.recallToBudget(); check(broker.usage().empty(), "unsent reservation canceled safely under pressure");
  broker.setForeground({});
  auto pending = *broker.reserve(request()); auto ack = broker.install(pending.key);
  auto pin = enforcer.allocate(pending.key, 8);
  broker.failClosed();
  check(!broker.acknowledgeInstall(ack), "late install ACK cannot publish after budget collapses");
  check(broker.usage() == pending.quota, "pressure recall retains physical charge");
  pin = {}; broker.reconcile(); broker.setForeground({});
  std::atomic<int> grants{0}; std::atomic<bool> failure{false};
  std::vector<std::thread> threads;
  for (int i = 0; i < 32; ++i) threads.emplace_back([&] {
    try { auto out = broker.admitRanked({request()}); grants += static_cast<int>(out.size()); }
    catch (...) { failure = true; }
  });
  for (auto& t : threads) t.join();
  check(!failure && grants == 8 && broker.usage() == h, "concurrent grants cannot oversubscribe any dimension");
  broker.setForeground({0, 16384, 0, 0});
  auto recalled = broker.recallToBudget();
  check(!recalled.empty() && broker.usage() == h, "recall planner does not spend pending releases");
  check(!broker.reserve(request()), "no grant on projected reclaim");
  broker.reconcile(); check(broker.usage().fits(broker.budget()), "acknowledged memory shrink satisfies pressure");
}
void retryAndPressureRegressions() {
  std::atomic<Time> clock{1}; Enforcer enforcer([&] { return clock.load(); });
  Broker broker(enforcer, {1000, 1000, 10000, 10000});
  broker.configureTenant(1, 1); broker.setForeground({});
  Request a = request(); a.checkpoint_bytes = 0; a.demand = {200,100,1000,1000}; a.benefit = 0.01;
  Request b = a; b.demand.cpu_millis = 800; b.benefit = 1; b.ttl_ms = 1000;
  auto ga = broker.admitRanked({a}).at(0);
  auto gb = broker.admitRanked({b}).at(0);
  auto pa = enforcer.allocate(ga.key, 16);
  auto pb = enforcer.allocate(gb.key, 16);
  clock.store(50); auto renewed = broker.renew(gb.key, 1000);
  check(broker.install(gb.key).lease.expires_at == renewed.expires_at, "retry install after renewal reads current status");
  clock.store(201); broker.setForeground({600,0,0,0});
  broker.recallToBudget(); // One old reservation expired autonomously.
  auto inv = enforcer.inventory();
  bool shrunkB = false;
  for (const auto& e : inv.entries) if (e.lease.key.id == gb.key.id && e.state == EnforcementState::Active)
    shrunkB |= e.lease.quota.cpu_millis <= 400;
  check(shrunkB, "expired reservation excluded from active recall selection");
  // A completed shrink's successor must be observed before stronger pressure.
  broker.failClosed(); broker.recallToBudget();
  check(!enforcer.canExecute({gb.key.id,gb.key.incarnation,gb.key.generation + 1}), "successor recalled on subsequent pressure");
  pa = {}; pb = {}; broker.reconcile(); check(broker.usage().empty(), "all stronger recalls complete");
  broker.setForeground({});
  auto late = *broker.reserve(a);
  clock.store(late.expires_at + 1);
  rejects([&] { broker.install(late.key); });
  check(broker.usage().empty(), "definitive local install rejection cannot strand budget");
}
void expiryDuringRecallSnapshot() {
  std::atomic<Time> clock{100}; std::atomic<bool> armed{false};
  std::atomic<unsigned> reads{0}; const auto caller = std::this_thread::get_id();
  Enforcer enforcer([&] {
    // Expire exactly after the authority/inventory snapshots, at requestRelease.
    if (armed && std::this_thread::get_id() == caller && ++reads == 3) clock.store(201);
    return clock.load();
  });
  Broker broker(enforcer, {1000, 1000, 1000, 1000});
  broker.configureTenant(1, 1); broker.setForeground({});
  Request r; r.tenant = 1; r.demand = {200,100,10,10}; r.benefit = 0.01;
  auto a = broker.admitRanked({r}).at(0); auto held = enforcer.allocate(a.key, 50);
  r.demand.cpu_millis = 800; r.benefit = 1; r.ttl_ms = 1000;
  auto b = broker.admitRanked({r}).at(0); (void)b;
  broker.setForeground({600,0,0,0}); armed = true;
  broker.recallToBudget(); // An expected concurrent expiry must not abort the loop.
  armed = false;
  auto inv = enforcer.inventory();
  check(inv.charged.cpu_millis <= 600, "remaining active quota shrinks after concurrent expiry");
  held = {}; broker.reconcile();
  check(broker.usage().fits(broker.budget()), "concurrent expiry cleanup yields feasible occupancy");
}
}
int main() {
  try {
    releaseAndShrink(); restartAndExpiry(); pressureAndConcurrency(); retryAndPressureRegressions(); expiryDuringRecallSnapshot();
    std::cout << "broker: ACK loss, shrink fencing, takeover, expiry, pressure and concurrent budgets passed\n";
    return 0;
  } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
