#include "smartlease/broker.hpp"
#include <algorithm>
#include <limits>

namespace smartlease {
namespace {
bool sameLease(const Lease& a, const Lease& b) {
  return a.key == b.key && a.quota == b.quota && a.expires_at == b.expires_at &&
         a.release_grace == b.release_grace;
}
bool sameReceipt(const ReleaseReceipt& a, const ReleaseReceipt& b) {
  return a.key == b.key && a.freed == b.freed &&
      a.successor.has_value() == b.successor.has_value() &&
      (!a.successor || sameLease(*a.successor, *b.successor));
}
BrokerState translate(EnforcementState state) {
  switch (state) {
    case EnforcementState::Active: return BrokerState::Active;
    case EnforcementState::PendingRelease: return BrokerState::PendingRelease;
    case EnforcementState::Quarantined: return BrokerState::Quarantined;
    case EnforcementState::Retired: return BrokerState::Retired;
  }
  throw std::logic_error("unknown enforcement state");
}
} // namespace

Broker::Broker(Enforcer& e, Resources physical, Resources guard)
    : enforcer_(e), physical_(physical), guard_(guard), residual_(Resources{}), incarnation_(0) {
  if (physical.empty() || !guard.fits(physical)) throw std::invalid_argument("invalid physical budget");
  incarnation_ = enforcer_.acquireBroker();
  reconcileLocked(enforcer_.inventory());
  // A fresh foreground sample is required, including after broker restart.
}
void Broker::checkAuthority() const {
  if (enforcer_.inventory().incarnation != incarnation_)
    throw std::runtime_error("broker incarnation fenced");
}
void Broker::configureTenant(std::uint64_t t, double w) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  credits_.configure(t, w);
}
void Broker::setForeground(Resources demand) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  residual_ = residual_budget(physical_, demand, guard_);
}
void Broker::failClosed() {
  std::lock_guard<std::mutex> lock(mutex_);
  residual_ = {};
}
Resources Broker::budget() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return residual_;
}
Resources Broker::usageLocked() const {
  Resources total;
  for (const auto& entry : ledger_) total = total + entry.second.charged;
  return total;
}
Resources Broker::usage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return usageLocked();
}
std::vector<Reservation> Broker::reservations() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<Reservation> out;
  for (const auto& entry : ledger_) out.push_back(entry.second);
  return out;
}
Reservation& Broker::findLocked(const LeaseKey& key) {
  auto it = ledger_.find(key);
  if (it == ledger_.end()) throw std::invalid_argument("unknown reservation");
  return it->second;
}
std::optional<Lease> Broker::reserve(const Request& r) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  if (!eligible(r) || !credits_.contains(r.tenant)) return {};
  auto used = usageLocked();
  if (!used.fits(residual_) || !r.demand.fits(residual_ - used)) return {};
  if (next_id_ == std::numeric_limits<std::uint64_t>::max())
    throw std::overflow_error("lease ID space exhausted");
  Lease l{{next_id_++, incarnation_, 1}, r.demand,
          checked_deadline(enforcer_.now(), r.ttl_ms), r.release_grace_ms};
  ledger_.emplace(l.key, Reservation{l, r, r.demand, BrokerState::PendingInstall, false});
  return l;
}
InstallAck Broker::install(const LeaseKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  auto& r = findLocked(key);
  if (r.state != BrokerState::PendingInstall && r.state != BrokerState::Active)
    throw std::logic_error("install after recall/retirement");
  if (r.state == BrokerState::Active) {
    // Renewal changes the installed deadline, not the immutable install request.
    // An install retry on an acknowledged grant is a status read.
    const auto inv = enforcer_.inventory();
    for (const auto& e : inv.entries)
      if (e.lease.key == key)
        return {e.lease, e.state == EnforcementState::Active};
    throw std::runtime_error("active grant missing from enforcer inventory");
  }
  if (!usageLocked().fits(residual_))
    throw std::runtime_error("installation denied under pressure");
  r.install_sent = true;
  try { return enforcer_.install(r.lease); }
  catch (...) {
    // This adapter is synchronous and process-local: after install has returned
    // an error there is no in-flight message. An authoritative absent record is
    // a definitive rejection. A remote transport would need a negative ACK or
    // an install-cancellation tombstone, not a timeout or an inventory omission.
    const auto inv = enforcer_.inventory();
    const auto found = std::find_if(inv.entries.begin(), inv.entries.end(),
        [&](const auto& e) { return e.lease.key == key; });
    if (inv.incarnation == incarnation_ && found == inv.entries.end()) {
      r.charged = {}; r.state = BrokerState::Retired;
    }
    throw;
  }
}
bool Broker::acknowledgeInstall(const InstallAck& ack) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  auto it = ledger_.find(ack.lease.key);
  if (it == ledger_.end() || !ack.active || !sameLease(it->second.lease, ack.lease)) return false;
  if (it->second.state != BrokerState::PendingInstall && it->second.state != BrokerState::Active) return false;
  if (!usageLocked().fits(residual_)) {
    enforcer_.requestRelease(ack.lease.key);
    it->second.state = BrokerState::PendingRelease;
    return false;
  }
  const auto inv = enforcer_.inventory();
  for (const auto& e : inv.entries)
    if (e.state == EnforcementState::Active && sameLease(e.lease, ack.lease)) {
      it->second.state = BrokerState::Active;
      return true;
    }
  return false;
}
std::optional<Lease> Broker::grant(const LeaseKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  auto it = ledger_.find(key);
  if (it == ledger_.end() || it->second.state != BrokerState::Active ||
      !enforcer_.canExecute(key)) return {};
  return it->second.lease;
}
bool Broker::cancelUninstalled(const LeaseKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  auto& r = findLocked(key);
  if (r.state != BrokerState::PendingInstall || r.install_sent) return false;
  auto inv = enforcer_.inventory();
  for (const auto& e : inv.entries) if (e.lease.key == key) return false;
  r.charged = {};
  r.state = BrokerState::Retired;
  return true;
}
Lease Broker::renew(const LeaseKey& key, Time ttl) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  auto& r = findLocked(key);
  if (key.incarnation != incarnation_ || r.state != BrokerState::Active || !ttl || ttl > 1000)
    throw std::invalid_argument("invalid renewal");
  if (!usageLocked().fits(residual_)) throw std::runtime_error("renewal denied under pressure");
  auto installed = enforcer_.renew(key, checked_deadline(enforcer_.now(), ttl));
  r.lease = installed;
  return installed;
}
void Broker::recall(const LeaseKey& key, Resources target) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  auto& r = findLocked(key);
  if (!target.fits(r.lease.quota)) throw std::invalid_argument("recall may not expand quota");
  enforcer_.requestRelease(key, target);
  // Even immediate cleanup does not return capacity until its receipt is processed.
  if (r.state != BrokerState::Retired) r.state = BrokerState::PendingRelease;
}
bool Broker::acknowledgeRelease(const ReleaseReceipt& ack) {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  auto it = ledger_.find(ack.key);
  if (it == ledger_.end()) return false;
  const auto evidence = enforcer_.pollRelease(ack.key);
  if (!evidence || !sameReceipt(ack, *evidence)) return false;
  reconcileLocked(enforcer_.inventory());
  return true;
}
void Broker::reconcileLocked(const EnforcerInventory& inv) {
  if (inv.incarnation != incarnation_) throw std::runtime_error("stale broker inventory");
  // Read receipts first to inherit tenant metadata across a shrink generation.
  for (const auto& receipt : inv.receipts) {
    if (!receipt.successor) continue;
    const auto old = ledger_.find(receipt.key);
    if (old == ledger_.end() || ledger_.count(receipt.successor->key)) continue;
    auto successor = old->second;
    successor.lease = *receipt.successor;
    successor.charged = receipt.successor->quota;
    successor.state = BrokerState::Active;
    successor.install_sent = true;
    successor.request.demand = receipt.successor->quota;
    ledger_.emplace(receipt.successor->key, successor);
  }
  for (const auto& e : inv.entries) {
    if (e.lease.key.id == std::numeric_limits<std::uint64_t>::max())
      throw std::overflow_error("lease ID space exhausted");
    next_id_ = std::max(next_id_, e.lease.key.id + 1);
    auto it = ledger_.find(e.lease.key);
    if (it == ledger_.end()) {
      // Orphaned old-incarnation allocations remain charged and cannot resume.
      Request orphan;
      orphan.tenant = 0;
      it = ledger_.emplace(e.lease.key, Reservation{e.lease, orphan, e.charged,
          translate(e.state), true}).first;
    }
    it->second.lease = e.lease;
    it->second.charged = e.charged;
    it->second.state = translate(e.state);
  }
  // Unknown pending installs are kept, not inferred released from a missing ACK.
}
void Broker::reconcile() {
  std::lock_guard<std::mutex> lock(mutex_);
  reconcileLocked(enforcer_.inventory());
}
std::vector<LeaseKey> Broker::recallToBudget() {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  std::vector<LeaseKey> selected;
  // Expiry/cleanup can advance inventory between a read and a shrink request.
  // Re-plan a bounded number of times; churn leaves work for the next interval.
  for (unsigned attempt = 0; attempt < 4; ++attempt) {
    const auto inventory = enforcer_.inventory();
    reconcileLocked(inventory); // Observe expiry and completed successor generations.
    bool changed = false;
    auto issue = [&](const LeaseKey& key, Resources target) {
      try { enforcer_.requestRelease(key, target); return true; }
      catch (const std::exception&) {
        const auto fresh = enforcer_.inventory();
        if (fresh.incarnation != incarnation_) throw;
        const auto before = std::find_if(inventory.entries.begin(), inventory.entries.end(),
            [&](const auto& e) { return e.lease.key == key; });
        const auto after = std::find_if(fresh.entries.begin(), fresh.entries.end(),
            [&](const auto& e) { return e.lease.key == key; });
        if (before != inventory.entries.end() && after != fresh.entries.end() &&
            (before->state != after->state || before->target != after->target ||
             !sameLease(before->lease, after->lease))) {
          changed = true;
          return false;
        }
        throw; // An unchanged record means a real request/backend error.
      }
    };
    Resources projected = usageLocked();
    std::vector<Reservation*> active;
    // Pending targets affect only the recall plan, never the allocation ledger.
    std::vector<const EnforcementEntry*> pending;
    for (const auto& e : inventory.entries) {
      auto known = ledger_.find(e.lease.key);
      if (known == ledger_.end() || !e.target || e.state == EnforcementState::Retired) continue;
      projected = projected - positive_difference(known->second.charged, *e.target);
      pending.push_back(&e);
    }
    for (const auto* e : pending) {
      if (projected.fits(residual_)) break;
      if (!e->target->empty()) {
        if (!issue(e->lease.key, {})) break; // stronger pressure supersedes a shrink
        projected = projected - *e->target;
        selected.push_back(e->lease.key);
      }
    }
    if (changed) continue;
    for (auto& entry : ledger_) {
      auto& r = entry.second;
      if (r.state == BrokerState::Active) active.push_back(&r);
      if (r.state == BrokerState::PendingInstall) {
        auto installed = std::find_if(inventory.entries.begin(), inventory.entries.end(),
            [&](const auto& e) { return e.lease.key == r.lease.key; });
        if (installed != inventory.entries.end() && installed->state == EnforcementState::Active)
          active.push_back(&r);
        else if (installed == inventory.entries.end() && !r.install_sent && !projected.fits(residual_)) {
          // This request has never left the broker, so no physical permission exists.
          projected = projected - r.charged;
          r.charged = {};
          r.state = BrokerState::Retired;
        }
      }
    }
    std::stable_sort(active.begin(), active.end(), [&](const auto* a, const auto* b) {
      auto score = [&](const Reservation* r) {
        return request_score(r->request, physical_, credits_.contains(r->request.tenant) ?
                             credits_.multiplier(r->request.tenant) : 0.25);
      };
      return score(a) < score(b);
    });
    for (auto* r : active) {
      if (projected.fits(residual_)) break;
      auto excess = positive_difference(projected, residual_);
      auto target = positive_difference(r->charged, excess);
      if (target == r->charged) continue; // This lease cannot relieve this dimension.
      if (!issue(r->lease.key, target)) break;
      r->state = BrokerState::PendingRelease;
      projected = projected - (r->charged - target);
      selected.push_back(r->lease.key);
    }
    if (changed) continue;
    return selected;
  }
  return selected;
}
void Broker::advanceCredits() {
  std::lock_guard<std::mutex> lock(mutex_);
  checkAuthority();
  std::map<std::uint64_t, double> shares;
  for (const auto& e : ledger_)
    if (credits_.contains(e.second.request.tenant))
      shares[e.second.request.tenant] += dominant_share(e.second.charged, physical_);
  credits_.advance(enforcer_.now(), shares);
}
std::vector<Lease> Broker::admitRanked(const std::vector<Request>& requests) {
  std::vector<std::pair<double, Request>> sorted;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    checkAuthority();
    for (const auto& r : requests)
      if (eligible(r) && credits_.contains(r.tenant))
        sorted.emplace_back(request_score(r, residual_, credits_.multiplier(r.tenant)), r);
  }
  std::stable_sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) { return a.first > b.first; });
  std::vector<Lease> grants;
  for (const auto& e : sorted) {
    if (auto reserved = reserve(e.second)) {
      auto ack = install(reserved->key);
      if (acknowledgeInstall(ack)) grants.push_back(ack.lease);
    }
  }
  return grants;
}
} // namespace smartlease
