#pragma once
#include "smartlease/controller.hpp"
#include "smartlease/enforcer.hpp"
#include <map>
#include <mutex>
#include <optional>
#include <vector>

namespace smartlease {
enum class BrokerState { PendingInstall, Active, PendingRelease, Quarantined, Retired };
struct Reservation {
  Lease lease;
  Request request;
  Resources charged;
  BrokerState state = BrokerState::PendingInstall;
  bool install_sent = false;
};

// The broker owns resource accounting; the enforcer owns physical proof.
// All mutation is serialized. The split reserve/install/ACK API permits lost
// messages to be exercised without returning unconfirmed capacity to the pool.
class Broker {
 public:
  Broker(Enforcer& enforcer, Resources physical_capacity, Resources guard = {});
  void configureTenant(std::uint64_t tenant, double weight);
  void setForeground(Resources demand);
  void failClosed(); // Missing/stale telemetry: residual budget becomes zero.
  Resources budget() const;
  Resources usage() const;
  std::uint64_t incarnation() const { return incarnation_; }
  std::vector<Reservation> reservations() const;

  std::optional<Lease> reserve(const Request& request);
  InstallAck install(const LeaseKey& key); // send; does NOT publish its reply
  bool acknowledgeInstall(const InstallAck& ack);
  std::optional<Lease> grant(const LeaseKey& key);
  bool cancelUninstalled(const LeaseKey& key);
  Lease renew(const LeaseKey& key, Time ttl_ms);
  void recall(const LeaseKey& key, Resources target = {});
  bool acknowledgeRelease(const ReleaseReceipt& ack);
  void reconcile(); // Authoritative inventory repairs lost ACKs and restart.

  // Pressure recall never spends projected reclaimed capacity. Shrink is
  // attempted in the constrained dimensions; progress still requires cleanup.
  std::vector<LeaseKey> recallToBudget();
  std::vector<Lease> admitRanked(const std::vector<Request>& requests);
  void advanceCredits();
 private:
  void checkAuthority() const;
  Resources usageLocked() const;
  Reservation& findLocked(const LeaseKey& key);
  void reconcileLocked(const EnforcerInventory& inventory);
  Enforcer& enforcer_;
  Resources physical_, guard_, residual_;
  std::uint64_t incarnation_, next_id_ = 1;
  std::map<LeaseKey, Reservation> ledger_;
  WeightedCredits credits_;
  mutable std::mutex mutex_;
};
} // namespace smartlease
