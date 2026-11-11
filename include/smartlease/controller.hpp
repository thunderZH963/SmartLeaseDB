#pragma once
#include "smartlease/types.hpp"
#include <map>
#include <mutex>
#include <optional>
#include <vector>

namespace smartlease {

// A sample is infrastructure demand, never the database's own occupancy.
// First observation initializes the EWMA. A stale forecast cannot admit work.
class DemandEstimator {
 public:
  DemandEstimator(Time smoothing_window_ms = 1000, Time stale_after_ms = 100);
  void observe(Resources demand, Time now);
  std::optional<Resources> forecast(Time now) const;
 private:
  Time window_, stale_after_;
  std::optional<Time> last_;
  std::array<long double, 4> mean_{};
  mutable std::mutex mutex_;
};

struct Request {
  std::uint64_t tenant = 0;
  Resources demand; // peak resident bytes include checkpoint/staged-output space
  std::uint64_t checkpoint_bytes = 0;
  Time estimated_release_ms = 0;
  Time ttl_ms = 100;
  Time release_grace_ms = 20;
  double benefit = 1.0;
  double urgency = 1.0;
  bool bounded_recovery = true;
};

// A conservative eligibility check: a byte bound alone cannot prove timely release.
bool eligible(const Request& request, Time maximum_ttl_ms = 1000);

class WeightedCredits {
 public:
  explicit WeightedCredits(double cap = 2.0);
  void configure(std::uint64_t tenant, double weight);
  bool contains(std::uint64_t tenant) const;
  // Usage is normalized dominant occupancy per tenant, summed across its leases.
  void advance(Time now, const std::map<std::uint64_t, double>& usage);
  double multiplier(std::uint64_t tenant) const;
  double balance(std::uint64_t tenant) const;
 private:
  struct Account { double weight, credit; };
  std::map<std::uint64_t, Account> accounts_;
  std::optional<Time> last_;
  double cap_;
};

double dominant_share(Resources demand, Resources capacity);
double request_score(const Request& request, Resources budget, double credit_multiplier);
Resources residual_budget(Resources capacity, Resources foreground, Resources guard);

} // namespace smartlease
