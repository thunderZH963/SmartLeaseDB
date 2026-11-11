#include "smartlease/controller.hpp"
#include <algorithm>
#include <cmath>
#include <limits>

namespace smartlease {
DemandEstimator::DemandEstimator(Time window, Time stale) : window_(window), stale_after_(stale) {
  if (!window || !stale) throw std::invalid_argument("positive estimator windows required");
}

void DemandEstimator::observe(Resources demand, Time now) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (last_ && now <= *last_) throw std::invalid_argument("samples must advance time");
  auto data = demand.values();
  const long double alpha = last_ ? -std::expm1(-static_cast<long double>(now - *last_) / window_) : 1;
  for (std::size_t i = 0; i < data.size(); ++i)
    mean_[i] = alpha * data[i] + (1 - alpha) * mean_[i];
  last_ = now;
}

std::optional<Resources> DemandEstimator::forecast(Time now) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!last_ || now < *last_ || now - *last_ > stale_after_) return {};
  std::array<std::uint64_t, 4> data{};
  for (std::size_t i = 0; i < data.size(); ++i) {
    auto v = std::ceil(mean_[i]);
    data[i] = v >= static_cast<long double>(std::numeric_limits<std::uint64_t>::max())
        ? std::numeric_limits<std::uint64_t>::max() : static_cast<std::uint64_t>(v);
  }
  return Resources::from(data);
}

bool eligible(const Request& r, Time max_ttl) {
  return r.tenant != 0 && r.bounded_recovery && !r.demand.empty() &&
         r.checkpoint_bytes <= r.demand.memory_bytes && r.ttl_ms > 0 &&
         r.ttl_ms <= max_ttl && r.release_grace_ms > 0 &&
         r.estimated_release_ms <= r.release_grace_ms &&
         std::isfinite(r.benefit) && r.benefit > 0 &&
         std::isfinite(r.urgency) && r.urgency > 0;
}

WeightedCredits::WeightedCredits(double cap) : cap_(cap) {
  if (!std::isfinite(cap) || cap <= 0) throw std::invalid_argument("invalid credit cap");
}
void WeightedCredits::configure(std::uint64_t t, double weight) {
  if (!t || !std::isfinite(weight) || weight <= 0)
    throw std::invalid_argument("positive tenant and finite weight required");
  auto found = accounts_.find(t);
  if (found == accounts_.end()) accounts_.emplace(t, Account{weight, 0});
  else found->second.weight = weight;
}
bool WeightedCredits::contains(std::uint64_t t) const { return accounts_.count(t) != 0; }
void WeightedCredits::advance(Time now, const std::map<std::uint64_t, double>& usage) {
  if (last_ && now < *last_) throw std::invalid_argument("credit clock regressed");
  for (auto entry : usage)
    if (!contains(entry.first) || !std::isfinite(entry.second) || entry.second < 0)
      throw std::invalid_argument("invalid tenant usage");
  if (!last_) { last_ = now; return; }
  long double total = 0;
  for (auto entry : accounts_) total += entry.second.weight;
  auto seconds = static_cast<long double>(now - *last_) / 1000;
  for (auto& entry : accounts_) {
    const auto it = usage.find(entry.first);
    const auto spent = it == usage.end() ? 0.0 : it->second;
    auto updated = entry.second.credit + seconds * (entry.second.weight / total - spent);
    entry.second.credit = static_cast<double>(std::clamp(updated, -static_cast<long double>(cap_), static_cast<long double>(cap_)));
  }
  last_ = now;
}
double WeightedCredits::multiplier(std::uint64_t t) const {
  return std::clamp(1.0 + balance(t) / cap_, 0.25, 2.0);
}
double WeightedCredits::balance(std::uint64_t t) const {
  auto it = accounts_.find(t);
  if (it == accounts_.end()) throw std::invalid_argument("unconfigured tenant");
  return it->second.credit;
}
double dominant_share(Resources demand, Resources capacity) {
  auto d = demand.values(), c = capacity.values();
  long double share = 0;
  for (std::size_t i = 0; i < d.size(); ++i) {
    if (!c[i] && d[i]) return std::numeric_limits<double>::infinity();
    if (c[i]) share = std::max(share, static_cast<long double>(d[i]) / c[i]);
  }
  return static_cast<double>(share);
}
double request_score(const Request& r, Resources budget, double multiplier) {
  if (!eligible(r) || !std::isfinite(multiplier) || multiplier <= 0) return 0;
  auto d = r.demand.values(), b = budget.values();
  long double cost = 1e-6L + static_cast<long double>(r.checkpoint_bytes) / (16 * 1024 * 1024);
  for (std::size_t i = 0; i < d.size(); ++i) {
    if (!b[i] && d[i]) return 0;
    if (b[i]) cost += static_cast<long double>(d[i]) / b[i];
  }
  long double value = static_cast<long double>(r.benefit) * r.urgency * multiplier / cost;
  return static_cast<double>(std::min(value, static_cast<long double>(std::numeric_limits<double>::max())));
}
Resources residual_budget(Resources capacity, Resources foreground, Resources guard) {
  return positive_difference(positive_difference(capacity, foreground), guard);
}
} // namespace smartlease
