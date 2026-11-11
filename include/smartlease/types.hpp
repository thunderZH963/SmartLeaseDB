#pragma once

#include <array>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <tuple>

namespace smartlease {

// Every timestamp is milliseconds on the enforcer's monotonic clock.
using Time = std::uint64_t;

struct Resources {
  std::uint64_t cpu_millis = 0; // 1000 = one core; this is a rate, not CPU time.
  std::uint64_t memory_bytes = 0;
  std::uint64_t network_bytes_per_sec = 0;
  std::uint64_t io_bytes_per_sec = 0;

  std::array<std::uint64_t, 4> values() const {
    return {cpu_millis, memory_bytes, network_bytes_per_sec, io_bytes_per_sec};
  }
  static Resources from(const std::array<std::uint64_t, 4>& a) {
    return {a[0], a[1], a[2], a[3]};
  }
  bool fits(const Resources& capacity) const {
    auto a = values(), b = capacity.values();
    for (std::size_t i = 0; i < a.size(); ++i) if (a[i] > b[i]) return false;
    return true;
  }
  bool empty() const { return *this == Resources{}; }
  bool operator==(const Resources& b) const { return values() == b.values(); }
  bool operator!=(const Resources& b) const { return !(*this == b); }
};

inline Resources operator+(const Resources& a, const Resources& b) {
  auto out = a.values(), rhs = b.values();
  for (std::size_t i = 0; i < out.size(); ++i) {
    if (rhs[i] > std::numeric_limits<std::uint64_t>::max() - out[i])
      throw std::overflow_error("resource addition overflow");
    out[i] += rhs[i];
  }
  return Resources::from(out);
}

inline Resources operator-(const Resources& a, const Resources& b) {
  if (!b.fits(a)) throw std::underflow_error("resource subtraction underflow");
  auto out = a.values(), rhs = b.values();
  for (std::size_t i = 0; i < out.size(); ++i) out[i] -= rhs[i];
  return Resources::from(out);
}

inline Resources positive_difference(const Resources& a, const Resources& b) {
  auto out = a.values(), rhs = b.values();
  for (std::size_t i = 0; i < out.size(); ++i)
    out[i] = out[i] > rhs[i] ? out[i] - rhs[i] : 0;
  return Resources::from(out);
}

inline Time checked_deadline(Time now, Time duration) {
  if (duration > std::numeric_limits<Time>::max() - now)
    throw std::overflow_error("deadline overflow");
  return now + duration;
}

struct LeaseKey {
  std::uint64_t id = 0;
  std::uint64_t incarnation = 0;
  std::uint64_t generation = 0;
  bool operator==(const LeaseKey& b) const {
    return std::tie(id, incarnation, generation) ==
           std::tie(b.id, b.incarnation, b.generation);
  }
  bool operator!=(const LeaseKey& b) const { return !(*this == b); }
  bool operator<(const LeaseKey& b) const {
    return std::tie(id, incarnation, generation) <
           std::tie(b.id, b.incarnation, b.generation);
  }
};

struct Lease {
  LeaseKey key;
  Resources quota;
  Time expires_at = 0;
  Time release_grace = 0;
};

} // namespace smartlease
