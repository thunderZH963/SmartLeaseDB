#include "smartlease/controller.hpp"
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>

using namespace smartlease;
namespace {
void check(bool condition, const char* why) { if (!condition) throw std::runtime_error(why); }
template<class Fn> void rejects(Fn fn) {
  bool rejected = false;
  try { fn(); } catch (const std::exception&) { rejected = true; }
  check(rejected, "expected rejection");
}
}
int main() {
  try {
    Resources h{8000, 16384, 100000, 50000}, fg{1000, 1000, 10000, 1000}, guard{800, 800, 10000, 5000};
    check(residual_budget(h, fg, guard) == Resources{6200, 14584, 80000, 44000}, "dimensional residual");
    check(residual_budget(h, {99999, 0, 0, 0}, guard).cpu_millis == 0, "foreground overload clamped");
    rejects([] { auto unused = Resources{1, 0, 0, 0} - Resources{2, 0, 0, 0}; (void)unused; });
    rejects([] { checked_deadline(std::numeric_limits<Time>::max(), 1); });
    rejects([] { auto unused = Resources{std::numeric_limits<std::uint64_t>::max(),0,0,0} + Resources{1,0,0,0}; (void)unused; });
    DemandEstimator ewma(1000, 100);
    check(!ewma.forecast(0), "no cold-start fabricated capacity");
    ewma.observe({1000, 2000, 0, 0}, 0);
    check(ewma.forecast(0)->memory_bytes == 2000, "first sample initialization");
    ewma.observe({2000, 4000, 0, 0}, 10);
    check(ewma.forecast(10)->cpu_millis == 1010, "time-scaled EWMA");
    check(!ewma.forecast(111) && !ewma.forecast(9), "stale/regressed forecast invalid");
    rejects([&] { ewma.observe({}, 9); });
    rejects([&] { ewma.observe({}, 10); });
    Request r; r.tenant = 1; r.demand = {1000, 4096, 1000, 1000}; r.checkpoint_bytes = 1024;
    check(eligible(r), "eligible bounded work");
    r.estimated_release_ms = 21;
    check(!eligible(r), "release time bound enforced");
    r.estimated_release_ms = 2; r.checkpoint_bytes = 8192;
    check(!eligible(r), "checkpoint envelope must fit resident peak");
    r.checkpoint_bytes = 1024; r.benefit = std::numeric_limits<double>::quiet_NaN();
    check(!eligible(r), "NaN priority rejected");
    r.benefit = 1;
    check(request_score(r, {}, 1) == 0, "zero-resource feasibility");
    check(request_score(r, h, 2) > request_score(r, h, 1), "weighted priority");
    WeightedCredits credits;
    credits.configure(1, 1); credits.configure(2, 3);
    credits.advance(0, {}); credits.advance(1000, {{1, 0.8}, {2, 0.2}});
    check(std::abs(credits.balance(1) + 0.55) < 1e-9, "usage charged against configured share");
    check(std::abs(credits.balance(2) - 0.55) < 1e-9, "under-served tenant gains credit");
    credits.advance(1000000, {});
    check(credits.balance(1) == 2 && credits.balance(2) == 2, "idle credit capped");
    rejects([&] { credits.advance(0, {}); });
    rejects([&] { credits.advance(1000001, {{3, 1}}); });
    std::cout << "controller: resource arithmetic, eligibility, EWMA and weighted credits passed\n";
    return 0;
  } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
