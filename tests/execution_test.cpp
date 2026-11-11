#include "smartlease/execution.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace smartlease;
namespace {
void check(bool value,const std::string& message) { if(!value) throw std::runtime_error(message); }
template<class F> void fails(F&& f,const std::string& message) { bool threw=false; try { f(); } catch(const std::exception&) { threw=true; } check(threw,message); }
OperatorSpec specification(OperatorKind kind=OperatorKind::ScanFilter,std::string morsel="m1") {
    OperatorSpec s; s.identity={"query","snapshot","i64-pair-v1","operator","predicate-v1",std::move(morsel)}; s.kind=kind; return s;
}
void finish(PortableOperator& op) { std::size_t guard=0; while(!op.finished()) { check(op.step(3)>0,"unfinished operator must progress"); check(++guard<100000,"operator failed to finish"); } check(op.step(3)==0,"completed operator did work"); }
struct Temp {
    std::filesystem::path path;
    Temp() {
        static std::atomic<unsigned> seq{0};
        const auto nonce=std::chrono::steady_clock::now().time_since_epoch().count();
        path=std::filesystem::temp_directory_path()/("smartlease-execution-"+std::to_string(nonce)+"-"+std::to_string(seq++));
        check(std::filesystem::create_directory(path),"temporary directory creation failed");
    }
    ~Temp() { std::error_code ec; std::filesystem::remove_all(path,ec); }
};
using Bytes=std::vector<std::uint8_t>;
void set64(Bytes& b,std::size_t pos,std::uint64_t v) { check(pos+8<=b.size(),"bad test offset"); for(int i=0;i<8;++i) { b[pos+static_cast<std::size_t>(i)]=static_cast<std::uint8_t>(v); v>>=8U; } }
std::uint32_t get32(const Bytes& b,std::size_t pos) { std::uint32_t n=0; for(int i=0;i<4;++i) n|=static_cast<std::uint32_t>(b.at(pos+static_cast<std::size_t>(i)))<<(8*i); return n; }
void reseal(Bytes& b) { std::uint64_t h=14695981039346656037ULL; for(std::size_t i=0;i<b.size()-8;++i) { h^=b[i]; h*=1099511628211ULL; } set64(b,b.size()-8,h); }
std::size_t after_identity(const Bytes& b) { std::size_t at=12; for(int i=0;i<6;++i) at+=4+get32(b,at); return at; }
Bytes read_file(const std::filesystem::path& path) { std::ifstream in(path,std::ios::binary); check(in.good(),"read fixture failed"); return Bytes(std::istreambuf_iterator<char>(in),{}); }
void write_file(const std::filesystem::path& path,const Bytes& b) { std::ofstream out(path,std::ios::binary|std::ios::trunc); out.write(reinterpret_cast<const char*>(b.data()),static_cast<std::streamsize>(b.size())); check(out.good(),"write fixture failed"); }

void resume_equivalence() {
    const std::vector<Row> input={{2,7},{1,3},{2,-4},{3,5},{-1,8},{1,9},{2,1},{8,0}};
    const std::vector<Row> build={{1,100},{2,200},{1,101},{-1,400},{2,201},{2,202},{7,0}};
    for(auto kind:{OperatorKind::ScanFilter,OperatorKind::GroupedAggregate,OperatorKind::PartitionedHashJoin}) {
        auto spec=specification(kind); spec.minimum_value=-2;
        auto used_build=kind==OperatorKind::PartitionedHashJoin?build:std::vector<Row>{};
        PortableOperator baseline(spec,input,used_build); finish(baseline);
        const auto expected=baseline.output();
        for(std::size_t stop=0;stop<25;++stop) {
            PortableOperator original(spec,input,used_build); check(original.step(0)==0,"zero budget moved cursor");
            check(original.step(stop)<=stop,"step exceeded budget");
            auto state=original.checkpoint(); auto resumed=PortableOperator::resume(spec,input,used_build,state);
            check(resumed.output()==original.output(),"staged output changed across resume");
            check(resumed.checkpoint()==state,"portable checkpoint not canonical after resume");
            finish(original); finish(resumed); check(original.output()==expected && resumed.output()==expected,"resume result differs from uninterrupted result");
        }
        PortableOperator chained(spec,input,used_build);
        while(!chained.finished()) { chained.step(1); chained=PortableOperator::resume(spec,input,used_build,chained.checkpoint()); }
        check(chained.output()==expected,"repeated single-step migration changed result");
    }
    auto agg=specification(OperatorKind::GroupedAggregate);
    PortableOperator aggregate(agg,{{1,4},{1,-2},{2,-8},{1,3},{2,1}}); finish(aggregate);
    check(aggregate.output()==std::vector<OutputRow>{{1,5,0,3,-2,4},{2,-7,0,2,-8,1}},"grouped SUM/COUNT/MIN/MAX semantics wrong");
    auto join=specification(OperatorKind::PartitionedHashJoin);
    PortableOperator joined(join,{{1,4},{1,5},{3,7}},{{1,9},{1,8},{1,9}}); finish(joined);
    check(joined.output()==std::vector<OutputRow>{{1,4,9,0,0,0},{1,4,8,0,0,0},{1,4,9,0,0,0},{1,5,9,0,0,0},{1,5,8,0,0,0},{1,5,9,0,0,0}},"join did not preserve duplicate multiplicity/order");
    for(auto kind:{OperatorKind::ScanFilter,OperatorKind::GroupedAggregate,OperatorKind::PartitionedHashJoin}) {
        auto spec=specification(kind); PortableOperator empty(spec,{}); check(empty.finished() && empty.output().empty(),"empty morsel should finish");
        auto resumed=PortableOperator::resume(spec,{}, {},empty.checkpoint()); check(resumed.finished(),"empty resume failed");
    }
    for(std::uint32_t part=0;part<3;++part) {
        auto spec=join; spec.partitions=3; spec.partition_index=part; spec.begin=1; spec.end=7;
        PortableOperator op(spec,input,build); op.step(2); auto restored=PortableOperator::resume(spec,input,build,op.checkpoint()); finish(restored);
        std::vector<OutputRow> expected;
        for(std::size_t i=1;i<7;++i) if(static_cast<std::uint64_t>(input[i].key)%3==part) for(const auto& b:build) if(b.key==input[i].key) expected.push_back({b.key,input[i].value,b.value,0,0,0});
        check(restored.output()==expected,"partitioned join differs from nested-loop oracle");
    }
}
void bounds_and_corruption() {
    auto spec=specification(); std::vector<Row> rows={{1,2},{2,3},{1,4}}; PortableOperator op(spec,rows); op.step(1); auto bytes=op.checkpoint();
    fails([&]{ op.checkpoint(bytes.size()-1); },"checkpoint byte bound not enforced");
    check(op.checkpoint(bytes.size())==bytes,"exact checkpoint byte bound rejected");
    for(std::size_t n=0;n<bytes.size();++n) { Bytes bad(bytes.begin(),bytes.begin()+static_cast<std::ptrdiff_t>(n)); fails([&]{ PortableOperator::resume(spec,rows,{},bad); },"truncated checkpoint accepted"); }
    for(std::size_t i=0;i<bytes.size();++i) { auto bad=bytes; bad[i]^=1; fails([&]{ PortableOperator::resume(spec,rows,{},bad); },"damaged checkpoint accepted"); }
    auto bad=bytes; bad.push_back(0); fails([&]{ PortableOperator::resume(spec,rows,{},bad); },"trailing garbage accepted");
    bad=bytes; const auto kind_at=after_identity(bytes); bad[kind_at]=99; reseal(bad); fails([&]{ PortableOperator::resume(spec,rows,{},bad); },"unknown operator kind accepted");
    bad=bytes; bad[8]=2; reseal(bad); fails([&]{ PortableOperator::resume(spec,rows,{},bad); },"unknown checkpoint version accepted");
    const auto cursor_at=kind_at+1+8+8+8+4+4+8+8+8+8;
    bad=bytes; set64(bad,cursor_at,4); reseal(bad); fails([&]{ PortableOperator::resume(spec,rows,{},bad); },"out-of-range cursor accepted");
    bad=bytes; set64(bad,cursor_at+8,1); reseal(bad); fails([&]{ PortableOperator::resume(spec,rows,{},bad); },"scan with join match cursor accepted");
    auto changed=spec; changed.identity.snapshot="new"; fails([&]{ PortableOperator::resume(changed,rows,{},bytes); },"wrong snapshot identity accepted");
    changed=spec; changed.minimum_value=0; fails([&]{ PortableOperator::resume(changed,rows,{},bytes); },"changed expression semantics accepted");
    auto altered=rows; altered[0].value=3; fails([&]{ PortableOperator::resume(spec,altered,{},bytes); },"changed immutable input accepted");
    changed=spec; changed.end=4; fails([&]{ PortableOperator invalid(changed,rows); },"out-of-range morsel accepted");
    changed=spec; changed.partitions=0; fails([&]{ PortableOperator invalid(changed,rows); },"zero partitions accepted");
    changed=spec; changed.limits.max_input_rows=1; fails([&]{ PortableOperator invalid(changed,rows); },"input row limit ignored");
    changed=spec; changed.limits.max_output_rows=1; PortableOperator bounded(changed,rows); bounded.step(1); fails([&]{ bounded.step(1); },"output bound ignored"); check(bounded.output().size()==1,"output bound failure changed output");
    auto agg=specification(OperatorKind::GroupedAggregate); agg.limits.max_groups=1; PortableOperator bounded_groups(agg,rows); bounded_groups.step(1); fails([&]{ bounded_groups.step(1); },"aggregate group bound ignored");
    for(auto pair:std::vector<std::vector<Row>>{{{1,INT64_MAX},{1,1}},{{1,INT64_MIN},{1,-1}}}) {
        auto s=specification(OperatorKind::GroupedAggregate); PortableOperator overflow(s,pair); overflow.step(1); auto before=overflow.checkpoint(); fails([&]{ overflow.step(1); },"SUM overflow not rejected"); check(overflow.checkpoint()==before,"overflow changed checkpoint state");
    }
    auto s=specification(OperatorKind::GroupedAggregate); PortableOperator edge(s,{{1,INT64_MIN},{1,INT64_MAX}}); finish(edge); check(edge.output().front().value==-1,"safe extreme SUM incorrect");
    auto js=specification(OperatorKind::PartitionedHashJoin); std::vector<Row> build={{1,10},{1,20},{1,30}}; PortableOperator j(js,{{1,2}},build); j.step(1); auto ck=j.checkpoint();
    build[0].value=11; fails([&]{ PortableOperator::resume(js,{{1,2}},build,ck); },"mutated build snapshot accepted");
    bad=ck; const auto jc=after_identity(bad)+1+8+8+8+4+4+8+8+8+8; set64(bad,jc+8,3); reseal(bad); fails([&]{ PortableOperator::resume(js,{{1,2}},{{1,10},{1,20},{1,30}},bad); },"invalid duplicate cursor accepted");
}
void durable_recovery() {
    Temp t; const MorselKey key{"query","snapshot","m1"}, pending{"query","snapshot","pending"};
    auto spec=specification(); std::vector<Row> rows={{1,3},{2,4},{3,5}}; PortableOperator op(spec,rows); op.step(1); auto ck=op.checkpoint(); std::uint64_t generation=0;
    {
        DurableCoordinator c(t.path.string()); auto first=c.begin(key); check(first.generation==1 && !first.complete,"new morsel state wrong");
        check(c.begin(key).generation==1,"duplicate begin advanced generation");
        fails([&]{ DurableCoordinator other(t.path.string()); },"two writers acquired same store");
        auto d=c.handoff(key,1,ck); check(d.result==HandoffResult::HandedOff && d.generation==2,"handoff did not fence");
        check(c.commit(key,1,"old",{})==CommitResult::Fenced,"late commit accepted");
        check(c.handoff(key,1,ck).result==HandoffResult::Fenced,"stale handoff accepted");
        check(c.state(key).output.empty() && c.state(key).checkpoint==ck,"uncommitted output exposed or checkpoint lost");
        c.begin(pending); generation=d.generation;
    }
    {
        DurableCoordinator c(t.path.string()); const auto resumed_state=c.state(key); check(resumed_state.generation==generation+1 && resumed_state.checkpoint==ck,"restart did not retain checkpoint and advance generation");
        check(c.commit(key,generation,"pre-restart",{})==CommitResult::Fenced,"pre-restart generation accepted");
        auto resumed=PortableOperator::resume(spec,rows,{},resumed_state.checkpoint); finish(resumed);
        generation=resumed_state.generation;
        check(c.commit(key,generation,"commit-a",resumed.output())==CommitResult::Committed,"commit rejected");
        check(c.commit(key,generation,"commit-a",resumed.output())==CommitResult::AlreadyCommitted,"commit retry not idempotent");
        check(c.commit(key,generation,"commit-b",resumed.output())==CommitResult::Conflict,"conflicting commit id accepted");
        auto changed=resumed.output(); changed[0].value++;
        check(c.commit(key,generation,"commit-a",changed)==CommitResult::Conflict,"changed payload accepted for same commit id");
        check(c.handoff(key,generation,ck).result==HandoffResult::AlreadyCommitted,"completed morsel handed off");
        check(c.state(key).complete && c.state(key).checkpoint.empty() && c.state(key).output==resumed.output(),"output and completion not consistent");
    }
    {
        DurableCoordinator c(t.path.string()); auto state=c.state(key); check(state.complete && state.generation==generation,"completed generation changed on recovery");
        check(c.state(pending).generation==3 && !c.state(pending).complete,"unfinished generation was not fenced on every restart");
        check(c.commit(key,generation,"commit-a",state.output)==CommitResult::AlreadyCommitted,"commit retry lost after restart");
        check(c.states().size()==2,"recovered morsel enumeration wrong");
        fails([&]{ c.handoff(pending,3,ck); },"checkpoint for another morsel accepted");
        auto wrong=ck; wrong.back()^=1; fails([&]{ c.handoff(pending,3,wrong); },"corrupt handoff checkpoint accepted");
        check(c.state(pending).generation==3,"failed handoff changed generation");
        check(c.handoff(pending,3).generation==4,"replay handoff failed");
    }
    auto original=read_file(t.path/"state.bin"); auto corrupt=original; corrupt[20]^=1; write_file(t.path/"state.bin",corrupt);
    fails([&]{ DurableCoordinator c(t.path.string()); },"corrupt durable snapshot accepted");
    write_file(t.path/"state.bin",original); { DurableCoordinator c(t.path.string()); check(c.state(key).complete,"valid snapshot recovery failed"); }
    std::filesystem::remove(t.path/"state.bin"); fails([&]{ DurableCoordinator c(t.path.string()); },"missing initialized state silently reset generations");
}
void commit_handoff_race() {
    Temp t; DurableCoordinator c(t.path.string()); const std::vector<OutputRow> output={{1,8,0,0,0,0}};
    for(unsigned trial=0;trial<32;++trial) {
        MorselKey key{"q","s",std::to_string(trial)}; c.begin(key); std::atomic<int> ready{0}; std::atomic<bool> go{false};
        CommitResult committed=CommitResult::Conflict; HandoffDecision handed{HandoffResult::Fenced,0}; std::exception_ptr e1,e2;
        std::thread writer([&] { try { ++ready; while(!go.load()) std::this_thread::yield(); committed=c.commit(key,1,"commit",output); } catch(...) { e1=std::current_exception(); } });
        std::thread revoker([&] { try { ++ready; while(!go.load()) std::this_thread::yield(); handed=c.handoff(key,1); } catch(...) { e2=std::current_exception(); } });
        while(ready.load()!=2) std::this_thread::yield(); go=true; writer.join(); revoker.join(); if(e1) std::rethrow_exception(e1); if(e2) std::rethrow_exception(e2);
        auto state=c.state(key);
        if(committed==CommitResult::Committed) check(handed.result==HandoffResult::AlreadyCommitted && state.complete && state.generation==1 && state.output==output,"commit-winning race inconsistent");
        else { check(committed==CommitResult::Fenced && handed.result==HandoffResult::HandedOff && !state.complete && state.output.empty() && state.generation==2,"handoff-winning race inconsistent"); check(c.commit(key,2,"replacement",output)==CommitResult::Committed,"replacement failed to commit"); }
    }
    for(const auto& state:c.states()) check(state.complete && state.output==output,"race lost or duplicated output");
}
void bounds_and_restart_crash() {
    Temp t; CoordinatorLimits small; small.max_morsels=1; small.max_output_rows=1;
    {
        DurableCoordinator c(t.path.string(),small); MorselKey key{"q","s","m"}; c.begin(key);
        fails([&]{ c.begin({"q","s","other"}); },"coordinator morsel bound ignored");
        fails([&]{ c.commit(key,1,"oversize",{{},{}}); },"coordinator output bound ignored"); check(!c.state(key).complete,"failed commit completed morsel");
        fails([&]{ c.state({"missing","s","m"}); },"unknown morsel accepted");
    }
#if defined(__unix__) || defined(__APPLE__)
    Temp crashed;
    const auto pid=::fork(); check(pid>=0,"fork failed");
    if(pid==0) {
        try { DurableCoordinator c(crashed.path.string()); MorselKey key{"q","s","committed"}; c.begin(key); c.commit(key,1,"id",{{1,7,0,0,0,0}}); c.begin({"q","s","pending"}); ::_exit(0); } catch(...) { ::_exit(2); }
    }
    int status=0; check(::waitpid(pid,&status,0)==pid && WIFEXITED(status) && WEXITSTATUS(status)==0,"child durable operations failed");
    { DurableCoordinator c(crashed.path.string()); auto state=c.state({"q","s","committed"}); check(state.complete && state.output==std::vector<OutputRow>{{1,7,0,0,0,0}},"process exit lost committed output"); check(c.state({"q","s","pending"}).generation==2,"process exit did not fence unfinished work"); }
#endif
}
} // namespace
int main() {
    try {
        resume_equivalence(); bounds_and_corruption(); durable_recovery(); commit_handoff_race(); bounds_and_restart_crash();
        std::cout << "execution_test: all checks passed\n"; return 0;
    } catch(const std::exception& e) { std::cerr << "execution_test: " << e.what() << '\n'; return 1; }
}
