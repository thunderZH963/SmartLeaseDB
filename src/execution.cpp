#include "smartlease/execution.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace smartlease {
namespace {
using Bytes = std::vector<std::uint8_t>;
constexpr std::uint32_t version = 1;
constexpr std::size_t max_id = 4096;
const std::string checkpoint_magic = "SLCKPT01";
const std::string store_magic = "SLSTORE1";
[[noreturn]] void invalid(const std::string& what) { throw std::runtime_error(what); }
void require(bool ok, const std::string& what) { if (!ok) invalid(what); }
std::uint64_t digest(const std::uint8_t* p, std::size_t n) {
    std::uint64_t h = 14695981039346656037ULL;
    for (std::size_t i = 0; i < n; ++i) { h ^= p[i]; h *= 1099511628211ULL; }
    return h;
}
void hash_word(std::uint64_t& h, std::uint64_t x) {
    for (int i = 0; i < 8; ++i) { h ^= x & 255U; h *= 1099511628211ULL; x >>= 8U; }
}
std::uint64_t rows_digest(const std::vector<Row>& rows) {
    std::uint64_t h = 14695981039346656037ULL;
    for (const auto& r : rows) { hash_word(h, static_cast<std::uint64_t>(r.key)); hash_word(h, static_cast<std::uint64_t>(r.value)); }
    return h;
}
std::int64_t signed_word(std::uint64_t v) {
    if (v <= static_cast<std::uint64_t>(INT64_MAX)) return static_cast<std::int64_t>(v);
    return -1 - static_cast<std::int64_t>(UINT64_MAX - v);
}
struct Writer {
    Bytes bytes;
    std::size_t limit;
    explicit Writer(std::size_t bound) : limit(bound) {}
    void room(std::size_t n) { require(bytes.size() <= limit && n <= limit - bytes.size(), "encoded state exceeds byte limit"); }
    void u8(std::uint8_t v) { room(1); bytes.push_back(v); }
    void u32(std::uint32_t v) { for (int i=0;i<4;++i) { u8(static_cast<std::uint8_t>(v)); v >>= 8U; } }
    void u64(std::uint64_t v) { for (int i=0;i<8;++i) { u8(static_cast<std::uint8_t>(v)); v >>= 8U; } }
    void i64(std::int64_t v) { u64(static_cast<std::uint64_t>(v)); }
    void raw(const std::uint8_t* p, std::size_t n) { room(n); if (n) bytes.insert(bytes.end(),p,p+n); }
    void str(const std::string& s) {
        require(!s.empty() && s.size() <= max_id && s.find('\0') == std::string::npos, "invalid identifier");
        u32(static_cast<std::uint32_t>(s.size())); raw(reinterpret_cast<const std::uint8_t*>(s.data()),s.size());
    }
    void header(const std::string& magic) { raw(reinterpret_cast<const std::uint8_t*>(magic.data()),8); u32(version); }
    Bytes finish() { const auto h = digest(bytes.data(),bytes.size()); u64(h); return std::move(bytes); }
};
struct Reader {
    const Bytes& bytes;
    std::size_t at = 0, end;
    Reader(const Bytes& b, std::size_t bound, const std::string& magic) : bytes(b), end(b.size()) {
        require(b.size() >= 20 && b.size() <= bound, "invalid state length");
        end -= 8;
        std::uint64_t stored = 0;
        for (int i=0;i<8;++i) stored |= static_cast<std::uint64_t>(b[end+static_cast<std::size_t>(i)]) << (8*i);
        require(digest(b.data(),end) == stored,"state checksum mismatch");
        require(std::equal(magic.begin(),magic.end(),b.begin()),"state format mismatch");
        at = 8; require(u32()==version,"unsupported state version");
    }
    std::size_t remaining() const { return end-at; }
    void need(std::size_t n) const { require(n <= remaining(),"truncated state"); }
    std::uint8_t u8() { need(1); return bytes[at++]; }
    std::uint32_t u32() { std::uint32_t v=0; for(int i=0;i<4;++i) v |= static_cast<std::uint32_t>(u8()) << (8*i); return v; }
    std::uint64_t u64() { std::uint64_t v=0; for(int i=0;i<8;++i) v |= static_cast<std::uint64_t>(u8()) << (8*i); return v; }
    std::int64_t i64() { return signed_word(u64()); }
    std::string str() {
        const auto n=u32(); require(n > 0 && n <= max_id,"invalid identifier length"); need(n);
        std::string s(reinterpret_cast<const char*>(bytes.data()+at),n); at += n;
        require(s.find('\0')==std::string::npos,"invalid identifier"); return s;
    }
    std::size_t count(std::size_t maximum, std::size_t width) {
        const auto n=u64(); require(n <= maximum && n <= remaining()/width,"invalid collection length"); return static_cast<std::size_t>(n);
    }
    Bytes blob(std::size_t maximum) { const auto n=count(maximum,1); Bytes b(bytes.begin()+static_cast<std::ptrdiff_t>(at),bytes.begin()+static_cast<std::ptrdiff_t>(at+n)); at+=n; return b; }
    void done() const { require(at==end,"trailing state data"); }
};
void identity(Writer& w,const ExecutionIdentity& id) {
    w.str(id.query); w.str(id.snapshot); w.str(id.schema); w.str(id.operator_id); w.str(id.expression_id); w.str(id.partition);
}
ExecutionIdentity identity(Reader& r) { ExecutionIdentity id; id.query=r.str(); id.snapshot=r.str(); id.schema=r.str(); id.operator_id=r.str(); id.expression_id=r.str(); id.partition=r.str(); return id; }
void validate_id(const std::string& s) { require(!s.empty() && s.size()<=max_id && s.find('\0')==std::string::npos,"invalid identifier"); }
void validate_spec(const OperatorSpec& s,std::uint64_t input_count) {
    for (const auto* v : {&s.identity.query,&s.identity.snapshot,&s.identity.schema,&s.identity.operator_id,&s.identity.expression_id,&s.identity.partition}) validate_id(*v);
    require(s.kind==OperatorKind::ScanFilter || s.kind==OperatorKind::GroupedAggregate || s.kind==OperatorKind::PartitionedHashJoin,"unknown operator kind");
    require(s.begin <= s.end && s.end <= input_count,"invalid morsel range");
    require(s.partitions>0 && s.partition_index<s.partitions,"invalid partition");
}
void output_row(Writer& w,const OutputRow& o) { w.i64(o.key); w.i64(o.value); w.i64(o.build_value); w.u64(o.count); w.i64(o.minimum); w.i64(o.maximum); }
OutputRow output_row(Reader& r) { OutputRow o; o.key=r.i64(); o.value=r.i64(); o.build_value=r.i64(); o.count=r.u64(); o.minimum=r.i64(); o.maximum=r.i64(); return o; }
void outputs(Writer& w,const std::vector<OutputRow>& rows) { w.u64(rows.size()); for(const auto& o:rows) output_row(w,o); }
std::vector<OutputRow> outputs(Reader& r,std::size_t maximum) {
    const auto n=r.count(maximum,48); std::vector<OutputRow> out; out.reserve(n); for(std::size_t i=0;i<n;++i) out.push_back(output_row(r)); return out;
}
std::int64_t add_exact(std::int64_t a,std::int64_t b) {
    if ((b>0 && a>INT64_MAX-b) || (b<0 && a<INT64_MIN-b)) throw std::overflow_error("SUM exceeds signed 64-bit range");
    return a+b;
}
struct Aggregate {
    std::int64_t sum=0, minimum=0, maximum=0;
    std::uint64_t count=0;
};
OutputRow aggregate_row(std::int64_t key,const Aggregate& a) { return {key,a.sum,0,a.count,a.minimum,a.maximum}; }
bool same_spec(const OperatorSpec& a,const OperatorSpec& b) {
    return a.identity==b.identity && a.kind==b.kind && a.minimum_value==b.minimum_value && a.begin==b.begin && a.end==b.end && a.partitions==b.partitions && a.partition_index==b.partition_index;
}
bool belongs(std::int64_t key,const OperatorSpec& s) { return static_cast<std::uint64_t>(key)%s.partitions==s.partition_index; }
struct SavedOperator {
    OperatorSpec spec;
    std::uint64_t input_count=0,input_hash=0,build_count=0,build_hash=0,cursor=0,match=0;
    std::map<std::int64_t,Aggregate> groups;
    std::vector<OutputRow> output;
};
SavedOperator decode_checkpoint(const Bytes& b,const OperatorLimits& limits,std::size_t bound) {
    Reader r(b,bound,checkpoint_magic); SavedOperator s;
    s.spec.identity=identity(r); s.spec.kind=static_cast<OperatorKind>(r.u8()); s.spec.minimum_value=r.i64();
    s.spec.begin=r.u64(); s.spec.end=r.u64(); s.spec.partitions=r.u32(); s.spec.partition_index=r.u32();
    s.input_count=r.u64(); s.input_hash=r.u64(); s.build_count=r.u64(); s.build_hash=r.u64(); s.cursor=r.u64(); s.match=r.u64();
    validate_spec(s.spec,s.input_count);
    require(s.input_count<=limits.max_input_rows && s.build_count<=limits.max_input_rows,"snapshot exceeds row bound");
    require(s.cursor>=s.spec.begin && s.cursor<=s.spec.end,"checkpoint cursor outside morsel");
    const auto n=r.count(limits.max_groups,40);
    std::uint64_t count=0;
    for(std::size_t i=0;i<n;++i) {
        auto key=r.i64(); Aggregate a; a.sum=r.i64(); a.count=r.u64(); a.minimum=r.i64(); a.maximum=r.i64();
        require(a.count>0 && a.minimum<=a.maximum && a.minimum>=s.spec.minimum_value && belongs(key,s.spec),"invalid aggregate state");
        require(a.count<=s.cursor-s.spec.begin-count,"aggregate count exceeds consumed input"); count+=a.count;
        require(a.count!=1 || (a.sum==a.minimum && a.minimum==a.maximum),"invalid singleton aggregate");
        require(s.groups.emplace(key,a).second,"duplicate aggregate key");
    }
    s.output=outputs(r,limits.max_output_rows); r.done();
    if(s.spec.kind==OperatorKind::GroupedAggregate) {
        require(s.match==0 && s.build_count==0,"unexpected aggregate join state");
        require(s.output.empty() || s.cursor==s.spec.end,"aggregate output precedes input boundary");
        require(s.output.size()<=s.groups.size(),"too many aggregate outputs");
        auto it=s.groups.begin(); for(const auto& o:s.output) { require(o==aggregate_row(it->first,it->second),"aggregate output/state mismatch"); ++it; }
    } else {
        require(s.groups.empty(),"unexpected grouped state");
        if(s.spec.kind==OperatorKind::ScanFilter) require(s.match==0 && s.build_count==0 && s.output.size()<=s.cursor-s.spec.begin,"invalid scan progress");
        else require((s.cursor<s.spec.end || s.match==0) && s.match<=s.build_count && s.match<=s.output.size(),"invalid join match cursor");
        for(const auto& o:s.output) require(o.count==0 && o.minimum==0 && o.maximum==0 && o.value>=s.spec.minimum_value && belongs(o.key,s.spec) && (s.spec.kind!=OperatorKind::ScanFilter || o.build_value==0),"invalid staged output");
    }
    return s;
}
std::size_t checkpoint_bound(std::size_t requested,std::size_t configured) { return requested==0 ? configured : std::min(requested,configured); }
} // namespace

bool OutputRow::operator==(const OutputRow& r) const { return std::tie(key,value,build_value,count,minimum,maximum)==std::tie(r.key,r.value,r.build_value,r.count,r.minimum,r.maximum); }
bool ExecutionIdentity::operator==(const ExecutionIdentity& r) const { return std::tie(query,snapshot,schema,operator_id,expression_id,partition)==std::tie(r.query,r.snapshot,r.schema,r.operator_id,r.expression_id,r.partition); }
bool MorselKey::operator==(const MorselKey& r) const { return std::tie(query,snapshot,morsel)==std::tie(r.query,r.snapshot,r.morsel); }
bool MorselKey::operator<(const MorselKey& r) const { return std::tie(query,snapshot,morsel)<std::tie(r.query,r.snapshot,r.morsel); }

struct PortableOperator::Impl {
    OperatorSpec spec;
    std::vector<Row> input,build;
    std::unordered_map<std::int64_t,std::vector<std::int64_t>> hash;
    std::map<std::int64_t,Aggregate> groups;
    std::vector<OutputRow> output;
    std::uint64_t cursor=0,match=0,input_hash=0,build_hash=0;
    bool done() const { return cursor==spec.end && (spec.kind!=OperatorKind::GroupedAggregate || output.size()==groups.size()); }
    void append(const OutputRow& o) { require(output.size()<spec.limits.max_output_rows,"staged output row bound exceeded"); output.push_back(o); }
};
PortableOperator::PortableOperator(OperatorSpec spec,std::vector<Row> input,std::vector<Row> build) : impl_(std::make_unique<Impl>()) {
    if(spec.end==UINT64_MAX) spec.end=input.size();
    validate_spec(spec,input.size());
    require(input.size()<=spec.limits.max_input_rows && build.size()<=spec.limits.max_input_rows,"input row bound exceeded");
    require(spec.kind==OperatorKind::PartitionedHashJoin || build.empty(),"non-join operator received build input");
    impl_->spec=std::move(spec); impl_->input=std::move(input); impl_->build=std::move(build); impl_->cursor=impl_->spec.begin;
    impl_->input_hash=rows_digest(impl_->input); impl_->build_hash=rows_digest(impl_->build);
    if(impl_->spec.kind==OperatorKind::PartitionedHashJoin) for(const auto& r:impl_->build) if(belongs(r.key,impl_->spec)) impl_->hash[r.key].push_back(r.value);
}
PortableOperator::~PortableOperator() = default;
PortableOperator::PortableOperator(PortableOperator&&) noexcept = default;
PortableOperator& PortableOperator::operator=(PortableOperator&&) noexcept = default;
std::size_t PortableOperator::step(std::size_t budget) {
    auto& p=*impl_; std::size_t work=0;
    while(work<budget && !p.done()) {
        if(p.cursor==p.spec.end) {
            auto it=p.output.empty()?p.groups.begin():p.groups.upper_bound(p.output.back().key);
            require(it!=p.groups.end(),"invalid aggregate emit position"); p.append(aggregate_row(it->first,it->second)); ++work; continue;
        }
        const auto& row=p.input[static_cast<std::size_t>(p.cursor)];
        if(row.value<p.spec.minimum_value || !belongs(row.key,p.spec)) { ++p.cursor; ++work; continue; }
        switch(p.spec.kind) {
        case OperatorKind::ScanFilter: p.append({row.key,row.value,0,0,0,0}); ++p.cursor; break;
        case OperatorKind::GroupedAggregate: {
            auto it=p.groups.find(row.key);
            if(it==p.groups.end()) { require(p.groups.size()<p.spec.limits.max_groups,"aggregate group bound exceeded"); p.groups.emplace(row.key,Aggregate{row.value,row.value,row.value,1}); }
            else { auto& a=it->second; auto sum=add_exact(a.sum,row.value); require(a.count<UINT64_MAX,"COUNT overflow"); a.sum=sum; ++a.count; a.minimum=std::min(a.minimum,row.value); a.maximum=std::max(a.maximum,row.value); }
            ++p.cursor; break;
        }
        case OperatorKind::PartitionedHashJoin: {
            auto it=p.hash.find(row.key);
            if(it==p.hash.end()) { require(p.match==0,"invalid unmatched join cursor"); ++p.cursor; }
            else { require(p.match<it->second.size(),"join match cursor outside build list"); p.append({row.key,row.value,it->second[static_cast<std::size_t>(p.match)],0,0,0}); ++p.match; if(p.match==it->second.size()) { p.match=0; ++p.cursor; } }
            break;
        }
        }
        ++work;
    }
    return work;
}
bool PortableOperator::finished() const { return impl_->done(); }
const std::vector<OutputRow>& PortableOperator::output() const { return impl_->output; }
const OperatorSpec& PortableOperator::spec() const { return impl_->spec; }
Bytes PortableOperator::checkpoint(std::size_t max_bytes) const {
    const auto& p=*impl_; Writer w(checkpoint_bound(max_bytes,p.spec.limits.max_checkpoint_bytes)); w.header(checkpoint_magic); identity(w,p.spec.identity);
    w.u8(static_cast<std::uint8_t>(p.spec.kind)); w.i64(p.spec.minimum_value); w.u64(p.spec.begin); w.u64(p.spec.end); w.u32(p.spec.partitions); w.u32(p.spec.partition_index);
    w.u64(p.input.size()); w.u64(p.input_hash); w.u64(p.build.size()); w.u64(p.build_hash); w.u64(p.cursor); w.u64(p.match);
    w.u64(p.groups.size()); for(const auto& kv:p.groups) { w.i64(kv.first); w.i64(kv.second.sum); w.u64(kv.second.count); w.i64(kv.second.minimum); w.i64(kv.second.maximum); }
    outputs(w,p.output); return w.finish();
}
PortableOperator PortableOperator::resume(OperatorSpec spec,std::vector<Row> input,std::vector<Row> build,const Bytes& checkpoint,std::size_t max_bytes) {
    auto saved=decode_checkpoint(checkpoint,spec.limits,checkpoint_bound(max_bytes,spec.limits.max_checkpoint_bytes));
    PortableOperator result(std::move(spec),std::move(input),std::move(build)); auto& p=*result.impl_;
    require(same_spec(p.spec,saved.spec),"checkpoint execution identity or semantics mismatch");
    require(p.input.size()==saved.input_count && p.input_hash==saved.input_hash && p.build.size()==saved.build_count && p.build_hash==saved.build_hash,"immutable snapshot content mismatch");
    if(p.spec.kind==OperatorKind::PartitionedHashJoin && saved.match>0) {
        const auto& row=p.input[static_cast<std::size_t>(saved.cursor)]; auto it=p.hash.find(row.key);
        require(row.value>=p.spec.minimum_value && belongs(row.key,p.spec) && it!=p.hash.end() && saved.match<it->second.size(),"invalid partial join probe");
        // A mid-probe cursor must be backed by the exact suffix already staged.
        const auto start=saved.output.size()-static_cast<std::size_t>(saved.match);
        for(std::size_t i=0;i<saved.match;++i) require(saved.output[start+i]==OutputRow{row.key,row.value,it->second[i],0,0,0},"join cursor/output mismatch");
    }
    p.cursor=saved.cursor; p.match=saved.match; p.groups=std::move(saved.groups); p.output=std::move(saved.output); return result;
}

namespace {
void key_write(Writer& w,const MorselKey& k) { w.str(k.query); w.str(k.snapshot); w.str(k.morsel); }
MorselKey key_read(Reader& r) { MorselKey k; k.query=r.str(); k.snapshot=r.str(); k.morsel=r.str(); return k; }
void key_valid(const MorselKey& k) { validate_id(k.query); validate_id(k.snapshot); validate_id(k.morsel); }
void checkpoint_for_key(const Bytes& b,const MorselKey& key,const CoordinatorLimits& limits) {
    if(b.empty()) return;
    OperatorLimits ol;
    // Coordinator validates format and structure; the resumed operator enforces
    // its own input/group limits and validates immutable snapshot fingerprints.
    ol.max_input_rows=std::numeric_limits<std::size_t>::max(); ol.max_groups=limits.max_checkpoint_bytes/40;
    ol.max_output_rows=limits.max_output_rows;
    auto s=decode_checkpoint(b,ol,limits.max_checkpoint_bytes);
    require(s.spec.identity.query==key.query && s.spec.identity.snapshot==key.snapshot && s.spec.identity.partition==key.morsel,"checkpoint belongs to another morsel");
}
using Records = std::map<MorselKey,MorselState>;
Bytes encode_store(const Records& records,const CoordinatorLimits& limits) {
    require(records.size()<=limits.max_morsels,"morsel limit exceeded"); Writer w(limits.max_store_bytes); w.header(store_magic); w.u64(records.size());
    for(const auto& kv:records) {
        const auto& s=kv.second; key_write(w,s.key); w.u64(s.generation); w.u8(s.complete?1:0);
        if(s.complete) w.str(s.commit_id);
        w.u64(s.checkpoint.size()); w.raw(s.checkpoint.data(),s.checkpoint.size()); outputs(w,s.output);
    }
    return w.finish();
}
Records decode_store(const Bytes& bytes,const CoordinatorLimits& limits) {
    Reader r(bytes,limits.max_store_bytes,store_magic); const auto n=r.count(limits.max_morsels,40); Records out;
    for(std::size_t i=0;i<n;++i) {
        MorselState s; s.key=key_read(r); s.generation=r.u64(); require(s.generation>0,"invalid persisted generation");
        auto flag=r.u8(); require(flag<=1,"invalid completion flag"); s.complete=flag==1; if(s.complete) s.commit_id=r.str();
        s.checkpoint=r.blob(limits.max_checkpoint_bytes); s.output=outputs(r,limits.max_output_rows);
        require(s.complete?s.checkpoint.empty():s.output.empty(),"inconsistent completion/output state"); checkpoint_for_key(s.checkpoint,s.key,limits);
        require(out.emplace(s.key,s).second,"duplicate persisted morsel");
    }
    r.done(); return out;
}
#if defined(__unix__) || defined(__APPLE__)
[[noreturn]] void io_error(const std::string& where) { throw std::system_error(errno,std::generic_category(),where); }
void sync_fd(int fd,const std::string& where) { int rc; do { rc=::fsync(fd); } while(rc<0 && errno==EINTR); if(rc<0) io_error(where); }
void write_all(int fd,const std::uint8_t* data,std::size_t size) {
    std::size_t at=0; while(at<size) { const auto n=::write(fd,data+at,size-at); if(n<0 && errno==EINTR) continue; if(n<=0) io_error("write coordinator state"); at+=static_cast<std::size_t>(n); }
}
Bytes read_all(int fd,std::size_t maximum) {
    struct stat st{}; if(::fstat(fd,&st)<0) io_error("stat coordinator state");
    require(S_ISREG(st.st_mode) && st.st_size>=0 && static_cast<std::uintmax_t>(st.st_size)<=maximum,"invalid coordinator file size/type");
    Bytes bytes(static_cast<std::size_t>(st.st_size)); std::size_t at=0;
    while(at<bytes.size()) { auto n=::read(fd,bytes.data()+at,bytes.size()-at); if(n<0 && errno==EINTR) continue; if(n<0) io_error("read coordinator state"); require(n!=0,"truncated coordinator file"); at+=static_cast<std::size_t>(n); }
    return bytes;
}
struct File {
    int fd=-1;
    explicit File(int descriptor=-1):fd(descriptor){}
    ~File(){ if(fd>=0) ::close(fd); }
    File(const File&)=delete; File& operator=(const File&)=delete;
};
#endif
} // namespace
struct DurableCoordinator::Impl {
    CoordinatorLimits limits;
    Records records;
    mutable std::mutex mutex;
    bool poisoned=false;
#if defined(__unix__) || defined(__APPLE__)
    File directory,lock;
#endif
    void healthy() const { require(!poisoned,"coordinator durability is uncertain; close and recover before reuse"); }
    void persist(Records next) {
        healthy(); auto data=encode_store(next,limits);
#if defined(__unix__) || defined(__APPLE__)
        try {
            if(::unlinkat(directory.fd,"state.tmp",0)<0 && errno!=ENOENT) io_error("remove stale coordinator temporary file");
            File file(::openat(directory.fd,"state.tmp",O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW|O_CLOEXEC,0600));
            if(file.fd<0) io_error("create coordinator temporary file");
            write_all(file.fd,data.data(),data.size()); sync_fd(file.fd,"fsync coordinator state");
            if(::renameat(directory.fd,"state.tmp",directory.fd,"state.bin")<0) io_error("publish coordinator state");
            sync_fd(directory.fd,"fsync coordinator directory"); records.swap(next);
        } catch(...) { poisoned=true; throw; }
#else
        (void)data; invalid("durable coordinator requires POSIX fsync/flock");
#endif
    }
};
DurableCoordinator::DurableCoordinator(std::string path,CoordinatorLimits limits):impl_(std::make_unique<Impl>()) {
    auto& p=*impl_; p.limits=limits;
    require(limits.max_store_bytes>=28 && limits.max_morsels>0,"invalid coordinator limits");
#if defined(__unix__) || defined(__APPLE__)
    require(!path.empty(),"empty coordinator directory");
    auto absolute=std::filesystem::absolute(path).lexically_normal();
    const bool created=std::filesystem::create_directory(absolute);
    if(created) { File parent(::open(absolute.parent_path().c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC)); if(parent.fd<0) io_error("open coordinator parent"); sync_fd(parent.fd,"fsync coordinator parent"); }
    p.directory.fd=::open(absolute.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC); if(p.directory.fd<0) io_error("open coordinator directory");
    p.lock.fd=::openat(p.directory.fd,"coordinator.lock",O_CREAT|O_RDWR|O_NOFOLLOW|O_CLOEXEC,0600); if(p.lock.fd<0) io_error("open coordinator lock");
    if(::flock(p.lock.fd,LOCK_EX|LOCK_NB)<0) io_error("coordinator already has a writer");
    auto marker=read_all(p.lock.fd,8); const std::string marker_text(marker.begin(),marker.end());
    require(marker.empty() || marker_text=="SLINIT01","corrupt coordinator initialization marker");
    File state(::openat(p.directory.fd,"state.bin",O_RDONLY|O_NOFOLLOW|O_CLOEXEC));
    if(state.fd<0) {
        if(errno!=ENOENT) io_error("open coordinator state");
        require(marker.empty(),"initialized coordinator is missing durable state"); p.persist({});
    } else {
        auto recovered=decode_store(read_all(state.fd,limits.max_store_bytes),limits);
        bool pending=false; for(auto& kv:recovered) if(!kv.second.complete) { require(kv.second.generation<UINT64_MAX,"placement generation exhausted"); ++kv.second.generation; pending=true; }
        if(pending) p.persist(std::move(recovered)); else p.records=std::move(recovered);
    }
    if(marker.empty()) { const auto* bytes=reinterpret_cast<const std::uint8_t*>("SLINIT01"); if(::lseek(p.lock.fd,0,SEEK_SET)<0) io_error("seek initialization marker"); write_all(p.lock.fd,bytes,8); sync_fd(p.lock.fd,"fsync initialization marker"); sync_fd(p.directory.fd,"fsync initialization directory"); }
#else
    (void)path; invalid("durable coordinator requires POSIX fsync/flock");
#endif
}
DurableCoordinator::~DurableCoordinator() = default;
MorselState DurableCoordinator::begin(const MorselKey& key) {
    auto& p=*impl_; std::lock_guard<std::mutex> guard(p.mutex); p.healthy(); key_valid(key);
    const auto it=p.records.find(key); if(it!=p.records.end()) return it->second;
    require(p.records.size()<p.limits.max_morsels,"morsel limit exceeded"); auto next=p.records; MorselState state; state.key=key; next.emplace(key,state); p.persist(std::move(next)); return state;
}
MorselState DurableCoordinator::state(const MorselKey& key) const {
    auto& p=*impl_; std::lock_guard<std::mutex> guard(p.mutex); p.healthy(); auto it=p.records.find(key); require(it!=p.records.end(),"unknown morsel"); return it->second;
}
std::vector<MorselState> DurableCoordinator::states() const {
    auto& p=*impl_; std::lock_guard<std::mutex> guard(p.mutex); p.healthy(); std::vector<MorselState> result; result.reserve(p.records.size()); for(const auto& kv:p.records) result.push_back(kv.second); return result;
}
CommitResult DurableCoordinator::commit(const MorselKey& key,std::uint64_t generation,const std::string& id,const std::vector<OutputRow>& output) {
    auto& p=*impl_; std::lock_guard<std::mutex> guard(p.mutex); p.healthy(); auto it=p.records.find(key); require(it!=p.records.end(),"unknown morsel");
    const auto& s=it->second; if(generation!=s.generation) return CommitResult::Fenced;
    if(s.complete) return s.commit_id==id && s.output==output ? CommitResult::AlreadyCommitted : CommitResult::Conflict;
    validate_id(id); require(output.size()<=p.limits.max_output_rows,"commit output row bound exceeded");
    auto next=p.records; auto& n=next.at(key); n.complete=true; n.commit_id=id; n.output=output; n.checkpoint.clear(); p.persist(std::move(next)); return CommitResult::Committed;
}
HandoffDecision DurableCoordinator::handoff(const MorselKey& key,std::uint64_t generation,const Bytes& checkpoint) {
    auto& p=*impl_; std::lock_guard<std::mutex> guard(p.mutex); p.healthy(); auto it=p.records.find(key); require(it!=p.records.end(),"unknown morsel");
    const auto& s=it->second;
    if(generation!=s.generation) return {HandoffResult::Fenced,s.generation};
    if(s.complete) return {HandoffResult::AlreadyCommitted,s.generation};
    checkpoint_for_key(checkpoint,key,p.limits); require(s.generation<UINT64_MAX,"placement generation exhausted");
    auto next=p.records; auto& n=next.at(key); ++n.generation; n.checkpoint=checkpoint; const auto new_generation=n.generation; p.persist(std::move(next)); return {HandoffResult::HandedOff,new_generation};
}
} // namespace smartlease
