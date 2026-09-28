#include "nix/expr/attr-set.hh"
#include "nix/expr/eval-inline.hh"

#include <algorithm>
#include <optional>

#if defined(__x86_64__) && defined(__SSE2__)
#  include <immintrin.h>
#endif

namespace nix {

const constinit Bindings Bindings::emptyBindings;

namespace {

using size_type = Bindings::size_type;

#if defined(__x86_64__) && defined(__SSE2__)
/**
 * Branch-light SIMD linear scan over `first[0..n)`, used by findIndex()
 * for small-to-medium chunks in place of binary search.
 *
 * Binary search's data-dependent, sequentially-dependent comparisons
 * don't predict well (each step's branch outcome depends on the
 * previous one, so the CPU can't speculate ahead reliably). A linear
 * scan comparing 4 `Symbol` (uint32) lanes at once via SSE2 trades
 * O(log n) mispredicted branches for O(n/4) branch-light SIMD compares,
 * which wins for small n despite the worse asymptotic complexity.
 *
 * The crossover is asymmetric and workload-mix-dependent: measured with
 * a standalone benchmark (matching this TU's release compile flags,
 * many independent attrset instances + a long non-repeating query
 * stream, to avoid artificially teaching the branch predictor one
 * fixed array -- see task notes) present-key lookups keep favoring the
 * SIMD scan out past 500 entries (~-13% to -30% vs. binary search),
 * while absent-key lookups are roughly a wash from ~210-260 entries
 * and a clear, growing loss for the SIMD scan from ~270 on (+5% at
 * 270, +15% by 320, +30%+ by 450). Under a conservative 50/50
 * present/absent weighting, the blended win/loss crossover lands
 * around ~350-360; `simdLinearScanMaxSize` is kept comfortably under
 * that for margin (and real workloads, which are usually
 * present-lookup-dominated, have even more headroom than this).
 *
 * SSE2 is x86_64 baseline (every x86_64 CPU has it), so this carries no
 * target attribute and is fully inlined into both clones of
 * Bindings::get() by LTO exactly as before this change.
 */
std::optional<size_type> simdFindIndex(const Symbol * first, size_type n, Symbol name) noexcept
{
    const __m128i key = _mm_set1_epi32(static_cast<int>(name.getId()));
    size_type i = 0;
    for (; i + 4 <= n; i += 4) {
        __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(first + i));
        __m128i cmp = _mm_cmpeq_epi32(key, data);
        int mask = _mm_movemask_ps(_mm_castsi128_ps(cmp));
        if (mask)
            return i + static_cast<size_type>(__builtin_ctz(static_cast<unsigned>(mask)));
    }
    for (; i < n; ++i)
        if (first[i] == name)
            return i;
    return std::nullopt;
}

constexpr size_type simdLinearScanMaxSize = 320;

/**
 * AVX2 (8-lane) widening of simdFindIndex(): same branch-light linear
 * scan, but comparing 8 `Symbol` (uint32) lanes per instruction via
 * `_mm256_cmpeq_epi32`. AVX2 is NOT x86_64 baseline (only guaranteed from
 * the x86-64-v3 microarchitecture level on), so this function must carry
 * `target("avx2")` and must never run on a CPU that lacks it.
 *
 * Unlike the SSE2 scan above, this is used *only* by the "avx2" clone of
 * Bindings::get() (@see the target_clones attribute on that definition,
 * below). Because get() itself is multiversioned via the same mechanism,
 * the "avx2" clone shares this function's target, and this call is fully
 * inlined into it by LTO (confirmed via -fopt-info-inline and by checking
 * with nm/objdump that no standalone avx2FindIndex machine code is
 * reachable from the resolved ifunc in the linked, whole-program-LTO'd
 * .so) -- unlike a prior attempt (see git history) that called an
 * equivalent target("avx2") leaf from a single, non-multiversioned
 * get(), where the target-attribute boundary blocked inlining into
 * get()'s hot call chain and lost more from that than the wider SIMD
 * compare won.
 *
 * The "default" clone's copy of the *same* source (target_clones shares
 * one parsed body across all clones -- there is no per-clone
 * preprocessing) also textually contains a call to this function, kept
 * safe only by the avx2Available() runtime check in findIndex() below.
 */
__attribute__((target("avx2"))) std::optional<size_type>
avx2FindIndex(const Symbol * first, size_type n, Symbol name) noexcept
{
    const __m256i key = _mm256_set1_epi32(static_cast<int>(name.getId()));
    size_type i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(first + i));
        __m256i cmp = _mm256_cmpeq_epi32(key, data);
        unsigned mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_castsi256_ps(cmp)));
        if (mask)
            return i + static_cast<size_type>(__builtin_ctz(mask));
    }
    for (; i < n; ++i)
        if (first[i] == name)
            return i;
    return std::nullopt;
}

/**
 * Located the same way simdLinearScanMaxSize was (standalone
 * microbenchmark, release flags, many independent arrays, 50/50
 * present/absent query mix, interleaved trials, Welch's t-test). AVX2
 * beats scalar binary search decisively up to ~675 entries, then a
 * noisy crossover zone from ~700-875, then loses clearly from ~900 on.
 * 640 sits comfortably under the start of that noisy zone. Separately,
 * AVX2 beat SSE2 at every size measured from 150 up to 1400.
 */
constexpr size_type avx2LinearScanMaxSize = 640;

/**
 * Test-only override for whether findIndex() believes AVX2 is available.
 * Real dispatch between the "avx2"/"default" *clones* of Bindings::get()
 * is done entirely by the compiler-generated ifunc resolver at process
 * load time and cannot be affected by this. This only overrides the
 * *internal* avx2Available() check below, inside whichever clone actually
 * got resolved and called -- letting tests force-exercise the SSE2/scalar
 * fallback logic from within the AVX2 clone on AVX2-capable test
 * hardware. (There is no way to force the opposite -- running the AVX2
 * path on hardware that actually lacks AVX2 -- for obvious reasons.)
 */
std::optional<bool> & avx2OverrideForTesting() noexcept
{
    static std::optional<bool> currentOverride;
    return currentOverride;
}

bool avx2Available() noexcept
{
    if (auto o = avx2OverrideForTesting())
        return *o;
    return __builtin_cpu_supports("avx2");
}
#endif

/**
 * Find the index of `name` within the first `n` entries of a single
 * (unlayered) chunk's dense, sorted `names` array, or std::nullopt if
 * absent. On x86_64/SSE2 targets, dispatches to a SIMD linear scan for
 * small-to-medium chunks -- AVX2 (avx2FindIndex()) if available, else
 * SSE2 (simdFindIndex()) -- and falls back to (scalar) binary search
 * otherwise: either because the chunk is large enough that binary
 * search's better asymptotics win out, or because this isn't an
 * x86_64/SSE2 target, in which case binary search is used
 * unconditionally.
 */
std::optional<size_type> findIndex(const Symbol * first, size_type n, Symbol name) noexcept
{
#if defined(__x86_64__) && defined(__SSE2__)
    if (avx2Available()) {
        if (n <= avx2LinearScanMaxSize)
            return avx2FindIndex(first, n, name);
    } else if (n <= simdLinearScanMaxSize) {
        return simdFindIndex(first, n, name);
    }
#endif
    auto last = first + n;
    auto i = std::lower_bound(first, last, name);
    if (i != last && *i == name)
        return static_cast<size_type>(i - first);
    return std::nullopt;
}

} // namespace

#if defined(__x86_64__) && defined(__SSE2__)
void Bindings::setAvx2OverrideForTesting(std::optional<bool> avx2IsAvailable) noexcept
{
    avx2OverrideForTesting() = avx2IsAvailable;
}

__attribute__((target_clones("avx2", "default")))
#else
void Bindings::setAvx2OverrideForTesting(std::optional<bool>) noexcept
{
    /* No SIMD dispatch exists to override on non-x86_64/SSE2 targets. */
}
#endif
std::optional<Attr>
Bindings::get(Symbol name) const noexcept
{
    auto getInChunk = [name](const Bindings & chunk) -> std::optional<Attr> {
        if (auto idx = findIndex(chunk.namesPtr(), chunk.numAttrs, name))
            return Attr(chunk.namesPtr()[*idx], chunk.valuesPtr()[*idx], chunk.posPtr()[*idx]);
        return std::nullopt;
    };

    const Bindings * currentChunk = this;
    while (currentChunk) {
        if (auto attr = getInChunk(*currentChunk))
            return attr;
        currentChunk = currentChunk->baseLayer;
    }

    return std::nullopt;
}

/* Allocate a new set of three parallel attribute arrays (names/pos/values)
   for an attribute set with a specific capacity. The space is implicitly
   reserved after the Bindings structure; @see Bindings::namesPtr() and
   friends for how it's carved up. */
Bindings * EvalMemory::allocBindings(size_t capacity)
{
    if (capacity == 0)
        /* Swear that we are not going to modify this. */
        return const_cast<Bindings *>(&Bindings::emptyBindings);
    if (capacity > std::numeric_limits<Bindings::size_type>::max())
        throw Error("attribute set of size %d is too big", capacity);
    stats.nrAttrsets++;
    stats.nrAttrsInAttrsets += capacity;
    auto * bindings = new (allocBytes(
        sizeof(Bindings) + capacity * (sizeof(Symbol) + sizeof(PosIdx) + sizeof(Value *)))) Bindings();
    bindings->capacity_ = static_cast<Bindings::size_type>(capacity);
    return bindings;
}

Value & BindingsBuilder::alloc(Symbol name, PosIdx pos)
{
    auto value = mem.get().allocValue();
    bindings->push_back(Attr(name, value, pos));
    return *value;
}

Value & BindingsBuilder::alloc(std::string_view name, PosIdx pos)
{
    return alloc(symbols.get().create(name), pos);
}

void Bindings::sort()
{
    if (numAttrs <= 1)
        return;

    /* Sort via a temporary array of the (still 16-byte) Attr value type,
       then scatter the result back into the three parallel arrays. Simpler
       than permuting three arrays in lockstep, and sorting an attrset is
       not on any hot path that would justify the extra complexity. */
    std::vector<Attr> tmp;
    tmp.reserve(numAttrs);
    for (size_type i = 0; i < numAttrs; ++i)
        tmp.emplace_back(namesPtr()[i], valuesPtr()[i], posPtr()[i]);

    std::sort(tmp.begin(), tmp.end());

    for (size_type i = 0; i < numAttrs; ++i) {
        namesPtr()[i] = tmp[i].name;
        posPtr()[i] = tmp[i].pos;
        valuesPtr()[i] = tmp[i].value;
    }
}

Value & Value::mkAttrs(BindingsBuilder & bindings)
{
    mkAttrs(bindings.finish());
    return *this;
}

} // namespace nix
