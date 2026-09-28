#include "nix/expr/tests/libexpr.hh"
#include "nix/expr/attr-set.hh"

#include <algorithm>
#include <random>
#include <unordered_set>
#include <vector>

namespace nix {

/**
 * Correctness check for Bindings::get()'s per-chunk search primitive
 * (SIMD linear scan for small/medium chunks, scalar binary search for
 * large ones and non-x86_64/SSE2 targets -- see findIndex() in
 * attr-set.cc). Builds attrsets of many sizes (straddling both the
 * SSE2/binary-search and AVX2/binary-search crossovers in both directions)
 * with randomized attribute names, and checks Bindings::get() against a
 * plain reference: every inserted name must resolve to its own value, and
 * names never inserted must be absent. Also covers layered (`//`-merged)
 * chains, which walk multiple chunks and must still find/skip the right
 * per-chunk entries.
 *
 * Every case runs under each of `avx2Overrides` (@see
 * Bindings::setAvx2OverrideForTesting): this forces get()'s *internal*
 * AVX2-vs-SSE2/scalar branch regardless of which choice would otherwise be
 * made, exercising the SSE2 and scalar-binary-search logic even on this
 * (AVX2-capable) test machine, where an un-overridden run would only ever
 * take the AVX2 branch. Note this does *not* -- and cannot -- force which
 * of get()'s two `target_clones` ifunc clones actually runs (that is
 * resolved once, before main(), based on real CPU features); it only
 * re-exercises the alternate logical branch from within whichever clone
 * the loader picked. On this AVX2-capable test machine that means the
 * literal "default" clone's own compiled code is never directly exercised
 * by these tests -- a known gap, noted in the task report.
 */
struct AttrSetGetTest : LibExprTest
{
};

namespace {
// A handful of sizes chosen to straddle the SSE2-linear-scan / binary-search
// crossover (Bindings::simdLinearScanMaxSize == 320) and the
// AVX2-linear-scan / binary-search crossover (avx2LinearScanMaxSize == 640)
// from both sides, plus the usual small-N edge cases.
const std::vector<size_t> testSizes = {
    0,   1,   2,   3,   4,   5,   7,   8,   9,   15,  16,  17,  31,
    32,  33,  63,  64,  100, 200, 256, 319, 320, 321, 500, 639, 640,
    641, 700, 900, 1000, 4096,
};

/**
 * RAII guard that forces Bindings::get()'s internal AVX2 dispatch decision
 * for the duration of a scope, restoring real detection on destruction.
 */
struct Avx2OverrideGuard
{
    explicit Avx2OverrideGuard(std::optional<bool> avx2IsAvailable)
    {
        Bindings::setAvx2OverrideForTesting(avx2IsAvailable);
    }

    ~Avx2OverrideGuard()
    {
        Bindings::setAvx2OverrideForTesting(std::nullopt);
    }
};

// std::nullopt: real (un-overridden) detection. false/true: forced, to
// exercise both logical branches regardless of what this test machine
// actually supports (@see Avx2OverrideGuard / Bindings::setAvx2OverrideForTesting).
const std::vector<std::optional<bool>> avx2Overrides = {std::nullopt, false, true};
} // namespace

TEST_F(AttrSetGetTest, presentAndAbsentKeysAtManySizes)
{
    for (auto avx2Override : avx2Overrides) {
    Avx2OverrideGuard guard(avx2Override);
    for (size_t size : testSizes) {
        for (unsigned seed = 0; seed < 5; ++seed) {
            std::mt19937 rng(size * 1000 + seed);

            // Unique random symbol names, each mapped to a distinguishable
            // integer value (its insertion index) so a hit can be checked
            // for both presence and correct payload.
            std::unordered_set<std::string> usedNames;
            std::vector<std::pair<Symbol, int64_t>> present;
            while (present.size() < size) {
                std::string name = "attr_" + std::to_string(rng());
                if (!usedNames.insert(name).second)
                    continue;
                present.emplace_back(createSymbol(name.c_str()), (int64_t) present.size());
            }

            auto builder = state.buildBindings(size);
            for (auto & [sym, val] : present) {
                auto & v = builder.alloc(sym);
                v.mkInt(val);
            }
            const Bindings * bindings = builder.finish();

            ASSERT_EQ(bindings->size(), size);

            // Every inserted name must be found with its own value.
            for (auto & [sym, val] : present) {
                auto attr = bindings->get(sym);
                ASSERT_TRUE(attr.has_value()) << "size=" << size << " seed=" << seed;
                EXPECT_EQ(attr->name, sym);
                ASSERT_NE(attr->value, nullptr);
                EXPECT_EQ(attr->value->integer().value, val);
            }

            // Absent names (never inserted) must not be found.
            for (unsigned i = 0; i < 50; ++i) {
                std::string absentName = "absent_" + std::to_string(rng());
                if (usedNames.count(absentName))
                    continue;
                Symbol absentSym = createSymbol(absentName.c_str());
                EXPECT_FALSE(bindings->get(absentSym).has_value()) << "size=" << size << " seed=" << seed;
            }

            // A symbol that was never even interned as an attribute name
            // anywhere in this attrset must also be absent -- exercises
            // the "no match found at all" path distinctly from "matched
            // some other slot".
            Symbol neverUsed = createSymbol(("zzz_never_" + std::to_string(size) + "_" + std::to_string(seed)).c_str());
            EXPECT_FALSE(bindings->get(neverUsed).has_value());
        }
    }
    }
}

TEST_F(AttrSetGetTest, layeredChainFindsRightLayerAndOverride)
{
    for (auto avx2Override : avx2Overrides) {
    Avx2OverrideGuard guard(avx2Override);
    // Base layer: sizes straddling the crossover.
    for (size_t baseSize : {size_t{0}, size_t{3}, size_t{16}, size_t{400}}) {
        for (size_t topSize : {size_t{0}, size_t{2}, size_t{20}}) {
            std::mt19937 rng(baseSize * 10000 + topSize);

            std::unordered_set<std::string> usedNames;
            auto makeUniqueSymbol = [&](const char * prefix) {
                std::string name;
                do {
                    name = std::string(prefix) + std::to_string(rng());
                } while (!usedNames.insert(name).second);
                return createSymbol(name.c_str());
            };

            std::vector<std::pair<Symbol, int64_t>> baseAttrs;
            for (size_t i = 0; i < baseSize; ++i)
                baseAttrs.emplace_back(makeUniqueSymbol("base_"), (int64_t) (1000 + i));

            auto baseBuilder = state.buildBindings(baseSize);
            for (auto & [sym, val] : baseAttrs)
                baseBuilder.alloc(sym).mkInt(val);
            const Bindings * base = baseBuilder.finish();

            // Top layer overrides the first attribute of the base (if any)
            // and adds some of its own.
            std::vector<std::pair<Symbol, int64_t>> topAttrs;
            if (baseSize > 0)
                topAttrs.emplace_back(baseAttrs[0].first, (int64_t) 9999);
            for (size_t i = 0; i < topSize; ++i)
                topAttrs.emplace_back(makeUniqueSymbol("top_"), (int64_t) (2000 + i));

            // A zero-capacity BindingsBuilder aliases the shared, immutable
            // Bindings::emptyBindings singleton (see EvalMemory::allocBindings);
            // layering onto/from it would corrupt global state. Real callers
            // (e.g. ExprOpUpdate::eval) special-case an empty top layer before
            // ever reaching layerOnTopOf(), so mirror that here.
            if (topAttrs.empty())
                continue;

            auto topBuilder = state.buildBindings(topAttrs.size());
            for (auto & [sym, val] : topAttrs)
                topBuilder.alloc(sym).mkInt(val);
            topBuilder.layerOnTopOf(*base);
            const Bindings * layered = topBuilder.finish();

            // Override wins.
            if (baseSize > 0) {
                auto attr = layered->get(baseAttrs[0].first);
                ASSERT_TRUE(attr.has_value());
                EXPECT_EQ(attr->value->integer().value, 9999);
            }

            // Non-overridden base attrs still visible through the chain.
            for (size_t i = 1; i < baseAttrs.size(); ++i) {
                auto attr = layered->get(baseAttrs[i].first);
                ASSERT_TRUE(attr.has_value());
                EXPECT_EQ(attr->value->integer().value, baseAttrs[i].second);
            }

            // Top-only attrs visible.
            for (size_t i = 1; i < topAttrs.size(); ++i) {
                auto attr = layered->get(topAttrs[i].first);
                ASSERT_TRUE(attr.has_value());
                EXPECT_EQ(attr->value->integer().value, topAttrs[i].second);
            }

            // Something never inserted anywhere is absent.
            Symbol neverUsed = makeUniqueSymbol("nope_");
            EXPECT_FALSE(layered->get(neverUsed).has_value());
        }
    }
    }
}

} // namespace nix
