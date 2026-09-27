#pragma once
///@file

#include "nix/expr/nixexpr.hh"
#include "nix/expr/symbol-table.hh"
#include "nix/util/comparator.hh"

#include <boost/container/static_vector.hpp>
#include <boost/iterator/function_output_iterator.hpp>

#include <algorithm>
#include <functional>
#include <ranges>
#include <optional>

#if defined(__x86_64__) && defined(__SSE2__)
#  include <emmintrin.h>
#endif

namespace nix {

class EvalMemory;
struct Value;

/**
 * Map one attribute name to its value. This is used as the by-value
 * "synthesized" return/element type of Bindings's lookup, indexing, and
 * iteration operations. Bindings itself does *not* store a contiguous array
 * of Attr (@see Bindings for the actual storage layout); this struct exists
 * purely as an ergonomic value type to hand data back to callers.
 */
struct Attr
{
    /* the placement of `name` and `pos` in this struct is important.
       both of them are uint32 wrappers, they are next to each other
       to make sure that Attr has no padding on 64 bit machines. that
       way we keep Attr size at two words with no wasted space. */
    Symbol name;
    PosIdx pos;
    Value * value = nullptr;

    Attr(Symbol name, Value * value, PosIdx pos = noPos)
        : name(name)
        , pos(pos)
        , value(value)
    {
    }

    constexpr Attr() {}

    auto operator<=>(const Attr & a) const
    {
        return name <=> a.name;
    }
};

static_assert(
    sizeof(Attr) == 2 * sizeof(uint32_t) + sizeof(Value *),
    "performance of the evaluator is highly sensitive to the size of Attr. "
    "avoid introducing any padding into Attr if at all possible, and do not "
    "introduce new fields that need not be present for almost every instance.");

/**
 * Bindings contains all the attributes of an attribute set. It is defined
 * by its size and its capacity, the capacity being the number of attribute
 * slots allocated after this structure, while the size corresponds to
 * the number of elements already inserted in this structure.
 *
 * Storage is split into three parallel arrays (a "structure of arrays"),
 * each sized to `capacity` and placed back-to-back right after the Bindings
 * header in the same allocation:
 *
 *   Symbol   names[capacity]   -- dense, 4 bytes/entry, sorted by name.
 *   PosIdx   pos[capacity]     -- 4 bytes/entry.
 *   Value *  values[capacity]  -- 8 bytes/entry, ordinary pointers into
 *                                 independently heap-allocated Values.
 *
 * This keeps the total bytes/entry at 16 (matching `sizeof(Attr)`), while
 * letting the hot binary-search path in Bindings::get() walk only the dense
 * `names` array (4x the entries per cache line versus walking full Attr
 * structs), touching `pos`/`values` only once, on a hit.
 *
 * Because there is no longer a contiguous, stable `Attr` array to hand out
 * pointers into, lookups synthesize an `Attr` by value (@see get(),
 * @see iterator).
 *
 * Bindings can be efficiently `//`-composed into an intrusive linked list of "layers"
 * that saves on copies and allocations. Each lookup (@see Bindings::get) traverses
 * this linked list until a matching attribute is found (thus overlays earlier in
 * the list take precedence). For iteration over the whole Bindings, an on-the-fly
 * k-way merge is performed by Bindings::iterator class.
 */
class Bindings
{
public:
    using size_type = uint32_t;

    PosIdx pos;

    /**
     * An instance of bindings objects with 0 attributes.
     * This object must never be modified.
     */
    static const constinit Bindings emptyBindings;

private:
    /**
     * Number of attributes actually in use (<= capacity_).
     */
    size_type numAttrs = 0;

    /**
     * Number of attributes with unique names in the layer chain.
     *
     * This is the *real* user-facing size of bindings, whereas @ref numAttrs is
     * an implementation detail of the data structure.
     */
    size_type numAttrsInChain = 0;

    /**
     * Length of the layers list.
     */
    uint32_t numLayers = 1;

    /**
     * Number of slots reserved in each of the three trailing arrays. Needed
     * to locate the `pos` and `values` arrays, which follow `names` at fixed
     * offsets of `capacity_ * sizeof(...)` bytes (regardless of `numAttrs`).
     */
    size_type capacity_ = 0;

    /**
     * Bindings that this attrset is "layered" on top of.
     */
    const Bindings * baseLayer = nullptr;

    constexpr Bindings() = default;
    Bindings(const Bindings &) = delete;
    Bindings(Bindings &&) = delete;
    Bindings & operator=(const Bindings &) = delete;
    Bindings & operator=(Bindings &&) = delete;

    ~Bindings() = default;

    friend class BindingsBuilder;

    /**
     * Maximum length of the Bindings layer chains.
     */
    static constexpr unsigned maxLayers = 8;

    /* Pointers into the trailing storage allocated right after this
       Bindings object (@see EvalMemory::allocBindings). Since sizeof(Bindings)
       is a multiple of alignof(Value*) (guaranteed: Bindings contains a
       pointer member, so the compiler pads sizeof(Bindings) to a multiple of
       its own 8-byte alignment), and capacity_ * 8 bytes separate `names`
       from `values`, `valuesPtr()` is always properly 8-byte aligned
       regardless of the value of capacity_. */

    Symbol * namesPtr() noexcept
    {
        return reinterpret_cast<Symbol *>(this + 1);
    }

    const Symbol * namesPtr() const noexcept
    {
        return reinterpret_cast<const Symbol *>(this + 1);
    }

    PosIdx * posPtr() noexcept
    {
        return reinterpret_cast<PosIdx *>(namesPtr() + capacity_);
    }

    const PosIdx * posPtr() const noexcept
    {
        return reinterpret_cast<const PosIdx *>(namesPtr() + capacity_);
    }

    Value ** valuesPtr() noexcept
    {
        return reinterpret_cast<Value **>(posPtr() + capacity_);
    }

    Value * const * valuesPtr() const noexcept
    {
        return reinterpret_cast<Value * const *>(posPtr() + capacity_);
    }

public:
    size_type size() const
    {
        return numAttrsInChain;
    }

    bool empty() const
    {
        return size() == 0;
    }

    class iterator
    {
    public:
        using value_type = Attr;
        using pointer = const value_type *;
        using reference = const value_type &;
        using difference_type = std::ptrdiff_t;
        using iterator_category = std::forward_iterator_tag;

        friend class Bindings;

    private:
        /**
         * A cursor over a single (unlayered) Bindings chunk's dense arrays.
         * Unlike the old design, there is no stable `Attr *` to point at, so
         * the cursor tracks a (chunk, idx) position and caches the `name` at
         * that position (needed as a plain lvalue field for GENERATE_CMP's
         * use of std::tie, which cannot bind to values returned by value).
         */
        struct BindingsCursor
        {
            /**
             * The chunk this cursor is iterating.
             */
            const Bindings * chunk;

            /**
             * Current position within chunk's arrays.
             */
            size_type idx;

            /**
             * One-past-the-end position within chunk's arrays.
             */
            size_type endIdx;

            /**
             * Priority of the value. Lesser values have more priority (i.e. they override
             * attributes that appear later in the linked list of Bindings).
             */
            uint32_t priority;

            /**
             * Cached chunk->namesPtr()[idx], kept in sync by increment()/consume().
             */
            Symbol name;

            Attr get() const noexcept
            {
                return Attr(name, chunk->valuesPtr()[idx], chunk->posPtr()[idx]);
            }

            bool empty() const noexcept
            {
                return idx == endIdx;
            }

            void increment() noexcept
            {
                ++idx;
                if (!empty())
                    name = chunk->namesPtr()[idx];
            }

            void consume(Symbol upTo) noexcept
            {
                while (!empty() && name <= upTo)
                    increment();
            }

            GENERATE_CMP(BindingsCursor, me->name, me->priority)
        };

        using QueueStorageType = boost::container::static_vector<BindingsCursor, maxLayers>;

        /**
         * Comparator implementing the override priority / name ordering
         * for BindingsCursor.
         */
        static constexpr auto comp = std::greater<BindingsCursor>();

        /**
         * A priority queue used to implement an on-the-fly k-way merge.
         */
        QueueStorageType cursorHeap;

        /**
         * Storage for the Attr synthesized at the iterator's current
         * position. There's no contiguous Attr array in Bindings to point
         * into anymore, so each iterator instance owns its own copy.
         */
        Attr currentAttr{};

        /**
         * Identifies the logical position `current` refers to, independent
         * of `currentAttr`'s storage address (which differs between copies
         * of an iterator at the same logical position). Used for equality.
         * nullptr means "at the end".
         */
        const Bindings * curChunk = nullptr;
        size_type curIdx = 0;

        /**
         * The attribute the iterator currently points to (== &currentAttr),
         * or nullptr at the end.
         */
        pointer current = nullptr;

        /**
         * Whether iterating over a single attribute and not a merge chain.
         */
        bool doMerge = true;

        void push(BindingsCursor cursor) noexcept
        {
            cursorHeap.push_back(cursor);
            std::ranges::make_heap(cursorHeap, comp);
        }

        [[nodiscard]] BindingsCursor pop() noexcept
        {
            std::ranges::pop_heap(cursorHeap, comp);
            auto cursor = cursorHeap.back();
            cursorHeap.pop_back();
            return cursor;
        }

        iterator & finished() noexcept
        {
            curChunk = nullptr;
            current = nullptr;
            return *this;
        }

        void syncCurrent(const BindingsCursor & cursor) noexcept
        {
            currentAttr = cursor.get();
            curChunk = cursor.chunk;
            curIdx = cursor.idx;
            current = &currentAttr;
        }

        void next(BindingsCursor cursor) noexcept
        {
            syncCurrent(cursor);
            cursor.increment();

            if (!cursor.empty())
                push(cursor);
        }

        std::optional<BindingsCursor> consumeAllUntilCurrentName() noexcept
        {
            auto cursor = pop();
            Symbol lastHandledName = currentAttr.name;

            while (cursor.name <= lastHandledName) {
                cursor.consume(lastHandledName);
                if (!cursor.empty())
                    push(cursor);

                if (cursorHeap.empty())
                    return std::nullopt;

                cursor = pop();
            }

            return cursor;
        }

        explicit iterator(const Bindings & attrs) noexcept
            : doMerge(attrs.baseLayer)
        {
            auto pushBindings = [this, priority = unsigned{0}](const Bindings & layer) mutable {
                push(
                    BindingsCursor{
                        .chunk = &layer,
                        .idx = 0,
                        .endIdx = layer.numAttrs,
                        .priority = priority++,
                        .name = layer.namesPtr()[0],
                    });
            };

            if (!doMerge) {
                if (attrs.empty())
                    return;

                pushBindings(attrs);
                syncCurrent(cursorHeap.front());

                return;
            }

            const Bindings * layer = &attrs;
            while (layer) {
                if (layer->numAttrs != 0)
                    pushBindings(*layer);
                layer = layer->baseLayer;
            }

            if (cursorHeap.empty())
                return;

            next(pop());
        }

    public:
        iterator() = default;

        iterator(const iterator & other) noexcept
            : cursorHeap(other.cursorHeap)
            , currentAttr(other.currentAttr)
            , curChunk(other.curChunk)
            , curIdx(other.curIdx)
            , current(other.current ? &currentAttr : nullptr)
            , doMerge(other.doMerge)
        {
        }

        iterator & operator=(const iterator & other) noexcept
        {
            cursorHeap = other.cursorHeap;
            currentAttr = other.currentAttr;
            curChunk = other.curChunk;
            curIdx = other.curIdx;
            current = other.current ? &currentAttr : nullptr;
            doMerge = other.doMerge;
            return *this;
        }

        reference operator*() const noexcept
        {
            return *current;
        }

        pointer operator->() const noexcept
        {
            return current;
        }

        iterator & operator++() noexcept
        {
            if (!doMerge) {
                auto & cursor = cursorHeap.front();
                cursor.increment();
                if (cursor.empty())
                    return finished();
                syncCurrent(cursor);
                return *this;
            }

            if (cursorHeap.empty())
                return finished();

            auto cursor = consumeAllUntilCurrentName();
            if (!cursor)
                return finished();

            next(*cursor);
            return *this;
        }

        iterator operator++(int) noexcept
        {
            iterator tmp = *this;
            ++*this;
            return tmp;
        }

        bool operator==(const iterator & rhs) const noexcept
        {
            return curChunk == rhs.curChunk && (curChunk == nullptr || curIdx == rhs.curIdx);
        }
    };

    using const_iterator = iterator;

    /**
     * A mutable reference-like proxy into the idx-th attribute's slot across
     * the three parallel arrays. There is no stable `Attr &` backing
     * Bindings' storage anymore (@see Bindings), so `operator[]` can't just
     * hand out a reference into a contiguous array; this proxies field
     * access (`.name`/`.pos`/`.value`) via real references into the arrays,
     * and supports whole-Attr assignment. Used for positional access into
     * an unlayered, not-yet-sorted Bindings under construction (@see
     * ExprAttrs::eval's `__overrides` handling, the sole caller).
     */
    struct Ref
    {
        Symbol & name;
        PosIdx & pos;
        Value * & value;

        Ref & operator=(const Attr & attr) noexcept
        {
            name = attr.name;
            pos = attr.pos;
            value = attr.value;
            return *this;
        }
    };

    Ref operator[](size_type idx) noexcept
    {
        if (isLayered()) [[unlikely]]
            unreachable();
        return Ref{namesPtr()[idx], posPtr()[idx], valuesPtr()[idx]};
    }

    /**
     * Read-only positional access, synthesizing an Attr by value. Safe to
     * bind directly to a `const Attr &` (unlike e.g. `get()`'s
     * `std::optional<Attr>`, a plain by-value return is a direct
     * reference-to-temporary binding, so the usual temporary lifetime
     * extension rules apply).
     */
    Attr operator[](size_type idx) const noexcept
    {
        if (isLayered()) [[unlikely]]
            unreachable();
        return Attr(namesPtr()[idx], valuesPtr()[idx], posPtr()[idx]);
    }

    void push_back(const Attr & attr)
    {
        auto idx = numAttrs++;
        namesPtr()[idx] = attr.name;
        posPtr()[idx] = attr.pos;
        valuesPtr()[idx] = attr.value;
        numAttrsInChain = numAttrs;
    }

private:
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
     * Measured empirically (see task notes) to win up to ~300-500 entries,
     * with a noisy crossover after that; `simdLinearScanMaxSize` is kept
     * well under the crossover for margin.
     */
    static std::optional<size_type> simdFindIndex(const Symbol * first, size_type n, Symbol name) noexcept
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

    static constexpr size_type simdLinearScanMaxSize = 256;
#endif

    /**
     * Find the index of `name` within the first `n` entries of a single
     * (unlayered) chunk's dense, sorted `names` array, or std::nullopt if
     * absent. Dispatches to a SIMD linear scan for small-to-medium chunks
     * (see simdFindIndex()) and falls back to (scalar) binary search
     * otherwise -- either because the chunk is large enough that binary
     * search's better asymptotics win out, or because this isn't an
     * x86_64/SSE2 target, in which case binary search is used
     * unconditionally.
     */
    static std::optional<size_type> findIndex(const Symbol * first, size_type n, Symbol name) noexcept
    {
#if defined(__x86_64__) && defined(__SSE2__)
        if (n <= simdLinearScanMaxSize)
            return simdFindIndex(first, n, name);
#endif
        auto last = first + n;
        auto i = std::lower_bound(first, last, name);
        if (i != last && *i == name)
            return static_cast<size_type>(i - first);
        return std::nullopt;
    }

public:
    /**
     * Get attribute by name, or std::nullopt if no such attribute exists.
     *
     * Returns a synthesized-by-value Attr (there is no stable `Attr *`
     * to point into anymore), but supports the usual
     * `if (auto attr = bindings->get(name)) { ...attr->value...; }`
     * pointer-like usage via std::optional's own operator-> / operator*.
     */
    std::optional<Attr> get(Symbol name) const noexcept
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

    /**
     * Test whether an attribute by this name exists anywhere in the layer
     * chain, without synthesizing an `Attr` (i.e. without touching `pos`/
     * `values` at all, even on a hit). For callers that only need a yes/no
     * answer, this avoids the wasted `pos`/`values` reads `get()` would pay
     * for a result that's immediately discarded.
     */
    bool contains(Symbol name) const noexcept
    {
        const Bindings * currentChunk = this;
        while (currentChunk) {
            auto first = currentChunk->namesPtr();
            auto last = first + currentChunk->numAttrs;
            if (std::binary_search(first, last, name))
                return true;
            currentChunk = currentChunk->baseLayer;
        }
        return false;
    }

    /**
     * Direct read-only view of a single (unlayered) chunk's dense parallel
     * arrays. For hot paths that want to run their own merge/scan against a
     * sorted key sequence instead of paying one binary search per key via
     * `get()`.
     *
     * Only valid when `!isLayered()`: a layered chain's attributes can live
     * in any layer, not just the top one, so scanning only this layer's
     * `names` would silently miss base-layer attributes.
     */
    struct DenseView
    {
        const Symbol * names;
        const PosIdx * pos;
        Value * const * values;
        size_type size;
    };

    DenseView denseView() const noexcept
    {
        assert(!isLayered());
        return DenseView{namesPtr(), posPtr(), valuesPtr(), numAttrs};
    }

    /**
     * Check if the layer chain is full.
     */
    bool isLayerListFull() const noexcept
    {
        return numLayers == Bindings::maxLayers;
    }

    /**
     * Test if the length of the linked list of layers is greater than 1.
     */
    bool isLayered() const noexcept
    {
        return numLayers > 1;
    }

    const_iterator begin() const
    {
        return const_iterator(*this);
    }

    const_iterator end() const
    {
        return const_iterator();
    }

    void sort();

    /**
     * Returns the attributes in lexicographically sorted order.
     *
     * Returns by value (there is no stable `Attr *` to hand out anymore).
     */
    std::vector<Attr> lexicographicOrder(const SymbolTable & symbols) const
    {
        std::vector<Attr> res;
        res.reserve(size());
        std::ranges::copy(*this, std::back_inserter(res));
        std::ranges::sort(res, [&](const Attr & a, const Attr & b) {
            std::string_view sa = symbols[a.name], sb = symbols[b.name];
            return sa < sb;
        });
        return res;
    }

    friend class EvalMemory;
};

static_assert(std::forward_iterator<Bindings::iterator>);
static_assert(std::ranges::forward_range<Bindings>);

/**
 * A wrapper around Bindings that ensures that its always in sorted
 * order at the end. The only way to consume a BindingsBuilder is to
 * call finish(), which sorts the bindings.
 */
class BindingsBuilder final
{
public:
    // needed by std::back_inserter
    using value_type = Attr;
    using size_type = Bindings::size_type;

private:
    Bindings * bindings;
    Bindings::size_type capacity_;

    friend class EvalMemory;

    BindingsBuilder(EvalMemory & mem, SymbolTable & symbols, Bindings * bindings, size_type capacity)
        : bindings(bindings)
        , capacity_(capacity)
        , mem(mem)
        , symbols(symbols)
    {
    }

    bool hasBaseLayer() const noexcept
    {
        return bindings->baseLayer;
    }

    /**
     * If the bindings gets "layered" on top of another we need to recalculate
     * the number of unique attributes in the chain.
     *
     * This is done by either iterating over the base "layer" and the newly added
     * attributes and counting duplicates. If the base "layer" is big this approach
     * is inefficient and we fall back to doing per-element binary search in the base
     * "layer".
     */
    void finishSizeIfNecessary()
    {
        if (!hasBaseLayer())
            return;

        auto & base = *bindings->baseLayer;
        auto names = std::span(bindings->namesPtr(), bindings->numAttrs);

        Bindings::size_type duplicates = 0;

        /* If the base bindings is smaller than the newly added attributes
           iterate using std::ranges::set_intersection to run in O(|base| + |attrs|) =
           O(|attrs|). Otherwise use an O(|attrs| * log(|base|)) per-attr binary
           search to check for duplicates. Note that if we are in this code path then
           |attrs| <= bindingsUpdateLayerRhsSizeThreshold, which 16 by default. We are
           optimizing for the case when a small attribute set gets "layered" on top of
           a much larger one. When attrsets are already small it's fine to do a linear
           scan, but we should avoid expensive iterations over large "base" attrsets. */
        if (names.size() > base.size()) {
            // Classic (non-ranges) set_intersection: `base` yields Attr and
            // `names` yields Symbol, and getting std::ranges::set_intersection's
            // projections to satisfy `mergeable` across those two different
            // value types isn't worth the trouble; a small heterogeneous
            // comparator does the same job.
            auto symbolOf = []<typename T>(const T & x) noexcept -> Symbol {
                if constexpr (std::is_same_v<T, Attr>)
                    return x.name;
                else
                    return x;
            };
            std::set_intersection(
                base.begin(),
                base.end(),
                names.begin(),
                names.end(),
                boost::make_function_output_iterator([&]([[maybe_unused]] auto && _) { ++duplicates; }),
                [&](const auto & a, const auto & b) { return symbolOf(a) < symbolOf(b); });
        } else {
            for (Symbol name : names) {
                if (base.get(name))
                    ++duplicates;
            }
        }

        bindings->numAttrsInChain = base.numAttrsInChain + names.size() - duplicates;
    }

public:
    std::reference_wrapper<EvalMemory> mem;
    std::reference_wrapper<SymbolTable> symbols;

    void insert(Symbol name, Value * value, PosIdx pos = noPos)
    {
        insert(Attr(name, value, pos));
    }

    void insert(const Attr & attr)
    {
        push_back(attr);
    }

    void push_back(const Attr & attr)
    {
        assert(bindings->numAttrs < capacity_);
        bindings->push_back(attr);
    }

    /**
     * "Layer" the newly constructured Bindings on top of another attribute set.
     *
     * This effectively performs an attribute set merge, while giving preference
     * to attributes from the newly constructed Bindings in case of duplicate attribute
     * names.
     *
     * This operation amortizes the need to copy over all attributes and allows
     * for efficient implementation of attribute set merges (ExprOpUpdate::eval).
     */
    void layerOnTopOf(const Bindings & base) noexcept
    {
        bindings->baseLayer = &base;
        bindings->numLayers = base.numLayers + 1;
    }

    Value & alloc(Symbol name, PosIdx pos = noPos);

    Value & alloc(std::string_view name, PosIdx pos = noPos);

    const Bindings * finish()
    {
        bindings->sort();
        finishSizeIfNecessary();
        return bindings;
    }

    const Bindings * alreadySorted()
    {
        finishSizeIfNecessary();
        return bindings;
    }

    size_t capacity() const noexcept
    {
        return capacity_;
    }

    void grow(BindingsBuilder newBindings)
    {
        for (auto & i : *bindings)
            newBindings.push_back(i);
        std::swap(*this, newBindings);
    }

    friend struct ExprAttrs;
};

} // namespace nix
