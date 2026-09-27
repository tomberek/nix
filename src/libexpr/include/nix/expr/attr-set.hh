#pragma once
///@file

#include "nix/expr/nixexpr.hh"
#include "nix/expr/symbol-table.hh"

#include <boost/container/static_vector.hpp>
#include <boost/iterator/function_output_iterator.hpp>

#include <algorithm>
#include <functional>
#include <ranges>
#include <optional>
#include <span>

namespace nix {

class EvalMemory;
class EvalState;
class Bindings;
struct Value;

/**
 * Map one attribute name to its value.
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
 * by its size and its capacity, the capacity being the number of Attr
 * elements allocated after this structure, while the size corresponds to
 * the number of elements already inserted in this structure.
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
     * Number of attributes in the attrs FAM (Flexible Array Member).
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
     * Bindings that this attrset is "layered" on top of.
     */
    const Bindings * baseLayer = nullptr;

    /* ponytail: prototype-only "shape sharing" path for builtins.mapAttrs
       (see docs/attrset-range-sharing-design.md, mechanism 1). Entirely
       separate from the baseLayer/numLayers general layering mechanism
       above -- do not conflate the two.

       First cut stored `shapeBase`/a scratch `Attr` as direct members of
       `Bindings` -- that grew *every* Bindings (24 -> 48 bytes), including
       the ~90-98% of real-world attrsets that never touch mapAttrs, and
       measured as a net memory *regression* on real nixpkgs/NixOS eval
       despite winning its own synthetic benchmark. Fixed by not adding
       any member to the class at all: for a shape-shared instance (only,
       flagged via shapeSharedFlag below), the leading bytes of the FAM
       region are repurposed as a `ShapeShareInfo` header, and the actual
       packed `Value*[numAttrs]` array starts right after it -- see
       shapeInfo()/shapeValuesArray()/shapeValuesArrayMut(). A plain
       Bindings never allocates that header, so sizeof(Bindings) stays at
       its original 24 bytes for every instance; only shape-shared
       instances' own single allocation is bigger (by sizeof(ShapeShareInfo)),
       which they can afford since they're the ones saving 8 bytes/slot. */
    struct ShapeShareInfo
    {
        /**
         * The *exact start* of another, definitely non-shape-shared
         * Bindings ("the names owner") whose `attrs[]` supplies every
         * name/pos pair for this instance, in the same order. Always a
         * real GC allocation's block start (never an interior offset),
         * so Boehm's disabled interior-pointer support
         * (GC_set_all_interior_pointers(0) in eval-gc.cc) is irrelevant
         * here -- exactly like baseLayer.
         */
        const Bindings * shapeBase;

        /**
         * Per-instance scratch Attr used to hand back a `const Attr *`
         * from get() without materializing a full Attr array. Safe
         * because nothing in this codebase holds two `get()` results
         * from the *same* Bindings instance simultaneously (verified by
         * inspection of all call sites at implementation time); a fresh
         * call overwrites it.
         */
        mutable Attr scratch;
    };

    ShapeShareInfo & shapeInfo() const noexcept
    {
        return *const_cast<ShapeShareInfo *>(reinterpret_cast<const ShapeShareInfo *>(attrs));
    }

    /**
     * Flexible array member of attributes (or, for shape-shared
     * instances, a `ShapeShareInfo` header followed by a packed
     * `Value*[numAttrs]` array -- see shapeInfo() above).
     */
    Attr attrs[0];

    constexpr Bindings() = default;
    Bindings(const Bindings &) = delete;
    Bindings(Bindings &&) = delete;
    Bindings & operator=(const Bindings &) = delete;
    Bindings & operator=(Bindings &&) = delete;

    ~Bindings() = default;

    friend class BindingsBuilder;
    friend class EvalMemory;

    /**
     * Maximum length of the Bindings layer chains.
     */
    static constexpr unsigned maxLayers = 8;

    /**
     * ponytail: stolen top bit of numLayers (which only ever needs
     * values 0..maxLayers==8) as a discriminant for the shape-sharing
     * variant above. Costs nothing extra since numLayers is already a
     * uint32_t with plenty of spare range.
     */
    static constexpr uint32_t shapeSharedFlag = 0x8000'0000u;

    /**
     * The real layer count, with the discriminant bit masked off.
     * Needed because `layerOnTopOf` and `isLayered()`/`isLayerListFull()`
     * need the count without the flag.
     */
    uint32_t rawNumLayers() const noexcept
    {
        return numLayers & ~shapeSharedFlag;
    }

public:
    bool isShapeShared() const noexcept
    {
        return (numLayers & shapeSharedFlag) != 0;
    }

    /**
     * Packed `Value*` array for a shape-shared instance. Only valid
     * when isShapeShared().
     */
    Value * const * shapeValuesArray() const noexcept
    {
        return reinterpret_cast<Value * const *>(reinterpret_cast<const char *>(attrs) + sizeof(ShapeShareInfo));
    }

    Value ** shapeValuesArrayMut() noexcept
    {
        return reinterpret_cast<Value **>(reinterpret_cast<char *>(attrs) + sizeof(ShapeShareInfo));
    }

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
        struct BindingsCursor
        {
            /**
             * Attr that the cursor currently points to.
             */
            pointer current;

            /**
             * One past the end pointer to the contiguous buffer of Attrs.
             */
            pointer end;

            /**
             * Priority of the value. Lesser values have more priority (i.e. they override
             * attributes that appear later in the linked list of Bindings).
             */
            uint32_t priority;

            pointer operator->() const noexcept
            {
                return current;
            }

            reference get() const noexcept
            {
                return *current;
            }

            bool empty() const noexcept
            {
                return current == end;
            }

            void increment() noexcept
            {
                ++current;
            }

            void consume(Symbol name) noexcept
            {
                while (!empty() && current->name <= name)
                    ++current;
            }

            GENERATE_CMP(BindingsCursor, me->current->name, me->priority)
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
         * The attribute the iterator currently points to.
         */
        pointer current = nullptr;

        /**
         * Whether iterating over a single attribute and not a merge chain.
         */
        bool doMerge = true;

        /* ponytail: prototype-only shape-sharing iteration mode (see
           Bindings::isShapeShared() above). Entirely separate from the
           k-way merge machinery above/below -- a shape-shared Bindings
           never participates in that machinery (isLayerListFull() is
           forced true for it, so it can never become someone else's
           baseLayer via the one and only layerOnTopOf() call site). This
           walks the names-owner's Attr array and this node's own
           Value* array in lockstep, synthesizing an Attr into
           `shapeScratch` at each step. Note: this makes `current` a
           self-referential pointer (&shapeScratch) that is NOT fixed up
           by the implicitly-generated copy constructor -- postfix ++
           (unused by any real call site: range-for, std::ranges::copy,
           std::set_difference all use prefix ++ on the source iterator)
           would alias stale/live scratch state. Fine for this prototype;
           would need a real fix (e.g. no cached scratch, or index-based
           dereference) before this became permanent. */
        bool shapeMode = false;
        pointer shapeNamesCur = nullptr;
        pointer shapeNamesEnd = nullptr;
        Value * const * shapeValuesCur = nullptr;
        Attr shapeScratch;

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
            current = nullptr;
            return *this;
        }

        void next(BindingsCursor cursor) noexcept
        {
            current = &cursor.get();
            cursor.increment();

            if (!cursor.empty())
                push(cursor);
        }

        std::optional<BindingsCursor> consumeAllUntilCurrentName() noexcept
        {
            auto cursor = pop();
            Symbol lastHandledName = current->name;

            while (cursor->name <= lastHandledName) {
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
            : doMerge(!attrs.isShapeShared() && attrs.baseLayer != nullptr)
        {
            if (attrs.isShapeShared()) {
                shapeMode = true;
                auto owner = attrs.shapeInfo().shapeBase;
                shapeNamesCur = owner->attrs;
                shapeNamesEnd = owner->attrs + owner->numAttrs;
                shapeValuesCur = attrs.shapeValuesArray();
                if (shapeNamesCur == shapeNamesEnd) {
                    shapeMode = false;
                    return;
                }
                shapeScratch = Attr(shapeNamesCur->name, *shapeValuesCur, shapeNamesCur->pos);
                current = &shapeScratch;
                return;
            }

            auto pushBindings = [this, priority = unsigned{0}](const Bindings & layer) mutable {
                auto first = layer.attrs;
                push(
                    BindingsCursor{
                        .current = first,
                        .end = first + layer.numAttrs,
                        .priority = priority++,
                    });
            };

            if (!doMerge) {
                if (attrs.empty())
                    return;

                current = attrs.attrs;
                pushBindings(attrs);

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
            if (shapeMode) {
                ++shapeNamesCur;
                ++shapeValuesCur;
                if (shapeNamesCur == shapeNamesEnd)
                    return finished();
                shapeScratch = Attr(shapeNamesCur->name, *shapeValuesCur, shapeNamesCur->pos);
                current = &shapeScratch;
                return *this;
            }

            if (!doMerge) {
                ++current;
                if (current == cursorHeap.front().end)
                    return finished();
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
            return current == rhs.current;
        }
    };

    using const_iterator = iterator;

    void push_back(const Attr & attr)
    {
        attrs[numAttrs++] = attr;
        numAttrsInChain = numAttrs;
    }

    /**
     * Get attribute by name or nullptr if no such attribute exists.
     */
    const Attr * get(Symbol name) const noexcept
    {
        auto getInChunk = [key = Attr{name, nullptr}](const Bindings & chunk) -> const Attr * {
            if (chunk.isShapeShared()) {
                auto owner = chunk.shapeInfo().shapeBase;
                auto first = owner->attrs;
                auto last = first + owner->numAttrs;
                const Attr * i = std::lower_bound(first, last, key);
                if (i == last || i->name != key.name)
                    return nullptr;
                auto idx = static_cast<size_t>(i - first);
                auto & scratch = chunk.shapeInfo().scratch;
                scratch = Attr(i->name, chunk.shapeValuesArray()[idx], i->pos);
                return &scratch;
            }
            auto first = chunk.attrs;
            auto last = first + chunk.numAttrs;
            const Attr * i = std::lower_bound(first, last, key);
            if (i != last && i->name == key.name)
                return i;
            return nullptr;
        };

        const Bindings * currentChunk = this;
        while (currentChunk) {
            const Attr * maybeAttr = getInChunk(*currentChunk);
            if (maybeAttr)
                return maybeAttr;
            /* A shape-shared chunk never has a real baseLayer chain of its
               own (see isLayerListFull() below, which guarantees a
               shape-shared Bindings can never become another Bindings's
               baseLayer via layerOnTopOf() -- the only call site of
               that method). So stop here instead of reading
               currentChunk->baseLayer, which is unused/null for a
               shape-shared chunk anyway. */
            currentChunk = currentChunk->isShapeShared() ? nullptr : currentChunk->baseLayer;
        }

        return nullptr;
    }

    /**
     * Check if the layer chain is full.
     */
    bool isLayerListFull() const noexcept
    {
        /* ponytail: force "full" for shape-shared Bindings so the one
           layerOnTopOf() call site (ExprOpUpdate::eval's `//`) always
           takes its full-copy fallback instead of ever making a
           shape-shared node someone else's baseLayer -- which the
           general k-way merge/get() traversal above cannot safely read
           (it's a packed Value* array, not a real Attr array). Keeps
           `//` itself completely unmodified per the prototype's scope. */
        return isShapeShared() || rawNumLayers() == Bindings::maxLayers;
    }

    /**
     * Test if the length of the linked list of layers is greater than 1.
     */
    bool isLayered() const noexcept
    {
        return !isShapeShared() && rawNumLayers() > 1;
    }

    const_iterator begin() const
    {
        return const_iterator(*this);
    }

    const_iterator end() const
    {
        return const_iterator();
    }

    Attr & operator[](size_type pos)
    {
        if (isLayered() || isShapeShared()) [[unlikely]]
            unreachable();
        return attrs[pos];
    }

    const Attr & operator[](size_type pos) const
    {
        if (isLayered() || isShapeShared()) [[unlikely]]
            unreachable();
        return attrs[pos];
    }

    void sort();

    /**
     * Returns the attributes in lexicographically sorted order.
     *
     * ponytail: returns by value (not `const Attr *`) so this works
     * uniformly for shape-shared Bindings too -- their iterator
     * synthesizes each Attr into a single shared scratch slot as it
     * advances, so collecting *pointers* into that transient state would
     * alias (every pointer in the result would end up referring to the
     * last element visited). Collecting by value sidesteps that
     * entirely, at the cost of a copy per attr, which is fine here: this
     * is a debug-printing/serialization path, not a hot loop.
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
        auto attrs = std::span(bindings->attrs, bindings->numAttrs);

        Bindings::size_type duplicates = 0;

        /* If the base bindings is smaller than the newly added attributes
           iterate using std::set_intersection to run in O(|base| + |attrs|) =
           O(|attrs|). Otherwise use an O(|attrs| * log(|base|)) per-attr binary
           search to check for duplicates. Note that if we are in this code path then
           |attrs| <= bindingsUpdateLayerRhsSizeThreshold, which 16 by default. We are
           optimizing for the case when a small attribute set gets "layered" on top of
           a much larger one. When attrsets are already small it's fine to do a linear
           scan, but we should avoid expensive iterations over large "base" attrsets. */
        if (attrs.size() > base.size()) {
            std::set_intersection(
                base.begin(),
                base.end(),
                attrs.begin(),
                attrs.end(),
                boost::make_function_output_iterator([&]([[maybe_unused]] auto && _) { ++duplicates; }));
        } else {
            for (const auto & attr : attrs) {
                if (base.get(attr.name))
                    ++duplicates;
            }
        }

        bindings->numAttrsInChain = base.numAttrsInChain + attrs.size() - duplicates;
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
        /* Mask off the discriminant bit before incrementing: a
           (hypothetically) shape-shared `base` must not make this wrapper
           look shape-shared too -- it isn't, it's a plain flat/layered
           node whose baseLayer happens to be one of those. */
        bindings->numLayers = base.rawNumLayers() + 1;
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
