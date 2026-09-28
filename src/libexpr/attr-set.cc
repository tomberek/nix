#include "nix/expr/attr-set.hh"
#include "nix/expr/eval-inline.hh"

#include <algorithm>

namespace nix {

const constinit Bindings Bindings::emptyBindings;

/* Allocate a new array of attributes for an attribute set with a specific
   capacity. The space is implicitly reserved after the Bindings
   structure. */
Bindings * EvalMemory::allocBindings(size_t capacity)
{
    if (capacity == 0)
        /* Swear that we are not going to modify this. */
        return const_cast<Bindings *>(&Bindings::emptyBindings);
    if (capacity > std::numeric_limits<Bindings::size_type>::max())
        throw Error("attribute set of size %d is too big", capacity);
    stats.nrAttrsets++;
    stats.nrAttrsInAttrsets += capacity;
    return new (allocBytes(sizeof(Bindings) + sizeof(Attr) * capacity)) Bindings();
}

namespace {
struct SpanCursor
{
    const Attr * current;
    const Attr * end;
    /* Lower priority value = higher actual priority (0 = topmost/highest). */
    uint32_t priority;

    bool empty() const noexcept
    {
        return current == end;
    }
};

/* Min-heap comparator: pop the cursor with the smallest name; on a tie,
   the one with the smallest priority value (i.e. highest actual priority). */
struct SpanCursorGreater
{
    bool operator()(const SpanCursor & a, const SpanCursor & b) const noexcept
    {
        if (a.current->name != b.current->name)
            return a.current->name > b.current->name;
        return a.priority > b.priority;
    }
};

/* Merge N sorted, possibly name-overlapping spans into one flat, sorted,
   duplicate-free Bindings, where spans[i] takes priority over spans[j] for
   i<j on name collisions (spans[0] highest priority). O(total * log N)
   via a small priority queue, regardless of N -- unlike a cascade of N-1
   pairwise merges, which costs O(N * total). */
const Bindings * mergeManySpans(EvalMemory & mem, SymbolTable & symbols, std::span<const std::span<const Attr>> spans)
{
    size_t totalCapacity = 0;
    for (auto & s : spans)
        totalCapacity += s.size();
    auto builder = mem.buildBindings(symbols, totalCapacity);

    std::vector<SpanCursor> heap;
    heap.reserve(spans.size());
    for (size_t p = 0; p < spans.size(); ++p)
        if (!spans[p].empty())
            heap.push_back({spans[p].data(), spans[p].data() + spans[p].size(), (uint32_t) p});
    std::ranges::make_heap(heap, SpanCursorGreater{});

    while (!heap.empty()) {
        std::ranges::pop_heap(heap, SpanCursorGreater{});
        SpanCursor c = heap.back();
        heap.pop_back();

        builder.push_back(*c.current);
        Symbol name = c.current->name;
        ++c.current;
        if (!c.empty()) {
            heap.push_back(c);
            std::ranges::push_heap(heap, SpanCursorGreater{});
        }

        /* Discard any other cursors that also had this name (lower-priority duplicates). */
        while (!heap.empty() && heap.front().current->name == name) {
            std::ranges::pop_heap(heap, SpanCursorGreater{});
            SpanCursor dup = heap.back();
            heap.pop_back();
            ++dup.current;
            if (!dup.empty()) {
                heap.push_back(dup);
                std::ranges::push_heap(heap, SpanCursorGreater{});
            }
        }
    }
    return builder.alreadySorted();
}
} // namespace

bool Bindings::compactTopLayers(EvalMemory & mem, SymbolTable & symbols) const
{
    /* Need at least k=3 layers to merge plus one remaining ancestor. */
    if (numLayers < 4)
        return false;

    boost::container::static_vector<const Bindings *, maxLayers> layers;
    for (const Bindings * layer = this; layer && layers.size() < maxLayers; layer = layer->baseLayer)
        layers.push_back(layer);
    size_t N = layers.size();

    if (N < 4)
        return false;

    /* Choose the prefix length k (3 <= k <= N-1) minimizing cost(k)/(k-2),
       where cost(k) is the total own size of layers[0..k-1] and (k-2) is
       the number of layer-chain slots freed by merging them into one. This
       is the amortized cost per slot freed if we keep re-choosing the same
       k every time the chain fills up again -- see the doc comment on the
       declaration for the full reasoning, including why this needs no
       special case for a layer that happens to be unusually large. */
    uint64_t runningSum = (uint64_t) layers[0]->numAttrs + layers[1]->numAttrs + layers[2]->numAttrs;
    size_t bestK = 3;
    uint64_t bestCost = runningSum;
    for (size_t k = 4; k <= N - 1; ++k) {
        runningSum += layers[k - 1]->numAttrs;
        /* runningSum/(k-2) < bestCost/(bestK-2), cross-multiplied to avoid floats. */
        if (runningSum * (bestK - 2) < bestCost * (k - 2)) {
            bestCost = runningSum;
            bestK = k;
        }
    }

    /* Merge layers[0..bestK-1]'s own attrs into one flat layer in a single
       O(total * log bestK) pass (layers[0] highest priority). */
    boost::container::static_vector<std::span<const Attr>, maxLayers> spans;
    for (size_t i = 0; i < bestK; ++i)
        spans.push_back(std::span<const Attr>(layers[i]->attrs, layers[i]->numAttrs));
    const Bindings * acc = mergeManySpans(mem, symbols, spans);

    const Bindings * remainder = layers[bestK];
    acc->baseLayer = remainder;
    acc->numLayers = remainder->numLayers + 1;
    acc->numAttrsInChain = 0;

    baseLayer = acc;
    numLayers = acc->numLayers + 1;
    return true;
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
    std::sort(attrs, attrs + numAttrs);
}

Value & Value::mkAttrs(BindingsBuilder & bindings)
{
    mkAttrs(bindings.finish());
    return *this;
}

} // namespace nix
