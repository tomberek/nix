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
} // namespace

/* k-way priority-queue merge of N sorted spans (spans[0] highest priority
   on name collisions), O(total * log N) -- avoids the O(N * total) cost of
   a cascade of pairwise merges. Not in the header: needs external linkage
   (not anonymous-namespace) purely so its `friend` declaration in Bindings
   resolves to this function, for access to maxLayers. */
const Bindings * mergeManySpans(EvalMemory & mem, SymbolTable & symbols, std::span<const std::span<const Attr>> spans)
{
    size_t totalCapacity = 0;
    for (auto & s : spans)
        totalCapacity += s.size();
    auto builder = mem.buildBindings(symbols, totalCapacity);

    boost::container::static_vector<SpanCursor, Bindings::maxLayers> heap;
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

bool Bindings::compactTopLayers(EvalMemory & mem, SymbolTable & symbols) const
{
    if (numLayers < 3)
        return false;

    boost::container::static_vector<const Bindings *, maxLayers> layers;
    for (const Bindings * layer = this; layer && layers.size() < maxLayers; layer = layer->baseLayer)
        layers.push_back(layer);
    size_t N = layers.size();

    if (N < 3)
        return false;

    /* Pick m (2 <= m <= N-1) minimizing cost(m)/(m-1), the amortized cost
       per chain slot freed by merging layers[1..m]. Self-corrects for an
       oversized layer without a special case: including it spikes cost
       more than slots freed, so the scan stops before it. layers[0]
       (`this`) is excluded since get()/iterator always check it first. */
    uint64_t runningSum = (uint64_t) layers[1]->numAttrs + layers[2]->numAttrs;
    size_t bestM = 2;
    uint64_t bestCost = runningSum;
    for (size_t m = 3; m < N; ++m) {
        runningSum += layers[m]->numAttrs;
        /* runningSum/(m-1) < bestCost/(bestM-1), cross-multiplied to avoid floats. */
        if (runningSum * (bestM - 1) < bestCost * (m - 1)) {
            bestCost = runningSum;
            bestM = m;
        }
    }

    /* O(total * log bestM) merge; layers[1] has highest priority. */
    boost::container::static_vector<std::span<const Attr>, maxLayers> spans;
    for (size_t i = 1; i <= bestM; ++i)
        spans.push_back(std::span<const Attr>(layers[i]->attrs, layers[i]->numAttrs));
    const Bindings * acc = mergeManySpans(mem, symbols, spans);

    /* bestM == N-1 means acc absorbed the whole rest of the chain including
       the root and is already flat; otherwise give it the remainder tail. */
    if (bestM < N - 1) {
        const Bindings * remainder = layers[bestM + 1];
        acc->baseLayer = remainder;
        acc->numLayers = remainder->numLayers + 1;
        acc->numAttrsInChain = 0;
    }

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
