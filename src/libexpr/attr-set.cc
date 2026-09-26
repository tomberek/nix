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

Value & BindingsBuilder::alloc(Symbol name, PosIdx pos)
{
    Value * value;
#if NIX_USE_BOEHMGC
    if (dedicatedValueAlloc) {
        /* Batch-allocate from a free list private to this builder, mirroring
           EvalMemory::allocValue()'s GC_malloc_many-based refill (@see
           eval-inline.hh) verbatim, just against `valueFreeList` instead of
           the globally shared thread_local cache. Each popped node is still
           an independently-valid, exact-base-address GC object (same
           GC_malloc_many contract as allocValue() -- no interior pointers,
           no change to Bindings' memory layout); only which free list a
           given alloc() call draws from changes. */
        if (!valueFreeList) {
            valueFreeList = GC_malloc_many(sizeof(Value));
            if (!valueFreeList)
                throw std::bad_alloc();
        }
        void * p = valueFreeList;
        valueFreeList = GC_NEXT(p);
        GC_NEXT(p) = nullptr;
        value = (Value *) p;
        mem.get().stats.nrValues++;
    } else
#endif
        value = mem.get().allocValue();
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
