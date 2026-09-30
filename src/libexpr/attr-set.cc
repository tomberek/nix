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

/* Allocate a Bindings that borrows source's attrs instead of copying them,
   with base as its baseLayer -- see Bindings::isBorrowing. Stores source's
   own address, not an interior pointer: this codebase runs Boehm with
   GC_set_all_interior_pointers(0), which only tracks the former. */
Bindings * EvalMemory::allocBorrowingBindings(const Bindings & source, const Bindings & base)
{
    assert(!source.isLayered());
    stats.nrAttrsets++;
    /* No new Attr storage, so no nrAttrsInAttrsets bump. */
    auto * b = new (allocBytes(sizeof(Bindings) + sizeof(const Bindings *))) Bindings();
    *reinterpret_cast<const Bindings **>(static_cast<void *>(b->attrs)) = &source;
    b->baseLayer = &base;
    b->numLayers = base.numLayers + 1;
    return b;
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
