#include "nix/expr/attr-set.hh"
#include "nix/expr/eval-inline.hh"

#include <algorithm>

namespace nix {

const constinit Bindings Bindings::emptyBindings;

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
