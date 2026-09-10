# LRUCache: `std::map` → `boost::unordered_flat_map`

Status: proposed, not implemented. `tomberek/optimise/lru_flat_map` exists
as a placeholder branch (currently identical to master, no commits yet).

## Summary

Replace the red-black tree backing `nix::LRUCache` with an open-addressed
hash table. On a large GC workload this reclaims 6–8% of total CPU with a
~30-line change in one header.

## Motivation

Profiling a `nix store gc` run on a store with 3.94M paths and 69k GC roots
(64-CPU host, 20 s `perf record -F 199`, 3904 samples):

```
10.55%  __memcmp_evex_movbe
 2.81%  std::_Rb_tree<StorePath,…LRUCache…>::find
```

Call-graph attribution of the `memcmp` samples:

```
__memcmp_evex_movbe
├─ 5.64%  Rb_tree<…LRUCache…>::find
│         ├─ 2.88%  Store::isValidPath
│         └─ 2.76%  Store::queryPathInfoFromClientCache
└─ 4.51%  Rb_tree<StorePath>::_M_insert_unique   (StorePathSet, separate issue)
```

Aggregate attributable to `LRUCache` tree ops: **~8.4% of CPU**.

The cache is sized at `path-info-cache-size = 65536` (`store-api.hh:279`), so
each lookup is `log₂(65 536) = 16` node visits, each doing a `memcmp` on the
~30-byte `StorePath::baseName`. For a whole-store GC visiting millions of
paths this dominates the pure-CPU portion of the traditional-GC code path.

## Current implementation

`src/libutil/include/nix/util/lru-cache.hh`:

```cpp
template<typename Key, typename Value, typename Compare = std::less<>>
class LRUCache
{
    struct LRUIterator;
    using Data = std::map<Key, std::pair<LRUIterator, Value>, Compare>;
    using LRU  = std::list<typename Data::iterator>;

    Data data;
    LRU  lru;
    …
};
```

`Data` is a red-black tree. Every `get`/`upsert` performs an ordered lookup,
comparing `StorePath` keys with `Compare` (defaulted to `std::less<>` for
heterogeneous lookup — used by `queryPathInfoFromClientCache`, which currently
looks up with a `StorePath` directly, so heterogeneous lookup is not exercised
in the GC hot path).

## Proposed implementation

Switch `Data` to `boost::unordered_flat_map`:

```cpp
#include <boost/unordered/unordered_flat_map.hpp>

template<
    typename Key,
    typename Value,
    typename Hash    = std::hash<Key>,
    typename KeyEq   = std::equal_to<>>
class LRUCache
{
    struct LRUIterator;
    using Data = boost::unordered_flat_map<
        Key,
        std::pair<LRUIterator, Value>,
        Hash,
        KeyEq>;
    using LRU  = std::list<typename Data::iterator>;
    …
};
```

`StorePath` already has both a hash (`path.hh:101`) and `operator==` /
`operator<=>` on the full `baseName`, so no new key infrastructure is needed
for the default instantiation. The hash is essentially free: the first eight
bytes of the base32 hash are reinterpreted as a `size_t`.

### Why `boost::unordered_flat_map` specifically

- Already a project dependency — `gc.cc:577` uses it for `referrersCache`.
- Open-addressed, cache-friendly bucket layout: one indirection per lookup vs.
  `log₂ n` cache-cold pointer chases for the RB tree.
- Value-stable *only across `insert` when there is no rehash*. **Iterators are
  invalidated by rehash**, which matters for `LRU` (see below).
- Faster than `std::unordered_map` on all measured workloads by a wide margin.

### LRU list invalidation

The current design stores `list<Data::iterator>` (an iterator to the map). With
`std::map`, node-based storage means map iterators are stable across
insert/erase of other elements. With `unordered_flat_map`, iterators are
**invalidated on rehash**.

Two solutions:

**Option A — store keys in the LRU list, not iterators.**

```cpp
using LRU = std::list<Key>;   // was: std::list<Data::iterator>
```

`promote` and `erase` do a map lookup via the key. This adds one hash lookup
per `promote` (which was previously a pointer deref). At ~4 ns for the extra
hash lookup vs. ~10 ns for the previous cache-cold tree traversal — net win.

**Option B — reserve enough buckets up front to prevent rehash.**

Call `data.reserve(capacity)` in the constructor. Since `LRUCache` never grows
past `capacity`, no rehash occurs and iterator stability is preserved.

Option B is preferred: zero API impact, no extra lookup on promote. `reserve`
on a `boost::unordered_flat_map` for 65 536 entries is a single allocation.

### Heterogeneous lookup

The current `Compare = std::less<>` template parameter allows lookup by
`std::string_view` etc. — but grepping the codebase shows every `LRUCache`
instantiation passes a `Key` (`StorePath`, `Realisation`, `Hash`) at every
call site. Removing heterogeneous lookup is safe; if we want to keep it,
provide a transparent hash:

```cpp
struct TransparentStorePathHash {
    using is_transparent = void;
    size_t operator()(const StorePath & p) const noexcept {
        return std::hash<StorePath>{}(p);
    }
    size_t operator()(std::string_view sv) const noexcept {
        return *reinterpret_cast<const size_t *>(sv.data());
    }
};
```

Not needed for the GC hot path; adding it later is non-breaking.

## Estimated impact

| Metric                            | Before      | After (est.) |
|-----------------------------------|-------------|--------------|
| CPU in `Rb_tree::find` (LRU)      | 2.81%       | ~0%          |
| CPU in `memcmp` (LRU-attributed)  | ~5.6%       | ~0.3%        |
| Total CPU savings                 | —           | **6–8%**     |
| Per-lookup latency (65k entries)  | ~16 memcmp  | 1 hash + 1 memcmp |

On the observed 4h 36m GC run this is roughly **25–30 min saved** wall-clock.
Marginal for a run of this size; substantial for the many callers of
`isValidPath`/`queryPathInfo` outside GC (evaluation, build scheduling,
`nix-store --query`, etc.).

## Complications and risks

1. **`std::less<>` template parameter is public API.** Any downstream code
   that instantiates `LRUCache<K, V, MyCompare>` breaks. Grep across the tree:

   ```
   src/libstore/store-api.hh:443   LRUCache<StorePath, PathInfoCacheValue>
   src/libstore/…                  (other Store subclasses inherit)
   src/libstore-tests/             tests only
   ```

   No external `Compare` uses. Safe to replace, but the template parameter
   change is technically an ABI/API break for the C++ library.

2. **Behaviour on `capacity == 0`.** The current code early-returns from
   `upsert` when `capacity == 0` (`lru-cache.hh:61`). `reserve(0)` on
   `unordered_flat_map` is a no-op, so preserve the guard.

3. **Iteration order.** `std::map` iterates in key order, `unordered_flat_map`
   does not. The public `LRUCache` API doesn't expose iteration, and the
   internal `LRU` list handles ordering — no functional change.

4. **Tests.** `src/libutil-tests/lru-cache.cc` exercises `get`, `upsert`,
   `erase`, `size`, `clear`. All order-independent. Existing tests should
   pass without modification.

5. **Memory.** `unordered_flat_map` has some slack for load factor (~0.875 max
   by default). For 65 536 entries, the underlying array is ~75k slots ×
   `sizeof(pair<Key, pair<LRUIterator, Value>>)`. Modest increase over the
   node-based map, well within budget on the machines that run GC.

## Alternatives considered

- **`absl::flat_hash_map`** — comparable perf, adds a dependency. Boost is
  already vendored.
- **`std::unordered_map`** — node-based, would not gain the cache-locality
  benefit. Not worth the churn.
- **Larger `pathInfoCacheSize`.** Would reduce miss rate but *increases* the
  per-lookup RB tree depth (`log₂ n` grows), making the current problem worse.
  Complementary to this change, not a substitute.

## Change scope

- **Files:** `src/libutil/include/nix/util/lru-cache.hh` (only).
- **Lines:** ~30 changed, mostly template parameter and one `#include`.
- **Downstream:** callers unchanged — the `Compare`-based template parameter
  is replaced with `Hash` and `KeyEqual`, both defaulted.

## Testing

1. `nix-build -A tests.functional` — existing suite covers `Store::isValidPath`
   and `queryPathInfo` paths that hit the cache.
2. `src/libutil-tests/lru-cache.cc` unchanged; should pass.
3. Micro-benchmark: fill cache to 65k entries, do 1M random `get`s. Expect
   ≥10× throughput improvement on the raw cache.
4. End-to-end: run `nix store gc --dry-run` on a large store, compare wall
   clock and `perf record` symbol breakdown against baseline.

## Related work

- `src/libstore/gc.cc:577` already uses `boost::unordered_flat_map` for the
  `referrersCache`, confirming both the dependency and the perf pattern.
- `boost::unordered_flat_set` could similarly replace `StorePathSet`
  (`std::set<StorePath>`), which shows up as another ~5% of CPU in the same
  profile (`Rb_tree<StorePath>::_M_insert_unique`). Larger surface area
  (`StorePathSet` is exposed all over the API), so tackle separately.
