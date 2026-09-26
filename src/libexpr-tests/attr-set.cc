#include "nix/expr/tests/libexpr.hh"

namespace nix {

class AttrSetValuePoolTest : public LibExprTest
{};

/* buildBindingsWithValues()'s alloc() must produce independently-valid,
   correctly-readable Values -- whether they come from the builder's own
   dedicated GC_malloc_many free list or (once that's exhausted) a freshly
   requested batch. */
TEST_F(AttrSetValuePoolTest, allocProducesUsableValues)
{
    auto builder = state.buildBindingsWithValues(3);
    Value & v0 = builder.alloc("a");
    Value & v1 = builder.alloc("b");
    Value & v2 = builder.alloc("c");
    v0.mkInt(1);
    v1.mkInt(2);
    v2.mkInt(3);
    auto bindings = builder.finish();

    ASSERT_EQ(bindings->size(), 3u);
    ASSERT_THAT(*bindings->get(createSymbol("a")), testing::Field(&Attr::value, &v0));
    EXPECT_EQ(v0.integer().value, 1);
    EXPECT_EQ(v1.integer().value, 2);
    EXPECT_EQ(v2.integer().value, 3);
}

/* Not every slot need be filled via alloc(): capacity can exceed the eventual
   size (e.g. a caller reserves room for a duplicate/skipped name). */
TEST_F(AttrSetValuePoolTest, capacityCanExceedNumAttrs)
{
    auto builder = state.buildBindingsWithValues(4);
    builder.alloc("a").mkInt(10);
    builder.alloc("b").mkInt(20);
    // Only 2 of the 4 reserved slots get filled.
    auto bindings = builder.finish();

    ASSERT_EQ(bindings->size(), 2u);
    ASSERT_TRUE(bindings->get(createSymbol("a")));
    EXPECT_EQ(bindings->get(createSymbol("a"))->value->integer().value, 10);
    EXPECT_EQ(bindings->get(createSymbol("b"))->value->integer().value, 20);
}

/* Allocating more Values than fit in a single GC_malloc_many batch must
   transparently refill the builder's dedicated free list and keep working. */
TEST_F(AttrSetValuePoolTest, allocSurvivesBatchRefill)
{
    constexpr int n = 5000;
    auto builder = state.buildBindingsWithValues(n);
    for (int i = 0; i < n; ++i)
        builder.alloc(fmt("a%d", i)).mkInt(i);
    auto bindings = builder.finish();

    ASSERT_EQ(bindings->size(), (size_t) n);
    EXPECT_EQ(bindings->get(createSymbol("a0"))->value->integer().value, 0);
    auto lastName = fmt("a%d", n - 1);
    EXPECT_EQ(bindings->get(createSymbol(lastName.c_str()))->value->integer().value, n - 1);
}

/* Values allocated by a dedicated builder are still independent GC objects,
   not carved out of the Bindings/Attr allocation itself. */
TEST_F(AttrSetValuePoolTest, allocIsNotColocatedWithBindings)
{
    auto builder = state.buildBindingsWithValues(2);
    Value & v0 = builder.alloc("a");
    auto bindings = builder.finish();

    auto bindingsStart = reinterpret_cast<const char *>(bindings);
    auto bindingsEnd = bindingsStart + sizeof(Bindings) + 2 * sizeof(Attr);
    auto valueAddr = reinterpret_cast<const char *>(&v0);
    EXPECT_TRUE(valueAddr < bindingsStart || valueAddr >= bindingsEnd);
}

/* The plain (non-dedicated) buildBindings() must keep behaving exactly as
   before: alloc() falls back to EvalMemory::allocValue()'s shared cache. */
TEST_F(AttrSetValuePoolTest, plainBuildBindingsStillWorks)
{
    auto builder = state.buildBindings(2);
    Value & v0 = builder.alloc("a");
    v0.mkInt(42);
    auto bindings = builder.finish();

    ASSERT_EQ(bindings->size(), 1u);
    EXPECT_EQ(bindings->get(createSymbol("a"))->value->integer().value, 42);
}

} // namespace nix
