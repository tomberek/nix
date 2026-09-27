#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include <stdexcept>

#include "nix/expr/eval.hh"
#include "nix/expr/tests/libexpr.hh"
#include "nix/expr/tests/gc.hh"
#include "nix/util/memory-source-accessor.hh"
#include "nix/util/finally.hh"

namespace nix {

TEST(nix_isAllowedURI, http_example_com)
{
    Strings allowed;
    allowed.push_back("http://example.com");

    ASSERT_TRUE(isAllowedURI("http://example.com", allowed));
    ASSERT_TRUE(isAllowedURI("http://example.com/foo", allowed));
    ASSERT_TRUE(isAllowedURI("http://example.com/foo/", allowed));
    ASSERT_FALSE(isAllowedURI("/", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.co", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.como", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.org", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.org/foo", allowed));
}

TEST(nix_isAllowedURI, http_example_com_foo)
{
    Strings allowed;
    allowed.push_back("http://example.com/foo");

    ASSERT_TRUE(isAllowedURI("http://example.com/foo", allowed));
    ASSERT_TRUE(isAllowedURI("http://example.com/foo/", allowed));
    ASSERT_FALSE(isAllowedURI("/foo", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.como", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.org/foo", allowed));
    // Broken?
    // ASSERT_TRUE(isAllowedURI("http://example.com/foo?ok=1", allowed));
}

TEST(nix_isAllowedURI, http)
{
    Strings allowed;
    allowed.push_back("http://");

    ASSERT_TRUE(isAllowedURI("http://", allowed));
    ASSERT_TRUE(isAllowedURI("http://example.com", allowed));
    ASSERT_TRUE(isAllowedURI("http://example.com/foo", allowed));
    ASSERT_TRUE(isAllowedURI("http://example.com/foo/", allowed));
    ASSERT_TRUE(isAllowedURI("http://example.com", allowed));
    ASSERT_FALSE(isAllowedURI("/", allowed));
    ASSERT_FALSE(isAllowedURI("https://", allowed));
    ASSERT_FALSE(isAllowedURI("http:foo", allowed));
}

TEST(nix_isAllowedURI, https)
{
    Strings allowed;
    allowed.push_back("https://");

    ASSERT_TRUE(isAllowedURI("https://example.com", allowed));
    ASSERT_TRUE(isAllowedURI("https://example.com/foo", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com/https:", allowed));
}

TEST(nix_isAllowedURI, absolute_path)
{
    Strings allowed;
    allowed.push_back("/var/evil"); // bad idea

    ASSERT_TRUE(isAllowedURI("/var/evil", allowed));
    ASSERT_TRUE(isAllowedURI("/var/evil/", allowed));
    ASSERT_TRUE(isAllowedURI("/var/evil/foo", allowed));
    ASSERT_TRUE(isAllowedURI("/var/evil/foo/", allowed));
    ASSERT_FALSE(isAllowedURI("/", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evi", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evilo", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evilo/", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evilo/foo", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com/var/evil", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com//var/evil", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com//var/evil/foo", allowed));
}

TEST(nix_isAllowedURI, file_url)
{
    Strings allowed;
    allowed.push_back("file:///var/evil"); // bad idea

    ASSERT_TRUE(isAllowedURI("file:///var/evil", allowed));
    ASSERT_TRUE(isAllowedURI("file:///var/evil/", allowed));
    ASSERT_TRUE(isAllowedURI("file:///var/evil/foo", allowed));
    ASSERT_TRUE(isAllowedURI("file:///var/evil/foo/", allowed));
    ASSERT_FALSE(isAllowedURI("/", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evi", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evilo", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evilo/", allowed));
    ASSERT_FALSE(isAllowedURI("/var/evilo/foo", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com/var/evil", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com//var/evil", allowed));
    ASSERT_FALSE(isAllowedURI("http://example.com//var/evil/foo", allowed));
    ASSERT_FALSE(isAllowedURI("http://var/evil", allowed));
    ASSERT_FALSE(isAllowedURI("http:///var/evil", allowed));
    ASSERT_FALSE(isAllowedURI("http://var/evil/", allowed));
    ASSERT_FALSE(isAllowedURI("file:///var/evi", allowed));
    ASSERT_FALSE(isAllowedURI("file:///var/evilo", allowed));
    ASSERT_FALSE(isAllowedURI("file:///var/evilo/", allowed));
    ASSERT_FALSE(isAllowedURI("file:///var/evilo/foo", allowed));
    ASSERT_FALSE(isAllowedURI("file:///", allowed));
    ASSERT_FALSE(isAllowedURI("file://", allowed));
}

TEST(nix_isAllowedURI, github_all)
{
    Strings allowed;
    allowed.push_back("github:");
    ASSERT_TRUE(isAllowedURI("github:", allowed));
    ASSERT_TRUE(isAllowedURI("github:foo/bar", allowed));
    ASSERT_TRUE(isAllowedURI("github:foo/bar/feat-multi-bar", allowed));
    ASSERT_TRUE(isAllowedURI("github:foo/bar?ref=refs/heads/feat-multi-bar", allowed));
    ASSERT_TRUE(isAllowedURI("github://foo/bar", allowed));
    ASSERT_FALSE(isAllowedURI("https://github:443/foo/bar/archive/master.tar.gz", allowed));
    ASSERT_FALSE(isAllowedURI("file://github:foo/bar/archive/master.tar.gz", allowed));
    ASSERT_FALSE(isAllowedURI("file:///github:foo/bar/archive/master.tar.gz", allowed));
    ASSERT_FALSE(isAllowedURI("github", allowed));
}

TEST(nix_isAllowedURI, github_org)
{
    Strings allowed;
    allowed.push_back("github:foo");
    ASSERT_FALSE(isAllowedURI("github:", allowed));
    ASSERT_TRUE(isAllowedURI("github:foo/bar", allowed));
    ASSERT_TRUE(isAllowedURI("github:foo/bar/feat-multi-bar", allowed));
    ASSERT_TRUE(isAllowedURI("github:foo/bar?ref=refs/heads/feat-multi-bar", allowed));
    ASSERT_FALSE(isAllowedURI("github://foo/bar", allowed));
    ASSERT_FALSE(isAllowedURI("https://github:443/foo/bar/archive/master.tar.gz", allowed));
    ASSERT_FALSE(isAllowedURI("file://github:foo/bar/archive/master.tar.gz", allowed));
    ASSERT_FALSE(isAllowedURI("file:///github:foo/bar/archive/master.tar.gz", allowed));
}

TEST(nix_isAllowedURI, non_scheme_colon)
{
    Strings allowed;
    allowed.push_back("https://foo/bar:");
    ASSERT_TRUE(isAllowedURI("https://foo/bar:", allowed));
    ASSERT_TRUE(isAllowedURI("https://foo/bar:/baz", allowed));
    ASSERT_FALSE(isAllowedURI("https://foo/bar:baz", allowed));
}

class EvalStateTest : public LibExprTest
{};

TEST_F(EvalStateTest, getBuiltins_ok)
{
    auto evaled = maybeThunk("builtins");
    auto & builtins = state.getBuiltins();
    ASSERT_TRUE(builtins.type() == nAttrs);
    ASSERT_EQ(evaled, &builtins);
}

TEST_F(EvalStateTest, getBuiltin_ok)
{
    auto & builtin = state.getBuiltin("toString");
    ASSERT_TRUE(builtin.type() == nFunction);
    // FIXME
    // auto evaled = maybeThunk("builtins.toString");
    // ASSERT_EQ(evaled, &builtin);
    auto & builtin2 = state.getBuiltin("true");
    ASSERT_EQ(state.forceBool(builtin2, noPos, "in unit test"), true);
}

TEST_F(EvalStateTest, getBuiltin_fail)
{
    ASSERT_THROW(state.getBuiltin("nonexistent"), EvalError);
}

class PureEvalTest : public LibExprTest
{
public:
    PureEvalTest()
        : LibExprTest(openStore("dummy://", {{"read-only", "false"}}), [](bool & readOnlyMode) {
            EvalSettings settings{readOnlyMode};
            settings.pureEval = true;
            settings.restrictEval = true;
            return settings;
        })
    {
    }
};

TEST_F(PureEvalTest, pathExists)
{
    ASSERT_THAT(eval("builtins.pathExists /."), IsFalse());
    ASSERT_THAT(eval("builtins.pathExists /nix"), IsFalse());
    ASSERT_THAT(eval("builtins.pathExists /nix/store"), IsFalse());

    {
        std::string contents = "Lorem ipsum";

        StringSource s{contents};
        auto path = state.store->addToStoreFromDump(
            s, "source", FileSerialisationMethod::Flat, ContentAddressMethod::Raw::Text, HashAlgorithm::SHA256);
        auto printed = store->printStorePath(path);

        ASSERT_THROW(eval(fmt("builtins.readFile %s", printed)), RestrictedPathError);
        ASSERT_THAT(eval(fmt("builtins.pathExists %s", printed)), IsFalse());

        ASSERT_THROW(eval("builtins.readDir /."), RestrictedPathError);
        state.allowPath(path); // FIXME: This shouldn't behave this way.
        ASSERT_THAT(eval("builtins.readDir /."), IsAttrsOfSize(0));
    }
}

#if NIX_USE_BOEHMGC
TEST_F(LibExprTest, gcThreadReportsFatalAssertion)
{
    EXPECT_FATAL_FAILURE_ON_ALL_THREADS(runOnGCThread([] { FAIL() << "worker assertion"; }), "worker assertion");
}

TEST_F(LibExprTest, gcThreadPropagatesException)
{
    EXPECT_THROW(runOnGCThread([] { throw std::runtime_error("worker exception"); }), std::runtime_error);
}

TEST_F(LibExprTest, resetFileCacheReleasesValues)
{
    auto accessor = make_ref<MemorySourceAccessor>();
    auto file = accessor->addFile(CanonPath("/test.nix"), "{ a = 1; }");

    auto weak = static_cast<void **>(GC_MALLOC_ATOMIC(sizeof(void *)));
    ASSERT_NE(nullptr, weak);
    *weak = nullptr;
    Finally cleanup([&] {
        GC_unregister_disappearing_link(weak);
        GC_FREE(weak);
    });

    runOnGCThread([&] {
        Value v;
        state.evalFile(file, v);
        ASSERT_EQ(nAttrs, v.type());
        auto attrs = const_cast<Bindings *>(v.attrs());
        *weak = attrs;
        ASSERT_EQ(GC_SUCCESS, GC_GENERAL_REGISTER_DISAPPEARING_LINK(weak, attrs));
    });
    ASSERT_FALSE(HasFatalFailure());

    /* The cached value keeps the attribute set alive. */
    GC_gcollect();
    ASSERT_NE(nullptr, *weak);

    /* Clearing the cache must not leave stale roots in its storage. */
    state.resetFileCache();
    for (int i = 0; i < 3; ++i)
        GC_gcollect();

    EXPECT_EQ(nullptr, *weak);
}

/* ponytail: GC-safety test for the mapAttrs "shape sharing" prototype
   (see docs/attrset-range-sharing-design.md, mechanism 1, and
   Bindings::isShapeShared() in attr-set.hh). A shape-shared Bindings
   stores a raw `const Bindings * shapeBase` pointing at the ORIGINAL
   input attrset's Bindings instead of copying its Symbol/PosIdx pairs.
   Because Boehm's interior-pointer support is disabled
   (GC_set_all_interior_pointers(0), eval-gc.cc), the only way this is
   safe is if `shapeBase` is an exact, GC-block-start pointer that the
   collector treats as a normal strong reference (same as
   Bindings::baseLayer already does). This test constructs the "owner"
   Bindings and a shape-shared node referencing it on a throwaway GC
   thread (so the calling thread's conservative stack scan can't
   accidentally keep the owner alive by coincidence), roots ONLY the
   shape-shared result via GC-heap storage, forces a real collection
   cycle, and checks: (1) the owner survived purely because shapeBase
   references it, and (2) the shape-shared result's keys/values still
   read back correctly afterward -- which is exactly what would go
   silently wrong (dangling shapeBase, corrupted reads) if the pointer
   were interior instead of block-start. */
TEST_F(LibExprTest, mapAttrsShapeSharingSurvivesGC)
{
    auto weakOwner = static_cast<void **>(GC_MALLOC_ATOMIC(sizeof(void *)));
    ASSERT_NE(nullptr, weakOwner);
    *weakOwner = nullptr;
    Finally cleanupOwner([&] {
        GC_unregister_disappearing_link(weakOwner);
        GC_FREE(weakOwner);
    });

    /* Strong root for the shape-shared RESULT, living on the GC heap
       (not the C++ call stack) so it is the only thing keeping the
       result -- and transitively, via shapeBase, the owner -- alive
       once the construction thread below exits. */
    auto rootedResult = static_cast<const Bindings **>(GC_MALLOC(sizeof(void *)));
    ASSERT_NE(nullptr, rootedResult);
    *rootedResult = nullptr;
    Finally cleanupResult([&] { GC_FREE(rootedResult); });

    runOnGCThread([&] {
        auto b = state.buildBindings(3);
        b.alloc("a").mkInt(1);
        b.alloc("b").mkInt(2);
        b.alloc("c").mkInt(3);
        const Bindings * owner = b.finish();

        *weakOwner = const_cast<Bindings *>(owner);
        ASSERT_EQ(GC_SUCCESS, GC_GENERAL_REGISTER_DISAPPEARING_LINK(weakOwner, owner));

        auto * shared = state.mem.allocShapeSharedBindings(owner, 3);
        ASSERT_TRUE(shared->isShapeShared());

        Value * v10 = state.allocValue();
        v10->mkInt(10);
        Value * v20 = state.allocValue();
        v20->mkInt(20);
        Value * v30 = state.allocValue();
        v30->mkInt(30);
        Value * newVals[3] = {v10, v20, v30};

        auto values = shared->shapeValuesArrayMut();
        for (size_t idx = 0; idx < 3; ++idx)
            values[idx] = newVals[idx];

        *rootedResult = shared;
    });
    ASSERT_FALSE(HasFatalFailure());

    /* The owner is reachable ONLY through the shape-shared result's
       shapeBase pointer at this point -- the construction thread that
       had `owner` on its stack has already exited and joined. */
    GC_gcollect();
    ASSERT_NE(nullptr, *weakOwner) << "owner Bindings was collected despite being referenced via shapeBase -- "
                                      "shapeBase is not being treated as a real GC root";

    auto * shared = *rootedResult;
    ASSERT_NE(nullptr, shared);

    auto a = shared->get(createSymbol("a"));
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->value->integer().value, 10);

    auto b2 = shared->get(createSymbol("b"));
    ASSERT_NE(b2, nullptr);
    EXPECT_EQ(b2->value->integer().value, 20);

    auto c = shared->get(createSymbol("c"));
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->value->integer().value, 30);

    std::vector<std::pair<std::string, int64_t>> got;
    for (auto & attr : *shared)
        got.push_back({std::string(state.symbols[attr.name]), attr.value->integer().value});
    std::vector<std::pair<std::string, int64_t>> expected = {{"a", 10}, {"b", 20}, {"c", 30}};
    EXPECT_EQ(got, expected);

    /* Not asserting that `owner` becomes collectible after this point:
       this test function's own C++ stack still holds `shared`/`a`/`b2`/
       `c` above, and Boehm's conservative stack scan legitimately treats
       those as roots for as long as they're in scope -- that's expected
       GC behavior, not something this prototype's shape-sharing changes
       could or should try to work around. */
}

#endif

} // namespace nix
