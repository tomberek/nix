#include "nix/expr/tests/libexpr.hh"

namespace nix {

/* Exercises `//`'s merge strategies by shape of input rather than by
   calling internals directly, so they stay valid if those are renamed
   or restructured. */
class AttrSetUpdateTest : public LibExprTest
{};

TEST_F(AttrSetUpdateTest, update_two_flat_sets)
{
    auto v = eval(R"(
        let
          base = { a = 1; b = 2; c = 3; };
          overlay = { b = 20; d = 4; };
          result = base // overlay;
        in
          result.a == 1 && result.b == 20 && result.c == 3 && result.d == 4
          && builtins.length (builtins.attrNames result) == 4
    )");
    ASSERT_THAT(v, IsTrue());
}

TEST_F(AttrSetUpdateTest, update_chain_of_comparably_sized_overlays)
{
    auto v = eval(R"(
        let
          base = { x0 = 0; };
          chained = builtins.foldl' (acc: i: acc // { "k${toString i}" = i; }) base (builtins.genList (i: i + 1) 20);
          overwritten = chained // { k10 = 9999; };
        in
          chained.x0 == 0
          && chained.k1 == 1 && chained.k20 == 20
          && builtins.length (builtins.attrNames chained) == 21
          && overwritten.k10 == 9999
          && overwritten.k9 == 9
          && builtins.length (builtins.attrNames overwritten) == 21
    )");
    ASSERT_THAT(v, IsTrue());
}

TEST_F(AttrSetUpdateTest, update_overlay_shadows_key_in_absorbed_layer)
{
    auto v = eval(R"(
        let
          base = { a = 1; };
          step1 = base // { b = 2; };
          step2 = step1 // { b = 99; };
        in
          step2.a == 1 && step2.b == 99
    )");
    ASSERT_THAT(v, IsTrue());
}

TEST_F(AttrSetUpdateTest, update_with_much_larger_rhs)
{
    auto v = eval(R"(
        let
          big = builtins.listToAttrs (map (i: { name = "k${toString i}"; value = i; }) (builtins.genList (i: i) 1200));
          small = { k5 = 999; extra = 111; };
          result = small // big;
        in
          result.extra == 111
          && result.k5 == 5
          && result.k0 == 0
          && result.k1199 == 1199
          && builtins.length (builtins.attrNames result) == 1201
    )");
    ASSERT_THAT(v, IsTrue());
}

} // namespace nix
