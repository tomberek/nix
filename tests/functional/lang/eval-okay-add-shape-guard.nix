let
  # The '+' operator caches "shape of operands last seen" per AST call site (see
  # ExprConcatStrings::eval). `add` is a single lambda whose body contains exactly one
  # '+' call site, reused by every call below -- so calling it repeatedly with different
  # operand type combinations exercises the cache hitting, missing, and being overwritten,
  # without ever being allowed to produce a wrong answer for a shape it wasn't primed for.
  add = a: b: a + b;

  seq1 = [ (add 1 1) (add 1.0 1) (add 1 1) ];
  seq2 = [ (add 2.0 2.0) (add 2 2) (add 2.0 2.0) ];
  seq3 = [ (add 3 3) (add "a" "b") (add 3 3) ];
  seq4 = [ (add /foo "/bar") (add 4 4) (add /foo "/bar") ];

  # A genuinely polymorphic call site (map applies the same lambda body, hence the same
  # '+' AST node, across a heterogeneously-typed list).
  mapped = map ({a, b}: add a b) [
    { a = 1; b = 1; }
    { a = 1.0; b = 1; }
    { a = 1; b = 1.0; }
    { a = "x"; b = "y"; }
    { a = 1; b = 1; }
  ];
in {
  eq1 = add 1 2 == 3;
  eq2 = add 1.0 2.0 == 3.0;
  eq3 = add 1 2.0 == 3.0;
  eq4 = add 1.0 2 == 3.0;
  eq5 = add "foo" "bar" == "foobar";

  seq1types = map builtins.typeOf seq1;
  seq1values = seq1 == [ 2 2.0 2 ];

  seq2types = map builtins.typeOf seq2;
  seq2values = seq2 == [ 4.0 4 4.0 ];

  seq3types = map builtins.typeOf seq3;
  seq3values = seq3 == [ 6 "ab" 6 ];

  seq4types = map builtins.typeOf seq4;
  seq4values = seq4 == [ (/foo + "/bar") 8 (/foo + "/bar") ];

  mappedTypes = map builtins.typeOf mapped;
  mappedValues = mapped == [ 2 2.0 2.0 "xy" 2 ];
}
