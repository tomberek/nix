let
  # Warm the '+' inline cache at this call site with two successful int+int evaluations
  # (map reuses the same lambda body, hence the same AST node, for every list element),
  # then hit it with an incompatible operand type. The cache must fall through to the
  # normal cascade and throw the usual type error -- not misuse the stale int/int fast path.
  results = map (x: 1 + x) [ 1 2 "foo" ];
in
  builtins.deepSeq results results
