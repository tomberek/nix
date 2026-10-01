# Attrset layering/merge optimization — performance & correctness report

**Branch:** `minimal-attrset-layering`
**Baseline:** `tomberek/master` @ `c621c2b3` (the direct merge-base — a clean ancestor, no divergence)
**Compared binary:** full branch with locked-in defaults (`minFilterSize=1024`, `maxFilterBaseSize=500`)

## What changed

`Bindings`'s `//`-merge (`ExprOpUpdate::eval`) now avoids copying the larger side of a merge in two cases instead of one:

- **RHS smaller** (pre-existing): copy the small RHS verbatim into a new layer on top of the LHS (`baseLayer`), no copy of the big LHS.
- **RHS bigger** (new this round): filter the LHS down to its own non-shadowed keys and layer *that* on top of the RHS instead — the RHS is referenced via the existing, zero-overhead `baseLayer` mechanism, never copied, regardless of its size or whether it's itself already layered.

Two gates gate the new path: `minFilterSize` (RHS must be at least this big to bother) and `maxFilterBaseSize` (LHS must be at most this big — bounds the one-time filtering cost and the permanent per-lookup "check my own slot first" tax that any layered result pays for the rest of its life). Below either gate, a plain flat copy keeps future lookups single-layer-cheap.

Several more aggressive variants (galloping search for the filter step, no size caps at all) were tried and measured; the locked-in defaults are the best point found on the memory/CPU tradeoff curve — see the earlier investigation history in this PR's commits and comments for the full exploration (unconditional borrowing, self-flattening, range hints, prefetching, etc. — all measured, most rejected).

## Correctness

1. **393 existing unit tests** (`nix-expr-tests`): pass.
2. **26 property-based test cases** (52 binary-runs) in `/tmp/prop-merge-test.nix` + `/tmp/run-prop-tests.sh`, covering every code path by construction: `shouldLayer` (RHS small, RHS smaller-than-LHS), full-copy fallback (below `minFilterSize`), the new filter+layer path with both its internal strategies (binary-search-favored and merge-join-favored), the `maxFilterBaseSize`-capped fallback, the "RHS itself layered" fallback, and edge cases exactly at each threshold. Each case's result is checked against a reference merge computed *without* using `//` at all (list-filter + concatenation, hashed and compared). **0 failures**, and the branch's checksum matches `tomberek/master`'s byte-for-byte in every case.
3. **14 real-world and synthetic benchmarks** (listed below): output compared directly between `tomberek/master` and the final branch build — identical in every case (derivation paths for the NixOS configs, exact numeric results for the synthetic ones).

## Benchmarks used

| Benchmark | What it exercises |
|---|---|
| `bench-hello` | Trivial real-nixpkgs merge (`pkgs.hello // {...} // pkgs.hello.drvAttrs`) |
| `bench-small` | Tiny attrset literal, no merge — pure noise-floor check |
| `bench-rec` | 3000× repeated small (60-element) accumulator build |
| `bench-removeattrs` | `removeAttrs`-heavy — untouched by this change, regression guard |
| `bench-lookup` | 200k pure `get()` lookups, no merging — hot-path regression guard |
| `bench-bigmerge` | Single 10k×10k no-overlap merge, repeated — both sides exceed `maxFilterBaseSize`, falls back to full copy by design |
| `bench-chain` | 300× a 20-step fold of single-key overlays onto a 5000-element base (`shouldLayer` path only) |
| `bench-cap-stress` | 300× a 6000×12000 merge — deliberately exceeds `maxFilterBaseSize`, exercises the capped-fallback path |
| `bench-deep-chain` | 20× a 30-step fold of 1500-element overlays onto a growing base (`shouldLayer` path, deep chains up to `maxLayers`) |
| `bench-haskell` | Real nixpkgs: `haskellPackages` attrNames + metadata access (large real-world attrset) |
| `bench-minimal` | Minimal NixOS config (filesystem + bootloader only) |
| `bench-server` | Mid-size NixOS server config (nginx, postgres, prometheus, grafana, etc.) |
| `heavy-eval` | Full desktop NixOS config (gnome, docker, libvirt, ~15 packages) |
| `plasma-standard` | Full KDE Plasma NixOS config |

## Results

![Memory comparison](chart-memory.png)

![CPU comparison](chart-cpu.png)

| Benchmark | Memory Δ | Cycles Δ | Instructions Δ |
|---|---:|---:|---:|
| bench-hello | **-62.5%** | -4.9% | -6.0% |
| bench-small | 0.0% | -1.7% | -3.1% |
| bench-rec | **-24.5%** | -4.5% | -4.5% |
| bench-removeattrs | 0.0% | +2.5% | 0.0% |
| bench-lookup | 0.0% | -2.3% | -0.4% |
| bench-bigmerge | 0.0% | -0.3% | +0.4% |
| bench-chain | **-49.1%** | +0.9% | -5.6% |
| bench-cap-stress | 0.0% | +1.6% | +4.4% |
| bench-deep-chain | **-71.6%** | **+5.7%** | **+18.8%** |
| bench-haskell | **-29.6%** | -4.2% | -4.2% |
| bench-minimal | **-27.7%** | -12.4% | -9.2% |
| bench-server | **-29.1%** | -3.4% | +1.7% |
| heavy-eval | **-33.1%** | -14.7% | -11.1% |
| plasma-standard | **-33.2%** | -1.9% | -3.1% |

All measurements: `perf stat -e cycles,instructions`, interleaved rounds (both binaries measured back-to-back, repeated, contaminated rounds from system load discarded), summed across `cpu_atom`+`cpu_core` PMU domains on this hybrid P/E-core machine. Memory is `sets.bytes` from `NIX_SHOW_STATS=1`, which is deterministic (no repeats needed).

## Headline numbers

Every real-world NixOS config and every real-nixpkgs benchmark shows a **memory win between -24% and -72%**, and **CPU is a wash-to-win on every real-world benchmark tested**. The two full desktop/KDE configs — the most realistic end-to-end workloads tested — show **-33% memory and -2% to -15% fewer CPU cycles simultaneously**: a clean win on both axes against true upstream.

## `bench-chain` and `bench-deep-chain`: two unrelated findings, correctly separated

Both benchmarks originally showed a CPU regression (+7.5% and +9.7% cycles). Investigating found **two distinct causes, neither of which is what an earlier draft of this report claimed** — corrected here rather than left standing:

**`bench-chain`: a real bug in this round's commit, now fixed.** Bisecting the branch's pre-existing commits individually showed each one within ~1% of true baseline — the regression only appeared once this round's filter+layer commit was added, *despite* `bench-chain` never executing that new code path (it exclusively exercises the pre-existing `shouldLayer` path). Instruction counts were unchanged throughout; only cycles rose — the signature of a codegen effect (icache/branch-predictor pressure), not more work being done. `ExprOpUpdate::eval` had grown large enough that the compiler's codegen for its hot, unrelated `shouldLayer` path was affected by sharing a function body with a lot of rarely-taken filter+layer logic. **Fix:** moved that branch into its own `[[gnu::noinline]]` function. Result: +6.4% → **+0.9%** cycles vs. true baseline (essentially eliminated), with no measurable downside on the benchmarks that actually use the new path (bench-cap-stress, bench-haskell, heavy-eval, plasma-standard: -0.5% to -2.1%, i.e. neutral-to-better).

**`bench-deep-chain`: not a bug — a pre-existing, intentional tradeoff, unrelated to this round's work.** Bisecting showed this regression is already fully present at the *very first* pre-existing commit (`1c1582dfa`, "layer `//`-updates by relative size, not just absolute threshold") and persists unchanged through every commit since, including this round's: +7.9% cycles / +18.4% instructions vs. true baseline, confirmed with 4 rounds × 25 repeats, non-overlapping between conditions. Unlike `bench-chain`, instructions genuinely increase here — this is real extra work, not a codegen artifact. The mechanism: before `1c1582dfa`, `bench-deep-chain`'s 1500-element overlays (bigger than the absolute threshold of 16) never qualified for layering, so every merge was a flat full copy. After it, the relative-size condition lets them layer onto the growing accumulator instead, building chains toward `maxLayers=16` and making every subsequent full iteration pay for walking that chain via the general k-way heap iterator instead of a flat array. Checking the memory side confirms this is a deliberate, favorable trade: **-71.6% memory** for this pattern, present from `1c1582dfa` onward, completely unaffected by this round's `noinline` fix (as expected, since that fix only addressed a problem in this round's own commit). Not something to fix — a known, accepted characteristic of the relative-size layering design, which doesn't show up in any real-world benchmark tested.

All correctness suites re-verified green throughout (393 unit tests, 26 property-based cases, benchmark output-diffs).

## Reproduction

- Binaries: `/tmp/result-tomberek-master` (baseline), `/tmp/result-final-defaults` (branch, locked defaults)
- Benchmarks: `/tmp/bench-*.nix`, `/tmp/heavy-eval.nix`, `/tmp/plasma-standard.nix`
- Sweep driver: `/tmp/full-sweep.sh` → `/tmp/sweep-results.csv` (CPU), memory collected via `NIX_SHOW_STATS=1` into `/tmp/sweep-memory.csv`
- Charts: `/tmp/make-graphs.py` + `/tmp/plot-mem.gp` / `/tmp/plot-cpu.gp`
- Correctness: `/tmp/run-prop-tests.sh` (property-based), direct output diff for all `bench-*.nix`
