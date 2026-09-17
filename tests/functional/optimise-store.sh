#!/usr/bin/env bash

source common.sh

# shellcheck disable=SC2016
outPath1=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo1"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link --auto-optimise-store)
# shellcheck disable=SC2016
outPath2=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo2"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link --auto-optimise-store)

TODO_NixOS # ignoring the client-specified setting 'auto-optimise-store', because it is a restricted setting and you are not a trusted user
  # TODO: only continue when trusted user or root

inode1="$(stat --format=%i "$outPath1"/foo)"
inode2="$(stat --format=%i "$outPath2"/foo)"
if [ "$inode1" != "$inode2" ]; then
    fail "inodes do not match"
fi

nlink="$(stat --format=%h "$outPath1"/foo)"
# 3 content links (outPath1/foo, outPath2/foo, one entry in .links or
# the sharded .hardlinks/sha256 farm) plus one .hardlinks/tracking mark
# per referencing StorePath (outPath1, outPath2).
if [ "$nlink" != 5 ]; then
    fail "link count incorrect"
fi

# shellcheck disable=SC2016
outPath3=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo3"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link)

inode3="$(stat --format=%i "$outPath3"/foo)"
if [ "$inode1" = "$inode3" ]; then
    fail "inodes match unexpectedly"
fi

# XXX: This should work through the daemon too
NIX_REMOTE="" nix-store --optimise

inode1="$(stat --format=%i "$outPath1"/foo)"
inode3="$(stat --format=%i "$outPath3"/foo)"
if [ "$inode1" != "$inode3" ]; then
    fail "inodes do not match"
fi

# shellcheck disable=SC2016
outPath4=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo4"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link)

NIX_REMOTE="" nix store optimise

inode1="$(stat --format=%i "$outPath1"/foo)"
inode4="$(stat --format=%i "$outPath4"/foo)"
if [ "$inode1" != "$inode4" ]; then
    fail "inodes do not match"
fi

# alias of optimise
if ! NIX_REMOTE="" nix store optimize; then
    fail "nix store optimize alias is not present"
fi

nix-store --gc

if [ -n "$(ls "$NIX_STORE_DIR"/.links)" ]; then
    fail ".links directory not empty after GC"
fi

# Test BLAKE3-based deduplication (.hardlinks/b3, mode-first sharded).
clearStoreIfPossible

export NIX_CONFIG="extra-experimental-features = blake3-hashes blake3-links"

# Build two derivations with identical regular files, an executable, and a
# symlink - same underlying bytes, three different modes, to exercise the
# mode-keyed shard trees (r/x/s) rather than just plain dedup.
# shellcheck disable=SC2016
outPath1b3=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo1"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo; cp $out/foo $out/bar; chmod +x $out/bar; ln -s foo $out/lnk"; }' | nix-build - --no-out-link)
# shellcheck disable=SC2016
outPath2b3=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo2"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo; cp $out/foo $out/bar; chmod +x $out/bar; ln -s foo $out/lnk"; }' | nix-build - --no-out-link)

NIX_REMOTE="" nix-store --optimise

if [ "$(stat --format=%i "$outPath1b3"/foo)" != "$(stat --format=%i "$outPath2b3"/foo)" ]; then
    fail "BLAKE3: foo inodes do not match"
fi

if [ "$(stat --format=%i "$outPath1b3"/bar)" != "$(stat --format=%i "$outPath2b3"/bar)" ]; then
    fail "BLAKE3: bar inodes do not match"
fi

# foo (regular, not executable) and bar (regular, executable) have
# identical bytes but must land in different mode trees, so they must
# NOT end up sharing an inode with each other.
if [ "$(stat --format=%i "$outPath1b3"/foo)" = "$(stat --format=%i "$outPath1b3"/bar)" ]; then
    fail "BLAKE3: foo and bar incorrectly share an inode despite different modes"
fi

if [ ! -x "$outPath1b3"/bar ]; then
    fail "BLAKE3: bar lost its executable bit after optimising"
fi

if [ "$(readlink "$outPath1b3"/lnk)" != foo ]; then
    fail "BLAKE3: lnk is no longer a symlink to foo after optimising"
fi

if [ -z "$(find "$NIX_STORE_DIR"/.hardlinks/b3 -mindepth 1 -maxdepth 3 -type d 2>/dev/null)" ]; then
    fail "BLAKE3: .hardlinks/b3 has no shard directories after optimising"
fi

NIX_REMOTE="" nix-store --verify --check-contents

nix-store --gc

if [ -n "$(find "$NIX_STORE_DIR"/.hardlinks/b3 -type f -o -type l 2>/dev/null)" ]; then
    fail ".hardlinks/b3 not empty after GC"
fi
