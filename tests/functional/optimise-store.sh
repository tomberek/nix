#!/usr/bin/env bash

source common.sh

requireDaemonNewerThan "2.4pre20211005"

clearStoreIfPossible

# shellcheck disable=SC2016
outPath1=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo1"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link)
# shellcheck disable=SC2016
outPath2=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo2"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link)
# shellcheck disable=SC2016
outPath3=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo3"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link)

# Files should NOT be linked yet (optimization happens later)
inode1="$(stat --format=%i "$outPath1"/foo)"
inode2="$(stat --format=%i "$outPath2"/foo)"
inode3="$(stat --format=%i "$outPath3"/foo)"

if [ "$inode1" = "$inode2" ] || [ "$inode1" = "$inode3" ]; then
    echo "inodes match unexpectedly before optimization"
    exit 1
fi

# Check that .links/sha256 directory structure was created
if [ ! -d "$NIX_STORE_DIR"/.links/sha256 ]; then
    echo ".links/sha256 directory was not created"
    exit 1
fi

# XXX: This should work through the daemon too
NIX_REMOTE="" nix-store --optimise

# After optimization, all three files with identical content should be hardlinked
inode1_after="$(stat --format=%i "$outPath1"/foo)"
inode2_after="$(stat --format=%i "$outPath2"/foo)"
inode3_after="$(stat --format=%i "$outPath3"/foo)"

if [ "$inode1_after" != "$inode2_after" ] || [ "$inode1_after" != "$inode3_after" ]; then
    echo "inodes do not match after optimization: $inode1_after vs $inode2_after vs $inode3_after"
    exit 1
fi

# Check link count (3 files + 1 replica in .links/sha256/ = 4 total links)
nlink="$(stat --format=%h "$outPath1"/foo)"
if [ "$nlink" != 4 ]; then
    echo "link count incorrect: expected 4, got $nlink"
    exit 1
fi

# Verify replicas exist in .links/sha256/ sharded structure
replica_count=$(find "$NIX_STORE_DIR"/.links/sha256/ -type f | wc -l)
if [ "$replica_count" -lt 1 ]; then
    echo "no replicas found in .links/sha256/"
    exit 1
fi

nix-store --gc

# Check that .links root only has sha256 subdirectory after GC
links_contents=$(ls "$NIX_STORE_DIR"/.links 2>/dev/null || true)
if [ "$links_contents" != "sha256" ] && [ -n "$links_contents" ]; then
    echo ".links directory should only contain sha256/ subdirectory after GC, found: $links_contents"
    exit 1
fi

# Check that sha256 shard and overflow directories are empty after GC
if [ -d "$NIX_STORE_DIR"/.links/sha256 ]; then
    for dir in "$NIX_STORE_DIR"/.links/sha256/*/; do
        if [ -n "$(ls "$dir" 2>/dev/null || true)" ]; then
            echo ".links/sha256 directory not empty after GC: $dir"
            exit 1
        fi
    done
fi

# Test the optimised-paths sidecar database (skip-tracking mechanism)
if [ -n "$(type -p sqlite3)" ]; then
    clearStoreIfPossible

    # shellcheck disable=SC2016
    outPath4=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo4"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link)
    # shellcheck disable=SC2016
    outPath5=$(echo 'with import '"${config_nix}"'; mkDerivation { name = "foo5"; builder = builtins.toFile "builder" "mkdir $out; echo hello > $out/foo"; }' | nix-build - --no-out-link)

    if [ ! -f "$NIX_STATE_DIR"/db/optimised.sqlite ]; then
        echo "optimised.sqlite sidecar database was not created"
        exit 1
    fi

    NIX_REMOTE="" nix-store --optimise

    for p in "$outPath4" "$outPath5"; do
        if ! sqlite3 "$NIX_STATE_DIR"/db/optimised.sqlite "select 1 from OptimisedPaths where path = '$p'" | grep -q 1; then
            echo "no OptimisedPaths row for '$p' after optimising"
            exit 1
        fi
    done

    # A second run should be a complete no-op: every path already has a
    # matching (path, narHash) row, so the anti-join finds nothing to do.
    secondRunOutput=$(NIX_REMOTE="" nix-store --optimise 2>&1)
    if [ "$secondRunOutput" != "0.0 KiB freed by hard-linking 0 files" ]; then
        echo "second optimise run was not a no-op: $secondRunOutput"
        exit 1
    fi

    # GC should prune the OptimisedPaths row for a path it deletes,
    # while leaving other rows (e.g. the surviving path) untouched.
    nix-store --delete "$outPath4"

    if sqlite3 "$NIX_STATE_DIR"/db/optimised.sqlite "select 1 from OptimisedPaths where path = '$outPath4'" | grep -q 1; then
        echo "deleted path's OptimisedPaths row was not pruned by GC"
        exit 1
    fi

    if ! sqlite3 "$NIX_STATE_DIR"/db/optimised.sqlite "select 1 from OptimisedPaths where path = '$outPath5'" | grep -q 1; then
        echo "surviving path's OptimisedPaths row was incorrectly removed"
        exit 1
    fi
fi
