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
