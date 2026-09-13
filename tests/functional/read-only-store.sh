#!/usr/bin/env bash

source common.sh

enableFeatures "read-only-local-store"

needLocalStore "cannot open store read-only when daemon has already opened it writeable"

TODO_NixOS

clearStore

happy () {
    # We can do a read-only query just fine with a read-only store
    nix --store local?read-only=true path-info "$dummyPath"

    # `local://` also works.
    nix --store local://?read-only=true path-info "$dummyPath"

    # We can "write" an already-present store-path a read-only store, because no IO is actually required
    nix-store --store local?read-only=true --add dummy
}
## Testing read-only mode without forcing the underlying store to actually be read-only

# Make sure the command fails when the store doesn't already have a database
expectStderr 1 nix-store --store local?read-only=true --add dummy | grepQuiet "database does not exist, and cannot be created in read-only mode"

# Make sure the store actually has a current-database, with at least one store object
dummyPath=$(nix-store --add dummy)

# Try again and make sure we fail when adding a item not already in the store
expectStderr 1 nix-store --store local?read-only=true --add eval.nix | grepQuiet "attempt to write a readonly database"

# Test a few operations that should work with the read-only store in its current state
happy

## Testing the optimised-paths sidecar database's read-only behaviour

# --optimise against a logically-read-only store (the store dir itself is
# still writable here, before we chmod it below) should succeed and must
# never write to the optimised-paths sidecar database.
nix-store --store local?read-only=true --optimise

if [ -n "$(type -p sqlite3)" ] && [ -f "$NIX_STATE_DIR"/db/optimised.sqlite ]; then
    if sqlite3 "file:$NIX_STATE_DIR/db/optimised.sqlite?mode=ro" \
        "insert into OptimisedPaths values ('x', 'y', 1)" 2>/dev/null; then
        echo "optimised.sqlite sidecar accepted a write while store is read-only"
        exit 1
    fi
fi

## Testing read-only mode with an underlying store that is actually read-only

# Ensure store is actually read-only
chmod -R -w "$TEST_ROOT"/store
chmod -R -w "$TEST_ROOT"/var

# Make sure we fail on add operations on the read-only store
# This is only for adding files that are not *already* in the store
# Should show enhanced error message with helpful context
expectStderr 1 nix-store --add eval.nix | grepQuiet "This command may have been run as non-root in a single-user Nix installation"
expectStderr 1 nix-store --store local?read-only=true --add eval.nix | grepQuiet "Permission denied"

# Test the same operations from before should again succeed
happy
