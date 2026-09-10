# xattr visibility during builds (seccomp)

Sandboxed builders cannot see or set extended attributes. From
`src/libstore/unix/build/linux-derivation-builder.cc`, the seccomp filter
returns `ENOTSUP` (not `EPERM`) for `listxattr`/`llistxattr`/`flistxattr`/
`getxattr`/`lgetxattr`/`fgetxattr`/`setxattr`/`lsetxattr`/`fsetxattr` inside
the build sandbox:

```cpp
/* Prevent builders from using EAs or ACLs. Not all filesystems
   support these, and they're not allowed in the Nix store because
   they're not representable in the NAR serialisation. */
if (seccomp_rule_add(ctx, SCMP_ACT_ERRNO(ENOTSUP), SCMP_SYS(listxattr), 0) != 0
    || seccomp_rule_add(ctx, SCMP_ACT_ERRNO(ENOTSUP), SCMP_SYS(getxattr), 0) != 0
    || seccomp_rule_add(ctx, SCMP_ACT_ERRNO(ENOTSUP), SCMP_SYS(setxattr), 0) != 0
    // ... (llistxattr/flistxattr/lgetxattr/fgetxattr/lsetxattr/fsetxattr similarly)
    )
    throw SysError("unable to add seccomp rule");
```

`ENOTSUP` (rather than `EPERM`) is intentional: builds that check `errno`
see "filesystem doesn't support this" rather than "access denied", so the
restriction reads as a portability fact rather than a security wall.

## Why this matters for `trusted.nix.optimised`

This is why `isPathOptimised()`/`markPathOptimised()`
(`src/libstore/optimise-store.cc`) must treat `ENOTSUP`/`EOPNOTSUPP` as
"feature unavailable, fall through" rather than a hard error — on a
filesystem that genuinely lacks xattr support the failure mode is
indistinguishable from a sandboxed builder probing the same syscall.

It also means the xattr marker is safe from a purity standpoint without any
extra work on our part:
- Builders can never read or forge `trusted.nix.optimised` (`ENOTSUP` inside
  the sandbox).
- Canonicalisation during `addToStore()` strips all xattrs from build
  outputs before they're registered, so a freshly-built path never carries
  a stale marker.
- The marker only ever exists on already-registered store paths, set by
  `nix store optimise` running as the daemon (outside the sandbox, no
  seccomp filter active) — never inside a build.
- Since xattrs aren't part of the NAR serialisation, they can't affect
  content-addressing or build determinism.

Seccomp is only active for sandboxed builds; `nix store optimise` and
`addToStore` canonicalisation both run as the daemon without the filter, so
they can read/write the marker normally. Non-sandboxed builds and platforms
without seccomp (macOS/FreeBSD) still can't tamper with the store directly
since builders don't have write access to `/nix/store` paths outside their
own build output.
