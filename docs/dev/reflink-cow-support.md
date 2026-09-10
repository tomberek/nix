# Reflink / copy-on-write support for file copying

Status: already implemented, not on this branch. See `tomberek/reflink`
(commit `09d615172`, "Add reflink/CoW support for file copying") and its
follow-up with a test suite on `io_uring` / `tomberek/io_uring`
(`f7d802fd5` "Add reflink (copy-on-write) support" + `d0655cf22` "tests: Add
reflink test suite"). This file is a pointer, not a spec — read those
branches for the actual implementation rather than reconstructing it from
here.

## What it does

Opportunistic reflink (CoW) support in `copyFile()`
(`src/libutil/file-system.cc`), via a new `tryReflink()` in
`src/libutil/reflink.cc`:

- Tier 1: `FICLONE` ioctl — Btrfs, XFS (`mkfs.xfs -m reflink=1`), Bcachefs,
  OCFS2.
- Tier 2: `copy_file_range()` — ZFS block cloning (needs
  `zfs_bclone_enabled=1`, kernel 5.19+/OpenZFS 2.2+).
- One-time feature detection via `std::atomic_flag` (same pattern used
  elsewhere in the codebase for `O_TMPFILE` detection), so filesystems that
  don't support it pay the failed-`ioctl` cost exactly once per process,
  not once per file.
- Falls back to a normal copy otherwise (ext4, tmpfs, etc.) — no
  regression on unsupported filesystems.

Only applies to regular files; symlinks/directories are unaffected. Reflinks
benefit store imports, cross-filesystem `moveFile` fallback (`EXDEV`), and
build-output copying from the sandbox.

## Relevance to the optimise-store work

Orthogonal to the `.links` sharding/xattr/flock work in this branch — that
work dedups by *hardlinking* identical file content within `/nix/store`;
reflink support speeds up *copies* (e.g. store imports, sandbox→store
output copies) where a hardlink isn't applicable (crossing subvolumes,
copy-then-mutate semantics). No code overlap, but both reduce store I/O
cost — worth mentioning together if either lands in a release-notes entry.

## Next step if picking this up

Diff `tomberek/reflink` (or the more complete `io_uring` branch, which adds
tests) against current `master` before reviving it — it predates several
months of upstream changes to `file-system.cc` and may need a rebase, not a
reimplementation.
