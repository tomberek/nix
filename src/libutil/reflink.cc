#include "nix/util/file-system.hh"
#include "nix/util/file-descriptor.hh"
#include "nix/util/logging.hh"

#include <cerrno>
#include <atomic>

#ifdef __linux__
#  include <sys/ioctl.h>
#  include <linux/fs.h>      // for FICLONE
#endif

#ifdef __APPLE__
#  include <sys/clonefile.h>
#endif

namespace nix {

/**
 * Try to create a reflink (copy-on-write clone) from src to dst.
 *
 * Tries multiple methods in order:
 * 1. FICLONE ioctl (Btrfs, XFS, Bcachefs, OCFS2)
 * 2. copy_file_range syscall (ZFS on Linux 5.19+/OpenZFS 2.2+)
 *
 * Returns true if reflink/block cloning succeeded, false if it should fall back to regular copy.
 * Throws an exception for actual errors (not just unsupported).
 */
bool tryReflink(const std::filesystem::path & src, const std::filesystem::path & dst)
{
    // Use atomic_flag to detect once if reflinks are unsupported
    // This prevents repeatedly trying FICLONE on filesystems that don't support it
    static std::atomic_flag reflinksUnsupported{};

    if (reflinksUnsupported.test()) {
        return false;
    }

#ifdef __linux__
    // On Linux, try FICLONE ioctl first (Btrfs, XFS, Bcachefs, OCFS2)
    // Then fall back to copy_file_range for ZFS block cloning
    AutoCloseFD srcFd = openFileReadonly(src, FinalSymlink::DontFollow);
    if (!srcFd) {
        throw SysError("opening source file %s for reflink", PathFmt(src));
    }

    auto srcStat = lstat(src);

    // Create destination file with same mode as source
    // Note: we use writeOnly=false because FICLONE requires O_RDWR on some kernel versions
    AutoCloseFD dstFd = openNewFileForWrite(dst, srcStat.st_mode & 0777, {
        .truncateExisting = false,
        .followSymlinksOnTruncate = false,
        .writeOnly = false
    });
    if (!dstFd) {
        throw SysError("creating destination file %s for reflink", PathFmt(dst));
    }

    // Attempt the reflink operation with FICLONE first
    if (ioctl(dstFd.get(), FICLONE, srcFd.get()) == 0) {
        // Success! The file now has the correct content and mode from srcStat
        // Just need to copy timestamps to match source
        dstFd.close(); // Close before setting timestamps
        setWriteTime(dst, srcStat);
        debug("successfully created reflink from %s to %s", PathFmt(src), PathFmt(dst));
        return true;
    }

    // FICLONE failed, check if we should try ZFS block cloning
    int ficlone_errno = errno;
    if (ficlone_errno == EOPNOTSUPP || ficlone_errno == ENOTTY || ficlone_errno == EINVAL) {
        // Try copy_file_range() for ZFS block cloning (Linux 5.19+, OpenZFS 2.2+)
        // This works on ZFS and potentially other filesystems
        off_t offset = 0;
        ssize_t result = copy_file_range(srcFd.get(), &offset, dstFd.get(), nullptr, srcStat.st_size, 0);

        if (result == static_cast<ssize_t>(srcStat.st_size)) {
            // Success via copy_file_range! (may or may not be zero-copy, but better than read/write loop)
            dstFd.close();
            setWriteTime(dst, srcStat);
            debug("successfully cloned file from %s to %s using copy_file_range", PathFmt(src), PathFmt(dst));
            return true;
        }

        // copy_file_range also failed or only partial copy
        // This is expected on older kernels or filesystems without support
        if (result < 0 && (errno == EOPNOTSUPP || errno == ENOSYS || errno == EXDEV || errno == EINVAL)) {
            // Both methods unsupported - mark and fall back
            reflinksUnsupported.test_and_set();
            debug("reflinks/block cloning not supported (FICLONE errno=%d, copy_file_range errno=%d), will use regular copy",
                  ficlone_errno, errno);

            // Clean up the destination file
            dstFd.close();
            tryUnlink(dst);
            return false;
        }

        // Partial copy or unexpected error from copy_file_range
        dstFd.close();
        tryUnlink(dst);
        if (result >= 0) {
            // Partial copy - shouldn't happen, fall back
            debug("copy_file_range returned partial result (%zd/%zu), falling back to regular copy",
                  result, srcStat.st_size);
            return false;
        }
        // Real error
        throw SysError("copy_file_range from %s to %s", PathFmt(src), PathFmt(dst));
    }

    if (ficlone_errno == EXDEV) {
        // Files on different filesystems
        reflinksUnsupported.test_and_set();
        debug("reflinks not supported across filesystems (EXDEV), will use regular copy");
        dstFd.close();
        tryUnlink(dst);
        return false;
    }

    // Some other error from FICLONE - this is a real problem
    throw SysError("reflinking from %s to %s", PathFmt(src), PathFmt(dst));

#elif defined(__APPLE__)
    // On macOS with APFS, use clonefile()
    if (clonefile(src.c_str(), dst.c_str(), 0) == 0) {
        debug("successfully created reflink from %s to %s", PathFmt(src), PathFmt(dst));
        return true;
    }

    if (errno == ENOTSUP || errno == EXDEV || errno == EINVAL) {
        reflinksUnsupported.test_and_set();
        debug("reflinks not supported on macOS (errno=%d), will use regular copy", errno);
        return false;
    }

    // EEXIST is okay - destination exists, we'll handle it
    if (errno == EEXIST) {
        return false;
    }

    throw SysError("reflinking from %s to %s", PathFmt(src), PathFmt(dst));

#else
    // Platform doesn't support reflinks
    reflinksUnsupported.test_and_set();
    return false;
#endif
}

} // namespace nix
