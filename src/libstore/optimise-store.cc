#include "nix/store/local-store.hh"
#include "nix/store/local-settings.hh"
#include "nix/util/signals.hh"
#include "nix/store/posix-fs-canonicalise.hh"
#include "nix/util/posix-source-accessor.hh"
#include "nix/util/file-system.hh"
#include "nix/util/base-nix-32.hh"

#include <cstdlib>
#include <cstring>
#include <numeric>
#ifdef __APPLE__
#  include <regex>
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>

#if NIX_SUPPORT_ACL
#  include <sys/xattr.h>
#endif

#include "store-config-private.hh"

namespace nix {

#if NIX_SUPPORT_ACL
// Use trusted.* namespace:
// - Works on all file types (including symlinks)
// - Requires CAP_SYS_ADMIN (nix-daemon has this)
// - Non-root users can't read it, so they get no optimization (acceptable)
static constexpr const char* XATTR_OPTIMISED = "trusted.nix.optimised";
#endif

static void makeWritable(const std::filesystem::path & path)
{
    auto st = lstat(path);
    chmod(path, st.st_mode | S_IWUSR);
}

struct MakeReadOnly
{
    std::filesystem::path path;

    MakeReadOnly(std::filesystem::path path)
        : path(std::move(path))
    {
    }

    ~MakeReadOnly()
    {
        try {
            /* This will make the path read-only. */
            if (!path.empty())
                canonicaliseTimestampAndPermissions(path.string());
        } catch (...) {
            ignoreExceptionInDestructor();
        }
    }
};

bool LocalStore::isPathOptimised(const std::filesystem::path & path) const
{
#if NIX_SUPPORT_ACL
    char buf[32];
    ssize_t size = lgetxattr(path.c_str(), XATTR_OPTIMISED, buf, sizeof(buf));

    if (size < 0) {
        if (errno == ENOTSUP || errno == EOPNOTSUPP) {
            // Filesystem doesn't support xattrs - feature disabled
            static std::atomic<bool> warned{false};
            if (!warned.exchange(true)) {
                debug("filesystem at %s doesn't support extended attributes, optimization tracking disabled", path.string());
            }
            return false;
        }
        // EPERM/EACCES means trusted.* but no CAP_SYS_ADMIN (non-root user)
        // Return false so non-root users re-process (slower but correct)
        if (errno == EPERM || errno == EACCES) {
            return false;
        }
        // No xattr (ENODATA/ENOATTR) = not optimised
        return false;
    }

    return true;  // xattr exists = already optimised
#else
    return false;  // Platform doesn't support xattrs: always re-optimize
#endif
}

void LocalStore::markPathOptimised(const std::filesystem::path & path)
{
#if NIX_SUPPORT_ACL
    std::string timestamp = std::to_string(time(nullptr));

    if (lsetxattr(path.c_str(), XATTR_OPTIMISED, timestamp.c_str(),
                  timestamp.size(), 0) < 0) {
        if (errno != ENOTSUP && errno != EOPNOTSUPP && errno != EROFS &&
            errno != EPERM && errno != EACCES) {
            // Log unexpected errors, but ignore permission errors (non-root users)
            debug("failed to mark path optimised: %s", strerror(errno));
        }
        // Don't fail optimization if xattr fails
    }
#endif
}

AutoCloseFD LocalStore::tryClaimPath(const std::filesystem::path & path)
{
#ifndef _WIN32
    // Open the store path for locking
    // O_RDONLY works for files and directories
    // O_NOFOLLOW prevents following symlinks (we want to lock the symlink itself)
    // O_NONBLOCK prevents blocking on FIFOs
    int fd = open(path.c_str(), O_RDONLY | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) {
        // Path doesn't exist, is a symlink, or can't be opened - skip it
        return AutoCloseFD{};
    }

    // Try to acquire exclusive lock, non-blocking
    if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
        // Successfully locked - return fd to keep lock held
        return AutoCloseFD{fd};
    }

    // Lock held by another optimizer (EWOULDBLOCK) or other error
    close(fd);
    return AutoCloseFD{};
#else
    // Windows doesn't have flock - allow concurrent optimization
    return AutoCloseFD{-1};
#endif
}

LocalStore::InodeHash LocalStore::loadInodeHash()
{
    debug("loading hash inodes in memory");
    InodeHash inodeHash;

    // Load old SHA256 links from .links/ for backward compatibility
    {
        AutoCloseDir dir(opendir(linksDir.string().c_str()));
        if (dir) {
            struct dirent * dirent;
            while (errno = 0, dirent = readdir(dir.get())) {
                checkInterrupt();
                std::string name = dirent->d_name;
                if (name == "." || name == ".." || name == "sha256")
                    continue;
                inodeHash.insert(dirent->d_ino);
            }
            if (errno)
                throw SysError("reading directory %1%", PathFmt(linksDir));
        }
    }

    // Load all links from .links/sha256/ subdirectories (shards + overflow)
    AutoCloseDir shardedRoot(opendir(linksShardedDir.string().c_str()));
    if (shardedRoot) {
        struct dirent * subdirent;
        while (errno = 0, subdirent = readdir(shardedRoot.get())) {
            checkInterrupt();
            std::string subname = subdirent->d_name;
            if (subname == "." || subname == "..")
                continue;
            auto subPath = linksShardedDir / subname;

            AutoCloseDir subDir(opendir(subPath.string().c_str()));
            if (!subDir) continue;

            struct dirent * dirent;
            while (errno = 0, dirent = readdir(subDir.get())) {
                checkInterrupt();
                inodeHash.insert(dirent->d_ino);
            }
            if (errno)
                throw SysError("reading directory %1%", PathFmt(subPath));
        }
        if (errno)
            throw SysError("reading directory %1%", PathFmt(linksShardedDir));
    }

    printMsg(lvlTalkative, "loaded %1% hash inodes", inodeHash.size());

    return inodeHash;
}

Strings LocalStore::readDirectoryIgnoringInodes(const std::filesystem::path & path, const InodeHash & inodeHash)
{
    Strings names;

    AutoCloseDir dir(opendir(path.string().c_str()));
    if (!dir)
        throw SysError("opening directory %s", PathFmt(path));

    struct dirent * dirent;
    while (errno = 0, dirent = readdir(dir.get())) { /* sic */
        checkInterrupt();

        if (inodeHash.count(dirent->d_ino)) {
            debug("'%1%' is already linked", dirent->d_name);
            continue;
        }

        std::string name = dirent->d_name;
        if (name == "." || name == "..")
            continue;
        names.push_back(name);
    }
    if (errno)
        throw SysError("reading directory %s", PathFmt(path));

    return names;
}

/* Concurrency model:
 * - Multiple optimisers can run concurrently (safe by design)
 * - GC can run concurrently with optimization
 * - Every syscall handles races without aborting:
 *     ENOENT    → source/replica GC'd mid-op, skip gracefully
 *     EEXIST    → concurrent optimiser created replica first, re-inspect and use it
 *     EMLINK    → replica full (32k links), try next overflow (.001, .002, etc.)
 *     ENOSPC    → shard directory full (ext4 htree limit), skip this file
 * - GC safety: GC only unlinks entries with st_nlink == 1, so replicas with
 *   >1 links are protected from deletion while being used by other optimisers
 */
void LocalStore::optimisePath_(
    Activity * act, OptimiseStats & stats, const std::filesystem::path & path, InodeHash & inodeHash, RepairFlag repair)
{
    checkInterrupt();

    auto st = lstat(path);

#ifdef __APPLE__
    /* HFS/macOS has some undocumented security feature disabling hardlinking for
       special files within .app dirs. Known affected paths include
       *.app/Contents/{PkgInfo,Resources/\*.lproj,_CodeSignature} and .DS_Store.
       See https://github.com/NixOS/nix/issues/1443 and
       https://github.com/NixOS/nix/pull/2230 for more discussion. */

    if (std::regex_search(path.string(), std::regex("\\.app/Contents/.+$"))) {
        debug("%s is not allowed to be linked in macOS", PathFmt(path));
        return;
    }
#endif

    if (S_ISDIR(st.st_mode)) {
        Strings names = readDirectoryIgnoringInodes(path, inodeHash);
        for (auto & i : names)
            optimisePath_(act, stats, path / i, inodeHash, repair);
        return;
    }

    /* We can hard link regular files and maybe symlinks. */
    if (!S_ISREG(st.st_mode)
#if CAN_LINK_SYMLINK
        && !S_ISLNK(st.st_mode)
#endif
    )
        return;

    /* Sometimes SNAFUs can cause files in the Nix store to be
       modified, in particular when running programs as root under
       NixOS (example: $fontconfig/var/cache being modified).  Skip
       those files.  FIXME: check the modification time. */
    if (S_ISREG(st.st_mode) && (st.st_mode & S_IWUSR)) {
        warn("skipping suspicious writable file '%s'", PathFmt(path));
        return;
    }

    /* This can still happen on top-level files. */
    if (st.st_nlink > 1 && inodeHash.count(st.st_ino)) {
        debug("%s is already linked, with %d other file(s)", PathFmt(path), st.st_nlink - 2);
        return;
    }

    /* Hash the file.  Note that hashPath() returns the hash over the
       NAR serialisation, which includes the execute bit on the file.
       Thus, executable and non-executable files with the same
       contents *won't* be linked (which is good because otherwise the
       permissions would be screwed up).

       Also note that if `path' is a symlink, then we're hashing the
       contents of the symlink (i.e. the result of readlink()), not
       the contents of the target (which may not even exist). */

    Hash hash = hashPath(makeFSSourceAccessor(path), FileSerialisationMethod::NixArchive, HashAlgorithm::SHA256).hash;
    std::string hashStr = hash.to_string(HashFormat::Nix32, false);
    debug("%s has hash '%s'", PathFmt(path), hashStr);

    // Sharded link path: .links/sha256/0ab/0ab12cdef...xyz
    // Use first 3 chars for shard (first char is always ‘0’ or ‘1’, giving 2048 shards)
    // Overflow replicas go in .links/sha256/overflow/HASH.001, .002, ...
    std::string shard = hashStr.substr(0, 3);
    std::filesystem::path shardDir = linksShardedDir / shard;
    std::filesystem::path linkPath = shardDir / hashStr;

    std::filesystem::path tempLink = makeTempPath(config->realStoreDir.get(), ".tmp-link");

    /* RAII cleanup for tempLink */
    bool tempLinkCreated = false;
    Finally cleanupTempLink([&]() {
        if (tempLinkCreated) {
            std::error_code ec;
            std::filesystem::remove(tempLink, ec);
        }
    });

    /* Try primary replica first, then overflow slots.
       For each candidate:
         1. Try create_hard_link(path, candidate) to ensure replica exists.
            - EEXIST: fine, already there (possibly from another optimizer)
            - EMLINK: path itself is full, give up entirely
            - ENOENT: path was GC’d, give up entirely
         2. Try create_hard_link(candidate, tempLink).
            - EMLINK: this replica is full, try next overflow slot
            - ENOENT: replica GC’d between steps, try next slot
    */
    std::optional<std::filesystem::path> replicaPath;
    for (int seq = 0; seq < 1000; ++seq) {
        std::filesystem::path candidatePath = seq == 0
            ? linkPath
            : linksOverflowDir / (hashStr + fmt(".%03d", seq));

        // Step 1: ensure replica exists.
        // In repair mode, check if an existing replica is corrupted and remove it.
        if (repair) {
            auto stReplica = maybeLstat(candidatePath);
            if (stReplica && (st.st_size != stReplica->st_size ||
                hash != hashPath(makeFSSourceAccessor(candidatePath), FileSerialisationMethod::NixArchive, HashAlgorithm::SHA256).hash)) {
                warn("removing corrupted link %s", PathFmt(candidatePath));
                unlinkIfExists(candidatePath);
            }
        }

        try {
            std::filesystem::create_hard_link(path, candidatePath);
            inodeHash.insert(st.st_ino);
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() == std::errc::file_exists) {
                // already exists - fine, proceed to step 2
            } else if (e.code() == std::errc::too_many_links) {
                // path itself is full - can’t optimise
                if (st.st_size)
                    printInfo("%1% has maximum number of links", PathFmt(path));
                return;
            } else if (e.code() == std::errc::no_such_file_or_directory) {
                debug("%1% was GC’d during optimization", PathFmt(path));
                return;
            } else if (e.code() == std::errc::no_space_on_device) {
                printInfo("cannot link %s to ‘%s’: %s", PathFmt(candidatePath), PathFmt(path), e.code().message());
                return;
            } else {
                throw SystemError(e.code(), "creating link %1%", PathFmt(candidatePath));
            }
        }

        // Step 2: link tempLink to replica - if full, try next overflow slot
        try {
            std::filesystem::create_hard_link(candidatePath, tempLink);
            tempLinkCreated = true;
            auto stTempLink = lstat(tempLink);
            inodeHash.insert(stTempLink.st_ino);
            /* Already linked to this replica - nothing to do. */
            if (st.st_ino == stTempLink.st_ino) {
                debug("%1% is already linked to %2%", PathFmt(path), PathFmt(candidatePath));
                return;
            }
            replicaPath = candidatePath;
            break;
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() == std::errc::too_many_links) {
                continue; // replica full - try next overflow slot
            } else if (e.code() == std::errc::no_such_file_or_directory) {
                continue; // replica GC’d between steps - try next slot
            }
            throw SystemError(e.code(), "creating hard link from %1% to %2%", PathFmt(candidatePath), PathFmt(tempLink));
        }
    }

    if (!replicaPath) {
        printInfo("%1% exceeded maximum overflow replicas (999)", PathFmt(linkPath));
        return;
    }

    printMsg(lvlTalkative, "linking %1% to %2%", PathFmt(path), PathFmt(*replicaPath));

    /* Make the containing directory writable, but only if it’s not
       the store itself (we don’t want or need to mess with its
       permissions). */
    const auto dirOfPath = path.parent_path();
    bool mustToggle = dirOfPath != config->realStoreDir.get();
    if (mustToggle)
        makeWritable(dirOfPath);

    /* When we’re done, make the directory read-only again and reset
       its timestamp back to 0. */
    MakeReadOnly makeReadOnly(mustToggle ? dirOfPath : std::filesystem::path{});

    /* Atomically replace the old file with the new hard link. */
    try {
        std::filesystem::rename(tempLink, path);
        tempLinkCreated = false; /* Successfully renamed, no cleanup needed */
    } catch (std::filesystem::filesystem_error & e) {
        if (e.code() == std::errc::too_many_links) {
            /* Some filesystems generate too many links on the rename,
               rather than on the original link.  (Probably it
               temporarily increases the st_nlink field before
               decreasing it again.) */
            debug("%s has reached maximum number of links", PathFmt(*replicaPath));
            return;
        } else if (e.code() == std::errc::no_such_file_or_directory) {
            /* Source was GC'd between tempLink creation and rename.
               Benign skip - tempLink cleanup happens via RAII. */
            debug("%1% was GC'd during optimization", PathFmt(path));
            return;
        }
        throw SystemError(e.code(), "renaming %1% to %2%", PathFmt(tempLink), PathFmt(path));
    }

    stats.filesLinked++;
    stats.bytesFreed += st.st_size;

    if (act)
        act->result(
            resFileLinked,
            st.st_size
#ifndef _WIN32
            ,
            st.st_blocks
#endif
        );
}

void LocalStore::optimiseStore(OptimiseStats & stats)
{
    Activity act(*logger, actOptimiseStore);

    auto paths = queryAllValidPaths();
    std::vector<StorePath> pathVec(paths.begin(), paths.end());

    // Use a coprime step size to iterate in a different order than other concurrent optimizers.
    // This avoids lockstep marching where multiple optimizers process the exact same paths
    // in the same order. Each optimizer (with different PID) gets a different traversal order.
    // No shuffle needed - O(1) space, deterministic, and just as effective.
    size_t n = pathVec.size();
    size_t step = 1;
    size_t start = 0;

    if (n > 1) {
        // Find coprime step size based on PID
        step = (getpid() % (n - 1)) + 1;  // Start with 1..n-1
        // Ensure coprime (guarantees visiting all elements exactly once)
        while (std::gcd(step, n) != 1 && step < n) {
            step++;
        }
        if (step >= n) step = 1;  // Fallback

        start = getpid() % n;
    }

    // Check if xattrs are usable by trying to list xattrs on linksDir.
    // If not usable, we need to pre-load the inode hash as a fallback.
    InodeHash inodeHash;
#if NIX_SUPPORT_ACL
    // Try to list xattrs on linksDir to detect if they're usable
    ssize_t size = llistxattr(linksDir.string().c_str(), nullptr, 0);
    if (size < 0) {
        // Any failure means we can't rely on xattrs for optimization tracking
        // Load inode hash as fallback to avoid duplicate work
        debug("cannot use xattrs for optimization tracking (%s), loading inode hash", strerror(errno));
        inodeHash = loadInodeHash();
    }
    // Otherwise: xattrs work (size >= 0)
    // Start with empty hash - xattr checks will skip already-optimised paths
#else
    // Platform doesn't support xattrs at compile time
    inodeHash = loadInodeHash();
#endif

    act.progress(0, pathVec.size());

    uint64_t done = 0;
    uint64_t skipped = 0;

    // Iterate using coprime step for different traversal order per optimizer
    for (size_t offset = 0; offset < pathVec.size(); ++offset) {
        checkInterrupt();

        size_t idx = (start + offset * step) % pathVec.size();
        auto & i = pathVec[idx];
        auto fullPath = config->realStoreDir.get() / i.to_string();

        // Check xattr FIRST - if optimised, skip expensive DB operations
        if (isPathOptimised(fullPath)) {
            debug("skipping already-optimised path '%s'", printStorePath(i));
            skipped++;
            done++;
            act.progress(done, pathVec.size());
            continue;
        }

        // Try to claim this path for optimization (prevents duplicate work with concurrent optimizers)
        auto lockFd = tryClaimPath(fullPath);
        if (!lockFd) {
            debug("another optimizer is processing '%s', skipping", printStorePath(i));
            skipped++;
            done++;
            act.progress(done, pathVec.size());
            continue;
        }

        // Only do DB operations for paths that need optimization
        addTempRoot(i);
        if (!isValidPath(i)) {
            /* path was GC'ed, probably */
            done++;
            act.progress(done, pathVec.size());
            continue;
        }

        {
            Activity act(*logger, lvlTalkative, actUnknown, fmt("optimising path '%s'", printStorePath(i)));
            optimisePath_(&act, stats, fullPath, inodeHash, NoRepair);
        }

        // Mark path as optimised
        markPathOptimised(fullPath);

        // lockFd goes out of scope here, automatically releasing the lock

        done++;
        act.progress(done, pathVec.size());
    }

    if (skipped > 0) {
        printInfo("skipped %d already-optimised paths", skipped);
    }
}

void LocalStore::optimiseStore()
{
    OptimiseStats stats;

    optimiseStore(stats);

    printInfo("%s freed by hard-linking %d files", renderSize(stats.bytesFreed), stats.filesLinked);
}

void LocalStore::optimisePath(const std::filesystem::path & path, RepairFlag repair)
{
    OptimiseStats stats;
    InodeHash inodeHash;

    if (config->getLocalSettings().autoOptimiseStore) {
        optimisePath_(nullptr, stats, path, inodeHash, repair);
        markPathOptimised(path);
    }
}

} // namespace nix
