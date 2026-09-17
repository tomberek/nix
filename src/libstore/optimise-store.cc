#include "nix/store/local-store.hh"
#include "nix/store/local-settings.hh"
#include "nix/util/signals.hh"
#include "nix/store/posix-fs-canonicalise.hh"
#include "nix/util/source-accessor.hh"
#include "nix/util/file-system.hh"

#include <cstdlib>
#include <cstring>
#include <climits>
#include <cassert>
#ifdef __APPLE__
#  include <regex>
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#include "store-config-private.hh"

namespace nix {

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

LocalStore::InodeHash LocalStore::loadInodeHash()
{
    debug("loading hash inodes in memory");
    InodeHash inodeHash;

    AutoCloseDir dir(opendir(linksDir.string().c_str()));
    if (!dir)
        throw SysError("opening directory %1%", PathFmt(linksDir));

    struct dirent * dirent;
    while (errno = 0, dirent = readdir(dir.get())) { /* sic */
        checkInterrupt();
        // We don't care if we hit non-hash files, anything goes
        inodeHash.insert(dirent->d_ino);
    }
    if (errno)
        throw SysError("reading directory %1%", PathFmt(linksDir));

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

void LocalStore::optimisePath_(
    Activity * act,
    OptimiseStats & stats,
    const std::filesystem::path & path,
    InodeHash & inodeHash,
    RepairFlag repair,
    std::filesystem::path relPath,
    std::optional<std::filesystem::path> & markRelPath)
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
            optimisePath_(act, stats, path / i, inodeHash, repair, relPath / i, markRelPath);
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
        markRelPath = relPath;
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
    debug("%s has hash '%s'", PathFmt(path), hash.to_string(HashFormat::Nix32, true));

    /* Check if this is a known hash. */
    std::filesystem::path linkPath = std::filesystem::path{linksDir} / hash.to_string(HashFormat::Nix32, false);

    /* Maybe delete the link, if it has been corrupted. */
    if (pathExists(linkPath)) {
        auto stLink = lstat(linkPath);
        if (st.st_size != stLink.st_size || (repair && hash != ({
                                                           hashPath(
                                                               makeFSSourceAccessor(linkPath),
                                                               FileSerialisationMethod::NixArchive,
                                                               HashAlgorithm::SHA256)
                                                               .hash;
                                                       }))) {
            // XXX: Consider overwriting linkPath with our valid version.
            warn("removing corrupted link %s", PathFmt(linkPath));
            warn(
                "There may be more corrupted paths."
                "\nYou should run `nix-store --verify --check-contents --repair` to fix them all");
            unlinkIfExists(linkPath);
        }
    }

    if (!pathExists(linkPath)) {
        /* Nope, create a hard link in the links directory. */
        try {
            std::filesystem::create_hard_link(path, linkPath);
            inodeHash.insert(st.st_ino);
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() == std::errc::file_exists) {
                /* Fall through if another process created ‘linkPath’ before
                   we did. */
            }

            else if (e.code() == std::errc::no_space_on_device) {
                /* On ext4, that probably means the directory index is
                   full.  When that happens, it's fine to ignore it: we
                   just effectively disable deduplication of this
                   file.
                   */
                printInfo("cannot link %s to '%s': %s", PathFmt(linkPath), PathFmt(path), e.code().message());
                return;
            }

            else
                throw SystemError(e.code(), "creating hard link from %1% to %2%", PathFmt(linkPath), PathFmt(path));
        }
    }

    /* Yes!  We've seen a file with the same contents.  Replace the
       current file with a hard link to that file. */
    auto stLink = maybeLstat(linkPath);

    /* A concurrent garbage collection may have removed the link in the
       links directory between the existence check above and now. Skip
       optimising this path; a later pass will dedup it. */
    if (!stLink)
        return;

    if (st.st_ino == stLink->st_ino) {
        debug("%1% is already linked to %2%", PathFmt(path), PathFmt(linkPath));
        markRelPath = relPath;
        return;
    }

    printMsg(lvlTalkative, "linking %1% to %2%", PathFmt(path), PathFmt(linkPath));

    /* Make the containing directory writable, but only if it's not
       the store itself (we don't want or need to mess with its
       permissions). */
    const auto dirOfPath = path.parent_path();
    bool mustToggle = dirOfPath != config->realStoreDir.get();
    if (mustToggle)
        makeWritable(dirOfPath);

    /* When we're done, make the directory read-only again and reset
       its timestamp back to 0. */
    MakeReadOnly makeReadOnly(mustToggle ? dirOfPath : std::filesystem::path{});

    std::filesystem::path tempLink = makeTempPath(config->realStoreDir.get(), ".tmp-link");

    try {
        std::filesystem::create_hard_link(linkPath, tempLink);
        inodeHash.insert(st.st_ino);
    } catch (std::filesystem::filesystem_error & e) {
        if (e.code() == std::errc::too_many_links) {
            /* Too many links to the same file (>= 32000 on most file
               systems).  This is likely to happen with empty files.
               Just shrug and ignore. */
            if (st.st_size)
                printInfo("%1% has maximum number of links", PathFmt(linkPath));
            return;
        }
        if (e.code() == std::errc::no_such_file_or_directory) {
            /* A concurrent garbage collection removed the link in the
               links directory. Skip optimising this path; a later pass
               will dedup it. */
            return;
        }
        throw SystemError(e.code(), "creating hard link from %1% to %2%", PathFmt(linkPath), PathFmt(tempLink));
    }

    /* Atomically replace the old file with the new hard link. */
    try {
        std::filesystem::rename(tempLink, path);
    } catch (std::filesystem::filesystem_error & e) {
        {
            std::error_code ec;
            remove(tempLink, ec); /* Clean up after ourselves. */
            if (ec)
                printError("unable to unlink %1%: %2%", PathFmt(tempLink), ec.message());
        }
        if (e.code() == std::errc::too_many_links) {
            /* Some filesystems generate too many links on the rename,
               rather than on the original link.  (Probably it
               temporarily increases the st_nlink field before
               decreasing it again.) */
            debug("%s has reached maximum number of links", PathFmt(linkPath));
            return;
        }
        throw SystemError(e.code(), "renaming %1% to %2%", PathFmt(tempLink), PathFmt(path));
    }

    markRelPath = relPath;

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

/* Sentinel mark filename for the empty-relPath case (single-file
   StorePaths, e.g. .drv files, where the representative file *is* the
   StorePath itself). An empty string can't be a filesystem entry name,
   so this can't be represented via percentEncodeMarkName - it needs an
   explicit out-of-band marker instead. "%" alone can never be produced
   by percentEncodeMarkName for a non-empty relPath: that function only
   ever emits a lone '%' as the first byte of a complete "%XX" triple,
   never on its own. */
static const std::string emptyRelPathMarkName = "%";

/* Encode a relPath into a single filename safe to place directly under
   trackingDir/<StorePath>/. '/' can't appear in a filename at all, and
   '%' is escaped too so the encoding round-trips unambiguously. Every
   other byte (including other reserved shell/filesystem characters)
   passes through unchanged - relPath components are already validated
   store-path-safe names, so this only needs to handle the one
   character '/' introduces when components are joined. Must not be
   called with an empty relPath - use emptyRelPathMarkName instead. */
static std::string percentEncodeMarkName(const std::filesystem::path & relPath)
{
    assert(!relPath.empty());
    std::string out;
    for (char c : relPath.native()) {
        if (c == '/' || c == '%') {
            out += '%';
            out += "0123456789ABCDEF"[(unsigned char) c >> 4];
            out += "0123456789ABCDEF"[(unsigned char) c & 0xf];
        } else
            out += c;
    }
    return out;
}

static std::optional<std::filesystem::path> percentDecodeMarkName(const std::string & name)
{
    if (name == emptyRelPathMarkName)
        return std::filesystem::path{};

    std::string out;
    for (size_t i = 0; i < name.size(); i++) {
        if (name[i] == '%') {
            if (i + 2 >= name.size())
                return std::nullopt;
            auto hex = [](char c) -> int {
                if (c >= '0' && c <= '9')
                    return c - '0';
                if (c >= 'A' && c <= 'F')
                    return c - 'A' + 10;
                return -1;
            };
            int hi = hex(name[i + 1]), lo = hex(name[i + 2]);
            if (hi < 0 || lo < 0)
                return std::nullopt;
            out += (char) ((hi << 4) | lo);
            i += 2;
        } else
            out += name[i];
    }
    return std::filesystem::path{out};
}

void LocalStore::writeOptimiseMark(const StorePath & storePath, const std::optional<std::filesystem::path> & markRelPath)
{
    if (!writeOptimiseMarks || !markRelPath)
        return;

    /* relPath is encoded into a single filename (percent-encoding '/'
       and '%', or the emptyRelPathMarkName sentinel if relPath itself
       is empty) so a mark is a fixed-depth lookup regardless of how
       deep the representative file sits, rather than one directory
       per path component. This trades away being able to reconstruct
       relPath by just walking directories with `ls`, and caps the
       encodable relPath length at NAME_MAX: if it doesn't fit, skip
       writing a mark this round (fail-open, same as the no-candidate
       case) rather than trying to represent it another way. */
    std::string encoded = markRelPath->empty() ? emptyRelPathMarkName : percentEncodeMarkName(*markRelPath);
    if (encoded.size() > NAME_MAX)
        return;

    auto markRoot = trackingDir / storePath.to_string();
    auto tmpDir = makeTempPath(trackingDir, ".tmp-mark");

    try {
        /* Build the new mark subtree in a private temp directory, then
           atomically rename it into place: this makes a concurrent
           optimiser writing a different (also valid) representative
           file for the same StorePath pick one winner or the other,
           never a two-entry directory with both - the race that an
           in-place remove_all()+create_hard_link() sequence can't rule
           out, since std::filesystem::rename() replacing a directory
           is a single atomic operation. */
        std::filesystem::create_directory(tmpDir);

        auto realPath = config->realStoreDir.get() / storePath.to_string();
        auto representative = markRelPath->empty() ? realPath : realPath / *markRelPath;

        std::filesystem::create_hard_link(representative, tmpDir / encoded);

        try {
            std::filesystem::rename(tmpDir, markRoot);
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() != std::errc::directory_not_empty)
                throw;
            /* markRoot already holds a stale mark (e.g. left by a
               crash, a deleted-and-recreated StorePath, or simply the
               previous valid mark being refreshed) - clear it and
               retry once. If a concurrent writer re-populates markRoot
               in between, this retry can also fail; that's absorbed by
               the catch-all below like any other best-effort race. */
            std::filesystem::remove_all(markRoot);
            std::filesystem::rename(tmpDir, markRoot);
        }
    } catch (std::filesystem::filesystem_error &) {
        /* Best-effort: never abort the run. */
        std::error_code ec;
        std::filesystem::remove_all(tmpDir, ec);
    }
}

bool LocalStore::hasValidOptimiseMark(const StorePath & storePath, const std::filesystem::path & realPath)
{
    auto markRoot = trackingDir / storePath.to_string();

    /* A genuine mark directory has exactly one entry, by construction.
       Any deviation - missing, extra entries, unreadable - is treated
       fail-safe: "unreadable, re-optimise". One opendir/readdir round
       regardless of how deep the encoded relPath's original directory
       chain would have been. */
    std::string onlyEntry;
    size_t count = 0;
    try {
        for (auto & entry : DirectoryIterator{markRoot}) {
            checkInterrupt();
            if (count == 1)
                return false; /* more than one entry: fail-safe */
            onlyEntry = entry.path().filename().string();
            count = 1;
        }
    } catch (SystemError &) {
        return false;
    }
    if (count != 1)
        return false;

    auto relPath = percentDecodeMarkName(onlyEntry);
    if (!relPath)
        return false; /* malformed name: fail-safe */

    auto markLeaf = markRoot / onlyEntry;
    auto st = maybeLstat(markLeaf);
    if (!st)
        return false;

    /* realPath / relPath with relPath empty appends a trailing
       separator, and lstat on that fails ENOTDIR for a regular file -
       maybeLstat then reports "doesn't exist". Must special-case empty
       relPath or single-file StorePaths (.drv files) never verify. */
    auto liveRealPath = relPath->empty() ? realPath : realPath / *relPath;
    auto liveSt = maybeLstat(liveRealPath);

    return liveSt && liveSt->st_ino == st->st_ino && liveSt->st_nlink > 1;
}

void LocalStore::optimiseStore(OptimiseStats & stats)
{
    Activity act(*logger, actOptimiseStore);

    auto paths = queryAllValidPaths();

    /* No longer seeded via loadInodeHash(): hasValidOptimiseMark()
       below skips most StorePaths outright, so that upfront full scan
       of .links would cost more than it saves. Still shared across the
       whole run (not reset per StorePath), so the inodeHash.count()
       fast path in optimisePath_ can fire for an inode inserted while
       processing an earlier StorePath. */
    InodeHash inodeHash;

    act.progress(0, paths.size());

    uint64_t done = 0;

    /* Registering each path as a temp root one at a time means
       acquiring/releasing the GC lock once per path, which shows up as
       real syscall cost at scale (flock/fcntl in the tens of thousands
       for a large store) even though the file walk itself is now
       skipped for marked paths. addTempRoots() already holds the lock
       once per call regardless of how many paths it's given - call it
       with chunks instead of one path at a time. */
    constexpr size_t tempRootBatchSize = 256;
    StorePathSet batch;

    for (auto it = paths.begin(); it != paths.end();) {
        batch.clear();
        for (; it != paths.end() && batch.size() < tempRootBatchSize; ++it)
            batch.insert(*it);
        addTempRoots(batch);

        for (auto & i : batch) {
            if (!isValidPath(i))
                continue; /* path was GC'ed, probably */

            auto realPath = config->realStoreDir.get() / i.to_string();

            if (hasValidOptimiseMark(i, realPath)) {
                done++;
                act.progress(done, paths.size());
                continue;
            }

            std::optional<std::filesystem::path> markRelPath;
            {
                Activity act(*logger, lvlTalkative, actUnknown, fmt("optimising path '%s'", printStorePath(i)));
                optimisePath_(&act, stats, realPath, inodeHash, NoRepair, "", markRelPath);
            }

            writeOptimiseMark(i, markRelPath);

            done++;
            act.progress(done, paths.size());
        }
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
    std::optional<std::filesystem::path> markRelPath;

    if (config->getLocalSettings().autoOptimiseStore)
        optimisePath_(nullptr, stats, path, inodeHash, repair, "", markRelPath);

    StorePath storePath{path.filename().string()};
    writeOptimiseMark(storePath, markRelPath);
}

} // namespace nix
