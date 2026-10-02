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

static void warnCorruptedLink(const std::filesystem::path & path)
{
    warn("removing corrupted link %s", PathFmt(path));
    warn(
        "There may be more corrupted paths."
        "\nYou should run `nix-store --verify --check-contents --repair` to fix them all");
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

    /* Links `path` to `candidatePath` (same content, already verified).
       Returns false only if `candidatePath` itself is full (EMLINK) -
       the caller should try the next overflow slot. Any other outcome
       (success, already-linked, EMLINK on `path`, GC race) returns
       true; the caller should stop. */
    auto tryLinkTo = [&](const std::filesystem::path & candidatePath, const PosixStat & stCandidate) -> bool {
        if (st.st_ino == stCandidate.st_ino) {
            debug("%1% is already linked to %2%", PathFmt(path), PathFmt(candidatePath));
            markRelPath = relPath;
            return true;
        }

        printMsg(lvlTalkative, "linking %1% to %2%", PathFmt(path), PathFmt(candidatePath));

        const auto dirOfPath = path.parent_path();
        bool mustToggle = dirOfPath != config->realStoreDir.get();
        if (mustToggle)
            makeWritable(dirOfPath);
        MakeReadOnly makeReadOnly(mustToggle ? dirOfPath : std::filesystem::path{});

        std::filesystem::path tempLink = makeTempPath(config->realStoreDir.get(), ".tmp-link");

        try {
            std::filesystem::create_hard_link(candidatePath, tempLink);
            inodeHash.insert(st.st_ino);
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() == std::errc::too_many_links) {
                /* This candidate is full - the caller should try a
                   different one (an overflow slot) if it has one. */
                return false;
            }
            if (e.code() == std::errc::no_such_file_or_directory) {
                /* A concurrent garbage collection removed the
                   candidate. Skip optimising this path; a later pass
                   will dedup it. */
                return true;
            }
            throw SystemError(e.code(), "creating hard link from %1% to %2%", PathFmt(candidatePath), PathFmt(tempLink));
        }

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
                /* Some filesystems generate too many links on the
                   rename, rather than on the original link. (Probably
                   it temporarily increases the st_nlink field before
                   decreasing it again.) Same "try another candidate"
                   outcome as the create_hard_link case above. */
                return false;
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
        return true;
    };

    if (experimentalFeatureSettings.isEnabled(Xp::BLAKE3Links)) {
        /* BLAKE3 hashes flat content, so same-byte files with
           different modes would collide - split into mode-keyed shard
           trees (.hardlinks/b3/<r|x|s>/...) instead. Alternative to
           the sha256 path below, not an addition to it: checked first,
           so it skips the NAR-hash pass entirely when enabled. */
        bool isSymlink = S_ISLNK(st.st_mode);
        Hash b3Hash = isSymlink ? hashString(HashAlgorithm::BLAKE3, readLink(path).string())
                                 : hashFile(HashAlgorithm::BLAKE3, path);
        std::string b3HashStr = b3Hash.to_string(HashFormat::Nix32, false);
        debug("%s has BLAKE3 hash '%s'", PathFmt(path), b3Hash.to_string(HashFormat::Nix32, true));

        auto modeDir = b3LinksDir / std::string(linkModeDirName(st.st_mode));
        std::string shard = b3HashStr.substr(0, 3);
        std::filesystem::path primaryPath = modeDir / shard / b3HashStr;

        auto hashCandidate = [&](const std::filesystem::path & candidatePath, mode_t candidateMode) {
            return S_ISLNK(candidateMode) ? hashString(HashAlgorithm::BLAKE3, readLink(candidatePath).string())
                                           : hashFile(HashAlgorithm::BLAKE3, candidatePath);
        };

        for (int seq = 0; seq < 1000; ++seq) {
            std::filesystem::path candidatePath =
                seq == 0 ? primaryPath : modeDir / "overflow" / (b3HashStr + fmt(".%03d", seq));

            if (!pathExists(candidatePath)) {
                try {
                    std::filesystem::create_hard_link(path, candidatePath);
                    inodeHash.insert(st.st_ino);
                } catch (std::filesystem::filesystem_error & e) {
                    if (e.code() == std::errc::file_exists) {
                        /* Lost a race with a concurrent optimiser; fall
                           through to inspect what's there now. */
                    } else if (e.code() == std::errc::too_many_links) {
                        if (st.st_size)
                            printInfo("%1% has maximum number of links", PathFmt(path));
                        return;
                    } else if (e.code() == std::errc::no_space_on_device) {
                        printInfo(
                            "cannot link %s to '%s': %s", PathFmt(candidatePath), PathFmt(path), e.code().message());
                        return;
                    } else {
                        throw SystemError(
                            e.code(), "creating hard link from %1% to %2%", PathFmt(candidatePath), PathFmt(path));
                    }
                }
            }

            auto stCandidate = maybeLstat(candidatePath);
            if (!stCandidate)
                continue; /* Concurrently GC'd; try the next slot. */

            /* Same corruption check as the sha256 path below, plus a
               mode check (a collision landing in the wrong mode tree
               would otherwise be silently accepted). */
            if (st.st_size != stCandidate->st_size
                || (st.st_mode & linkModeMask) != (stCandidate->st_mode & linkModeMask)
                || (repair && b3Hash != hashCandidate(candidatePath, stCandidate->st_mode))) {
                warnCorruptedLink(candidatePath);
                unlinkIfExists(candidatePath);
                continue;
            }

            if (tryLinkTo(candidatePath, *stCandidate))
                return;
            /* candidatePath itself turned out to be full; try the next
               overflow slot. */
        }

        printInfo("%1% exceeded maximum overflow replicas (999)", PathFmt(primaryPath));
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
    debug("%s has hash '%s'", PathFmt(path), hash.to_string(HashFormat::Nix32, true));

    /* Check the old flat .links/<hash> first - still valid forever,
       but never written to again; new links go to the sharded farm
       below (.hardlinks/sha256/<prefix>/<hash>, with numbered overflow
       replicas once a shard's hardlink ceiling is hit). */
    std::filesystem::path linkPath = linksDir / hashStr;
    auto stLinkAtCheck = maybeLstat(linkPath);
    bool foundInFlatLinks = stLinkAtCheck.has_value();

    if (foundInFlatLinks
        && (st.st_size != stLinkAtCheck->st_size || (repair && hash != ({
                                                                    hashPath(
                                                                        makeFSSourceAccessor(linkPath),
                                                                        FileSerialisationMethod::NixArchive,
                                                                        HashAlgorithm::SHA256)
                                                                        .hash;
                                                                })))) {
        // XXX: Consider overwriting linkPath with our valid version.
        warnCorruptedLink(linkPath);
        unlinkIfExists(linkPath);
        foundInFlatLinks = false;
    }

    if (foundInFlatLinks) {
        /* Re-stat rather than reusing stLinkAtCheck: the repair-mode
           hash check above can take a while, during which a
           concurrent GC could have removed linkPath. */
        auto stLink = maybeLstat(linkPath);
        if (!stLink)
            return; /* Concurrent GC race; a later pass will dedup it. */
        if (tryLinkTo(linkPath, *stLink))
            return;
        /* linkPath itself is full (EMLINK) - fall through to the
           sharded farm below instead of giving up, same as the
           sharded farm's own overflow-slot retry. */
        foundInFlatLinks = false;
    }

    {
        /* Not in the flat layout (or just removed above as corrupted)
           - use the sharded farm. Shard prefix is the hash's first 3
           Nix32 characters (2048 shards); once a shard's primary
           replica hits the hardlink ceiling, fall through to numbered
           overflow replicas (.hardlinks/sha256/overflow/<hash>.NNN, up
           to 999) rather than give up on this file. */
        std::string shard = hashStr.substr(0, 3);
        std::filesystem::path primaryPath = shardedLinksDir / shard / hashStr;

        for (int seq = 0; seq < 1000; ++seq) {
            std::filesystem::path candidatePath =
                seq == 0 ? primaryPath : shardedLinksOverflowDir / (hashStr + fmt(".%03d", seq));

            if (!pathExists(candidatePath)) {
                /* Nope, create a hard link at this candidate slot. */
                try {
                    std::filesystem::create_hard_link(path, candidatePath);
                    inodeHash.insert(st.st_ino);
                } catch (std::filesystem::filesystem_error & e) {
                    if (e.code() == std::errc::file_exists) {
                        /* Lost a race with a concurrent optimiser; fall
                           through to inspect what's there now. */
                    } else if (e.code() == std::errc::too_many_links) {
                        /* `path` itself already has the maximum number
                           of links - unrelated to which slot we're
                           trying, no point trying another. */
                        if (st.st_size)
                            printInfo("%1% has maximum number of links", PathFmt(path));
                        return;
                    } else if (e.code() == std::errc::no_space_on_device) {
                        printInfo(
                            "cannot link %s to '%s': %s", PathFmt(candidatePath), PathFmt(path), e.code().message());
                        return;
                    } else {
                        throw SystemError(
                            e.code(), "creating hard link from %1% to %2%", PathFmt(candidatePath), PathFmt(path));
                    }
                }
            }

            auto stCandidate = maybeLstat(candidatePath);
            if (!stCandidate)
                continue; /* Concurrently GC'd; try the next slot. */

            /* Size mismatch is corruption regardless of repair mode
               (mirrors the flat .links check above); the full hash
               check is gated on repair since it's expensive. */
            if (st.st_size != stCandidate->st_size
                || (repair
                    && hash
                        != hashPath(
                               makeFSSourceAccessor(candidatePath),
                               FileSerialisationMethod::NixArchive,
                               HashAlgorithm::SHA256)
                               .hash)) {
                warnCorruptedLink(candidatePath);
                unlinkIfExists(candidatePath);
                continue; /* Recreate this slot on the next pass. */
            }

            if (tryLinkTo(candidatePath, *stCandidate))
                return;
            /* candidatePath itself turned out to be full; try the next
               overflow slot. */
        }

        printInfo("%1% exceeded maximum overflow replicas (999)", PathFmt(primaryPath));
        return;
    }
}

/* Sentinel for the empty-relPath case (single-file StorePaths, e.g.
   .drv files, whose representative file *is* the StorePath itself). An
   empty string can't be a filename, so this needs an explicit marker.
   Safe: percentEncodeMarkName never emits a bare '%' alone, only as
   the first byte of a full "%XX" triple. */
static const std::string emptyRelPathMarkName = "%";

/* Encode relPath into a single filename under trackingDir/<StorePath>/:
   '/' can't appear in a filename, so escape it (and '%', to keep the
   encoding unambiguous). Must not be called with an empty relPath -
   use emptyRelPathMarkName instead. */
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
    if (!supportsOptimiseMarks() || !markRelPath)
        return;

    /* Encoding relPath into one filename makes a mark a fixed-depth
       lookup regardless of how deep the representative file sits, at
       the cost of capping it at NAME_MAX - if it doesn't fit, skip
       writing a mark this round (fail-open). */
    std::string encoded = markRelPath->empty() ? emptyRelPathMarkName : percentEncodeMarkName(*markRelPath);
    if (encoded.size() > NAME_MAX)
        return;

    auto markRoot = trackingDir / storePath.to_string();
    auto tmpDir = makeTempPath(trackingDir, ".tmp-mark");

    try {
        /* Build the subtree in a private temp dir, then rename it into
           place atomically: a concurrent optimiser writing a different
           representative file for the same StorePath picks one winner,
           never a two-entry directory - rename() replacing a directory
           is atomic, unlike remove_all()+create_hard_link(). */
        std::filesystem::create_directory(tmpDir);

        auto realPath = config->realStoreDir.get() / storePath.to_string();
        auto representative = markRelPath->empty() ? realPath : realPath / *markRelPath;

        std::filesystem::create_hard_link(representative, tmpDir / encoded);

        try {
            std::filesystem::rename(tmpDir, markRoot);
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() != std::errc::directory_not_empty)
                throw;
            /* markRoot already holds a stale mark (crash, deleted-and-
               recreated StorePath, or a refresh) - clear and retry
               once. A concurrent re-populate can still lose this race;
               absorbed by the catch-all below. */
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

    /* A genuine mark directory has exactly one entry. Any deviation -
       missing, extra entries, unreadable - is fail-safe: re-optimise. */
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

    /* hasValidOptimiseMark() below skips most StorePaths outright, so
       there's no upfront seeding of inodeHash - it's still shared
       across the whole run, so the fast path in optimisePath_ can
       still fire for an inode inserted while processing an earlier
       StorePath. */
    InodeHash inodeHash;

    act.progress(0, paths.size());

    uint64_t done = 0;

    /* addTempRoots() already batches the GC-lock acquisition - call it
       with chunks instead of one path at a time, which matters at
       scale even once the file walk itself is skipped for marked
       paths. */
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
