#include "nix/util/environment-variables.hh"
#include "nix/util/file-system.hh"
#include "nix/util/file-path.hh"
#include "nix/util/file-path-impl.hh"
#include "nix/util/signals.hh"
#include "nix/util/finally.hh"
#include "nix/util/serialise.hh"
#include "nix/util/util.hh"

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <sstream>
#include <filesystem>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>

#include <boost/iostreams/device/mapped_file.hpp>

#ifdef _WIN32
# include <io.h>
#endif

#include "nix/util/strings-inline.hh"

#include "util-config-private.hh"

namespace nix {

DirectoryIterator::DirectoryIterator(const std::filesystem::path& p) {
    try {
        // **Attempt to create the underlying directory_iterator**
        it_ = std::filesystem::directory_iterator(p);
    } catch (const std::filesystem::filesystem_error& e) {
        // **Catch filesystem_error and throw SysError**
        // Adapt the error message as needed for SysError
        throw SysError("cannot read directory %s", p);
    }
}

DirectoryIterator& DirectoryIterator::operator++() {
    // **Attempt to increment the underlying iterator**
    std::error_code ec;
    it_.increment(ec);
    if (ec) {
        // Try to get path info if possible, might fail if iterator is bad
        try {
            if (it_ != std::filesystem::directory_iterator{}) {
                throw SysError("cannot read directory past %s: %s", it_->path(), ec.message());
            }
        } catch (...) {
            throw SysError("cannot read directory");
        }
    }
    return *this;
}

bool isAbsolute(PathView path)
{
    return std::filesystem::path { path }.is_absolute();
}


Path absPath(PathView path, std::optional<PathView> dir, bool resolveSymlinks)
{
    std::string scratch;

    if (!isAbsolute(path)) {
        // In this case we need to call `canonPath` on a newly-created
        // string. We set `scratch` to that string first, and then set
        // `path` to `scratch`. This ensures the newly-created string
        // lives long enough for the call to `canonPath`, and allows us
        // to just accept a `std::string_view`.
        if (!dir) {
#ifdef __GNU__
            /* GNU (aka. GNU/Hurd) doesn't have any limitation on path
               lengths and doesn't define `PATH_MAX'.  */
            char *buf = getcwd(NULL, 0);
            if (buf == NULL)
#else
            char buf[PATH_MAX];
            if (!getcwd(buf, sizeof(buf)))
#endif
                throw SysError("cannot get cwd");
            scratch = concatStrings(buf, "/", path);
#ifdef __GNU__
            free(buf);
#endif
        } else
            scratch = concatStrings(*dir, "/", path);
        path = scratch;
    }
    return canonPath(path, resolveSymlinks);
}

std::filesystem::path absPath(const std::filesystem::path & path, bool resolveSymlinks)
{
    return absPath(path.string(), std::nullopt, resolveSymlinks);
}

Path canonPath(PathView path, bool resolveSymlinks)
{
    assert(path != "");

    if (!isAbsolute(path))
        throw Error("not an absolute path: '%1%'", path);

    // For Windows
    auto rootName = std::filesystem::path { path }.root_name();

    /* This just exists because we cannot set the target of `remaining`
       (the callback parameter) directly to a newly-constructed string,
       since it is `std::string_view`. */
    std::string temp;

    /* Count the number of times we follow a symlink and stop at some
       arbitrary (but high) limit to prevent infinite loops. */
    unsigned int followCount = 0, maxFollow = 1024;

    auto ret = canonPathInner<OsPathTrait<char>>(
        path,
        [&followCount, &temp, maxFollow, resolveSymlinks]
        (std::string & result, std::string_view & remaining) {
            if (resolveSymlinks && std::filesystem::is_symlink(result)) {
                if (++followCount >= maxFollow)
                    throw Error("infinite symlink recursion in path '%1%'", remaining);
                remaining = (temp = concatStrings(readLink(result), remaining));
                if (isAbsolute(remaining)) {
                    /* restart for symlinks pointing to absolute path */
                    result.clear();
                } else {
                    result = dirOf(result);
                    if (result == "/") {
                        /* we don’t want trailing slashes here, which `dirOf`
                           only produces if `result = /` */
                        result.clear();
                    }
                }
            }
        });

    if (!rootName.empty())
        ret = rootName.string() + std::move(ret);
    return ret;
}


Path dirOf(const PathView path)
{
    Path::size_type pos = OsPathTrait<char>::rfindPathSep(path);
    if (pos == path.npos)
        return ".";
    return std::filesystem::path{path}.parent_path().string();
}


std::string_view baseNameOf(std::string_view path)
{
    if (path.empty())
        return "";

    auto last = path.size() - 1;
    while (last > 0 && OsPathTrait<char>::isPathSep(path[last]))
        last -= 1;

    auto pos = OsPathTrait<char>::rfindPathSep(path, last);
    if (pos == path.npos)
        pos = 0;
    else
        pos += 1;

    return path.substr(pos, last - pos + 1);
}


bool isInDir(const std::filesystem::path & path, const std::filesystem::path & dir)
{
    /* Note that while the standard doesn't guarantee this, the
      `lexically_*` functions should do no IO and not throw. */
    auto rel = path.lexically_relative(dir);
    /* Method from
       https://stackoverflow.com/questions/62503197/check-if-path-contains-another-in-c++ */
    return !rel.empty() && rel.native()[0] != OS_STR('.');
}


bool isDirOrInDir(const std::filesystem::path & path, const std::filesystem::path & dir)
{
    return path == dir || isInDir(path, dir);
}


struct stat stat(const Path & path)
{
    struct stat st;
    if (stat(path.c_str(), &st))
        throw SysError("getting status of '%1%'", path);
    return st;
}

#ifdef _WIN32
# define STAT stat
#else
# define STAT lstat
#endif

struct stat lstat(const Path & path)
{
    struct stat st;
    if (STAT(path.c_str(), &st))
        throw SysError("getting status of '%1%'", path);
    return st;
}


std::optional<struct stat> maybeLstat(const Path & path)
{
    std::optional<struct stat> st{std::in_place};
    if (STAT(path.c_str(), &*st))
    {
        if (errno == ENOENT || errno == ENOTDIR)
            st.reset();
        else
            throw SysError("getting status of '%s'", path);
    }
    return st;
}


bool pathExists(const std::filesystem::path & path)
{
    return maybeLstat(path.string()).has_value();
}

bool pathAccessible(const std::filesystem::path & path)
{
    try {
        return pathExists(path.string());
    } catch (SysError & e) {
        // swallow EPERM
        if (e.errNo == EPERM) return false;
        throw;
    }
}


Path readLink(const Path & path)
{
    checkInterrupt();
    return std::filesystem::read_symlink(path).string();
}


std::string readFile(const Path & path)
{
    AutoCloseFD fd = toDescriptor(open(path.c_str(), O_RDONLY
// TODO
#ifndef _WIN32
       | O_CLOEXEC
#endif
       ));
    if (!fd)
        throw SysError("opening file '%1%'", path);
    return readFile(fd.get());
}

std::string readFile(const std::filesystem::path & path)
{
    return readFile(os_string_to_string(PathViewNG { path }));
}

void readFile(const Path & path, Sink & sink, bool memory_map)
{
    // Memory-map the file for faster processing where possible.
    if (memory_map) {
        try {
            boost::iostreams::mapped_file_source mmap(path);
            if (mmap.is_open()) {
                sink({mmap.data(), mmap.size()});
                return;
            }
        } catch (const boost::exception & e) {
        }
        debug("memory-mapping failed for path: %s", path);
    }

    // Stream the file instead if memory-mapping fails or is disabled.
    AutoCloseFD fd = toDescriptor(open(path.c_str(), O_RDONLY
// TODO
#ifndef _WIN32
       | O_CLOEXEC
#endif
       ));
    if (!fd)
        throw SysError("opening file '%s'", path);
    drainFD(fd.get(), sink);
}


void writeFile(const Path & path, std::string_view s, mode_t mode, bool sync)
{
    AutoCloseFD fd = toDescriptor(open(path.c_str(), O_WRONLY | O_TRUNC | O_CREAT
// TODO
#ifndef _WIN32
       | O_CLOEXEC
#endif
       , mode));
    if (!fd)
        throw SysError("opening file '%1%'", path);
    try {
        writeFull(fd.get(), s);
    } catch (Error & e) {
        e.addTrace({}, "writing file '%1%'", path);
        throw;
    }
    if (sync)
        fd.fsync();
    // Explicitly close to make sure exceptions are propagated.
    fd.close();
    if (sync)
        syncParent(path);
}


void writeFile(const Path & path, Source & source, mode_t mode, bool sync)
{
    AutoCloseFD fd = toDescriptor(open(path.c_str(), O_WRONLY | O_TRUNC | O_CREAT
// TODO
#ifndef _WIN32
       | O_CLOEXEC
#endif
       , mode));
    if (!fd)
        throw SysError("opening file '%1%'", path);

    std::array<char, 64 * 1024> buf;

    try {
        while (true) {
            try {
                auto n = source.read(buf.data(), buf.size());
                writeFull(fd.get(), {buf.data(), n});
            } catch (EndOfFile &) { break; }
        }
    } catch (Error & e) {
        e.addTrace({}, "writing file '%1%'", path);
        throw;
    }
    if (sync)
        fd.fsync();
    // Explicitly close to make sure exceptions are propagated.
    fd.close();
    if (sync)
        syncParent(path);
}

void syncParent(const Path & path)
{
    AutoCloseFD fd = toDescriptor(open(dirOf(path).c_str(), O_RDONLY, 0));
    if (!fd)
        throw SysError("opening file '%1%'", path);
    fd.fsync();
}


void recursiveSync(const Path & path)
{
    /* If it's a file or symlink, just fsync and return. */
    auto st = lstat(path);
    if (S_ISREG(st.st_mode)) {
        AutoCloseFD fd = toDescriptor(open(path.c_str(), O_RDONLY, 0));
        if (!fd)
            throw SysError("opening file '%1%'", path);
        fd.fsync();
        return;
    } else if (S_ISLNK(st.st_mode))
        return;

    /* Otherwise, perform a depth-first traversal of the directory and
       fsync all the files. */
    std::deque<std::filesystem::path> dirsToEnumerate;
    dirsToEnumerate.push_back(path);
    std::vector<std::filesystem::path> dirsToFsync;
    while (!dirsToEnumerate.empty()) {
        auto currentDir = dirsToEnumerate.back();
        dirsToEnumerate.pop_back();
        for (auto & entry : DirectoryIterator(currentDir)) {
            auto st = entry.symlink_status();
            if (std::filesystem::is_directory(st)) {
                dirsToEnumerate.emplace_back(entry.path());
            } else if (std::filesystem::is_regular_file(st)) {
                AutoCloseFD fd = toDescriptor(open(entry.path().string().c_str(), O_RDONLY, 0));
                if (!fd)
                    throw SysError("opening file '%1%'", entry.path());
                fd.fsync();
            }
        }
        dirsToFsync.emplace_back(std::move(currentDir));
    }

    /* Fsync all the directories. */
    for (auto dir = dirsToFsync.rbegin(); dir != dirsToFsync.rend(); ++dir) {
        AutoCloseFD fd = toDescriptor(open(dir->string().c_str(), O_RDONLY, 0));
        if (!fd)
            throw SysError("opening directory '%1%'", *dir);
        fd.fsync();
    }
}


static void _deletePath(Descriptor parentfd, const std::filesystem::path & path, uint64_t & bytesFreed)
{
#ifndef _WIN32
    checkInterrupt();

    std::string name(baseNameOf(path.native()));

    struct stat st;
    if (fstatat(parentfd, name.c_str(), &st,
            AT_SYMLINK_NOFOLLOW) == -1) {
        if (errno == ENOENT) return;
        throw SysError("getting status of %1%", path);
    }

    if (!S_ISDIR(st.st_mode)) {
        /* We are about to delete a file. Will it likely free space? */

        switch (st.st_nlink) {
            /* Yes: last link. */
            case 1:
                bytesFreed += st.st_size;
                break;
            /* Maybe: yes, if 'auto-optimise-store' or manual optimisation
               was performed. Instead of checking for real let's assume
               it's an optimised file and space will be freed.

               In worst case we will double count on freed space for files
               with exactly two hardlinks for unoptimised packages.
             */
            case 2:
                bytesFreed += st.st_size;
                break;
            /* No: 3+ links. */
            default:
                break;
        }
    }

    if (S_ISDIR(st.st_mode)) {
        /* Make the directory accessible. */
        const auto PERM_MASK = S_IRUSR | S_IWUSR | S_IXUSR;
        if ((st.st_mode & PERM_MASK) != PERM_MASK) {
            if (fchmodat(parentfd, name.c_str(), st.st_mode | PERM_MASK, 0) == -1)
                throw SysError("chmod %1%", path);
        }

        int fd = openat(parentfd, path.c_str(), O_RDONLY);
        if (fd == -1)
            throw SysError("opening directory %1%", path);
        AutoCloseDir dir(fdopendir(fd));
        if (!dir)
            throw SysError("opening directory %1%", path);

        struct dirent * dirent;
        while (errno = 0, dirent = readdir(dir.get())) { /* sic */
            checkInterrupt();
            std::string childName = dirent->d_name;
            if (childName == "." || childName == "..") continue;
            _deletePath(dirfd(dir.get()), path + "/" + childName, bytesFreed);
        }
        if (errno) throw SysError("reading directory %1%", path);
    }

    int flags = S_ISDIR(st.st_mode) ? AT_REMOVEDIR : 0;
    if (unlinkat(parentfd, name.c_str(), flags) == -1) {
        if (errno == ENOENT) return;
        throw SysError("cannot unlink %1%", path);
    }
#else
    // TODO implement
    throw UnimplementedError("_deletePath");
#endif
}

static void _deletePath(const std::filesystem::path & path, uint64_t & bytesFreed)
{
    Path dir = dirOf(path.string());
    if (dir == "")
        dir = "/";

    AutoCloseFD dirfd = toDescriptor(open(dir.c_str(), O_RDONLY));
    if (!dirfd) {
        if (errno == ENOENT) return;
        throw SysError("opening directory '%1%'", path);
    }

    _deletePath(dirfd.get(), path, bytesFreed);
}


void deletePath(const std::filesystem::path & path)
{
    uint64_t dummy;
    deletePath(path, dummy);
}

void createDir(const Path & path, mode_t mode)
{
    if (mkdir(path.c_str()
#ifndef _WIN32
                , mode
#endif
                ) == -1)
        throw SysError("creating directory '%1%'", path);
}

void createDirs(const std::filesystem::path & path)
{
    try {
       std::filesystem::create_directories(path);
    } catch (std::filesystem::filesystem_error & e) {
        throw SysError("creating directory '%1%'", path.string());
    }
}


void deletePath(const std::filesystem::path & path, uint64_t & bytesFreed)
{
    //Activity act(*logger, lvlDebug, "recursively deleting path '%1%'", path);
    bytesFreed = 0;
    _deletePath(path, bytesFreed);
}


//////////////////////////////////////////////////////////////////////

AutoDelete::AutoDelete() : del{false} {}

AutoDelete::AutoDelete(const std::filesystem::path & p, bool recursive) : _path(p)
{
    del = true;
    this->recursive = recursive;
}

AutoDelete::~AutoDelete()
{
    try {
        if (del) {
            if (recursive)
                deletePath(_path);
            else {
                std::filesystem::remove(_path);
            }
        }
    } catch (...) {
        ignoreExceptionInDestructor();
    }
}

void AutoDelete::cancel()
{
    del = false;
}

void AutoDelete::reset(const std::filesystem::path & p, bool recursive) {
    _path = p;
    this->recursive = recursive;
    del = true;
}

//////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////

std::string defaultTempDir() {
    return getEnvNonEmpty("TMPDIR").value_or("/tmp");
}

static Path tempName(Path tmpRoot, const Path & prefix, bool includePid,
    std::atomic<unsigned int> & counter)
{
    tmpRoot = canonPath(tmpRoot.empty() ? defaultTempDir() : tmpRoot, true);
    if (includePid)
        return fmt("%1%/%2%-%3%-%4%", tmpRoot, prefix, getpid(), counter++);
    else
        return fmt("%1%/%2%-%3%", tmpRoot, prefix, counter++);
}

Path createTempDir(const Path & tmpRoot, const Path & prefix,
    bool includePid, bool useGlobalCounter, mode_t mode)
{
    static std::atomic<unsigned int> globalCounter = 0;
    std::atomic<unsigned int> localCounter = 0;
    auto & counter(useGlobalCounter ? globalCounter : localCounter);

    while (1) {
        checkInterrupt();
        Path tmpDir = tempName(tmpRoot, prefix, includePid, counter);
        if (mkdir(tmpDir.c_str()
#ifndef _WIN32 // TODO abstract mkdir perms for Windows
                    , mode
#endif
                    ) == 0) {
#ifdef __FreeBSD__
            /* Explicitly set the group of the directory.  This is to
               work around around problems caused by BSD's group
               ownership semantics (directories inherit the group of
               the parent).  For instance, the group of /tmp on
               FreeBSD is "wheel", so all directories created in /tmp
               will be owned by "wheel"; but if the user is not in
               "wheel", then "tar" will fail to unpack archives that
               have the setgid bit set on directories. */
            if (chown(tmpDir.c_str(), (uid_t) -1, getegid()) != 0)
                throw SysError("setting group of directory '%1%'", tmpDir);
#endif
            return tmpDir;
        }
        if (errno != EEXIST)
            throw SysError("creating directory '%1%'", tmpDir);
    }
}


std::pair<AutoCloseFD, Path> createTempFile(const Path & prefix)
{
    Path tmpl(defaultTempDir() + "/" + prefix + ".XXXXXX");
    // Strictly speaking, this is UB, but who cares...
    // FIXME: use O_TMPFILE.
    AutoCloseFD fd = toDescriptor(mkstemp((char *) tmpl.c_str()));
    if (!fd)
        throw SysError("creating temporary file '%s'", tmpl);
#ifndef _WIN32
    unix::closeOnExec(fd.get());
#endif
    return {std::move(fd), tmpl};
}

void createSymlink(const Path & target, const Path & link)
{
    try {
        std::filesystem::create_symlink(target, link);
    } catch (std::filesystem::filesystem_error & e) {
        throw SysError("creating symlink '%1%' -> '%2%'", link, target);
    }
}

void replaceSymlink(const std::filesystem::path & target, const std::filesystem::path & link)
{
    for (unsigned int n = 0; true; n++) {
        auto tmp = link.parent_path() /std::filesystem::path{fmt(".%d_%s", n, link.filename().string())};
        tmp = tmp.lexically_normal();

        try {
            std::filesystem::create_symlink(target, tmp);
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() == std::errc::file_exists) continue;
            throw SysError("creating symlink %1% -> %2%", tmp, target);
        }

        try {
            std::filesystem::rename(tmp, link);
        } catch (std::filesystem::filesystem_error & e) {
            if (e.code() == std::errc::file_exists) continue;
            throw SysError("renaming %1% to %2%", tmp, link);
        }


        break;
    }
}

void setWriteTime(const std::filesystem::path & path, const struct stat & st)
{
    setWriteTime(path, st.st_atime, st.st_mtime, S_ISLNK(st.st_mode));
}

void copyFile(const std::filesystem::path & from, const std::filesystem::path & to, bool andDelete)
{
    auto fromStatus =std::filesystem::symlink_status(from);

    // Mark the directory as writable so that we can delete its children
    if (andDelete &&std::filesystem::is_directory(fromStatus)) {
        std::filesystem::permissions(from, std::filesystem::perms::owner_write, std::filesystem::perm_options::add | std::filesystem::perm_options::nofollow);
    }

    if (std::filesystem::is_symlink(fromStatus) ||std::filesystem::is_regular_file(fromStatus)) {
        std::filesystem::copy(from, to, std::filesystem::copy_options::copy_symlinks | std::filesystem::copy_options::overwrite_existing);
    } else if (std::filesystem::is_directory(fromStatus)) {
        std::filesystem::create_directory(to);
        for (auto & entry : DirectoryIterator(from)) {
            copyFile(entry, to / entry.path().filename(), andDelete);
        }
    } else {
        throw Error("file %s has an unsupported type", from);
    }

    setWriteTime(to, lstat(from.string().c_str()));
    if (andDelete) {
        if (!std::filesystem::is_symlink(fromStatus))
            std::filesystem::permissions(from, std::filesystem::perms::owner_write, std::filesystem::perm_options::add | std::filesystem::perm_options::nofollow);
        std::filesystem::remove(from);
    }
}

void moveFile(const Path & oldName, const Path & newName)
{
    try {
        std::filesystem::rename(oldName, newName);
    } catch (std::filesystem::filesystem_error & e) {
        auto oldPath = std::filesystem::path(oldName);
        auto newPath = std::filesystem::path(newName);
        // For the move to be as atomic as possible, copy to a temporary
        // directory
        std::filesystem::path temp = createTempDir(
            os_string_to_string(PathViewNG { newPath.parent_path() }),
            "rename-tmp");
        Finally removeTemp = [&]() { std::filesystem::remove(temp); };
        auto tempCopyTarget = temp / "copy-target";
        if (e.code().value() == EXDEV) {
            std::filesystem::remove(newPath);
            warn("can’t rename %s as %s, copying instead", oldName, newName);
            copyFile(oldPath, tempCopyTarget, true);
            std::filesystem::rename(
                os_string_to_string(PathViewNG { tempCopyTarget }),
                os_string_to_string(PathViewNG { newPath }));
        }
    }
}

//////////////////////////////////////////////////////////////////////

bool isExecutableFileAmbient(const std::filesystem::path & exe) {
    // Check file type, because directory being executable means
    // something completely different.
    // `is_regular_file` follows symlinks before checking.
    return std::filesystem::is_regular_file(exe)
        && access(exe.string().c_str(),
#ifdef WIN32
        0 // TODO do better
#else
        X_OK
#endif
        ) == 0;
}

std::filesystem::path makeParentCanonical(const std::filesystem::path & rawPath)
{
    std::filesystem::path path(absPath(rawPath));;
    try {
        auto parent = path.parent_path();
        if (parent == path) {
            // `path` is a root directory => trivially canonical
            return parent;
        }
        return std::filesystem::canonical(parent) / path.filename();
    } catch (std::filesystem::filesystem_error & e) {
        throw SysError("canonicalising parent path of '%1%'", path);
    }
}

bool chmodIfNeeded(const std::filesystem::path & path, mode_t mode, mode_t mask)
{
    auto pathString = path.string();
    auto prevMode = lstat(pathString).st_mode;

    if (((prevMode ^ mode) & mask) == 0)
        return false;

    if (chmod(pathString.c_str(), mode) != 0)
        throw SysError("could not set permissions on '%s' to %o", pathString, mode);

    return true;
}

} // namespace nix
