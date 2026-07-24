#include "nix/store/gc-store.hh"
#include "nix/store/local-gc.hh"
#include "nix/store/local-settings.hh"
#include "nix/store/local-store.hh"
#include "nix/store/path.hh"
#include "nix/util/configuration.hh"
#include "nix/util/environment-variables.hh"
#include "nix/util/finally.hh"
#include "nix/util/unix-domain-socket.hh"
#include "nix/util/signals.hh"
#include "nix/util/serialise.hh"
#include "nix/util/util.hh"
#include "nix/util/file-system.hh"
#include "nix/store/posix-fs-canonicalise.hh"

#include "store-config-private.hh"

#include <sqlite3.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/regex.hpp>
#include <queue>
#include <thread>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <variant>
#if HAVE_STATVFS
#  include <sys/statvfs.h>
#endif
#ifndef _WIN32
#  include <poll.h>
#  include <sys/socket.h>
#  include <sys/un.h>
#endif
#include <sys/types.h>
#include <unistd.h>

namespace nix {

static std::string gcSocketPath = "gc-socket/socket";
static std::string gcRootsDir = "gcroots";

void LocalStore::addIndirectRoot(const std::filesystem::path & path)
{
    std::string hash = hashString(HashAlgorithm::SHA1, path.string()).to_string(HashFormat::Nix32, false);
    auto realRoot = canonPath(config->stateDir.get() / gcRootsDir / "auto" / hash);
    makeSymlink(realRoot, path);
}

void LocalStore::createTempRootsFile()
{
    auto fdTempRoots(_fdTempRoots.lock());

    /* Create the temporary roots file for this process. */
    if (*fdTempRoots)
        return;

    while (1) {
        if (pathExists(fnTempRoots))
            /* It *must* be stale, since there can be no two
               processes with the same pid. */
            tryUnlink(fnTempRoots);

        *fdTempRoots = openLockFile(fnTempRoots, true);

        debug("acquiring write lock on %s", PathFmt(fnTempRoots));
        lockFile(fdTempRoots->get(), ltWrite, true);

        /* Check whether the garbage collector didn't get in our
           way. */
        if (getFileSize(fdTempRoots->get()) == 0)
            break;

        /* The garbage collector deleted this file before we could get
           a lock.  (It won't delete the file after we get a lock.)
           Try again. */
    }
}

void LocalStore::addTempRoot(const StorePath & path)
{
    if (config->readOnly) {
        debug(
            "Read-only store doesn't support creating lock files for temp roots, but nothing can be deleted anyways.");
        return;
    }

    createTempRootsFile();

    /* Open/create the global GC lock file. */
    {
        auto fdGCLock(_fdGCLock.lock());
        if (!*fdGCLock)
            *fdGCLock = openGCLock();
    }

restart:
    /* Try to acquire a shared global GC lock (non-blocking). This
       only succeeds if the garbage collector is not currently
       running. */
    FdLock gcLock(_fdGCLock.lock()->get(), ltRead, false, "");

    if (!gcLock.acquired) {
        /* We couldn't get a shared global GC lock, so the garbage
           collector is running. So we have to connect to the garbage
           collector and inform it about our root. */
        auto fdRootsSocket(_fdRootsSocket.lock());

        if (!*fdRootsSocket) {
            auto socketPath = config->stateDir.get() / gcSocketPath;
            debug("connecting to '%s'", PathFmt(socketPath));
            *fdRootsSocket = createUnixDomainSocket();
            try {
                nix::connect(toSocket(fdRootsSocket->get()), socketPath);
            } catch (SystemError & e) {
                /* The garbage collector may have exited or not
                   created the socket yet, so we need to restart. */
                if (e.is(std::errc::connection_refused) || e.is(std::errc::no_such_file_or_directory)) {
                    debug("GC socket connection refused: %s", e.msg());
                    fdRootsSocket->close();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    goto restart;
                }
                throw;
            }
        }

        try {
            debug("sending GC root '%s'", printStorePath(path));
            writeFull(fdRootsSocket->get(), printStorePath(path) + "\n", false);
            char c;
            readFull(fdRootsSocket->get(), &c, 1);
            assert(c == '1');
            debug("got ack for GC root '%s'", printStorePath(path));
        } catch (SystemError & e) {
            /* The garbage collector may have exited, so we need to
               restart. */
            if (e.is(std::errc::broken_pipe) || e.is(std::errc::connection_reset)) {
                debug("GC socket disconnected");
                fdRootsSocket->close();
                goto restart;
            }
            throw;
        } catch (EndOfFile & e) {
            debug("GC socket disconnected");
            fdRootsSocket->close();
            goto restart;
        }
    }

    /* Record the store path in the temporary roots file so it will be
       seen by a future run of the garbage collector. */
    auto s = printStorePath(path) + '\0';
    writeFull(_fdTempRoots.lock()->get(), s);
}

static std::string censored = "{censored}";

void LocalStore::findTempRoots(Roots & tempRoots, bool censor)
{
    /* Read the `temproots' directory for per-process temporary root
       files. */
    for (auto & i : DirectoryIterator{tempRootsDir}) {
        checkInterrupt();
        auto name = i.path().filename().string();
        if (name[0] == '.') {
            // Ignore hidden files. Some package managers (notably portage) create
            // those to keep the directory alive.
            continue;
        }
        auto path = i.path();

        pid_t pid = std::stoi(name);

        debug("reading temporary root file %1%", PathFmt(path));
        AutoCloseFD fd(toDescriptor(open(
            path.string().c_str(),
#ifndef _WIN32
            O_CLOEXEC |
#endif
                O_RDWR,
            0666)));
        if (!fd) {
            /* It's okay if the file has disappeared. */
            if (errno == ENOENT)
                continue;
            throw SysError("opening temporary roots file %1%", PathFmt(path));
        }

        /* Try to acquire a write lock without blocking.  This can
           only succeed if the owning process has died.  In that case
           we don't care about its temporary roots. */
        if (lockFile(fd.get(), ltWrite, false)) {
            printInfo("removing stale temporary roots file %1%", PathFmt(path));
            tryUnlink(path);
            writeFull(fd.get(), "d");
            continue;
        }

        /* Read the entire file. */
        auto contents = readFile(fd.get());

        /* Extract the roots. */
        std::string::size_type pos = 0, end;

        while ((end = contents.find((char) 0, pos)) != std::string::npos) {
            auto root = std::string_view(contents).substr(pos, end - pos);
            debug("got temporary root '%s'", root);
            tempRoots[parseStorePath(root)].emplace(censor ? censored : fmt("{temp:%d}", pid));
            pos = end + 1;
        }
    }
}

void LocalStore::findRoots(const std::filesystem::path & path, std::filesystem::file_type type, Roots & roots)
{
    auto foundRoot = [&](const std::filesystem::path & path, const std::filesystem::path & target) {
        try {
            auto storePath = toStorePath(target.string()).first;
            if (isValidPath(storePath))
                roots[std::move(storePath)].emplace(path.string());
            else
                printInfo("skipping invalid root from %1% to %2%", PathFmt(path), PathFmt(target));
        } catch (BadStorePath &) {
        }
    };

    try {

        if (type == std::filesystem::file_type::unknown)
            type = std::filesystem::symlink_status(path).type();

        if (type == std::filesystem::file_type::directory) {
            for (auto & i : DirectoryIterator{path}) {
                checkInterrupt();
                findRoots(i.path(), i.symlink_status().type(), roots);
            }
        }

        else if (type == std::filesystem::file_type::symlink) {
            auto target = readLink(path);
            if (isInStore(target.string()))
                foundRoot(path, target);

            /* Handle indirect roots. */
            else {
                auto parentPath = path.parent_path();
                target = absPath(target, &parentPath);
                if (!pathExists(target)) {
                    if (isInDir(path, config->stateDir.get() / gcRootsDir / "auto")) {
                        printInfo("removing stale link from %1% to %2%", PathFmt(path), PathFmt(target));
                        tryUnlink(path);
                    }
                } else {
                    if (!std::filesystem::is_symlink(target))
                        return;
                    auto target2 = readLink(target);
                    if (isInStore(target2.string()))
                        foundRoot(target, target2);
                }
            }
        }

        else if (type == std::filesystem::file_type::regular) {
            auto storePath = maybeParseStorePath(storeDir + "/" + std::string(baseNameOf(path.string())));
            if (storePath && isValidPath(*storePath))
                roots[std::move(*storePath)].emplace(path.string());
        }

    }

    catch (std::filesystem::filesystem_error & e) {
        /* We only ignore permanent failures. */
        if (e.code() == std::errc::permission_denied || e.code() == std::errc::no_such_file_or_directory
            || e.code() == std::errc::not_a_directory)
            printInfo("cannot read potential root %1%", PathFmt(path));
        else
            throw SystemError(e.code(), "finding GC roots in %1%", PathFmt(path));
    }

    catch (SystemError & e) {
        /* We only ignore permanent failures. */
        if (e.is(std::errc::permission_denied) || e.is(std::errc::no_such_file_or_directory)
            || e.is(std::errc::not_a_directory))
            printInfo("cannot read potential root %1%", PathFmt(path));
        else
            throw;
    }
}

void LocalStore::findRootsNoTemp(Roots & roots, bool censor)
{
    /* Process direct roots in {gcroots,profiles}. */
    findRoots(config->stateDir.get() / gcRootsDir, std::filesystem::file_type::unknown, roots);
    findRoots(config->stateDir.get() / "profiles", std::filesystem::file_type::unknown, roots);

    /* Add additional roots returned by different platforms-specific
       heuristics.  This is typically used to add running programs to
       the set of roots (to prevent them from being garbage collected). */
    findRuntimeRoots(roots, censor);
}

Roots LocalStore::findRoots(bool censor)
{
    Roots roots;
    findRootsNoTemp(roots, censor);

    findTempRoots(roots, censor);

    return roots;
}

static Roots requestRuntimeRoots(const LocalStoreConfig & config, const std::filesystem::path & socketPath)
{
    Roots roots;

    auto socket = connect(socketPath);
    auto socketSource = FdSource(socket.get());

    while (1) {
        auto line = socketSource.readLine(true, '\0');
        if (line == "")
            break;
        roots[config.parseStorePath(line)].insert(censored);
    };

    return roots;
}

void LocalStore::findRuntimeRoots(Roots & roots, bool censor)
{
    Roots unchecked;

    if (config->useRootsDaemon) {
        experimentalFeatureSettings.require(Xp::LocalOverlayStore);
        unchecked = requestRuntimeRoots(*config, config->getRootsSocketPath());
    } else {
        unchecked = findRuntimeRootsUnchecked(*config);
    }

    for (auto & [path, links] : unchecked) {
        if (!isValidPath(path))
            continue;
        debug("got additional root '%1%'", printStorePath(path));
        if (censor)
            roots[path].insert(censored);
        else
            roots[path].insert(links.begin(), links.end());
    }
}

struct GCLimitReached
{};

void LocalStore::collectGarbage(const GCOptions & options, GCResults & results)
{
    const auto & gcSettings = config->getLocalSettings().getGCSettings();

    bool shouldDelete = options.action == GCOptions::gcDeleteDead || options.action == GCOptions::gcDeleteSpecific;

    boost::unordered_flat_set<StorePath, std::hash<StorePath>> roots, dead, alive;

    /* Return early if nothing to delete */
    if (std::visit(
            overloaded{
                [](const GCOptions::SpecificPaths & pathsToDelete) { return pathsToDelete.paths.empty(); },
                [](const GCOptions::WholeStore & _) { return false; }},
            options.pathsToDelete))
        return;

    struct Shared
    {
        // The temp roots only store the hash part to make it easier to
        // ignore suffixes like '.lock', '.chroot' and '.check'.
        boost::unordered_flat_set<std::string, StringViewHash, std::equal_to<>> tempRoots;

        // Hash parts of store paths currently being deleted. Clients trying
        // to add a temp root for one of these paths must wait until deletion
        // is complete. A set (rather than a single optional) so that a
        // batched deletion in fast GC can mark all of its in-flight paths at
        // once; the traditional GC uses a single-element set.
        boost::unordered_flat_set<std::string, StringViewHash, std::equal_to<>> pending;
    };

    Sync<Shared> _shared;

    std::condition_variable wakeup;

    if (shouldDelete)
        deletePath(reservedPath);

    /* Acquire the global GC root. Note: we don't use fdGCLock
       here because then in auto-gc mode, another thread could
       downgrade our exclusive lock. */
    auto fdGCLock = openGCLock();
    FdLock gcLock(fdGCLock.get(), ltWrite, true, "waiting for the big garbage collector lock...");

    /* Synchronisation point to test ENOENT handling in
       addTempRoot(), see tests/gc-non-blocking.sh. */
    if (auto p = getEnv("_NIX_TEST_GC_SYNC_1"))
        readFile(*p);

    /* Start the server for receiving new roots. */
    auto socketPath = config->stateDir.get() / gcSocketPath;
    createDirs(socketPath.parent_path());
    auto fdServer = createUnixDomainSocket(socketPath, 0666);

    // TODO nonblocking socket on windows?
#ifdef _WIN32
    throw UnimplementedError("External GC client not implemented yet");
#else
    if (fcntl(fdServer.get(), F_SETFL, fcntl(fdServer.get(), F_GETFL) | O_NONBLOCK) == -1)
        throw SysError("making socket %s non-blocking", PathFmt(socketPath));

    Pipe shutdownPipe;
    shutdownPipe.create();

    std::thread serverThread([&]() {
        Sync<std::map<int, std::thread>> connections;

        Finally cleanup([&]() {
            debug("GC roots server shutting down");
            fdServer.close();
            while (true) {
                auto item = remove_begin(*connections.lock());
                if (!item)
                    break;
                auto & [fd, thread] = *item;
                shutdown(fd, SHUT_RDWR);
                thread.join();
            }
        });

        while (true) {
            std::vector<struct pollfd> fds;
            fds.push_back({.fd = shutdownPipe.readSide.get(), .events = POLLIN});
            fds.push_back({.fd = fdServer.get(), .events = POLLIN});
            auto count = poll(fds.data(), fds.size(), -1);
            assert(count != -1);

            if (fds[0].revents)
                /* Parent is asking us to quit. */
                break;

            if (fds[1].revents) {
                /* Accept a new connection. */
                assert(fds[1].revents & POLLIN);
                AutoCloseFD fdClient = accept(fdServer.get(), nullptr, nullptr);
                if (!fdClient)
                    continue;

                debug("GC roots server accepted new client");

                /* Process the connection in a separate thread. */
                auto fdClient_ = fdClient.get();
                std::thread clientThread([&, fdClient = std::move(fdClient)]() {
                    Finally cleanup([&]() {
                        auto conn(connections.lock());
                        auto i = conn->find(fdClient.get());
                        if (i != conn->end()) {
                            i->second.detach();
                            conn->erase(i);
                        }
                    });

                    /* On macOS, accepted sockets inherit the
                       non-blocking flag from the server socket, so
                       explicitly make it blocking. */
                    if (fcntl(fdClient.get(), F_SETFL, fcntl(fdClient.get(), F_GETFL) & ~O_NONBLOCK) == -1)
                        panic("Could not set non-blocking flag on client socket");

                    FdSource source(fdClient.get());
                    while (true) {
                        try {
                            auto path = source.readLine();
                            auto storePath = maybeParseStorePath(path);
                            if (storePath) {
                                debug("got new GC root '%s'", path);
                                auto hashPart = storePath->hashPart();
                                auto shared(_shared.lock());
                                shared->tempRoots.emplace(hashPart);
                                /* If this path is currently being
                                   deleted, then we have to wait until
                                   deletion is finished to ensure that
                                   the client doesn't start
                                   re-creating it before we're
                                   done. FIXME: ideally we would use a
                                   FD for this so we don't block the
                                   poll loop. */
                                while (shared->pending.contains(hashPart)) {
                                    debug("synchronising with deletion of path '%s'", path);
                                    shared.wait(wakeup);
                                }
                            } else
                                printError("received garbage instead of a root from client");
                            writeFull(fdClient.get(), "1", false);
                        } catch (Error & e) {
                            debug("reading GC root from client: %s", e.msg());
                            break;
                        }
                    }
                });

                connections.lock()->insert({fdClient_, std::move(clientThread)});
            }
        }
    });

    Finally stopServer([&]() {
        writeFull(shutdownPipe.writeSide.get(), "x", false);
        wakeup.notify_all();
        if (serverThread.joinable())
            serverThread.join();
    });

#endif

    /* For fast GC with --prune-older-than, set up a small pool of
       background deletion threads. Each trashed store path is its own
       subdir under trashDir, so workers unlink disjoint filesystem
       subtrees and don't contend on any shared inodes beyond the trashDir
       parent itself. Sizing: unlink is disk-metadata-bound, and ext4
       serialises on the containing dir's htree lock, so returns diminish
       fast. 4 workers is enough to keep the NVMe queue saturated without
       thrashing. */
    constexpr size_t deleterThreadCount = 4;
    std::vector<std::thread> deleterThreads;
    std::shared_ptr<Sync<std::vector<std::filesystem::path>>> deleteQueue;
    std::shared_ptr<std::atomic<bool>> stopDeleter;
    std::filesystem::path trashDir;

    if (options.pruneOlderThan) {
        auto trashBase = config->realStoreDir.get() / ".gc-trash";
        if (!std::filesystem::exists(trashBase)) {
            createDir(trashBase, 0700);
        } else {
            chmod(trashBase.c_str(), 0700);
        }

        try {
            for (auto & entry : std::filesystem::directory_iterator(trashBase)) {
                if (entry.is_directory()) {
                    printInfo("cleaning up interrupted GC trash: %s", entry.path().filename().string());
                    std::filesystem::remove_all(entry.path());
                }
            }
        } catch (std::filesystem::filesystem_error & e) {
            logWarning({.msg = HintFmt("failed to clean up old GC trash: %1%", e.what())});
        }

        trashDir = trashBase / std::to_string(getpid());
        createDir(trashDir, 0700);

        deleteQueue = std::make_shared<Sync<std::vector<std::filesystem::path>>>();
        stopDeleter = std::make_shared<std::atomic<bool>>(false);

        for (size_t i = 0; i < deleterThreadCount; ++i) {
            deleterThreads.emplace_back([deleteQueue, stopDeleter, config = this->config, &wakeup]() {
                while (true) {
                    std::filesystem::path trashedPath;
                    {
                        auto queue = deleteQueue->lock();
                        while (queue->empty() && !*stopDeleter)
                            queue.wait_for(wakeup, std::chrono::milliseconds(100));
                        if (queue->empty() && *stopDeleter)
                            return;
                        trashedPath = std::move(queue->back());
                        queue->pop_back();
                    }

                    uint64_t bytesFreed = 0;
                    try {
                        deletePath(trashedPath, bytesFreed);
                    } catch (Interrupted & e) {
                        return;
                    } catch (SystemError & e) {
                        if (!config->ignoreGcDeleteFailure)
                            logError({.msg = HintFmt("background deletion failed: %1%", e.msg())});
                    }
                }
            });
        }
    }

    Finally stopDeleterThreads([&] {
        if (stopDeleter) {
            *stopDeleter = true;
            wakeup.notify_all();
            for (auto & t : deleterThreads)
                if (t.joinable())
                    t.join();
            /* All workers have finished; safe to remove the (now-empty) trashDir. */
            try {
                std::filesystem::remove_all(trashDir);
            } catch (...) {
                ignoreExceptionExceptInterrupt();
            }
        }
    });

    /* Find the roots.  Since we've grabbed the GC lock, the set of
       permanent roots cannot increase now. */
    printInfo("finding garbage collector roots...");
    Roots rootMap;
    if (!options.ignoreLiveness)
        findRootsNoTemp(rootMap, true);

    for (auto & i : rootMap)
        roots.insert(i.first);

    /* Read the temporary roots created before we acquired the global
       GC root. Any new roots will be sent to our socket. */
    Roots tempRoots;
    findTempRoots(tempRoots, true);
    for (auto & root : tempRoots) {
        _shared.lock()->tempRoots.emplace(root.first.hashPart());
        roots.insert(root.first);
    }

    /* Synchronisation point for testing, see tests/functional/gc-non-blocking.sh. */
    if (auto p = getEnv("_NIX_TEST_GC_SYNC_2"))
        readFile(*p);

    /* Fast incremental GC: delete only leaf paths older than threshold.
       Supports multi-round execution to clean up deep dependency chains in one invocation. */
    if (options.pruneOlderThan) {
        if (!dynamic_cast<LocalStore *>(this)) {
            throw Error(
                "Fast incremental GC (--prune-older-than) requires direct store access. "
                "Remote store support will be added in a future version.");
        }

        auto cutoffTime = time(nullptr) - *options.pruneOlderThan;

        /* Compute the alive-id set inside sqlite via a recursive CTE and
           materialise it into a temp table for the leaf-selection query
           to anti-join against. This replaces a C++ computeFSClosure walk
           that costs ~1 sqlite roundtrip per alive path (millions on a
           real store) with one B-tree traversal inside the DB.

           Locking discipline matches the rest of this function: extract
           the connection pointer under the state lock, then use it while
           holding gc.lock (the process-level flock) for serialisation. */
        SQLiteStmt stmt;
        {
            auto state(_state->lock());
            sqlite3 * db = state->db;

            auto execSql = [&](const std::string & sql) {
                if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK)
                    SQLiteError::throw_(db, "executing SQL '%s'", sql);
            };

            SQLiteTxn txn(db);

            execSql("drop table if exists temp.AliveGCRoots");
            execSql("create temp table AliveGCRoots (path text primary key not null)");
            execSql("drop table if exists temp.AliveGC");
            execSql("create temp table AliveGC (id integer primary key not null)");

            /* Seed the roots temp table. */
            {
                SQLiteStmt insertRoot;
                insertRoot.create(db, "insert or ignore into AliveGCRoots(path) values (?)");
                for (auto & p : roots)
                    insertRoot.use()(printStorePath(p)).exec();
            }

            /* Build the recursive CTE. Edge branches are only included
               when the corresponding keep-* setting is on so the query
               planner isn't asked to consider dead branches. */
            std::string recursiveBranches =
                "select r.reference from Refs r join Alive a on r.referrer = a.id";
            if (gcSettings.keepOutputs)
                recursiveBranches +=
                    " union"
                    " select v.id from Alive a"
                    "   join DerivationOutputs d on d.drv = a.id"
                    "   join ValidPaths v on v.path = d.path";
            if (gcSettings.keepDerivations)
                recursiveBranches +=
                    " union"
                    " select deriv.id from Alive a"
                    "   join ValidPaths child on child.id = a.id"
                    "   join ValidPaths deriv on deriv.path = child.deriver"
                    "   where child.deriver is not null";

            execSql(
                "insert into AliveGC(id)"
                " with recursive Alive(id) as ("
                "   select v.id from ValidPaths v join AliveGCRoots r on v.path = r.path"
                "   union"
                "   " + recursiveBranches +
                " )"
                " select id from Alive");

            txn.commit();

            /* Prepare the leaf-selection statement under the same lock. */
            stmt.create(db, R"(
                SELECT v.path, v.narSize FROM ValidPaths v
                WHERE v.registrationTime < ?
                  AND NOT EXISTS (
                    SELECT 1 FROM Refs r
                    WHERE r.reference = v.id AND r.reference != r.referrer
                  )
                  AND v.id NOT IN (SELECT id FROM AliveGC)
            )");
        }

        /* Dry-run mode: just report what would be deleted. The alive set
           (permanent + temp roots at GC start, plus their keep-* closure)
           is already excluded by the SQL query; only temp roots created
           after the CTE ran need a runtime check. */
        if (options.action == GCOptions::gcReturnDead) {
            auto use = stmt.use()(cutoffTime);
            while (use.next()) {
                auto path = use.getStr(0);
                auto narSize = use.isNull(1) ? 0 : use.getInt(1);
                auto hash = std::string(parseStorePath(path).hashPart());

                auto shared(_shared.lock());
                if (shared->tempRoots.contains(hash))
                    continue;

                results.paths.insert(path);
                if (narSize > 0)
                    results.bytesFreed += narSize;
            }
            return;
        }

        /* Multi-round deletion: delete current leafs in each round.
           Within a round, invalidations are batched into a single
           SQLiteTxn to collapse the ~500 individual DELETE transactions
           into one bulk write. Each individual DELETE otherwise contends
           on SQLITE_BUSY with concurrent readers (e.g. long-lived
           nix-daemon connections holding old WAL snapshots), and a
           single-path retry storm can drop throughput from thousands of
           writes/s to tens of writes/s. */
        constexpr size_t invalidateBatchSize = 256;
        uint64_t totalDeleted = 0;

        /* Represents a path selected for deletion in the current batch. */
        struct Pending
        {
            std::string path;
            std::string hash;
            std::filesystem::path realPath;
            std::filesystem::path trashPath;
            int64_t narSize;
        };

        for (uint64_t round = 1; round <= options.pruneRounds; round++) {
            uint64_t roundDeleted = 0;
            bool limitReached = false;

            /* Flush a batch: bulk-invalidate in one txn, then move files to
               trash and enqueue for background unlink. tempRoots-pending
               entries stay set until this returns. */
            auto flushBatch = [&](std::vector<Pending> & batch) {
                if (batch.empty())
                    return;

                /* Step 1: bulk-invalidate DB in one transaction. On busy,
                   retrySQLite retries the whole batch. */
                retrySQLite<void>([&]() {
                    auto state(_state->lock());
                    sqlite3 * db = state->db;

                    auto execSql = [&](const std::string & sql) {
                        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK)
                            SQLiteError::throw_(db, "executing SQL '%s'", sql);
                    };

                    SQLiteTxn txn(db);

                    execSql("drop table if exists temp.InvalidateBatch");
                    execSql("create temp table InvalidateBatch (path text primary key not null)");

                    {
                        SQLiteStmt ins;
                        ins.create(db, "insert into InvalidateBatch(path) values (?)");
                        for (auto & p : batch)
                            ins.use()(p.path).exec();
                    }

                    /* Foreign keys on Refs cascade from ValidPaths on delete,
                       so this single DELETE cleans up Refs rows too. */
                    execSql("delete from ValidPaths where path in (select path from InvalidateBatch)");
                    execSql("drop table temp.InvalidateBatch");

                    txn.commit();

                    for (auto & p : batch)
                        invalidatePathInfoCacheFor(parseStorePath(p.path));
                });

                /* Step 2: rename each path to .gc-trash and enqueue for
                   background unlink. If a rename fails and
                   ignoreGcDeleteFailure is set, we log and continue —
                   the DB is already invalidated so the orphan file
                   just wastes space until the next verify/GC. */
                for (auto & p : batch) {
                    try {
                        std::filesystem::rename(p.realPath, p.trashPath);
                        deleteQueue->lock()->push_back(p.trashPath);
                        if (p.narSize > 0)
                            results.bytesFreed += p.narSize;
                    } catch (std::filesystem::filesystem_error & e) {
                        if (!config->ignoreGcDeleteFailure)
                            throw;
                        logWarning({.msg = HintFmt("ignoring GC failure for %1%: %2%", PathFmt(p.realPath), e.what())});
                    }
                }
                wakeup.notify_all();

                /* Step 3: clear the pending set (used by build clients to
                   block until in-flight deletions complete). */
                {
                    auto shared(_shared.lock());
                    for (auto & p : batch)
                        shared->pending.erase(p.hash);
                    wakeup.notify_all();
                }

                roundDeleted += batch.size();
                totalDeleted += batch.size();
                batch.clear();
            };

            std::vector<Pending> batch;
            batch.reserve(invalidateBatchSize);

            try {
                auto use = stmt.use()(cutoffTime);
                while (use.next()) {
                    checkInterrupt();
                    auto path = use.getStr(0);
                    auto narSize = use.isNull(1) ? 0 : use.getInt(1);
                    auto hash = std::string(parseStorePath(path).hashPart());

                    /* Check tempRoots at batch-collection time and mark
                       this path as pending so clients trying to add a
                       temp root for it will wait until we finish. */
                    {
                        auto shared(_shared.lock());
                        if (shared->tempRoots.contains(hash))
                            continue;
                        shared->pending.insert(hash);
                    }

                    Pending p{
                        .path = path,
                        .hash = hash,
                        .realPath = config->realStoreDir.get() / baseNameOf(path),
                        .trashPath = trashDir / (hash + "-" + std::to_string(totalDeleted + batch.size())),
                        .narSize = narSize,
                    };

                    printInfo("deleting '%s'", path);
                    results.paths.insert(path);
                    batch.push_back(std::move(p));

                    if (batch.size() >= invalidateBatchSize)
                        flushBatch(batch);

                    if (results.bytesFreed >= options.maxFreed) {
                        limitReached = true;
                        break;
                    }
                }
            } catch (GCLimitReached &) {
                limitReached = true;
            }

            /* Flush any remaining paths, even if we're bailing out. */
            flushBatch(batch);

            if (limitReached) {
                printInfo(
                    "fast GC: round %d/%d: deleted %d paths (limit reached)", round, options.pruneRounds, roundDeleted);
                printInfo("fast GC: total: deleted %d paths, freed %s", totalDeleted, renderSize(results.bytesFreed));
                return;
            }

            printInfo("fast GC: round %d/%d: deleted %d paths", round, options.pruneRounds, roundDeleted);

            /* Stop early if no paths were deleted in this round */
            if (roundDeleted == 0) {
                printInfo("fast GC: no more paths to delete, stopping after %d rounds", round);
                break;
            }
        }

        printInfo("fast GC: total: deleted %d paths, freed %s", totalDeleted, renderSize(results.bytesFreed));
        return;
    }

    /* Helper function that deletes a path from the store and throws
       GCLimitReached if we've deleted enough garbage. */
    auto deleteFromStore = [&](std::string_view baseName, bool isKnownPath) {
        assert(!std::filesystem::path(baseName).is_absolute());
        /* Using `std::string` since this is the logical store dir. Hopefully that is the right choice. */
        std::string path = storeDir + "/" + std::string(baseName);
        auto realPath = config->realStoreDir.get() / std::string(baseName);

        /* There may be temp directories in the store that are still in use
           by another process. We need to be sure that we can acquire an
           exclusive lock before deleting them. */
        if (baseName.find("tmp-", 0) == 0) {
            /* TODO Reconsider whether Follow is the right choice, here */
            auto tmpDirFd = openDirectory(realPath, FinalSymlink::Follow);
            if (!tmpDirFd || !lockFile(tmpDirFd.get(), ltWrite, false)) {
                debug("skipping locked tempdir %s", PathFmt(realPath));
                return;
            }
        }

        printInfo("deleting '%1%'", path);

        results.paths.insert(path);

        uint64_t bytesFreed;
        deleteStorePath(realPath, bytesFreed, isKnownPath);

        results.bytesFreed += bytesFreed;

        if (results.bytesFreed > options.maxFreed) {
            printInfo("deleted more than %d bytes; stopping", options.maxFreed);
            throw GCLimitReached();
        }
    };

    boost::unordered_flat_map<StorePath, StorePathSet, std::hash<StorePath>> referrersCache;

    /* Helper function that visits all paths reachable from `start`
       via the referrers edges and optionally derivers and derivation
       output edges. If none of those paths are roots, then all
       visited paths are garbage and are deleted. */
    auto maybeDeleteReferrersClosure = [&](const StorePath & start) {
        StorePathSet visited;
        std::queue<StorePath> todo;

        /* Wake up any GC client waiting for deletion of the paths in
           'visited' to finish. */
        Finally releasePending([&]() {
            auto shared(_shared.lock());
            shared->pending.clear();
            wakeup.notify_all();
        });

        auto enqueue = [&](const StorePath & path) {
            if (visited.insert(path).second)
                todo.push(path);
        };

        enqueue(start);

        while (auto path = pop(todo)) {
            checkInterrupt();

            /* Bail out if we've previously discovered that this path
               is alive. */
            if (alive.contains(*path)) {
                debug("cannot delete '%s' because '%s' is alive", printStorePath(start), printStorePath(*path));
                alive.insert(start);
                return;
            }

            /* If we've previously deleted this path, we don't have to
               handle it again. */
            if (dead.contains(*path))
                continue;

            auto markAlive = [&]() {
                alive.insert(*path);
                alive.insert(start);
                try {
                    StorePathSet closure;
                    bool includeOutputs = false;
                    bool includeDerivers = false;
                    std::visit(
                        overloaded{
                            [&](const GCOptions::WholeStore &) {
                                includeOutputs = gcSettings.keepOutputs;
                                includeDerivers = gcSettings.keepDerivations;
                            },
                            [](const GCOptions::SpecificPaths &) {},
                        },
                        options.pathsToDelete);
                    computeFSClosure(
                        *path,
                        closure,
                        /* flipDirection */ false,
                        includeOutputs,
                        includeDerivers);
                    for (auto & p : closure)
                        alive.insert(p);
                } catch (InvalidPath &) {
                }
            };

            /* If this is a root, bail out. */
            if (roots.contains(*path)) {
                debug("cannot delete '%s' because it's a root", printStorePath(*path));
                return markAlive();
            }

            if (std::visit(
                    overloaded{
                        [&](const GCOptions::SpecificPaths & pathsToDelete) {
                            if (!pathsToDelete.deleteReferrers && !pathsToDelete.paths.contains(*path)) {
                                debug(
                                    "cannot delete '%s' because '%s' is not in the specified paths to delete",
                                    printStorePath(start),
                                    printStorePath(*path));
                                return true;
                            }
                            return false;
                        },
                        [](const GCOptions::WholeStore & _) { return false; },
                    },
                    options.pathsToDelete))
                return;

            {
                auto hashPart = path->hashPart();
                auto shared(_shared.lock());
                if (shared->tempRoots.contains(hashPart)) {
                    debug("cannot delete '%s' because it's a temporary root", printStorePath(*path));
                    return markAlive();
                }
                shared->pending.insert(std::string(hashPart));
            }

            if (isValidPath(*path)) {

                /* Visit the referrers of this path. */
                auto i = referrersCache.find(*path);
                if (i == referrersCache.end()) {
                    StorePathSet referrers;
                    queryGCReferrers(*path, referrers);
                    referrersCache.emplace(*path, std::move(referrers));
                    i = referrersCache.find(*path);
                }
                for (auto & p : i->second)
                    enqueue(p);

                std::visit(
                    overloaded{
                        [&](const GCOptions::WholeStore &) {
                            /* If keep-derivations is set and this is a derivation, then we only want to delete this
                             * derivation if we can also delete all its outputs, so visit the derivation outputs. */
                            if (gcSettings.keepDerivations && path->isDerivation())
                                for (auto & [name, maybeOutPath] : queryPartialDerivationOutputMap(*path))
                                    if (maybeOutPath && isValidPath(*maybeOutPath)
                                        && queryPathInfo(*maybeOutPath)->deriver == path)
                                        enqueue(*maybeOutPath);

                            /* If keep-outputs is set, we only want to delete this path if we
                             * can also delete its derivers, so visit the derivers. */
                            if (gcSettings.keepOutputs) {
                                auto derivers = queryValidDerivers(*path);
                                for (auto & i : derivers)
                                    enqueue(i);
                            }
                        },
                        [](const GCOptions::SpecificPaths &) {},
                    },
                    options.pathsToDelete);
            }
        }
        for (auto & path : topoSortPaths(visited)) {
            if (!dead.insert(path).second)
                continue;
            if (shouldDelete) {
                try {
                    invalidatePathChecked(path);
                    deleteFromStore(path.to_string(), true);
                    referrersCache.erase(path);
                } catch (PathInUse & e) {
                    // If we end up here, it's likely a new occurrence
                    // of https://github.com/NixOS/nix/issues/11923
                    printError("BUG: %s", e.what());
                }
            }
        }
    };

    try {
        /* Either delete all garbage paths, or just the specified paths. */
        std::visit(
            overloaded{
                [&](const GCOptions::SpecificPaths & pathsToDelete) {
                    switch (options.action) {
                    case GCOptions::gcDeleteDead:
                        printInfo("deleting garbage within specified paths...");
                        break;
                    case GCOptions::gcDeleteSpecific:
                        printInfo("deleting specified paths...");
                        break;
                    case GCOptions::gcReturnDead:
                    case GCOptions::gcReturnLive:
                        printInfo("determining live/dead paths...");
                    }

                    for (auto & i : pathsToDelete.paths) {
                        maybeDeleteReferrersClosure(i);

                        if (options.action == GCOptions::gcDeleteSpecific && !dead.contains(i))
                            throw Error(
                                "Cannot delete path '%1%' since it is still alive. "
                                "To find out why, use: "
                                "nix-store --query --roots and nix-store --query --referrers",
                                printStorePath(i));
                        else if (!dead.contains(i))
                            debug("cannot delete '%s' because it's still alive", printStorePath(i));
                    }
                },
                [&](const GCOptions::WholeStore & _) {
                    if (options.maxFreed == 0)
                        return;

                    switch (options.action) {
                    case GCOptions::gcDeleteDead:
                        printInfo("deleting garbage...");
                        break;
                    case GCOptions::gcDeleteSpecific:
                        throw Error("Cannot delete the entire store");
                    case GCOptions::gcReturnDead:
                    case GCOptions::gcReturnLive:
                        printInfo("determining live/dead paths...");
                    }

                    AutoCloseDir dir(opendir(config->realStoreDir.get().string().c_str()));
                    if (!dir)
                        throw SysError("opening directory %1%", PathFmt(config->realStoreDir.get()));

                    /* Read the store and delete all paths that are invalid or
                    unreachable. We don't use readDirectory() here so that
                    GCing can start faster. */
                    auto linksName = linksDir.filename();
                    struct dirent * dirent;
                    while (errno = 0, dirent = readdir(dir.get())) {
                        checkInterrupt();
                        std::string name = dirent->d_name;
                        if (name == "." || name == ".." || name == linksName)
                            continue;

                        if (auto storePath = maybeParseStorePath(storeDir + "/" + name))
                            maybeDeleteReferrersClosure(*storePath);
                        else
                            deleteFromStore(name, false);
                    }
                },
            },
            options.pathsToDelete);
    } catch (GCLimitReached & e) {
    }

    if (options.action == GCOptions::gcReturnLive) {
        for (auto & i : alive)
            results.paths.insert(printStorePath(i));
        return;
    }

    if (options.action == GCOptions::gcReturnDead) {
        for (auto & i : dead)
            results.paths.insert(printStorePath(i));
        return;
    }

    /* Unlink all files in /nix/store/.links that have a link count of 1,
       which indicates that there are no other links and so they can be
       safely deleted.  FIXME: race condition with optimisePath(): we
       might see a link count of 1 just before optimisePath() increases
       the link count. */
    if (options.action == GCOptions::gcDeleteDead || options.action == GCOptions::gcDeleteSpecific) {
        printInfo("deleting unused links...");

        AutoCloseDir dir(opendir(linksDir.string().c_str()));
        if (!dir)
            throw SysError("opening directory %1%", PathFmt(linksDir));

        int64_t actualSize = 0, unsharedSize = 0;

        struct dirent * dirent;
        while (errno = 0, dirent = readdir(dir.get())) {
            checkInterrupt();
            std::string name = dirent->d_name;
            if (name == "." || name == "..")
                continue;
            auto path = linksDir / name;

            auto st = lstat(path);

            if (st.st_nlink != 1) {
                actualSize += st.st_size;
                unsharedSize += (st.st_nlink - 1) * st.st_size;
                continue;
            }

            printMsg(lvlTalkative, "deleting unused link %1%", PathFmt(path));

            unlink(path);

            /* Do not account for deleted file here. Rely on deletePath()
               accounting.  */
        }

        int64_t overhead =
#ifdef _WIN32
            0
#else
            [&] {
                auto st = stat(linksDir);
                return st.st_blocks * 512ULL;
            }()
#endif
            ;

        printInfo("note: hard linking is currently saving %s", renderSize(unsharedSize - actualSize - overhead));
    }

    /* While we're at it, vacuum the database. */
    // if (options.action == GCOptions::gcDeleteDead) vacuumDB();
}

void LocalStore::autoGC(bool sync)
{
#if HAVE_STATVFS
    const auto & gcSettings = config->getLocalSettings().getGCSettings();

    static auto fakeFreeSpaceFile = getEnv("_NIX_TEST_FREE_SPACE_FILE");

    auto getAvail = [this]() -> uint64_t {
        if (fakeFreeSpaceFile)
            return std::stoll(readFile(*fakeFreeSpaceFile));

        struct statvfs st;
        if (statvfs(config->realStoreDir.get().c_str(), &st))
            throw SysError("getting filesystem info about '%s'", PathFmt(config->realStoreDir.get()));

        return (uint64_t) st.f_bavail * st.f_frsize;
    };

    std::shared_future<void> future;

    {
        auto state(_state->lock());

        if (state->gcRunning) {
            future = state->gcFuture;
            debug("waiting for auto-GC to finish");
            goto sync;
        }

        auto now = std::chrono::steady_clock::now();

        if (now < state->lastGCCheck + std::chrono::seconds(gcSettings.minFreeCheckInterval))
            return;

        auto avail = getAvail();

        state->lastGCCheck = now;

        if (avail >= gcSettings.minFree || avail >= gcSettings.maxFree)
            return;

        if (avail > state->availAfterGC * 0.97)
            return;

        state->gcRunning = true;

        std::promise<void> promise;
        future = state->gcFuture = promise.get_future().share();

        std::thread([promise{std::move(promise)}, this, avail, getAvail, &gcSettings]() mutable {
            try {

                /* Wake up any threads waiting for the auto-GC to finish. */
                Finally wakeup([&]() {
                    auto state(_state->lock());
                    state->gcRunning = false;
                    state->lastGCCheck = std::chrono::steady_clock::now();
                    promise.set_value();
                });

                GCOptions options;
                options.maxFreed = gcSettings.maxFree - avail;

                printInfo("running auto-GC to free %d bytes", options.maxFreed);

                GCResults results;

                collectGarbage(options, results);

                _state->lock()->availAfterGC = getAvail();

            } catch (...) {
                // FIXME: we could propagate the exception to the
                // future, but we don't really care. (what??)
                ignoreExceptionInDestructor();
            }
        }).detach();
    }

sync:
    // Wait for the future outside of the state lock.
    if (sync)
        future.get();
#endif
}

} // namespace nix
