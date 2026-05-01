#include "nix/util/archive.hh"
#include "nix/store/binary-cache-store.hh"
#include "nix/store/local-store.hh"
#include "nix/store/local-settings.hh"
#include "nix/store/posix-fs-canonicalise.hh"
#include "nix/util/compression.hh"
#include "nix/store/derivations.hh"
#include "nix/util/source-accessor.hh"
#include "nix/store/nar-info-disk-cache.hh"
#include "nix/store/nar-info.hh"
#include "nix/util/sync.hh"
#include "nix/store/remote-fs-accessor.hh"
#include "nix/util/nar-accessor.hh"
#include "nix/util/thread-pool.hh"
#include "nix/util/callback.hh"
#include "nix/util/signals.hh"
#include "nix/util/archive.hh"

#include <chrono>
#include <future>
#include <regex>
#include <sstream>

#include <nlohmann/json.hpp>

namespace nix {

void BinaryCacheStoreConfig::anchor() {}

void BinaryCacheStore::anchor() {}

BinaryCacheStore::BinaryCacheStore(Config & config)
    : config{config}
{
    if (auto & skf = config.secretKeyFile.get())
        signers.push_back(std::make_unique<LocalSigner>(SecretKey{readFile(*skf)}));

    if (config.secretKeyFiles != "") {
        std::stringstream ss(config.secretKeyFiles);
        std::string keyPath;
        while (std::getline(ss, keyPath, ',')) {
            signers.push_back(std::make_unique<LocalSigner>(SecretKey{readFile(keyPath)}));
        }
    }

    StringSink sink;
    sink << narVersionMagic1;
    narMagic = sink.s;
}

void BinaryCacheStore::init()
{
    auto cacheInfo = getNixCacheInfo();
    if (!cacheInfo) {
        std::string info = "StoreDir: " + storeDir + "\n";

        /* Try to check if .links/ directory exists to advertise Links support */
        try {
            if (fileExists(".links")) {
                info += "Links: 1\n";
            }
        } catch (...) {
            /* If we can't check, don't advertise Links support */
        }

        upsertFile(cacheInfoFile, std::move(info), "text/x-nix-cache-info");
    } else {
        for (auto & line : tokenizeString<Strings>(*cacheInfo, "\n")) {
            size_t colon = line.find(':');
            if (colon == std::string::npos)
                continue;
            auto name = line.substr(0, colon);
            auto value = trim(line.substr(colon + 1, std::string::npos));
            if (name == "StoreDir") {
                if (value != storeDir)
                    throw Error(
                        "binary cache '%s' is for Nix stores with prefix '%s', not '%s'",
                        config.getHumanReadableURI(),
                        value,
                        storeDir);
            } else if (name == "WantMassQuery") {
                config.wantMassQuery.setDefault(value == "1");
            } else if (name == "Priority") {
                config.priority.setDefault(std::stoi(value));
            } else if (name == "Links") {
                /* Parse Links capability (future use) */
            }
        }
    }
}

std::optional<std::string> BinaryCacheStore::getNixCacheInfo()
{
    return getFile(cacheInfoFile);
}

void BinaryCacheStore::upsertFile(
    const std::string & path, std::string && data, const std::string & mimeType, uint64_t sizeHint)
{
    StringSource source{data};
    upsertFile(path, source, mimeType, sizeHint);
}

void BinaryCacheStore::getFile(const std::string & path, Callback<std::optional<std::string>> callback) noexcept
{
    try {
        callback(getFile(path));
    } catch (...) {
        callback.rethrow();
    }
}

void BinaryCacheStore::getFile(const std::string & path, Sink & sink)
{
    std::promise<std::optional<std::string>> promise;
    getFile(path, {[&](std::future<std::optional<std::string>> result) {
                try {
                    promise.set_value(result.get());
                } catch (...) {
                    promise.set_exception(std::current_exception());
                }
            }});
    sink(*promise.get_future().get());
}

std::optional<std::string> BinaryCacheStore::getFile(const std::string & path)
{
    StringSink sink;
    try {
        getFile(path, sink);
    } catch (NoSuchBinaryCacheFile &) {
        return std::nullopt;
    }
    return std::move(sink.s);
}

std::string BinaryCacheStore::narInfoFileFor(const StorePath & storePath)
{
    return std::string(storePath.hashPart()) + ".narinfo";
}

void BinaryCacheStore::writeNarInfo(ref<NarInfo> narInfo)
{
    auto narInfoFile = narInfoFileFor(narInfo->path);

    upsertFile(narInfoFile, narInfo->to_string(*this), "text/x-nix-narinfo");

    pathInfoCache->lock()->upsert(narInfo->path, PathInfoCacheValue{.value = std::shared_ptr<NarInfo>(narInfo)});

    if (diskCache)
        diskCache->upsertNarInfo(
            config.getReference().render(/*FIXME withParams=*/false),
            std::string(narInfo->path.hashPart()),
            std::shared_ptr<NarInfo>(narInfo));
}

void BinaryCacheStore::uploadToLinks(NarListing & listing, std::shared_ptr<NarAccessor> narAccessor)
{
    struct FileToUpload {
        CanonPath path;
        NarListing::Regular * reg;
        std::string content;
    };

    std::vector<FileToUpload> files;

    /* First pass: collect all files and read content once */
    std::function<void(NarListing &, const CanonPath &)> collect =
        [&](NarListing & node, const CanonPath & path) {
            if (auto * reg = std::get_if<NarListing::Regular>(&node.raw)) {
                StringSink contentSink;
                narAccessor->readFile(path, contentSink);
                files.push_back({path, reg, std::move(contentSink.s)});
            }
            else if (auto * dir = std::get_if<NarListing::Directory>(&node.raw)) {
                for (auto & [name, child] : dir->entries) {
                    collect(child, path / name);
                }
            }
        };

    collect(listing, CanonPath::root);

    /* Second pass: process files in parallel */
    ThreadPool threadPool(25);

    for (auto & file : files) {
        threadPool.enqueue([&, file = &file]() {
            checkInterrupt();

            /* Compute link hash using same method as LocalStore::optimisePath_
               This hashes the file serialized as NAR (which includes executable bit) */
            HashSink hashSink(HashAlgorithm::SHA256);

            /* Serialize file as a NAR entry (same as hashPath with NixArchive) */
            StringSink narSink;
            narSink << "nix-archive-1";
            narSink << "(";
            narSink << "type";
            narSink << "regular";
            if (file->reg->executable) {
                narSink << "executable";
                narSink << "";
            }
            narSink << "contents";
            narSink << file->content;
            narSink << ")";
            hashSink(narSink.s);

            Hash linkHash = hashSink.finish().hash;
            file->reg->contents.linkHash = linkHash;

            /* Upload to .links/<hash> with compression if not exists */
            auto linkBase = ".links/" + linkHash.to_string(HashFormat::Nix32, false);
            auto linkFile = linkBase
                + (config.compression == CompressionAlgo::xz       ? ".xz"
                   : config.compression == CompressionAlgo::bzip2  ? ".bz2"
                   : config.compression == CompressionAlgo::zstd   ? ".zst"
                   : config.compression == CompressionAlgo::lzip   ? ".lzip"
                   : config.compression == CompressionAlgo::lz4    ? ".lz4"
                   : config.compression == CompressionAlgo::brotli ? ".br"
                                                                   : "");

            if (!fileExists(linkFile)) {
                /* Compress the content (already read once) */
                StringSink compressedSink;
                bool parallel = config.parallelCompression.overridden ? config.parallelCompression.get()
                                                                      : config.compression.get() == CompressionAlgo::zstd;
                auto compressionSink = makeCompressionSink(config.compression, compressedSink, parallel, config.compressionLevel);
                (*compressionSink)(file->content);
                compressionSink->finish();

                StringSource compressedSource(compressedSink.s);
                upsertFile(linkFile, compressedSource, "application/octet-stream", compressedSink.s.size());

                auto originalSize = file->reg->contents.fileSize.value_or(0);
                auto compressedSize = compressedSink.s.size();
                auto ratio = originalSize > 0 ? (1.0 - (double)compressedSize / originalSize) * 100.0 : 0.0;
                printMsg(lvlDebug, "uploaded to %s (%d bytes, %.1f%% compression)",
                         linkFile, compressedSize, ratio);
            }
        });
    }

    threadPool.process();
}

bool BinaryCacheStore::allFilesHaveLinkHash(const NarListing & listing)
{
    /* Recursively check if all regular files have linkHash */
    std::function<bool(const NarListing &)> check = [&](const NarListing & node) {
        if (auto * reg = std::get_if<NarListing::Regular>(&node.raw)) {
            return reg->contents.linkHash.has_value();
        }
        if (auto * dir = std::get_if<NarListing::Directory>(&node.raw)) {
            for (auto & [_, child] : dir->entries) {
                if (!check(child))
                    return false;
            }
        }
        /* Symlinks don't need linkHash */
        return true;
    };

    return check(listing);
}

void BinaryCacheStore::substituteWithLinks(
    LocalStore & dst,
    const ValidPathInfo & info,
    const NarListing & listing)
{
    Activity act(
        *logger,
        actSubstitute,
        Logger::Fields{dst.printStorePath(info.path), config.getHumanReadableURI()});

    auto destPath = dst.config->realStoreDir.get() / info.path.to_string();

    /* Ensure parent directory exists */
    createDirs(destPath.parent_path());

    struct FileToDownload {
        Hash linkHash;
        std::filesystem::path dest;
        bool executable;
        uint64_t size;
        std::string linkFile;
        CompressionAlgo compression;
    };

    std::vector<FileToDownload> toDownload;
    std::vector<std::pair<Hash, std::filesystem::path>> toReuse;
    std::atomic<uint64_t> bytesDownloaded{0};
    std::atomic<uint64_t> bytesReused{0};

    /* First pass: create directory structure and collect files to download/reuse */
    std::function<void(const NarListing &, const std::filesystem::path &)> collect =
        [&](const NarListing & node, const std::filesystem::path & dest) {

        if (auto * reg = std::get_if<NarListing::Regular>(&node.raw)) {
            if (!reg->contents.linkHash) {
                throw Error("regular file missing linkHash in listing");
            }

            auto & linkHash = *reg->contents.linkHash;

            /* Check if we already have this file in local .links/ */
            if (dst.hasLinkedFile(linkHash)) {
                toReuse.push_back({linkHash, dest});
                bytesReused += reg->contents.fileSize.value_or(0);
            } else {
                /* Find compressed link file in cache */
                auto linkBase = ".links/" + linkHash.to_string(HashFormat::Nix32, false);
                std::string linkFile;
                CompressionAlgo compression = CompressionAlgo::none;

                /* Try each compression format in order of preference */
                for (auto algo : {CompressionAlgo::zstd, CompressionAlgo::xz, CompressionAlgo::bzip2,
                                  CompressionAlgo::lz4, CompressionAlgo::brotli, CompressionAlgo::lzip, CompressionAlgo::none}) {
                    auto ext = (algo == CompressionAlgo::xz       ? ".xz"
                                : algo == CompressionAlgo::bzip2  ? ".bz2"
                                : algo == CompressionAlgo::zstd   ? ".zst"
                                : algo == CompressionAlgo::lzip   ? ".lzip"
                                : algo == CompressionAlgo::lz4    ? ".lz4"
                                : algo == CompressionAlgo::brotli ? ".br"
                                                                  : "");
                    auto candidate = linkBase + ext;
                    if (fileExists(candidate)) {
                        linkFile = candidate;
                        compression = algo;
                        break;
                    }
                }

                if (linkFile.empty()) {
                    throw Error("link file not found for hash %s", linkHash.to_string(HashFormat::Nix32, false));
                }

                toDownload.push_back({linkHash, dest, reg->executable, reg->contents.fileSize.value_or(0), linkFile, compression});
            }
        }
        else if (auto * dir = std::get_if<NarListing::Directory>(&node.raw)) {
            /* Create directory */
            createDirs(dest);

            /* Recursively process directory entries */
            for (auto & [name, child] : dir->entries) {
                collect(child, dest / name);
            }
        }
        else if (auto * sym = std::get_if<NarListing::Symlink>(&node.raw)) {
            /* Create symlink */
            createSymlink(sym->target, dest);
        }
    };

    collect(listing, destPath);

    /* Second pass: download files in parallel */
    if (!toDownload.empty()) {
        ThreadPool threadPool(25);

        for (auto & file : toDownload) {
            threadPool.enqueue([&, file = &file]() {
                checkInterrupt();

                /* Download and decompress if needed */
                StringSink decompressedSink;
                if (file->compression != CompressionAlgo::none) {
                    StringSink compressedSink;
                    getFile(file->linkFile, compressedSink);

                    /* Convert CompressionAlgo to string for makeDecompressionSink */
                    std::string compressionStr =
                        (file->compression == CompressionAlgo::xz       ? "xz"
                         : file->compression == CompressionAlgo::bzip2  ? "bzip2"
                         : file->compression == CompressionAlgo::zstd   ? "zstd"
                         : file->compression == CompressionAlgo::lzip   ? "lzip"
                         : file->compression == CompressionAlgo::lz4    ? "lz4"
                         : file->compression == CompressionAlgo::brotli ? "br"
                                                                        : "none");

                    auto decompressor = makeDecompressionSink(compressionStr, decompressedSink);
                    (*decompressor)(compressedSink.s);
                    decompressor->finish();
                } else {
                    getFile(file->linkFile, decompressedSink);
                }

                StringSource contentSource(decompressedSink.s);

                /* Add to local .links/ with correct permissions (verifies hash) */
                dst.addToLinks(file->linkHash, contentSource, file->executable);

                bytesDownloaded += file->size;

                printMsg(lvlDebug, "downloaded %s (%s)",
                    file->dest.filename().string(),
                    renderSize(file->size));
            });
        }

        threadPool.process();
    }

    /* Third pass: hard-link everything (both downloaded and reused) */
    for (auto & [linkHash, dest] : toReuse) {
        dst.hardLinkFromLinks(linkHash, dest, false /* resetPermissions */);
    }

    for (auto & file : toDownload) {
        dst.hardLinkFromLinks(file.linkHash, file.dest, false /* resetPermissions */);

        /* Set executable bit if needed */
        if (file.executable) {
            auto st = lstat(file.dest);
            chmod(file.dest, st.st_mode | S_IXUSR | S_IXGRP | S_IXOTH);
        }
    }

    /* Canonicalize permissions and timestamps */
    CanonicalizePathMetadataOptions options{NIX_WHEN_SUPPORT_ACLS(dst.config->getLocalSettings().ignoredAcls)};
    canonicalisePathMetaData(destPath, options);

    /* Register the path as valid */
    dst.registerValidPath(info);

    printMsg(
        lvlInfo,
        "links-based substitution complete: %s downloaded, %s reused (%d%% savings)",
        renderSize(bytesDownloaded.load()),
        renderSize(bytesReused.load()),
        bytesReused > 0 ? (int)(100.0 * bytesReused / (bytesDownloaded + bytesReused)) : 0);
}

bool BinaryCacheStore::tryCopyPathWithLinks(Store & dstStore, const StorePath & storePath)
{
    /* Only works when destination is a LocalStore */
    auto * localStore = dynamic_cast<LocalStore *>(&dstStore);
    if (!localStore) {
        return false;
    }

    /* Try to get .ls file */
    auto lsPath = storePath.hashPart() + ".ls";
    std::optional<std::string> lsContent;

    try {
        lsContent = getFile(lsPath);
    } catch (...) {
        debug("no .ls file for %s", printStorePath(storePath));
        return false;
    }

    if (!lsContent) {
        debug("empty .ls file for %s", printStorePath(storePath));
        return false;
    }

    /* Parse .ls file */
    try {
        auto j = nlohmann::json::parse(*lsContent);

        /* Check version */
        int version = j.value("version", 1);
        if (version < 2) {
            debug(".ls file is version %d, need version 2 for Links", version);
            return false;
        }

        /* Parse listing */
        auto listing = j["root"].get<NarListing>();

        /* Check if all files have linkHash */
        if (!allFilesHaveLinkHash(listing)) {
            debug("not all files have linkHash in .ls file");
            return false;
        }

        /* Get path info for validation */
        auto info = queryPathInfo(storePath);

        /* Perform Links-based substitution */
        substituteWithLinks(*localStore, *info, listing);

        return true;

    } catch (std::exception & e) {
        debug("failed to parse or use .ls file: %s", e.what());
        return false;
    }
}

ref<const ValidPathInfo> BinaryCacheStore::addToStoreCommon(
    Source & narSource, RepairFlag repair, CheckSigsFlag checkSigs, fun<ValidPathInfo(HashResult)> mkInfo)
{
    auto fdTemp = createAnonymousTempFile();

    auto now1 = std::chrono::steady_clock::now();

    /* Read the NAR simultaneously into a CompressionSink+FileSink (to
       write the compressed NAR to disk), into a HashSink (to get the
       NAR hash), and into a NarAccessor (to get the NAR listing). */
    HashSink fileHashSink{HashAlgorithm::SHA256};
    std::shared_ptr<NarAccessor> narAccessor;
    HashSink narHashSink{HashAlgorithm::SHA256};
    auto narContent = std::make_shared<std::string>();
    {
        FdSink fileSink(fdTemp.get());
        TeeSink teeSinkCompressed{fileSink, fileHashSink};
        bool parallel = config.parallelCompression.overridden ? config.parallelCompression.get()
                                                              : config.compression.get() == CompressionAlgo::zstd;
        auto compressionSink =
            makeCompressionSink(config.compression, teeSinkCompressed, parallel, config.compressionLevel);
        TeeSink teeSinkUncompressed{*compressionSink, narHashSink};

        /* Also save NAR content for lazy accessor */
        LambdaSink narContentSink([&](std::string_view data) {
            narContent->append(data.data(), data.size());
        });
        TeeSink narSaveTee{teeSinkUncompressed, narContentSink};

        TeeSource teeSource{narSource, narSaveTee};
        auto listing = parseNarListing(teeSource);
        compressionSink->finish();
        fileSink.flush();

        /* Create lazy NAR accessor with saved content */
        narAccessor = makeLazyNarAccessor(
            std::move(listing),
            [narContent](uint64_t offset, uint64_t length, Sink & sink) {
                if (offset + length > narContent->size())
                    throw Error("NAR byte range out of bounds");
                sink(std::string_view(narContent->data() + offset, length));
            });
    }

    auto now2 = std::chrono::steady_clock::now();

    auto info = mkInfo(narHashSink.finish());
    auto narInfo = make_ref<NarInfo>(info);
    narInfo->compression = config.compression.to_string(); // FIXME: Make NarInfo use CompressionAlgo
    auto [fileHash, fileSize] = fileHashSink.finish();
    narInfo->fileHash = fileHash;
    narInfo->fileSize = fileSize;
    narInfo->url = "nar/" + narInfo->fileHash->to_string(HashFormat::Nix32, false) + ".nar"
                   + (config.compression == CompressionAlgo::xz       ? ".xz"
                      : config.compression == CompressionAlgo::bzip2  ? ".bz2"
                      : config.compression == CompressionAlgo::zstd   ? ".zst"
                      : config.compression == CompressionAlgo::lzip   ? ".lzip"
                      : config.compression == CompressionAlgo::lz4    ? ".lz4"
                      : config.compression == CompressionAlgo::brotli ? ".br"
                                                                      : "");

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now2 - now1).count();
    printMsg(
        lvlTalkative,
        "copying path '%1%' (%2% bytes, compressed %3$.1f%% in %4% ms) to binary cache",
        printStorePath(narInfo->path),
        info.narSize,
        ((1.0 - (double) fileSize / info.narSize) * 100.0),
        duration);

    /* Verify that all references are valid. This may do some .narinfo
       reads, but typically they'll already be cached. */
    for (auto & ref : info.references)
        try {
            if (ref != info.path)
                queryPathInfo(ref);
        } catch (InvalidPath &) {
            throw Error(
                "cannot add '%s' to the binary cache because the reference '%s' is not valid",
                printStorePath(info.path),
                printStorePath(ref));
        }

    /* Optionally write a JSON file containing a listing of the
       contents of the NAR. */
    if (config.writeNARListing) {
        auto listing = narAccessor->getListing();
        int version = 1;

        /* Try to add Links support (upload files to .links/ with hashes) */
        try {
            uploadToLinks(listing, narAccessor);
            version = 2;
        } catch (Error & e) {
            /* Failed to upload links - continue with v1 format */
            debug("Links upload failed: %s", e.what());
        }

        nlohmann::json j = {
            {"version", version},
            {"root", listing},
        };

        upsertFile(std::string(info.path.hashPart()) + ".ls", j.dump(), "application/json");
    }

    /* Optionally maintain an index of DWARF debug info files
       consisting of JSON files named 'debuginfo/<build-id>' that
       specify the NAR file and member containing the debug info. */
    if (config.writeDebugInfo) {

        CanonPath buildIdDir("lib/debug/.build-id");

        if (auto st = narAccessor->maybeLstat(buildIdDir); st && st->type == SourceAccessor::tDirectory) {

            ThreadPool threadPool(25);

            auto doFile = [&](std::string member, std::string key, std::string target) {
                checkInterrupt();

                nlohmann::json json;
                json["archive"] = target;
                json["member"] = member;

                // FIXME: or should we overwrite? The previous link may point
                // to a GC'ed file, so overwriting might be useful...
                if (fileExists(key))
                    return;

                printMsg(lvlTalkative, "creating debuginfo link from '%s' to '%s'", key, target);

                upsertFile(key, json.dump(), "application/json");
            };

            std::regex regex1("^[0-9a-f]{2}$");
            std::regex regex2("^[0-9a-f]{38}\\.debug$");

            for (auto & [s1, _type] : narAccessor->readDirectory(buildIdDir)) {
                auto dir = buildIdDir / s1;

                if (narAccessor->lstat(dir).type != SourceAccessor::tDirectory || !std::regex_match(s1, regex1))
                    continue;

                for (auto & [s2, _type] : narAccessor->readDirectory(dir)) {
                    auto debugPath = dir / s2;

                    if (narAccessor->lstat(debugPath).type != SourceAccessor::tRegular || !std::regex_match(s2, regex2))
                        continue;

                    auto buildId = s1 + s2;

                    std::string key = "debuginfo/" + buildId;
                    std::string target = "../" + narInfo->url;

                    threadPool.enqueue(std::bind(doFile, std::string(debugPath.rel()), key, target));
                }
            }

            threadPool.process();
        }
    }

    /* Atomically write the NAR file. */
    if (repair || !fileExists(narInfo->url)) {
        FdSource source{fdTemp.get()};
        source.restart(); /* Seek back to the start of the file. */
        stats.narWrite++;
        upsertFile(narInfo->url, source, "application/x-nix-nar", narInfo->fileSize);
    } else
        stats.narWriteAverted++;

    stats.narWriteBytes += info.narSize;
    stats.narWriteCompressedBytes += fileSize;
    stats.narWriteCompressionTimeMs += duration;

    narInfo->sign(*this, signers);

    /* Atomically write the NAR info file.*/
    writeNarInfo(narInfo);

    stats.narInfoWrite++;

    return narInfo;
}

void BinaryCacheStore::addToStore(
    const ValidPathInfo & info, Source & narSource, RepairFlag repair, CheckSigsFlag checkSigs)
{
    if (!repair && isValidPath(info.path)) {
        // FIXME: copyNAR -> null sink
        narSource.drain();
        return;
    }

    addToStoreCommon(narSource, repair, checkSigs, {[&](HashResult nar) {
                         /* FIXME reinstate these, once we can correctly do hash modulo sink as
                            needed. We need to throw here in case we uploaded a corrupted store path. */
                         // assert(info.narHash == nar.first);
                         // assert(info.narSize == nar.second);
                         return info;
                     }});
}

StorePath BinaryCacheStore::addToStoreFromDump(
    Source & dump,
    std::string_view name,
    FileSerialisationMethod dumpMethod,
    ContentAddressMethod hashMethod,
    HashAlgorithm hashAlgo,
    const StorePathSet & references,
    RepairFlag repair)
{
    std::optional<Hash> caHash;
    std::string nar;

    // Calculating Git hash from NAR stream not yet implemented. May not
    // be possible to implement in single-pass if the NAR is in an
    // inconvenient order. Could fetch after uploading, however.
    if (hashMethod.getFileIngestionMethod() == FileIngestionMethod::Git)
        unsupported("addToStoreFromDump");

    if (auto * dump2p = dynamic_cast<StringSource *>(&dump)) {
        auto & dump2 = *dump2p;
        // Hack, this gives us a "replayable" source so we can compute
        // multiple hashes more easily.
        //
        // Only calculate if the dump is in the right format, however.
        if (static_cast<FileIngestionMethod>(dumpMethod) == hashMethod.getFileIngestionMethod())
            caHash = hashString(HashAlgorithm::SHA256, dump2.s);
        switch (dumpMethod) {
        case FileSerialisationMethod::NixArchive:
            // The dump is already NAR in this case, just use it.
            nar = dump2.s;
            break;
        case FileSerialisationMethod::Flat: {
            // The dump is Flat, so we need to convert it to NAR with a
            // single file.
            StringSink s;
            dumpString(dump2.s, s);
            nar = std::move(s.s);
            break;
        }
        }
    } else {
        // Otherwise, we have to do th same hashing as NAR so our single
        // hash will suffice for both purposes.
        if (dumpMethod != FileSerialisationMethod::NixArchive || hashAlgo != HashAlgorithm::SHA256)
            unsupported("addToStoreFromDump");
    }
    StringSource narDump{nar};

    // Use `narDump` if we wrote to `nar`.
    Source & narDump2 = nar.size() > 0 ? static_cast<Source &>(narDump) : dump;

    return addToStoreCommon(
               narDump2,
               repair,
               CheckSigs,
               [&](HashResult nar) {
                   auto info = ValidPathInfo::makeFromCA(
                       *this,
                       name,
                       ContentAddressWithReferences::fromParts(
                           hashMethod,
                           caHash ? *caHash : nar.hash,
                           {
                               .others = references,
                               // caller is not capable of creating a self-reference, because this is content-addressed
                               // without modulus
                               .self = false,
                           }),
                       nar.hash);
                   info.narSize = nar.numBytesDigested;
                   return info;
               })
        ->path;
}

bool BinaryCacheStore::isValidPathUncached(const StorePath & storePath)
{
    // FIXME: this only checks whether a .narinfo with a matching hash
    // part exists. So ‘f4kb...-foo’ matches ‘f4kb...-bar’, even
    // though they shouldn't. Not easily fixed.
    return fileExists(narInfoFileFor(storePath));
}

std::optional<StorePath> BinaryCacheStore::queryPathFromHashPart(const std::string & hashPart)
{
    auto pseudoPath = StorePath(hashPart + "-" + MissingName);
    try {
        auto info = queryPathInfo(pseudoPath);
        return info->path;
    } catch (InvalidPath &) {
        return std::nullopt;
    }
}

void BinaryCacheStore::narFromPath(const StorePath & storePath, Sink & sink)
{
    auto info = queryPathInfo(storePath).cast<const NarInfo>();

    uint64_t narSize = 0;

    LambdaSink uncompressedSink{
        [&](std::string_view data) {
            narSize += data.size();
            sink(data);
        },
        [&]() {
            stats.narRead++;
            // stats.narReadCompressedBytes += nar->size(); // FIXME
            stats.narReadBytes += narSize;
        }};

    auto decompressor = makeDecompressionSink(info->compression, uncompressedSink);

    try {
        getFile(info->url, *decompressor);
    } catch (NoSuchBinaryCacheFile & e) {
        throw SubstituteGone(std::move(e.info()));
    }

    decompressor->finish();

    // Note: don't do anything here because it's never reached if we're called as a coroutine.
}

void BinaryCacheStore::queryPathInfoUncached(
    const StorePath & storePath, Callback<std::shared_ptr<const ValidPathInfo>> callback) noexcept
{
    auto callbackPtr = std::make_shared<decltype(callback)>(std::move(callback));

    try {
        auto uri = config.getReference().render(/*FIXME withParams=*/false);
        auto storePathS = printStorePath(storePath);
        auto act = std::make_shared<Activity>(
            *logger,
            lvlTalkative,
            actQueryPathInfo,
            fmt("querying info about '%s' on '%s'", storePathS, uri),
            Logger::Fields{storePathS, uri});
        PushActivity pact(act->id);

        auto narInfoFile = narInfoFileFor(storePath);

        getFile(narInfoFile, {[=, this](std::future<std::optional<std::string>> fut) {
                    try {
                        auto data = fut.get();

                        if (!data)
                            return (*callbackPtr)({});

                        stats.narInfoRead++;

                        (*callbackPtr)(
                            (std::shared_ptr<ValidPathInfo>) std::make_shared<NarInfo>(*this, *data, narInfoFile));

                        (void) act; // force Activity into this lambda to ensure it stays alive
                    } catch (...) {
                        callbackPtr->rethrow();
                    }
                }});
    } catch (...) {
        callbackPtr->rethrow();
    }
}

StorePath BinaryCacheStore::addToStore(
    std::string_view name,
    const SourcePath & path,
    ContentAddressMethod method,
    HashAlgorithm hashAlgo,
    const StorePathSet & references,
    PathFilter & filter,
    RepairFlag repair)
{
    /* FIXME: Make BinaryCacheStore::addToStoreCommon support
       non-recursive+sha256 so we can just use the default
       implementation of this method in terms of addToStoreFromDump. */

    auto h = hashPath(path, method.getFileIngestionMethod(), hashAlgo, filter).first;

    auto source = sinkToSource([&](Sink & sink) { path.dumpPath(sink, filter); });
    return addToStoreCommon(
               *source,
               repair,
               CheckSigs,
               [&](HashResult nar) {
                   auto info = ValidPathInfo::makeFromCA(
                       *this,
                       name,
                       ContentAddressWithReferences::fromParts(
                           method,
                           h,
                           {
                               .others = references,
                               // caller is not capable of creating a self-reference, because this is content-addressed
                               // without modulus
                               .self = false,
                           }),
                       nar.hash);
                   info.narSize = nar.numBytesDigested;
                   return info;
               })
        ->path;
}

std::string BinaryCacheStore::makeRealisationPath(const DrvOutput & id)
{
    return realisationsPrefix + "/" + id.drvPath.to_string() + "/" + id.outputName + ".doi";
}

void BinaryCacheStore::queryRealisationUncached(
    const DrvOutput & id, Callback<std::shared_ptr<const UnkeyedRealisation>> callback) noexcept
{
    auto outputInfoFilePath = makeRealisationPath(id);

    auto callbackPtr = std::make_shared<decltype(callback)>(std::move(callback));

    Callback<std::optional<std::string>> newCallback = {[=](std::future<std::optional<std::string>> fut) {
        try {
            auto data = fut.get();
            if (!data)
                return (*callbackPtr)({});

            std::shared_ptr<const UnkeyedRealisation> realisation;
            try {
                realisation = std::make_shared<const UnkeyedRealisation>(nlohmann::json::parse(*data));
            } catch (Error & e) {
                e.addTrace(
                    {},
                    "while parsing file '%s' as a build trace value for key '%s'",
                    outputInfoFilePath,
                    id.to_string());
                throw;
            }
            return (*callbackPtr)(std::move(realisation));
        } catch (...) {
            callbackPtr->rethrow();
        }
    }};

    getFile(outputInfoFilePath, std::move(newCallback));
}

void BinaryCacheStore::registerDrvOutput(const Realisation & info)
{
    if (diskCache)
        diskCache->upsertRealisation(config.getReference().render(/*FIXME withParams=*/false), info);
    upsertFile(
        makeRealisationPath(info.id),
        static_cast<nlohmann::json>(static_cast<const UnkeyedRealisation &>(info)).dump(),
        "application/json");
}

ref<RemoteFSAccessor> BinaryCacheStore::getRemoteFSAccessor(bool requireValidPath)
{
    return make_ref<RemoteFSAccessor>(ref<Store>(shared_from_this()), requireValidPath, config.localNarCache);
}

ref<SourceAccessor> BinaryCacheStore::getFSAccessor(bool requireValidPath)
{
    return getRemoteFSAccessor(requireValidPath);
}

std::shared_ptr<SourceAccessor> BinaryCacheStore::getFSAccessor(const StorePath & storePath, bool requireValidPath)
{
    return getRemoteFSAccessor(requireValidPath)->accessObject(storePath);
}

void BinaryCacheStore::addSignatures(const StorePath & storePath, const std::set<Signature> & sigs)
{
    /* Note: this is inherently racy since there is no locking on
       binary caches. In particular, with S3 this unreliable, even
       when addSignatures() is called sequentially on a path, because
       S3 might return an outdated cached version. */

    auto narInfo = make_ref<NarInfo>((NarInfo &) *queryPathInfo(storePath));

    narInfo->sigs.insert(sigs.begin(), sigs.end());

    writeNarInfo(narInfo);
}

std::optional<std::string> BinaryCacheStore::getBuildLogExact(const StorePath & path)
{
    auto logPath = "log/" + std::string(baseNameOf(printStorePath(path)));

    debug("fetching build log from binary cache '%s/%s'", config.getHumanReadableURI(), logPath);

    return getFile(logPath);
}

void BinaryCacheStore::addBuildLog(const StorePath & drvPath, std::string_view log)
{
    assert(drvPath.isDerivation());

    upsertFile(
        "log/" + std::string(drvPath.to_string()),
        (std::string) log, // FIXME: don't copy
        "text/plain; charset=utf-8");
}

} // namespace nix
