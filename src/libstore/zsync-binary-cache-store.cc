//#include "binary-cache-store.hh"
#include "http-binary-cache-store.hh"
#include "filetransfer.hh"
#include "globals.hh"
#include "nar-info-disk-cache.hh"
#include "callback.hh"
#include "util.hh"


namespace nix {

MakeError(UploadToHTTP, Error);
MakeError(NotImplemented, Error);

struct ZsyncBinaryCacheStoreConfig : virtual HttpBinaryCacheStoreConfig
{
    using HttpBinaryCacheStoreConfig::HttpBinaryCacheStoreConfig;

    const std::string name() override { return "Http+zsync Binary Cache Store"; }
};

class ZsyncBinaryCacheStore : public virtual ZsyncBinaryCacheStoreConfig, public virtual HttpBinaryCacheStore, public virtual BinaryCacheStore
{
private:

    Path cacheUri;

    struct State
    {
        bool enabled = true;
        std::chrono::steady_clock::time_point disabledUntil;
    };

    Sync<State> _state;

public:

    ZsyncBinaryCacheStore(
        const std::string & scheme,
        const Path & _cacheUri,
        const Params & params)
        : StoreConfig(params)
        , BinaryCacheStoreConfig(params)
        , HttpBinaryCacheStoreConfig(params)
        , ZsyncBinaryCacheStoreConfig(params)
        , Store(params)
        , BinaryCacheStore(params)
        , HttpBinaryCacheStore(scheme.substr(0,scheme.find("+")),_cacheUri,params)
        , cacheUri(scheme + "://" + _cacheUri)
    {
        if (cacheUri.back() == '/')
            cacheUri.pop_back();
        auto myScheme = scheme.substr(0,scheme.find("+"));
        cacheUri = myScheme + "://" + _cacheUri;

        diskCache = getNarInfoDiskCache();
    }

    std::string getUri() override
    {
        return cacheUri;
    }

    void init() override
    {
        // FIXME: do this lazily?
        if (auto cacheInfo = diskCache->cacheExists(cacheUri)) {
            wantMassQuery.setDefault(cacheInfo->wantMassQuery ? "true" : "false");
            priority.setDefault(fmt("%d", cacheInfo->priority));
        } else {
            try {
                BinaryCacheStore::init();
            } catch (UploadToHTTP &) {
                throw Error("'%s' does not appear to be a binary cache", cacheUri);
            }
            diskCache->createCache(cacheUri, storeDir, wantMassQuery, priority);
        }
    }

    static std::set<std::string> uriSchemes()
    {
        auto ret = std::set<std::string>({"http+zsync", "https+zsync"});
        return ret;
    }

protected:

    void maybeDisable()
    {
        auto state(_state.lock());
        if (state->enabled && settings.tryFallback) {
            int t = 60;
            printError("disabling binary cache '%s' for %s seconds", getUri(), t);
            state->enabled = false;
            state->disabledUntil = std::chrono::steady_clock::now() + std::chrono::seconds(t);
        }
    }

    void checkEnabled()
    {
        auto state(_state.lock());
        if (state->enabled) return;
        if (std::chrono::steady_clock::now() > state->disabledUntil) {
            state->enabled = true;
            debug("re-enabling binary cache '%s'", getUri());
            return;
        }
        throw SubstituterDisabled("substituter '%s' is disabled", getUri());
    }

    bool fileExists(const std::string & path) override
    {
        checkEnabled();

        try {
            FileTransferRequest request(makeRequest(path));
            request.head = true;
            getFileTransfer()->download(request);
            return true;
        } catch (FileTransferError & e) {
            /* S3 buckets return 403 if a file doesn't exist and the
               bucket is unlistable, so treat 403 as 404. */
            if (e.error == FileTransfer::NotFound || e.error == FileTransfer::Forbidden)
                return false;
            maybeDisable();
            throw;
        }
    }

    void upsertFile(const std::string & path,
        std::shared_ptr<std::basic_iostream<char>> istream,
        const std::string & mimeType) override
    {
        //throw NotImplemented("not implemented: uploading to Zsync+HTTP binary cache at '%s'", cacheUri);
        auto req = makeRequest(path);
        req.data = std::make_shared<string>(StreamToSourceAdapter(istream).drain());
        req.mimeType = mimeType;
        try {
            getFileTransfer()->upload(req);
        } catch (FileTransferError & e) {
            throw UploadToHTTP("while uploading to HTTP binary cache at '%s': %s", cacheUri, e.msg());
        }
    }

    FileTransferRequest makeRequest(const std::string & path)
    {
        return FileTransferRequest(
            hasPrefix(path, "https://") || hasPrefix(path, "http://") || hasPrefix(path, "file://")
            ? path
            : cacheUri + "/" + path);

    }

    void getFile(const std::string & path, Sink & sink) override
    {
        if (!hasSuffix(path,".nar.gz")){
                return HttpBinaryCacheStore::getFile(path,sink);
        }
        checkEnabled();
        auto basePath = baseNameOf(path);
        RunOptions options(
            "sh",
            { "-c", ("zsync " + cacheUri + "/" + path + ".zsync $(find /tmp/*.nar.gz -iname '*.nar.gz' -printf '-i %p ') 1>&2" + "; cat ").append(basePath) }
        );
        options.chdir = "/tmp";
        options.standardOut = &sink;
        try {
            runProgram2(options);
        } catch (ExecError & e) {
            throw;
        }
    }
    std::shared_ptr<std::string> getFile(const std::string & path) {
        StringSink sink;
        try {
            getFile(path, sink);
        } catch (NoSuchBinaryCacheFile &) {
            return nullptr;
        }
        return sink.s;
    }

    void getFile(const std::string & path,
        Callback<std::shared_ptr<std::string>> callback) noexcept
    {
            try {
                callback(getFile(path));
            } catch (...) { callback.rethrow(); }
        }

};

static RegisterStoreImplementation<ZsyncBinaryCacheStore, ZsyncBinaryCacheStoreConfig> regZsyncBinaryCacheStore;

}
