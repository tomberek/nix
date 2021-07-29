#include "binary-cache-store.hh"
#include "filetransfer.hh"
#include "globals.hh"
#include "nar-info-disk-cache.hh"
#include "callback.hh"

namespace nix {

struct HttpBinaryCacheStoreConfig : virtual BinaryCacheStoreConfig
{
    using BinaryCacheStoreConfig::BinaryCacheStoreConfig;

    const std::string name() { return "Http Binary Cache Store"; }
};

class HttpBinaryCacheStore : public virtual HttpBinaryCacheStoreConfig, public virtual BinaryCacheStore
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
    HttpBinaryCacheStore(
        const std::string & scheme,
        const Path & _cacheUri,
        const Params & params);

    std::string getUri() override;

    void init() override;

    static std::set<std::string> uriSchemes();

protected:

    void maybeDisable();

    void checkEnabled();

    bool fileExists(const std::string & path) override;

    void upsertFile(const std::string & path,
        std::shared_ptr<std::basic_iostream<char>> istream,
        const std::string & mimeType) override;

    FileTransferRequest makeRequest(const std::string & path);

    void getFile(const std::string & path, Sink & sink) override;

    void getFile(const std::string & path,
        Callback<std::shared_ptr<std::string>> callback) noexcept override;

};

static nix::RegisterStoreImplementation<HttpBinaryCacheStore, HttpBinaryCacheStoreConfig> regHttpBinaryCacheStore;
}
