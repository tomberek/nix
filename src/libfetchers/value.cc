#include "fetchers.hh"
#include "store-api.hh"
#include <nlohmann/json.hpp>

namespace nix::fetchers {

struct ValueInputScheme : InputScheme
{
    std::optional<Input> inputFromURL(const ParsedURL & url) override
    {
        if (url.scheme != "value") return {};

        if (url.authority && *url.authority != "")
            throw Error("value URL '%s' should not have an authority ('%s')", url.url, *url.authority);

        Input input;
        input.attrs.insert_or_assign("type", "value");
        input.attrs.insert_or_assign("value", url.path);

        for (auto & [name, value] : url.query)
            if (name == "rev" || name == "narHash")
                input.attrs.insert_or_assign(name, value);
            else if (name == "revCount" || name == "lastModified") {
                if (auto n = string2Int<uint64_t>(value))
                    input.attrs.insert_or_assign(name, *n);
                else
                    throw Error("value URL '%s' has invalid parameter '%s'", url.to_string(), name);
            }
            else
                throw Error("value URL '%s' has unsupported parameter '%s'", url.to_string(), name);

        return input;
    }

    std::optional<Input> inputFromAttrs(const Attrs & attrs) override
    {
        getStrAttr(attrs, "value");

        for (auto & [name, value] : attrs)
            /* Allow the user to pass in "fake" tree info
               attributes. This is useful for making a pinned tree
               work the same as the repository from which is exported
               (e.g. value:/nix/store/...-source?lastModified=1585388205&rev=b0c285...). */
            if (name == "type" || name == "rev" || name == "revCount" || name == "lastModified" || name == "narHash" || name == "value")
                // checked in Input::fromAttrs
                ;
            else
                throw Error("unsupported value input attribute '%s'", name);

        Input input;
        input.attrs = attrs;
        input.attrs.insert_or_assign("type", "value");
        return input;
    }

    ParsedURL toURL(const Input & input) override
    {
        auto query = attrsToQuery(input.attrs);
        query.erase("value");
        query.erase("type");
        return ParsedURL {
            .scheme = "value",
            .path = getStrAttr(input.attrs, "value"),
            .query = query,
        };
    }

    bool hasAllInfo(const Input & input) override
    {
        return true;
    }

    std::optional<Path> getSourcePath(const Input & input) override
    {
        return getStrAttr(input.attrs, "value");
    }

    void markChangedFile(const Input & input, std::string_view file, std::optional<std::string> commitMsg) override
    {
        // nothing to do
    }

    std::pair<Tree, Input> fetch(ref<Store> store, const Input & input) override
    {
        auto value = getStrAttr(input.attrs, "value");

        Activity act(*logger, lvlTalkative, actUnknown, fmt("creating '%s'", value));

        // FIXME: check whether access to 'path' is allowed.
        auto storePath = store->addTextToStore("value",value,StorePathSet{},NoRepair);

        if (storePath.name() != "value" && store->isValidPath(storePath))
            store->addTempRoot(storePath);

        return {
            Tree(store->toRealPath(storePath), std::move(storePath)),
            input
        };
    }
};

static auto rValueInputScheme = OnStartup([] { registerInputScheme(std::make_unique<ValueInputScheme>()); });

}
