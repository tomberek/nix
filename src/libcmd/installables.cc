#include "globals.hh"
#include "installables.hh"
#include "installable-derived-path.hh"
#include "installable-attr-path.hh"
#include "outputs-spec.hh"
#include "users.hh"
#include "util.hh"
#include "command.hh"
#include "attr-path.hh"
#include "common-eval-args.hh"
#include "derivations.hh"
#include "eval-inline.hh"
#include "eval.hh"
#include "eval-settings.hh"
#include "get-drvs.hh"
#include "store-api.hh"
#include "shared.hh"
#include "eval-cache.hh"
#include "url.hh"
#include "registry.hh"
#include "build-result.hh"

#include <regex>
#include <queue>

#include <nlohmann/json.hpp>

#include "strings-inline.hh"

namespace nix {

namespace fs { using namespace std::filesystem; }

SourceExprCommand::SourceExprCommand()
{
    addFlag({
        .longName = "file",
        .shortName = 'f',
        .description =
            "Interpret [*installables*](@docroot@/command-ref/new-cli/nix.md#installables) as attribute paths relative to the Nix expression stored in *file*. "
            "If *file* is the character -, then a Nix expression will be read from standard input. "
            "Implies `--impure`.",
        .category = installablesCategory,
        .labels = {"file"},
        .handler = {&file},
        .completer = completePath
    });

    addFlag({
        .longName = "expr",
        .description = "Interpret [*installables*](@docroot@/command-ref/new-cli/nix.md#installables) as attribute paths relative to the Nix expression *expr*.",
        .category = installablesCategory,
        .labels = {"expr"},
        .handler = {&expr}
    });
}

MixReadOnlyOption::MixReadOnlyOption()
{
    addFlag({
        .longName = "read-only",
        .description =
            "Do not instantiate each evaluated derivation. "
            "This improves performance, but can cause errors when accessing "
            "store paths of derivations during evaluation.",
        .handler = {&settings.readOnlyMode, true},
    });
}

Args::CompleterClosure SourceExprCommand::getCompleteInstallable()
{
    return [this](AddCompletions & completions, size_t, std::string_view prefix) {
        completeInstallable(completions, prefix);
    };
}

void SourceExprCommand::completeInstallable(AddCompletions & completions, std::string_view prefix)
{
    try {
        if (file) {
            completions.setType(AddCompletions::Type::Attrs);

            evalSettings.pureEval = false;
            auto state = getEvalState();
            auto e =
                state->parseExprFromFile(
                    resolveExprPath(
                        lookupFileArg(*state, *file)));

            Value root;
            state->eval(e, root);

            auto autoArgs = getAutoArgs(*state);

            std::string prefix_ = std::string(prefix);
            auto sep = prefix_.rfind('.');
            std::string searchWord;
            if (sep != std::string::npos) {
                searchWord = prefix_.substr(sep + 1, std::string::npos);
                prefix_ = prefix_.substr(0, sep);
            } else {
                searchWord = prefix_;
                prefix_ = "";
            }

            auto [v, pos] = findAlongAttrPath(*state, prefix_, *autoArgs, root);
            Value &v1(*v);
            state->forceValue(v1, pos);
            Value v2;
            state->autoCallFunction(*autoArgs, v1, v2);

            if (v2.type() == nAttrs) {
                for (auto & i : *v2.attrs()) {
                    std::string_view name = state->symbols[i.name];
                    if (name.find(searchWord) == 0) {
                        if (prefix_ == "")
                            completions.add(std::string(name));
                        else
                            completions.add(prefix_ + "." + name);
                    }
                }
            }
        }
    } catch (EvalError&) {
        // Don't want eval errors to mess-up with the completion engine, so let's just swallow them
    }
}

DerivedPathWithInfo Installable::toDerivedPath()
{
    auto buildables = toDerivedPaths();
    if (buildables.size() != 1)
        throw Error("installable '%s' evaluates to %d derivations, where only one is expected", what(), buildables.size());
    return std::move(buildables[0]);
}

static StorePath getDeriver(
    ref<Store> store,
    const Installable & i,
    const StorePath & drvPath)
{
    auto derivers = store->queryValidDerivers(drvPath);
    if (derivers.empty())
        throw Error("'%s' does not have a known deriver", i.what());
    // FIXME: use all derivers?
    return *derivers.begin();
}

Installables SourceExprCommand::parseInstallables(
    ref<Store> store, std::vector<std::string> ss)
{
    Installables result;

    if (file || expr) {
        if (file && expr)
            throw UsageError("'--file' and '--expr' are exclusive");

        // FIXME: backward compatibility hack
        if (file) evalSettings.pureEval = false;

        auto state = getEvalState();
        auto vFile = state->allocValue();

        if (file == "-") {
            auto e = state->parseStdin();
            state->eval(e, *vFile);
        }
        else if (file) {
            auto dir = absPath(getCommandBaseDir());
            state->evalFile(lookupFileArg(*state, *file, &dir), *vFile);
        }
        else {
            Path dir = absPath(getCommandBaseDir());
            auto e = state->parseExprFromString(*expr, state->rootPath(dir));
            state->eval(e, *vFile);
        }

        for (auto & s : ss) {
            auto [prefix, extendedOutputsSpec] = ExtendedOutputsSpec::parse(s);
            result.push_back(
                make_ref<InstallableAttrPath>(
                    InstallableAttrPath::parse(
                        state, *this, vFile, std::move(prefix), std::move(extendedOutputsSpec))));
        }

    } else {

        for (auto & s : ss) {
            std::exception_ptr ex;

            auto [prefix_, extendedOutputsSpec_] = ExtendedOutputsSpec::parse(s);
            // To avoid clang's pedantry
            auto prefix = std::move(prefix_);
            auto extendedOutputsSpec = std::move(extendedOutputsSpec_);

            if (prefix.find('/') != std::string::npos) {
                try {
                    result.push_back(make_ref<InstallableDerivedPath>(
                        InstallableDerivedPath::parse(store, prefix, extendedOutputsSpec.raw)));
                    continue;
                } catch (BadStorePath &) {
                } catch (...) {
                    if (!ex)
                        ex = std::current_exception();
                }
            }

            std::rethrow_exception(ex);
        }
    }

    return result;
}

ref<Installable> SourceExprCommand::parseInstallable(
    ref<Store> store, const std::string & installable)
{
    auto installables = parseInstallables(store, {installable});
    assert(installables.size() == 1);
    return installables.front();
}

static SingleBuiltPath getBuiltPath(ref<Store> evalStore, ref<Store> store, const SingleDerivedPath & b)
{
    return std::visit(
        overloaded{
            [&](const SingleDerivedPath::Opaque & bo) -> SingleBuiltPath {
                return SingleBuiltPath::Opaque { bo.path };
            },
            [&](const SingleDerivedPath::Built & bfd) -> SingleBuiltPath {
                auto drvPath = getBuiltPath(evalStore, store, *bfd.drvPath);
                // Resolving this instead of `bfd` will yield the same result, but avoid duplicative work.
                SingleDerivedPath::Built truncatedBfd {
                    .drvPath = makeConstantStorePathRef(drvPath.outPath()),
                    .output = bfd.output,
                };
                auto outputPath = resolveDerivedPath(*store, truncatedBfd, &*evalStore);
                return SingleBuiltPath::Built {
                    .drvPath = make_ref<SingleBuiltPath>(std::move(drvPath)),
                    .output = { bfd.output, outputPath },
                };
            },
        },
        b.raw());
}

std::vector<BuiltPathWithResult> Installable::build(
    ref<Store> evalStore,
    ref<Store> store,
    Realise mode,
    const Installables & installables,
    BuildMode bMode)
{
    std::vector<BuiltPathWithResult> res;
    for (auto & [_, builtPathWithResult] : build2(evalStore, store, mode, installables, bMode))
        res.push_back(builtPathWithResult);
    return res;
}

static void throwBuildErrors(
    std::vector<KeyedBuildResult> & buildResults,
    const Store & store)
{
    std::vector<KeyedBuildResult> failed;
    for (auto & buildResult : buildResults) {
        if (!buildResult.success()) {
            failed.push_back(buildResult);
        }
    }

    auto failedResult = failed.begin();
    if (failedResult != failed.end()) {
        if (failed.size() == 1) {
            failedResult->rethrow();
        } else {
            StringSet failedPaths;
            for (; failedResult != failed.end(); failedResult++) {
                if (!failedResult->errorMsg.empty()) {
                    logError(ErrorInfo{
                        .level = lvlError,
                        .msg = failedResult->errorMsg,
                    });
                }
                failedPaths.insert(failedResult->path.to_string(store));
            }
            throw Error("build of %s failed", concatStringsSep(", ", quoteStrings(failedPaths)));
        }
    }
}

std::vector<std::pair<ref<Installable>, BuiltPathWithResult>> Installable::build2(
    ref<Store> evalStore,
    ref<Store> store,
    Realise mode,
    const Installables & installables,
    BuildMode bMode)
{
    if (mode == Realise::Nothing)
        settings.readOnlyMode = true;

    struct Aux
    {
        ref<ExtraPathInfo> info;
        ref<Installable> installable;
    };

    std::vector<DerivedPath> pathsToBuild;
    std::map<DerivedPath, std::vector<Aux>> backmap;

    for (auto & i : installables) {
        for (auto b : i->toDerivedPaths()) {
            pathsToBuild.push_back(b.path);
            backmap[b.path].push_back({.info = b.info, .installable = i});
        }
    }

    std::vector<std::pair<ref<Installable>, BuiltPathWithResult>> res;

    switch (mode) {

    case Realise::Nothing:
    case Realise::Derivation:
        printMissing(store, pathsToBuild, lvlError);

        for (auto & path : pathsToBuild) {
            for (auto & aux : backmap[path]) {
                std::visit(overloaded {
                    [&](const DerivedPath::Built & bfd) {
                        auto outputs = resolveDerivedPath(*store, bfd, &*evalStore);
                        res.push_back({aux.installable, {
                            .path = BuiltPath::Built {
                                .drvPath = make_ref<SingleBuiltPath>(getBuiltPath(evalStore, store, *bfd.drvPath)),
                                .outputs = outputs,
                             },
                            .info = aux.info}});
                    },
                    [&](const DerivedPath::Opaque & bo) {
                        res.push_back({aux.installable, {
                            .path = BuiltPath::Opaque { bo.path },
                            .info = aux.info}});
                    },
                }, path.raw());
            }
        }

        break;

    case Realise::Outputs: {
        if (settings.printMissing)
            printMissing(store, pathsToBuild, lvlInfo);

        auto buildResults = store->buildPathsWithResults(pathsToBuild, bMode, evalStore);
        throwBuildErrors(buildResults, *store);
        for (auto & buildResult : buildResults) {
            for (auto & aux : backmap[buildResult.path]) {
                std::visit(overloaded {
                    [&](const DerivedPath::Built & bfd) {
                        std::map<std::string, StorePath> outputs;
                        for (auto & [outputName, realisation] : buildResult.builtOutputs)
                            outputs.emplace(outputName, realisation.outPath);
                        res.push_back({aux.installable, {
                            .path = BuiltPath::Built {
                                .drvPath = make_ref<SingleBuiltPath>(getBuiltPath(evalStore, store, *bfd.drvPath)),
                                .outputs = outputs,
                            },
                            .info = aux.info,
                            .result = buildResult}});
                    },
                    [&](const DerivedPath::Opaque & bo) {
                        res.push_back({aux.installable, {
                            .path = BuiltPath::Opaque { bo.path },
                            .info = aux.info,
                            .result = buildResult}});
                    },
                }, buildResult.path.raw());
            }
        }

        break;
    }

    default:
        assert(false);
    }

    return res;
}

BuiltPaths Installable::toBuiltPaths(
    ref<Store> evalStore,
    ref<Store> store,
    Realise mode,
    OperateOn operateOn,
    const Installables & installables)
{
    if (operateOn == OperateOn::Output) {
        BuiltPaths res;
        for (auto & p : Installable::build(evalStore, store, mode, installables))
            res.push_back(p.path);
        return res;
    } else {
        if (mode == Realise::Nothing)
            settings.readOnlyMode = true;

        BuiltPaths res;
        for (auto & drvPath : Installable::toDerivations(store, installables, true))
            res.emplace_back(BuiltPath::Opaque{drvPath});
        return res;
    }
}

StorePathSet Installable::toStorePathSet(
    ref<Store> evalStore,
    ref<Store> store,
    Realise mode, OperateOn operateOn,
    const Installables & installables)
{
    StorePathSet outPaths;
    for (auto & path : toBuiltPaths(evalStore, store, mode, operateOn, installables)) {
        auto thisOutPaths = path.outPaths();
        outPaths.insert(thisOutPaths.begin(), thisOutPaths.end());
    }
    return outPaths;
}

StorePaths Installable::toStorePaths(
    ref<Store> evalStore,
    ref<Store> store,
    Realise mode, OperateOn operateOn,
    const Installables & installables)
{
    StorePaths outPaths;
    for (auto & path : toBuiltPaths(evalStore, store, mode, operateOn, installables)) {
        auto thisOutPaths = path.outPaths();
        outPaths.insert(outPaths.end(), thisOutPaths.begin(), thisOutPaths.end());
    }
    return outPaths;
}

StorePath Installable::toStorePath(
    ref<Store> evalStore,
    ref<Store> store,
    Realise mode, OperateOn operateOn,
    ref<Installable> installable)
{
    auto paths = toStorePathSet(evalStore, store, mode, operateOn, {installable});

    if (paths.size() != 1)
        throw Error("argument '%s' should evaluate to one store path", installable->what());

    return *paths.begin();
}

StorePathSet Installable::toDerivations(
    ref<Store> store,
    const Installables & installables,
    bool useDeriver)
{
    StorePathSet drvPaths;

    for (const auto & i : installables)
        for (const auto & b : i->toDerivedPaths())
            std::visit(overloaded {
                [&](const DerivedPath::Opaque & bo) {
                    drvPaths.insert(
                        bo.path.isDerivation()
                            ? bo.path
                        : useDeriver
                            ? getDeriver(store, *i, bo.path)
                        : throw Error("argument '%s' did not evaluate to a derivation", i->what()));
                },
                [&](const DerivedPath::Built & bfd) {
                    drvPaths.insert(resolveDerivedPath(*store, *bfd.drvPath));
                },
            }, b.path.raw());

    return drvPaths;
}

RawInstallablesCommand::RawInstallablesCommand()
{
    addFlag({
        .longName = "stdin",
        .description = "Read installables from the standard input. No default installable applied.",
        .handler = {&readFromStdIn, true}
    });

    expectArgs({
        .label = "installables",
        .handler = {&rawInstallables},
        .completer = getCompleteInstallable(),
    });
}

void RawInstallablesCommand::applyDefaultInstallables(std::vector<std::string> & rawInstallables)
{
    if (rawInstallables.empty()) {
        // FIXME: commands like "nix profile install" should not have a
        // default, probably.
        rawInstallables.push_back(".");
    }
}

void RawInstallablesCommand::run(ref<Store> store)
{
    if (readFromStdIn && !isatty(STDIN_FILENO)) {
        std::string word;
        while (std::cin >> word) {
            rawInstallables.emplace_back(std::move(word));
        }
    } else {
        applyDefaultInstallables(rawInstallables);
    }
    run(store, std::move(rawInstallables));
}

void InstallablesCommand::run(ref<Store> store, std::vector<std::string> && rawInstallables)
{
    auto installables = parseInstallables(store, rawInstallables);
    run(store, std::move(installables));
}

InstallableCommand::InstallableCommand()
    : SourceExprCommand()
{
    expectArgs({
        .label = "installable",
        .optional = true,
        .handler = {&_installable},
        .completer = getCompleteInstallable(),
    });
}

void InstallableCommand::run(ref<Store> store)
{
    auto installable = parseInstallable(store, _installable);
    run(store, std::move(installable));
}

void BuiltPathsCommand::applyDefaultInstallables(std::vector<std::string> & rawInstallables)
{
    if (rawInstallables.empty() && !all)
        rawInstallables.push_back(".");
}

BuiltPaths toBuiltPaths(const std::vector<BuiltPathWithResult> & builtPathsWithResult)
{
    BuiltPaths res;
    for (auto & i : builtPathsWithResult)
        res.push_back(i.path);
    return res;
}

}
