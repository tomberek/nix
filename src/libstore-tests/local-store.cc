#include <gtest/gtest.h>

#include "nix/store/local-store.hh"
#include "nix/store/store-open.hh"
#include "nix/util/posix-source-accessor.hh"
#include "nix/util/source-path.hh"

// Needed for template specialisations. This is not good! When we
// overhaul how store configs work, this should be fixed.
#include "nix/util/args.hh"
#include "nix/util/config-impl.hh"
#include "nix/util/abstract-setting-to-json.hh"

#include <fstream>
#include <sys/stat.h>

namespace nix {

TEST(LocalStore, storeDir_absolutePath)
{
    std::filesystem::path storeDir =
#ifdef _WIN32
        "C:\\";
#else
        "/";
#endif
    storeDir /= "nix";
    storeDir /= "store";
    LocalStoreConfig config{"", {{"store", storeDir.string()}}};
    EXPECT_EQ(config.storeDir, storeDir.string());
}

TEST(LocalStore, storeDir_relativePath_rejected)
{
    EXPECT_THROW(LocalStoreConfig("", {{"store", (std::filesystem::path{"nix"} / "store").string()}}), UsageError);
}

TEST(LocalStore, storeDir_empty_rejected)
{
    EXPECT_THROW(LocalStoreConfig("", {{"store", ""}}), UsageError);
}

TEST(LocalStore, constructConfig_rootQueryParam)
{
#ifdef _WIN32
    constexpr std::string_view root = "C:\\foo\\bar";
#else
    constexpr std::string_view root = "/foo/bar";
#endif
    LocalStoreConfig config{
        "",
        {
            {
                "root",
                std::string{root},
            },
        },
    };

    EXPECT_EQ(config.rootDir.get(), std::optional<AbsolutePath>{std::string{root}});
}

TEST(LocalStore, constructConfig_rootPath)
{
#ifdef _WIN32
    constexpr std::string_view root = "C:\\foo\\bar";
#else
    constexpr std::string_view root = "/foo/bar";
#endif
    LocalStoreConfig config{std::string{root}, {}};

    EXPECT_EQ(config.rootDir.get(), std::optional<AbsolutePath>{std::string{root}});
}

TEST(LocalStore, constructConfig_to_string)
{
    LocalStoreConfig config{"", {}};
    EXPECT_EQ(config.getReference().to_string(), "local");
}

/* Links API tests */

TEST(LocalStore, getFileLinkHash_consistency)
{
    /* Test that getFileLinkHash produces consistent hashes for the same content */
    auto tempDir = std::filesystem::temp_directory_path() / "nix-test-links";
    std::filesystem::create_directories(tempDir);

    auto testFile = tempDir / "test-file";
    std::ofstream(testFile) << "test content\n";

    /* Use openStore with isolated paths */
    auto storeDir = tempDir / "store";
    auto stateDir = tempDir / "var";
    std::filesystem::create_directories(storeDir);
    std::filesystem::create_directories(stateDir);

    auto storeRef = openStore("local", {
        {"store", storeDir.string()},
        {"state", stateDir.string()},
        {"log", (stateDir / "log").string()}
    });
    auto * store = dynamic_cast<LocalStore *>(&*storeRef);
    ASSERT_NE(store, nullptr);

    /* Get hash twice - should be identical */
    auto hash1 = store->getFileLinkHash(testFile);
    auto hash2 = store->getFileLinkHash(testFile);

    EXPECT_EQ(hash1, hash2);

    /* Cleanup */
    std::filesystem::remove_all(tempDir);
}

TEST(LocalStore, hasLinkedFile_nonExistent)
{
    auto tempDir = std::filesystem::temp_directory_path() / "nix-test-links2";
    std::filesystem::create_directories(tempDir);

    auto storeDir = tempDir / "store";
    auto stateDir = tempDir / "var";
    std::filesystem::create_directories(storeDir);
    std::filesystem::create_directories(stateDir);

    auto storeRef = openStore("local", {
        {"store", storeDir.string()},
        {"state", stateDir.string()},
        {"log", (stateDir / "log").string()}
    });
    auto * store = dynamic_cast<LocalStore *>(&*storeRef);
    ASSERT_NE(store, nullptr);

    /* Check for a non-existent hash */
    Hash fakeHash = hashString(HashAlgorithm::SHA256, "nonexistent");
    EXPECT_FALSE(store->hasLinkedFile(fakeHash));

    std::filesystem::remove_all(tempDir);
}

TEST(LocalStore, addToLinks_and_retrieve)
{
    auto tempDir = std::filesystem::temp_directory_path() / "nix-test-links3";
    std::filesystem::create_directories(tempDir);

    auto storeDir = tempDir / "store";
    auto stateDir = tempDir / "var";
    std::filesystem::create_directories(storeDir);
    std::filesystem::create_directories(stateDir);

    auto storeRef = openStore("local", {
        {"store", storeDir.string()},
        {"state", stateDir.string()},
        {"log", (stateDir / "log").string()}
    });
    auto * store = dynamic_cast<LocalStore *>(&*storeRef);
    ASSERT_NE(store, nullptr);

    /* Create test content */
    std::string testContent = "Hello, Links!";
    StringSource source(testContent);

    /* Compute what the hash should be */
    auto testFile = tempDir / "test-content";
    std::ofstream(testFile) << testContent;
    auto expectedHash = store->getFileLinkHash(testFile);

    /* Add to .links/ */
    StringSource source2(testContent);
    auto linkPath = store->addToLinks(expectedHash, source2);

    /* Verify it exists */
    EXPECT_TRUE(store->hasLinkedFile(expectedHash));
    EXPECT_TRUE(std::filesystem::exists(linkPath));

    /* Verify content matches */
    std::ifstream ifs(linkPath);
    std::string retrievedContent((std::istreambuf_iterator<char>(ifs)),
                                  std::istreambuf_iterator<char>());
    EXPECT_EQ(testContent, retrievedContent);

    std::filesystem::remove_all(tempDir);
}

TEST(LocalStore, addToLinks_idempotent)
{
    auto tempDir = std::filesystem::temp_directory_path() / "nix-test-links4";
    std::filesystem::create_directories(tempDir);

    auto storeDir = tempDir / "store";
    auto stateDir = tempDir / "var";
    std::filesystem::create_directories(storeDir);
    std::filesystem::create_directories(stateDir);

    auto storeRef = openStore("local", {
        {"store", storeDir.string()},
        {"state", stateDir.string()},
        {"log", (stateDir / "log").string()}
    });
    auto * store = dynamic_cast<LocalStore *>(&*storeRef);
    ASSERT_NE(store, nullptr);

    std::string testContent = "Idempotent test";
    auto testFile = tempDir / "test-content";
    std::ofstream(testFile) << testContent;
    auto hash = store->getFileLinkHash(testFile);

    /* Add once */
    StringSource source1(testContent);
    auto path1 = store->addToLinks(hash, source1);

    /* Add again - should return same path without error */
    StringSource source2(testContent);
    auto path2 = store->addToLinks(hash, source2);

    EXPECT_EQ(path1, path2);

    std::filesystem::remove_all(tempDir);
}

TEST(LocalStore, hardLinkFromLinks_creates_link)
{
    auto tempDir = std::filesystem::temp_directory_path() / "nix-test-links5";
    std::filesystem::create_directories(tempDir);

    auto storeDir = tempDir / "store";
    auto stateDir = tempDir / "var";
    std::filesystem::create_directories(storeDir);
    std::filesystem::create_directories(stateDir);

    auto storeRef = openStore("local", {
        {"store", storeDir.string()},
        {"state", stateDir.string()},
        {"log", (stateDir / "log").string()}
    });
    auto * store = dynamic_cast<LocalStore *>(&*storeRef);
    ASSERT_NE(store, nullptr);

    /* Add a file to .links/ */
    std::string testContent = "Link test";
    auto testFile = tempDir / "test-content";
    std::ofstream(testFile) << testContent;
    auto hash = store->getFileLinkHash(testFile);

    StringSource source(testContent);
    store->addToLinks(hash, source);

    /* Create a hard link to a destination */
    auto destFile = tempDir / "destination";
    store->hardLinkFromLinks(hash, destFile);

    /* Verify destination exists and has same content */
    EXPECT_TRUE(std::filesystem::exists(destFile));

    std::ifstream ifs(destFile);
    std::string retrievedContent((std::istreambuf_iterator<char>(ifs)),
                                  std::istreambuf_iterator<char>());
    EXPECT_EQ(testContent, retrievedContent);

    /* Verify they're hard-linked (same inode) */
    auto linkPath = store->config->realStoreDir.get() / ".links" / hash.to_string(HashFormat::Nix32, false);
    auto stat1 = std::filesystem::status(linkPath);
    auto stat2 = std::filesystem::status(destFile);

    struct stat s1, s2;
    stat(linkPath.string().c_str(), &s1);
    stat(destFile.string().c_str(), &s2);
    EXPECT_EQ(s1.st_ino, s2.st_ino);

    std::filesystem::remove_all(tempDir);
}

TEST(LocalStore, links_basic_deduplication)
{
    /* Test that Links-based deduplication works at the LocalStore level */
    auto tempDir = std::filesystem::temp_directory_path() / "nix-test-links-dedup";
    std::filesystem::create_directories(tempDir);

    auto storeDir = tempDir / "store";
    auto stateDir = tempDir / "var";
    std::filesystem::create_directories(storeDir);
    std::filesystem::create_directories(stateDir);

    auto storeRef = openStore("local", {
        {"store", storeDir.string()},
        {"state", stateDir.string()},
        {"log", (stateDir / "log").string()}
    });
    auto * store = dynamic_cast<LocalStore *>(&*storeRef);
    ASSERT_NE(store, nullptr);

    /* Create two files with same content */
    std::string sharedContent = "Shared file content\n";
    auto testFile1 = tempDir / "file1.txt";
    auto testFile2 = tempDir / "file2.txt";
    std::ofstream(testFile1) << sharedContent;
    std::ofstream(testFile2) << sharedContent;

    /* Compute link hashes - should be identical */
    auto hash1 = store->getFileLinkHash(testFile1);
    auto hash2 = store->getFileLinkHash(testFile2);
    EXPECT_EQ(hash1, hash2);

    /* Add first file to .links/ */
    StringSource source1(sharedContent);
    auto linkPath = store->addToLinks(hash1, source1);

    /* Verify it exists */
    EXPECT_TRUE(store->hasLinkedFile(hash1));
    EXPECT_TRUE(std::filesystem::exists(linkPath));

    /* Add second file with same content - should be idempotent */
    StringSource source2(sharedContent);
    auto linkPath2 = store->addToLinks(hash2, source2);
    EXPECT_EQ(linkPath, linkPath2);

    /* Create hard links from .links/ to new locations */
    auto dest1 = tempDir / "dest1.txt";
    auto dest2 = tempDir / "dest2.txt";

    store->hardLinkFromLinks(hash1, dest1);
    store->hardLinkFromLinks(hash2, dest2);

    /* Verify both destinations exist */
    EXPECT_TRUE(std::filesystem::exists(dest1));
    EXPECT_TRUE(std::filesystem::exists(dest2));

    /* Verify they all have the same inode (hard linked) */
    struct stat s1, s2, s3;
    stat(linkPath.string().c_str(), &s1);
    stat(dest1.string().c_str(), &s2);
    stat(dest2.string().c_str(), &s3);

    EXPECT_EQ(s1.st_ino, s2.st_ino);
    EXPECT_EQ(s1.st_ino, s3.st_ino);

    /* Verify content is correct */
    std::ifstream ifs(dest1);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                         std::istreambuf_iterator<char>());
    EXPECT_EQ(content, sharedContent);

    std::filesystem::remove_all(tempDir);
}

} // namespace nix
