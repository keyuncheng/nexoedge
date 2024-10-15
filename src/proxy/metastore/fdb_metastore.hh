// SPDX-License-Identifier: Apache-2.0

/**
 * @brief FoundationDB-based metadata store
 *
 */

#ifndef __FDB_METASTORE_HH__
#define __FDB_METASTORE_HH__

#include <pthread.h>
#include <mutex>
#include <string>
#include <utility>

// See also: https://apple.github.io/foundationdb/api-c.html

#define FDB_API_VERSION 730 // FDB client api version (must be specified before implementation)
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>
#include <boost/uuid/uuid.hpp>
#include <nlohmann/json.hpp>

#include "metastore.hh"

// hard-code constants
const std::string _FDBClusterFile = "/etc/foundationdb/fdb.cluster";

class FDBMetaStore : public MetaStore
{
public:
    FDBMetaStore();
    ~FDBMetaStore();

    /**
     * See MetaStore::putMeta()
     **/
    bool putMeta(const File &f);

    /**
     * See MetaStore::getMeta()
     **/
    bool getMeta(File &f, int getBlocks = 3);

    /**
     * See MetaStore::deleteMeta()
     **/
    bool deleteMeta(File &f);

    /**
     * See MetaStore::renameMeta()
     **/
    bool renameMeta(File &sf, File &df);

    /**
     * See MetaStore::updateTimestamps()
     **/
    bool updateTimestamps(const File &f);

    /**
     * See MetaStore::updateChunks()
     **/
    int updateChunks(const File &f, int version);

    /**
     * See MetaStore::getFileName(boost::uuids::uuid, File)
     **/
    bool getFileName(boost::uuids::uuid fuuid, File &f);

    /**
     * See MetaStore::getFileList()
     **/
    unsigned int getFileList(FileInfo **list, unsigned char namespaceId = INVALID_NAMESPACE_ID, bool withSize = true, bool withTime = true, bool withVersions = false, std::string prefix = "");

    /**
     * See MetaStore::getFolderList()
     **/
    unsigned int getFolderList(std::vector<std::string> &list, unsigned char namespaceId = INVALID_NAMESPACE_ID, std::string prefix = "", bool skipSubfolders = true);

    /**
     * See MetaStore::getMaxNumKeysSupported()
     **/
    unsigned long int getMaxNumKeysSupported();

    /**
     * See MetaStore::getNumFiles()
     **/
    unsigned long int getNumFiles();

    /**
     * See MetaStore::getNumFilesToRepair()
     **/
    unsigned long int getNumFilesToRepair();

    /**
     * See MetaStore::getFilesToRepair()
     **/
    int getFilesToRepair(int numFiles, File files[]);

    /**
     * See MetaStore::markFileAsNeedsRepair()
     **/
    bool markFileAsNeedsRepair(const File &file);

    /**
     * See MetaStore::markFileAsRepaired()
     **/
    bool markFileAsRepaired(const File &file);

    /**
     * See MetaStore::markFileAsPendingWriteToCloud()
     **/
    bool markFileAsPendingWriteToCloud(const File &file);

    /**
     * See MetaStore::markFileAsWrittenToCloud()
     **/
    bool markFileAsWrittenToCloud(const File &file, bool removePending = false);

    /**
     * See MetaStore::getFilesPendingWriteToCloud()
     **/
    int getFilesPendingWriteToCloud(int numFiles, File files[]);

    /**
     * See MetaStore::updateFileStatus()
     **/
    bool updateFileStatus(const File &file);

    /**
     * See MetaStore::getNextFileForTaskCheck()
     **/
    bool getNextFileForTaskCheck(File &file);

    /**
     * See MetaStore::lockFile()
     **/
    bool lockFile(const File &file);

    /**
     * See MetaStore::unlockFile()
     **/
    bool unlockFile(const File &file);

    /**
     * See MetaStore::addChunkToJournal()
     **/
    bool addChunkToJournal(const File &file, const Chunk &chunk, int containerId, bool isWrite);

    /**
     * See MetaStore::updateChunkInJournal()
     **/
    bool updateChunkInJournal(const File &file, const Chunk &chunk, bool isWrite, bool deleteRecord, int containerId);

    /**
     * See MetaStore::getFileJournal()
     **/
    void getFileJournal(const FileInfo &file, std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> &records);

    /**
     * See MetaStore::getFilesWithJournal()
     **/
    int getFilesWithJounal(FileInfo **list);

    /**
     * See MetaStore::fileHasJournal()
     **/
    bool fileHasJournal(const File &file);

private:
    // FDB-based variables
    pthread_t _fdb_network_thread;
    FDBDatabase *_db;

    std::mutex _lock;
    std::string _taskScanIt;
    bool _endOfPendingWriteSet;

    // FDB operations
    void exitOnError(fdb_error_t err);
    static void *runNetwork(void *args);
    FDBDatabase *getDatabase(std::string clusterFile);
    std::pair<bool, std::string> getValue(std::string key);
    void setValueAndCommit(std::sgetring key, std::string value);

    // helper functions (mostly copied from RedisMetaStore)
    bool getFileName(char fileUuidKey[], File &f);
    int genFileKey(unsigned char namespaceId, const char *name, int nameLength, char key[]);
    int genVersionedFileKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[]);
    int genFileVersionListKey(unsigned char namespaceId, const char *name, int nameLength, char key[]);
    bool genFileUuidKey(unsigned char namespaceId, boost::uuids::uuid uuid, char key[]);
    std::string getFilePrefix(const char name[], bool noEndingSlash = false);
    int genChunkKeyPrefix(int chunkId, char prefix[]);
    const char *getBlockKeyPrefix(bool unique);
    int genBlockKey(int blockId, char prefix[], bool unqiue);
};

#endif // define __FDB_METASTORE_HH__
