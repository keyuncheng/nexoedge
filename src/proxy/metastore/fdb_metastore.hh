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

// See also: https://apple.github.io/foundationdb/api-c.html

#define FDB_API_VERSION 730 // FDB client api version (must be specified before implementation)
#include <foundationdb/fdb_c.h>
#include <boost/uuid/uuid.hpp>

#include "metastore.hh"

// hard-code constants
const std::string _clusterFile = "/usr/local/etc/foundationdb/fdb.cluster";

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
    fdb_error fdb_err;
    pthread_t fdb_network_thread;
    FDBDatabase *fdb;

    std::mutex _lock;
    td::string _taskScanIt;
    bool _endOfPendingWriteSet;

    void exitOnError(fdb_error_t err);
    void runNetwork(void *dummy);
    FDBDatabase *getDatabase(const char *clusterFile);

    string getValue(string key);
    void setValueAndCommit(string key, string value);
}

#endif // define __FDB_METASTORE_HH__