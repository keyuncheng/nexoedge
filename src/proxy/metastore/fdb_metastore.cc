// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h> // exit(), strtol()
#include <stdio.h>  // sprintf()
#include <boost/uuid/uuid_io.hpp>
#include <glog/logging.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include "fdb_metastore.hh"
#include "../../common/config.hh"
#include "../../common/define.hh"

// change defs's prefix to FDB_
#define FDB_NUM_RESERVED_SYSTEM_KEYS (8)
#define FDB_FILE_LOCK_KEY "//snccFLock"
#define FDB_FILE_PIN_STAGED_KEY "//snccFPinStaged"
#define FDB_FILE_REPAIR_KEY "//snccFRepair"
#define FDB_FILE_PENDING_WRITE_KEY "//snccFPendingWrite"
#define FDB_FILE_PENDING_WRITE_COMP_KEY "//snccFPendingWriteComp"
#define FDB_BG_TASK_PENDING_KEY "//snccFBgTask"
#define FDB_DIR_LIST_KEY "//snccDirList"
#define FDB_JL_LIST_KEY "//snccJournalFSet"

#define FDB_MAX_KEY_SIZE (64)
#define FDB_NUM_REQ_FIELDS (10)

static std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength);

FDBMetaStore::FDBMetaStore()
{
    // didn't use for now
    Config &config = Config::getInstance();

    LOG(INFO) << "FDBMetaStore:: MetaStoreType: " << config.getProxyMetaStoreType();

    // select API version
    fdb_select_api_version(FDB_API_VERSION);
    LOG(INFO) << "creating FDB Client connection, selected API version: " << FDB_API_VERSION;

    // setup network and connect to database
    exitOnError(fdb_setup_network());
    if (pthread_create(&_fdb_network_thread, NULL, FDBMetaStore::runNetwork, NULL))
    {
        LOG(ERROR) << "FDBMetaStore:: failed to create network thread";
        exit(1);
    }
    _db = getDatabase(_FDBClusterFile);

    _taskScanIt = "0";
    _endOfPendingWriteSet = true;
    LOG(INFO) << "FoundationDB MetaStore connection init; clusterFile: " << _FDBClusterFile;
}

FDBMetaStore::~FDBMetaStore()
{
    // destroy database and stop network
    fdb_database_destroy(_db);
    exitOnError(fdb_stop_network());
    pthread_join(_fdb_network_thread, NULL);
}

bool FDBMetaStore::putMeta(const File &f)
{
    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX];

    // file key (format: namespace_filename)
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);
    // file prefix
    std::string filePrefix = getFilePrefix(fileKey);

    // TODO: find current version of the file, and perform updates accordingly
    int curFileVersion = -1;

    // set metadata for the file<curFileVersion>, format: serialized JSON
    // string
    bool isEmptyFile = f.size == 0;
    unsigned char *codingState = isEmptyFile || f.codingMeta.codingState == NULL ? (unsigned char *)"" : f.codingMeta.codingState;
    int deleted = isEmptyFile ? f.isDeleted : 0;
    size_t numUniqueBlocks = ;
    size_t numDuplicateBlocks = f.duplicateBlocks.size();

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // key: filename, value: JSON obj string
    nlohmann::json *j = new nlohmann::json();

    // records for the current version
    char verFileKey[PATH_MAX];
    int vnameLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, verFileKey);

    nlohmann::json *vj = new nlohmann::json();
    vj->["name"] = std::string(f.name, f.name + f.nameLength);
    vj->["uuid"] = boost::uuids::to_string(f.uuid);
    vj->["size"] = std::to_string(f.size);
    vj->["numC"] = std::to_string(f.numChunks);
    vj->["sc"] = f.storageClass;
    vj->["cs"] = std::string(f.codingMeta.coding);
    vj->["n"] = std::to_string(f.codingMeta.n);
    vj->["k"] = std::to_string(f.codingMeta.k);
    vj->["f"] = std::to_string(f.codingMeta.f);
    vj->["maxCS"] = std::to_string(f.codingMeta.maxChunkSize);
    vj->["codingStateS"] = std::to_string(f.codingMeta.codingStateSize);
    vj->["codingState"] = std::string(codingState);
    vj->["numS"] = std::string(f.numStripes);
    vj->["ver"] = std::to_string(f.version);
    vj->["ctime"] = std::to_string(f.ctime);
    vj->["atime"] = std::to_string(f.atime);
    vj->["mtime"] = std::to_string(f.mtime);
    vj->["tctime"] = std::to_string(f.tctime);
    vj->["md5"] = std::string(f.md5, f.md5 + MD5_DIGEST_LENGTH);
    vj->["sg_size"] = std::to_string(f.staged.size);
    vj->["sg_sc"] = f.staged.storageClass;
    vj->["sg_cs"] = std::to_string(f.staged.codingMeta.coding);
    vj->["sg_n"] = std::to_string(f.staged.codingMeta.n);
    vj->["sg_k"] = std::to_string(f.staged.codingMeta.k);
    vj->["sg_f"] = std::to_string(f.staged.codingMeta.f);
    vj->["sg_maxCS"] = std::to_string(f.staged.codingMeta.maxChunkSize);
    vj->["sg_mtime"] = std::to_string(f.staged.mtime);
    vj->["dm"] = std::to_string(deleted);
    vj->["numUB"] = std::to_string(numUniqueBlocks);
    vj->["numDB"] = std::to_string(numDuplicateBlocks);

    // container ids
    char chunkName[MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++)
    {
        genChunkKeyPrefix(f.chunks[i].getChunkId(), chunkName);
        std::string cidKey = std::string(chunkName) + std::string("-cid");
        vj->[cidKey.c_str()] = std::to_string(f.containerIds[i]);
        std::string csizeKey = std::string(chunkName) + std::string("-size");
        vj->[csizeKey.c_str()] = std::to_string(f.chunks[i].size);
        std::string cmd5Key = std::string(chunkName) + std::string("-md5");
        vj->[cmd5Key.c_str()] = std::string(f.chunks[i].md5, f.chunks[i].md5 + MD5_DIGEST_LENGTH);
        std::string cmd5Bad = std::string(chunkName) + std::string("-bad");
        vj->[cmd5Bad.c_str()] = std::to_string((f.chunksCorrupted ? f.chunksCorrupted[i] : 0));
    }

    // deduplication fingerprints and block mapping
    char blockName[MAX_KEY_SIZE];
    size_t blockId = 0;
    for (auto it = f.uniqueBlocks.begin(); it != f.uniqueBlocks.end(); it++, blockId++)
    { // deduplication fingerprints
        genBlockKey(blockId, blockName, /* is unique */ true);
        std::string fp = it->second.first.get();
        // logical offset, length, fingerprint, physical offset
        vj->[std::string(blockName).c_str()] = std::to_string(&it->first._offset) + std::to_string(&it->first._length) + fp.data() + std::to_string(it->second.second);
    }
    blockId = 0;
    for (auto it = f.duplicateBlocks.begin(); it != f.duplicateBlocks.end(); it++, blockId++)
    {
        genBlockKey(blockId, blockName, /* is unique */ false);
        std::string fp = it->second.get();
        // logical offset, length, fingerprint
        vj->[std::string(blockName).c_str()] = std::to_string(&it->first._offset) + std::to_string(&it->first._length) + fp.data();
    }

    char fidKey[MAX_KEY_SIZE + 64];
    int setKey = 0;

    // add uuid-to-file-name maping
    if (genFileUuidKey(f.namespaceId, f.uuid, fidKey) == false)
    {
        LOG(WARNING) << "File uuid " << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    }
    else
    {
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fidKey), MAX_KEY_SIZE + 64, reinterpret_cast<const uint8_t *>(f.name), value.size());
        setKey += 1;
    }

    // set key
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(key.c_str()), key.size(), reinterpret_cast<const uint8_t *>(value.c_str()), value.size());

    FDBFuture *fset = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(fset));

    fdb_future_destroy(fset);

    // TODO:
    // update the corresponding directory prefix set of this file
    // redisAppendCommand(
    //     _cxt, "SADD %s %b", prefix.c_str(), filename, (size_t)nameLength);
    // // update global directory list
    // redisAppendCommand(
    //     _cxt, "SADD %s %s", DIR_LIST_KEY, prefix.c_str());
    // setKey += 2;

    LOG(INFO) << "FDBMetaStore:: setValue(); key: " << key << ", value: " << value;

    return true;
}

bool FDBMetaStore::getMeta(File &f, int getBlocks)
{
    return true;
}

bool FDBMetaStore::deleteMeta(File &f)
{
    return true;
}

bool FDBMetaStore::renameMeta(File &sf, File &df)
{
    return true;
}

bool FDBMetaStore::updateTimestamps(const File &f)
{
    return true;
}

int FDBMetaStore::updateChunks(const File &f, int version)
{
    return true;
}

bool FDBMetaStore::getFileName(boost::uuids::uuid fuuid, File &f)
{
    return true;
}

unsigned int FDBMetaStore::getFileList(FileInfo **list, unsigned char namespaceId, bool withSize, bool withTime, bool withVersions, std::string prefix)
{
    return 0;
}

unsigned int FDBMetaStore::getFolderList(std::vector<std::string> &list, unsigned char namespaceId, std::string prefix, bool skipSubfolders)
{
    return 0;
}

unsigned long int FDBMetaStore::getMaxNumKeysSupported()
{
    return 0;
}

unsigned long int FDBMetaStore::getNumFiles()
{
    return 0;
}

unsigned long int FDBMetaStore::getNumFilesToRepair()
{
    return 0;
}

int FDBMetaStore::getFilesToRepair(int numFiles, File files[])
{
    return 0;
}

bool FDBMetaStore::markFileAsNeedsRepair(const File &file)
{
    return false;
}

bool FDBMetaStore::markFileAsRepaired(const File &file)
{
    return false;
}

bool FDBMetaStore::markFileAsPendingWriteToCloud(const File &file)
{
    return false;
}

bool FDBMetaStore::markFileAsWrittenToCloud(const File &file, bool removePending)
{
    return false;
}

int FDBMetaStore::getFilesPendingWriteToCloud(int numFiles, File files[])
{
    return false;
}

bool FDBMetaStore::updateFileStatus(const File &file)
{
    return true;
}

bool FDBMetaStore::getNextFileForTaskCheck(File &file)
{
    return true;
}

bool FDBMetaStore::lockFile(const File &file)
{
    return true;
}

bool FDBMetaStore::unlockFile(const File &file)
{
    return true;
}

bool FDBMetaStore::addChunkToJournal(const File &file, const Chunk &chunk, int containerId, bool isWrite)
{
    extractJournalFieldKeyParts("", 0);
    return true;
}

bool FDBMetaStore::updateChunkInJournal(const File &file, const Chunk &chunk, bool isWrite, bool deleteRecord, int containerId)
{
    return true;
}

void FDBMetaStore::getFileJournal(const FileInfo &file, std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> &records)
{
}

int FDBMetaStore::getFilesWithJounal(FileInfo **list)
{
    return 0;
}

bool FDBMetaStore::fileHasJournal(const File &file)
{
    return true;
}

void FDBMetaStore::exitOnError(fdb_error_t err)
{
    if (err)
    {
        LOG(ERROR) << "FoundationDB MetaStore error: " << fdb_get_error(err);
        exit(1);
    }
    return;
}

void *FDBMetaStore::runNetwork(void *args)
{
    fdb_error_t err = fdb_run_network();
    if (err)
    {
        LOG(ERROR) << "FDBMetaStore::runNetwork fdb_run_network() error";
        exit(1);
    }

    return NULL;
}

FDBDatabase *FDBMetaStore::getDatabase(std::string clusterFile)
{
    FDBDatabase *db;
    exitOnError(fdb_create_database(clusterFile.c_str(), &db));
    return db;
}

std::pair<bool, std::string> FDBMetaStore::getValue(std::string key)
{
    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));
    FDBFuture *fget = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(key.c_str()), key.size(), 0); // not set snapshot
    exitOnError(fdb_future_block_until_ready(fget));
    // create future
    fdb_bool_t key_present;
    const uint8_t *value = NULL;
    int value_length;
    exitOnError(fdb_future_get_value(fget, &key_present, &value, &value_length));
    fdb_future_destroy(fget);

    bool is_found = false;
    std::string ret_val;
    // DEBUG
    if (key_present)
    {
        is_found = true;
        ret_val = reinterpret_cast<const char *>(value);
        LOG(INFO)
            << "FDBMetaStore:: getValue(); key: " << key << ", value: " << ret_val;
    }
    else
    {
        LOG(INFO) << "FDBMetaStore:: getValue(); key: " << key << ", value not found";
    }

    // destroy transaction; no need to commit read-only transaction
    fdb_transaction_destroy(tx);

    return std::pair<bool, std::string>(is_found, ret_val);
}

void FDBMetaStore::setValueAndCommit(std::string key, std::string value)
{
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(key.c_str()), key.size(), reinterpret_cast<const uint8_t *>(value.c_str()), value.size());

    FDBFuture *fset = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(fset));

    fdb_future_destroy(fset);

    LOG(INFO) << "FDBMetaStore:: setValue(); key: " << key << ", value: " << value;

    return;
}

int FDBMetaStore::genFileKey(unsigned char namespaceId, const char *name, int nameLength, char key[])
{
    return snprintf(key, PATH_MAX, "%d_%*s", namespaceId, nameLength, name);
}

int FDBMetaStore::genFileVersionListKey(unsigned char namespaceId, const char *name, int nameLength, char key[])
{
    return snprintf(key, PATH_MAX, "//vl%d_%*s", namespaceId, nameLength, name);
}

std::string FDBMetaStore::getFilePrefix(const char name[], bool noEndingSlash)
{
    const char *slash = strrchr(name, '/'), *us = strchr(name, '_');
    std::string prefix("//pf_");
    // file on root directory, or root directory (ends with one '/')
    if (slash == NULL || us + 1 == slash)
    {
        prefix.append(name, us - name + 1);
        return noEndingSlash ? prefix : prefix.append("/");
    }
    // sub-directory
    return prefix.append(name, slash - name);
}

std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength)
{
    std::string fieldKey(field, fieldLength);

    // expected format 'c<chunk_id>-<type>-<container_id>', e.g., c00-op-1
    size_t delimiter1 = fieldKey.find("-");
    size_t delimiter2 = fieldKey.find("-", delimiter1 + 1);
    if (delimiter1 == std::string::npos || delimiter2 == std::string::npos)
    {
        return std::make_tuple(INVALID_CHUNK_ID, "", INVALID_CONTAINER_ID);
    }

    int chunkId, containerId;
    std::string type;
    // first part is 'c[0-9]+'
    chunkId = strtol(fieldKey.substr(1, delimiter1 - 1).c_str(), NULL, 10);
    // second part is a string
    type = fieldKey.substr(delimiter1 + 1, delimiter2 - delimiter1 - 1);
    // third part is a '[0-9]+'
    containerId = strtol(fieldKey.substr(delimiter2 + 1).c_str(), NULL, 10);

    return std::make_tuple(chunkId, type, containerId);
}