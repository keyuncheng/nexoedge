// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h> // exit(), strtol()
#include <stdio.h>  // sprintf()
#include <boost/uuid/uuid_io.hpp>
#include <glog/logging.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <sstream>

#include "fdb_metastore.hh"
#include "../../common/config.hh"
#include "../../common/define.hh"
#include "../../common/checksum_calculator.hh"

// change defs's prefix to FDB_
#define FDB_NUM_RESERVED_SYSTEM_KEYS (9)
#define FDB_FILE_LOCK_KEY "//snccFLock"
#define FDB_FILE_PIN_STAGED_KEY "//snccFPinStaged"
#define FDB_FILE_REPAIR_KEY "//snccFRepair"
#define FDB_FILE_PENDING_WRITE_KEY "//snccFPendingWrite"
#define FDB_FILE_PENDING_WRITE_COMP_KEY "//snccFPendingWriteComp"
#define FDB_BG_TASK_PENDING_KEY "//snccFBgTask"
#define FDB_DIR_LIST_KEY "//snccDirList"
#define FDB_JL_LIST_KEY "//snccJournalFSet"
// new keys
#define FDB_NUM_FILES_KEY "//snccFSize"

#define FDB_MAX_KEY_SIZE (64)
#define FDB_NUM_REQ_FIELDS (10)
#define FDB_FILE_PREFIX "//pf_"

static std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength);

FDBMetaStore::FDBMetaStore()
{
    Config &config = Config::getInstance();

    LOG(INFO) << "FDBMetaStore::FDBMetaStore() MetaStoreType: " << config.getProxyMetaStoreType();

    // select API version
    fdb_select_api_version(FDB_API_VERSION);
    LOG(INFO) << "FDBMetaStore::FDBMetaStore() creating FDB Client connection, selected API version: " << FDB_API_VERSION;

    // init network
    exitOnError(fdb_setup_network());
    if (pthread_create(&_fdb_network_thread, NULL, FDBMetaStore::runNetwork, NULL))
    {
        LOG(ERROR) << "FDBMetaStore::FDBMetaStore() failed to create network thread";
        exit(1);
    }

    // init database
    _db = getDatabase(_FDBClusterFile);

    _taskScanIt = "0";
    _endOfPendingWriteSet = true;

    LOG(INFO) << "FDBMetaStore::FDBMetaStore() MetaStore initialized, clusterFile: " << _FDBClusterFile;
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
    char fileKey[PATH_MAX], verFileKey[PATH_MAX];

    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);
    int verFileKeyLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, verFileKey);

    Config &config = Config::getInstance();

    // create a file version summary
    // format: "size mtime md5 isDeleted numChunks"
    std::string fVerSummary;
    fVerSummary.append(std::to_string(f.size).append(" "));
    fVerSummary.append(std::to_string(f.mtime).append(" "));
    // convert md5 to string
    std::string fileMd5 = ChecksumCalculator::toHex(f.md5, MD5_DIGEST_LENGTH);
    // fVerSummary.append(fileMd5.append(" "));
    fVerSummary.append(std::to_string(((f.size == 0) ? f.isDeleted : 0)).append(" "));
    fVerSummary.append(std::to_string(f.numChunks));

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, std::string(fileKey, fileKeyLength), fileMetaStr);

    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;

    if (fileMetaExist == false)
    { // No file meta: init the new version
        // version id
        fmj["verId"] = nlohmann::json::array();
        fmj["verId"].push_back(std::to_string(f.version));
        // version name
        fmj["verName"] = nlohmann::json::array();
        fmj["verName"].push_back(std::string(verFileKey, verFileKeyLength));
        // version summary
        fmj["verSummary"] = nlohmann::json::array();
        fmj["verSummary"].push_back(fVerSummary);
    }
    else
    { // File meta exists
        if (parseStrToJSONObj(fileMetaStr, fmj) == false)
        {
            exit(1);
        }

        // check whether versioning is enabled
        bool keepVersion = !config.overwriteFiles();
        if (keepVersion == false)
        {
            // clear all previous versions in FDB
            for (auto prevVerFileKey : fmj["verName"])
            {
                std::string keyStr = prevVerFileKey.get<std::string>();
                fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(keyStr.c_str()), keyStr.size());
            }

            // only keep the input file version
            fmj["verId"].clear();
            fmj["verId"].push_back(f.version);
            fmj["verName"].clear();
            fmj["verName"].push_back(std::string(verFileKey, verFileKeyLength));
            fmj["verSummary"].clear();
            fmj["verSummary"].push_back(fVerSummary);
        }
        else
        {
            // if the version is not stored, insert it in ascending order
            if (std::find(fmj["verId"].begin(), fmj["verId"].end(), std::string(verFileKey, verFileKeyLength)) == fmj["verId"].end())
            {
                size_t pos;
                for (pos = 0; pos < fmj["verId"].size(); pos++)
                {
                    if (f.version < fmj["verId"][pos].get<int>())
                    {
                        break;
                    }
                }
                fmj["verId"].insert(fmj["verId"].begin() + pos, f.version);
                fmj["verName"].insert(fmj["verName"].begin() + pos, std::string(verFileKey, verFileKeyLength));
                fmj["verSummary"].insert(fmj["verSummary"].begin() + pos, fVerSummary);
            }
        }
    }

    // Store file meta into FDB
    std::string fmjStr = fmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());
    delete fmjPtr;

    /**
     * @brief Versioned file metadata format
     * @Key verFileKey
     * @Value JSON string
     *
     */
    // Create metadata for the current file version
    bool isEmptyFile = f.size == 0;
    unsigned char *codingState = isEmptyFile || f.codingMeta.codingState == NULL ? (unsigned char *)"" : f.codingMeta.codingState;
    int deleted = isEmptyFile ? f.isDeleted : 0;
    size_t numUniqueBlocks = f.uniqueBlocks.size();
    size_t numDuplicateBlocks = f.duplicateBlocks.size();

    nlohmann::json *verFmjPtr = new nlohmann::json();
    auto &verFmj = *verFmjPtr;
    verFmj["name"] = std::string(f.name, f.nameLength);
    verFmj["uuid"] = boost::uuids::to_string(f.uuid);
    verFmj["size"] = f.size;
    verFmj["numC"] = f.numChunks;
    verFmj["sc"] = f.storageClass;
    verFmj["cs"] = f.codingMeta.coding;
    verFmj["n"] = f.codingMeta.n;
    verFmj["k"] = f.codingMeta.k;
    verFmj["f"] = f.codingMeta.f;
    verFmj["maxCS"] = f.codingMeta.maxChunkSize;
    verFmj["codingStateS"] = f.codingMeta.codingStateSize;
    verFmj["codingState"] = ChecksumCalculator::toHex(codingState, f.codingMeta.codingStateSize);
    verFmj["numS"] = f.numStripes;
    verFmj["ver"] = f.version;
    verFmj["ctime"] = f.ctime;
    verFmj["atime"] = f.atime;
    verFmj["mtime"] = f.mtime;
    verFmj["tctime"] = f.tctime;
    verFmj["md5"] = fileMd5;
    verFmj["sg_size"] = f.staged.size;
    verFmj["sg_sc"] = f.staged.storageClass;
    verFmj["sg_cs"] = f.staged.codingMeta.coding;
    verFmj["sg_n"] = f.staged.codingMeta.n;
    verFmj["sg_k"] = f.staged.codingMeta.k;
    verFmj["sg_f"] = f.staged.codingMeta.f;
    verFmj["sg_maxCS"] = f.staged.codingMeta.maxChunkSize;
    verFmj["sg_mtime"] = f.staged.mtime;
    verFmj["dm"] = deleted;
    verFmj["numUB"] = numUniqueBlocks;
    verFmj["numDB"] = numDuplicateBlocks;

    // container ids
    char chunkName[FDB_MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++)
    {
        int chunkNameLength = genChunkKeyPrefix(f.chunks[i].getChunkId(), chunkName);
        std::string chunkNameStr(chunkName, chunkNameLength);
        std::string cidKey = chunkNameStr + std::string("-cid");
        verFmj[cidKey.c_str()] = f.containerIds[i];
        std::string csizeKey = chunkNameStr + std::string("-size");
        verFmj[csizeKey.c_str()] = f.chunks[i].size;
        std::string cmd5Key = chunkNameStr + std::string("-md5");
        verFmj[cmd5Key.c_str()] = ChecksumCalculator::toHex(f.chunks[i].md5, MD5_DIGEST_LENGTH);
        std::string cmd5Bad = chunkNameStr + std::string("-bad");
        verFmj[cmd5Bad.c_str()] = (f.chunksCorrupted ? f.chunksCorrupted[i] : 0);
    }

    // deduplication fingerprints and block mapping
    char blockName[FDB_MAX_KEY_SIZE];
    size_t blockId = 0;
    for (auto it = f.uniqueBlocks.begin(); it != f.uniqueBlocks.end(); it++, blockId++)
    { // deduplication fingerprints
        int blockNameLength = genBlockKey(blockId, blockName, /* is unique */ true);
        std::string blockNameStr(blockName, blockNameLength);
        std::string fp = it->second.first.get();
        // logical offset, length, fingerprint, physical offset
        verFmj[blockNameStr.c_str()] = std::to_string(it->first._offset) + std::to_string(it->first._length) + fp.data() + std::to_string(it->second.second);
    }
    blockId = 0;
    for (auto it = f.duplicateBlocks.begin(); it != f.duplicateBlocks.end(); it++, blockId++)
    {
        int blockNameLength = genBlockKey(blockId, blockName, /* is unique */ false);
        std::string blockNameStr(blockName, blockNameLength);
        std::string fp = it->second.get();
        // logical offset, length, fingerprint
        verFmj[blockNameStr.c_str()] = std::to_string(it->first._offset) + std::to_string(it->first._length) + fp.data();
    }

    std::string verFmjStr = verFmj.dump();
    // Store versioned file meta into FDB
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());
    delete verFmjPtr;

    // add uuid-to-file-name maping
    char fUUIDKey[FDB_MAX_KEY_SIZE + 64];
    memset(fUUIDKey, 0, FDB_MAX_KEY_SIZE + 64);
    if (genFileUuidKey(f.namespaceId, f.uuid, fUUIDKey) == false)
    {
        LOG(WARNING) << "File uuid " << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    }
    else
    {
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fUUIDKey), FDB_MAX_KEY_SIZE + 64, reinterpret_cast<const uint8_t *>(f.name), f.nameLength);
    }

    // add filename to file Prefix Set
    std::string filePrefix = getFilePrefix(fileKey);
    std::string fPrefixListStr;
    bool fPrefixListExist = getValueInTX(tx, filePrefix, fPrefixListStr);

    nlohmann::json *fpljPtr = new nlohmann::json();
    auto &fplj = *fpljPtr;
    if (fPrefixListExist == false)
    {
        // create the set and add the fileKey
        fplj["list"] = nlohmann::json::array();
        fplj["list"].push_back(fileKey);
    }
    else
    {
        // add fileKey to the list (avoid duplication)
        if (parseStrToJSONObj(fPrefixListStr, fplj) == false)
        {
            exit(1);
        }
        if (std::find(fplj["list"].begin(), fplj["list"].end(), std::string(fileKey, fileKeyLength)) == fplj["list"].end())
        {
            fplj["list"].push_back(std::string(fileKey, fileKeyLength));
        }
    }
    std::string fpljStr = fplj.dump();
    delete fpljPtr;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size(), reinterpret_cast<const uint8_t *>(fpljStr.c_str()), fpljStr.size());

    // Update the directory list
    std::string dirListStr;
    bool dirListExist = getValueInTX(tx, std::string(FDB_DIR_LIST_KEY), dirListStr);

    nlohmann::json *dljPtr = new nlohmann::json();
    auto &dlj = *dljPtr;

    if (dirListExist == false)
    {
        // create the list and add file prefix to the list
        dlj["list"] = nlohmann::json::array();
        dlj["list"].push_back(filePrefix.c_str());
    }
    else
    {
        // add filePrefix to the list (avoid duplication)
        if (parseStrToJSONObj(dirListStr, dlj) == false)
        {
            exit(1);
        }
        if (std::find(dlj["list"].begin(), dlj["list"].end(), filePrefix) == dlj["list"].end())
        {
            dlj["list"].push_back(filePrefix);
        }
    }

    std::string dljStr = dlj.dump();
    delete dljPtr;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_DIR_LIST_KEY), std::string(FDB_DIR_LIST_KEY).size(), reinterpret_cast<const uint8_t *>(dljStr.c_str()), dljStr.size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));

    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: putMeta() finished";

    return true;
}

bool FDBMetaStore::getMeta(File &f, int getBlocks)
{
    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX], verFileKey[PATH_MAX];

    // file key
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    // versioned file key
    int verFileKeyLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, verFileKey);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, std::string(fileKey, fileKeyLength), fileMetaStr);

    if (fileMetaExist == false)
    { // file metadata not exist: report and return
        LOG(WARNING) << "FDBMetaStore::getMeta() failed to get metadata for file " << f.name;

        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    // parse fileMeta as JSON object
    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;

    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    // check file version
    if (f.version == -1)
    { // version not specified: retrieved the latest version
        std::string lastVerFileKey = fmj["verName"].back().get<std::string>();
        memcpy(verFileKey, lastVerFileKey.c_str(), lastVerFileKey.size());
        verFileKeyLength = lastVerFileKey.size();
    }
    else
    { // versioned file
        // check whether the file version exists
        if (std::find(fmj["verName"].begin(), fmj["verName"].end(), std::string(verFileKey, verFileKeyLength)) == fmj["verName"].end())
        {
            LOG(WARNING) << "FDBMetaStore::getMeta() file version not exists, file: " << f.name << ", version: " << f.version;

            // commit transaction
            delete fmjPtr;
            FDBFuture *cmt = fdb_transaction_commit(tx);
            exitOnError(fdb_future_block_until_ready(cmt));
            fdb_future_destroy(cmt);

            return false;
        }
    }
    delete fmjPtr;

    size_t numUniqueBlocks = 0, numDuplicateBlocks = 0;

    // find metadata for current file version
    std::string verFileMetaStr;
    bool verFileMetaExist = getValueInTX(tx, std::string(verFileKey, verFileKeyLength), verFileMetaStr);
    if (verFileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::getMeta() failed to find version in MetaStore, file: " << f.name << ", version: " << f.version;
        exit(1);
    }

    nlohmann::json *verFmjPtr = new nlohmann::json();
    auto &verFmj = *verFmjPtr;
    if (parseStrToJSONObj(verFileMetaStr, verFmj) == false)
    {
        exit(1);
    }

    // parse fields
    f.size = verFmj["size"].get<unsigned long>();
    f.numChunks = verFmj["numC"].get<int>();
    f.numStripes = verFmj["numS"].get<int>();
    std::string retrievedUUID = verFmj["uuid"].get<std::string>();
    if (f.setUUID(retrievedUUID) == false)
    {
        LOG(WARNING) << "FDBMetaStore::getMeta() invalid UUID: " << retrievedUUID;

        // commit transaction and return
        delete verFmjPtr;
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    f.storageClass = verFmj["sc"].get<std::string>();
    f.codingMeta.coding = verFmj["cs"].get<unsigned char>();
    f.codingMeta.n = verFmj["n"].get<int>();
    f.codingMeta.k = verFmj["k"].get<int>();
    f.codingMeta.f = verFmj["f"].get<int>();
    f.codingMeta.maxChunkSize = verFmj["maxCS"].get<int>();
    f.codingMeta.codingStateSize = verFmj["codingStateS"].get<int>();
    if (f.codingMeta.codingStateSize > 0)
    {
        f.codingMeta.codingState = new unsigned char[f.codingMeta.codingStateSize];
        ChecksumCalculator::unHex(verFmj["codingState"].get<std::string>(), f.codingMeta.codingState, f.codingMeta.codingStateSize);
    }
    f.version = verFmj["ver"].get<int>();
    f.ctime = verFmj["ctime"].get<time_t>();
    f.atime = verFmj["atime"].get<time_t>();
    f.mtime = verFmj["mtime"].get<time_t>();
    f.tctime = verFmj["tctime"].get<time_t>();
    ChecksumCalculator::unHex(verFmj["md5"].get<std::string>(), f.md5, MD5_DIGEST_LENGTH);
    f.staged.size = verFmj["sg_size"].get<int>();
    f.staged.storageClass = verFmj["sg_sc"].get<std::string>();
    f.staged.codingMeta.maxChunkSize = verFmj["sg_cs"].get<int>();
    f.staged.codingMeta.n = verFmj["sg_n"].get<int>();
    f.staged.codingMeta.k = verFmj["sg_k"].get<int>();
    f.staged.codingMeta.f = verFmj["sg_f"].get<int>();
    f.staged.codingMeta.maxChunkSize = verFmj["sg_maxCS"].get<int>();
    f.staged.mtime = verFmj["sg_mtime"].get<time_t>();
    f.isDeleted = verFmj["dm"].get<int>();
    numUniqueBlocks = verFmj["numUB"].get<int>();
    numDuplicateBlocks = verFmj["numDB"].get<int>();

    // get container ids and attributes
    if (!f.initChunksAndContainerIds())
    {
        LOG(ERROR) << "FDBMetaStore::getMeta() Failed to allocate space for container ids";
        return false;
    }

    char chunkName[FDB_MAX_KEY_SIZE];
    for (int chunkId = 0; chunkId < f.numChunks; chunkId++)
    {
        int chunkNameLength = genChunkKeyPrefix(chunkId, chunkName);
        std::string chunkNameStr(chunkName, chunkNameLength);
        std::string cidKey = chunkNameStr + std::string("-cid");
        std::string csizeKey = chunkNameStr + std::string("-size");
        std::string cmd5Key = chunkNameStr + std::string("-md5");
        std::string cbadKey = chunkNameStr + std::string("-bad");

        f.containerIds[chunkId] = verFmj[cidKey.c_str()].get<int>();
        f.chunks[chunkId].size = verFmj[csizeKey.c_str()].get<int>();
        std::string chunkMd5 = verFmj["codingState"].get<std::string>();
        ChecksumCalculator::unHex(chunkMd5, f.chunks[chunkId].md5, MD5_DIGEST_LENGTH);
        f.chunksCorrupted[chunkId] = verFmj[cbadKey.c_str()].get<int>();
        f.chunks[chunkId].setId(f.namespaceId, f.uuid, chunkId);
        f.chunks[chunkId].data = 0;
        f.chunks[chunkId].freeData = true;
        f.chunks[chunkId].fileVersion = f.version;
    }

    // get block attributes for deduplication
    BlockLocation::InObjectLocation loc;
    Fingerprint fp;
    char blockName[FDB_MAX_KEY_SIZE];

    if (getBlocks == 1 || getBlocks == 3)
    { // unique blocks
        size_t pOffset = 0;
        size_t noFpOfs = sizeof(unsigned long int) + sizeof(unsigned int);
        size_t hasFpOfs = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH;
        size_t lengthWithFp = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH + sizeof(int);
        for (size_t blockId = 0; blockId < numUniqueBlocks; blockId++)
        {
            int blockNameLength = genBlockKey(blockId, blockName, /* is unique */ true);
            std::string blockNameStr(blockName, blockNameLength);
            std::string blockStr = verFmj[blockNameStr.c_str()].get<std::string>();
            loc._offset = std::stoull(blockStr.substr(pOffset, sizeof(unsigned long int)));
            loc._length = std::stoull(blockStr.substr(pOffset + sizeof(unsigned long int), sizeof(unsigned int)));
            if (verFmj[blockNameStr.c_str()].size() >= lengthWithFp)
            {
                std::string fpStr = blockStr.substr(pOffset + hasFpOfs, SHA256_DIGEST_LENGTH);
                fp.set(fpStr.c_str(), SHA256_DIGEST_LENGTH);
            }
            else
            {
                std::string fpStr = blockStr.substr(pOffset + noFpOfs, SHA256_DIGEST_LENGTH);
                fp.set(fpStr.c_str(), SHA256_DIGEST_LENGTH);
            }

            auto followIt = f.uniqueBlocks.end(); // hint is the item after the element to insert for c++11, and before the element for c++98
            f.uniqueBlocks.emplace_hint(followIt, std::make_pair(loc, std::make_pair(fp, pOffset)));
        }
    }

    if (getBlocks == 2 || getBlocks == 3)
    { // duplicate blocks
        size_t noFpOfs = sizeof(unsigned long int) + sizeof(unsigned int);
        size_t lengthWithFp = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH;

        for (size_t blockId = 0; blockId < numDuplicateBlocks; blockId++)
        {
            int blockNameLength = genBlockKey(blockId, blockName, /* is unique */ false);
            std::string blockNameStr(blockName, blockNameLength);
            loc._offset = std::stoull(verFmj[blockNameStr.c_str()].get<std::string>().substr(0, sizeof(unsigned long int)));
            loc._length = std::stoull(verFmj[blockNameStr.c_str()].get<std::string>().substr(sizeof(unsigned long int), sizeof(unsigned int)));

            if (verFmj[blockNameStr.c_str()].size() >= lengthWithFp)
            {
                std::string fpStr = verFmj[blockNameStr.c_str()].get<std::string>().substr(noFpOfs, SHA256_DIGEST_LENGTH);
                fp.set(fpStr.c_str(), SHA256_DIGEST_LENGTH);
            }

            auto followIt = f.duplicateBlocks.end(); // hint is the item after the element to insert for c++11, and before the element for c++98
            f.duplicateBlocks.emplace_hint(followIt, std::make_pair(loc, fp));
        }
    }

    // commit transaction
    delete verFmjPtr;

    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore::getMeta() finished";

    return true;
}

bool FDBMetaStore::deleteMeta(File &f)
{
    std::lock_guard<std::mutex> lk(_lock);

    Config &config = Config::getInstance();

    char fileKey[PATH_MAX], verFileKey[PATH_MAX];

    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    bool isVersioned = !config.overwriteFiles();
    bool lazyDeletion = false;
    bool ret = true;
    int versionToDelete = f.version;

    // DLOG(INFO) << "FDBMetaStore::deleteMeta() start to delete file " << f.name << ", version: " << f.version << ", fileKey: " << std::string(fileKey, fileKeyLength);

    // versioning enabled but a version is not specified, mark as deleted
    if ((isVersioned || lazyDeletion) && versionToDelete == -1)
    {
        f.isDeleted = true;
        f.size = 0;
        f.version = 0; // set to version 0
        f.numChunks = 0;
        f.numStripes = 0;
        f.mtime = time(NULL);
        memset(f.md5, 0, MD5_DIGEST_LENGTH);
        ret = putMeta(f);
        // tell the caller not to remove the data
        f.version = -1;
        DLOG(INFO) << "FDBMetaStore::deleteMeta() Remove the current version " << f.version << " of file " << f.name;
        return ret;
    }

    // check existing file versions
    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, std::string(fileKey, fileKeyLength), fileMetaStr);

    if (fileMetaExist == false)
    { // file metadata not exist: report and return
        LOG(WARNING) << "FDBMetaStore::putMeta() failed to get metadata for file " << f.name;

        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    // parse fileMeta as JSON object
    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;
    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    // non-versioned: directly remove verFileKey
    if (isVersioned == false)
    {
        std::string verFileKeyStr = fmj["verName"].back().get<std::string>();
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(verFileKeyStr.c_str()), verFileKeyStr.size());
        fmj["verId"].clear();
        fmj["verName"].clear();
        fmj["verSummary"].clear();
    }

    // delete a specific version
    if (isVersioned == true && versionToDelete != -1)
    {
        int numVersions = fmj["verId"].size();

        if (numVersions == 0)
        {
            LOG(WARNING) << "FDBMetaStore::deleteMeta() No version found for file " << f.name;

            FDBFuture *cmt = fdb_transaction_commit(tx);
            exitOnError(fdb_future_block_until_ready(cmt));
            fdb_future_destroy(cmt);

            return false;
        }

        // check whether the version exists in version list
        auto it = std::find(fmj["verId"].begin(), fmj["verId"].end(), versionToDelete);
        if (it == fmj["verId"].end())
        {
            // handle error
            LOG(WARNING) << "FDBMetaStore::deleteMeta() version not found for file " << f.name << ", version: " << versionToDelete;

            FDBFuture *cmt = fdb_transaction_commit(tx);
            exitOnError(fdb_future_block_until_ready(cmt));
            fdb_future_destroy(cmt);

            return false;
        }

        // remove a specific version
        int idx = std::distance(fmj["verId"].begin(), it);
        fmj["verId"].erase(fmj["verId"].begin() + idx);
        fmj["verName"].erase(fmj["verName"].begin() + idx);
        fmj["verSummary"].erase(fmj["verSummary"].begin() + idx);

        int verFileKeyLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, verFileKey);
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength);
    }

    // remove file meta
    bool removeFileMeta = (isVersioned && fmj["verId"].size() == 0) || !isVersioned;
    if (!removeFileMeta)
    {
        // update file meta (only remove a specified version)
        std::string fmjStr = fmj.dump();
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());

        // commit transaction
        delete fmjPtr;
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return true;
    }

    // directly remove file meta
    delete fmjPtr;
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength);

    char fUuidKey[FDB_MAX_KEY_SIZE + 64];
    memset(fUuidKey, 0, FDB_MAX_KEY_SIZE + 64);
    f.genUUID();

    // remove file uuid key
    if (!genFileUuidKey(f.namespaceId, f.uuid, fUuidKey))
    {
        LOG(WARNING) << "File uuid" << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    }
    else
    {
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(fUuidKey), FDB_MAX_KEY_SIZE + 64);
    }

    // remove from file prefix set
    std::string filePrefix = getFilePrefix(fileKey);
    std::string fPrefixListStr;
    bool fPrefixSetExist = getValueInTX(tx, filePrefix, fPrefixListStr);
    if (fPrefixSetExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::deleteMeta() Error finding file prefix set";
        exit(1);
    }
    nlohmann::json *fpljPtr = new nlohmann::json();
    auto &fplj = *fpljPtr;
    if (parseStrToJSONObj(fPrefixListStr, fplj) == false)
    {
        exit(1);
    }

    // remove fileKey from the list
    auto it = std::find(fplj["list"].begin(), fplj["list"].end(), std::string(fileKey, fileKeyLength));
    if (it != fplj["list"].end())
    {
        fplj["list"].erase(it);
    }

    int numFilesInPrefixList = fplj["list"].size();

    if (numFilesInPrefixList > 0)
    { // update prefix list
        std::string fpljStr = fplj.dump();
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size(), reinterpret_cast<const uint8_t *>(fpljStr.c_str()), fpljStr.size());
    }
    else
    { // the prefix list has no file, remove the prefix list
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size());
    }
    delete fpljPtr;

    // remove file prefix from directory set
    if (numFilesInPrefixList == 0)
    {
        std::string dirListStr;
        bool dirListExist = getValueInTX(tx, std::string(FDB_DIR_LIST_KEY), dirListStr);
        if (dirListExist == false)
        {
            LOG(ERROR) << "FDBMetaStore::deleteMeta() Error finding directory list";
            exit(1);
        }

        nlohmann::json *dljPtr = new nlohmann::json();
        auto &dlj = *dljPtr;
        if (parseStrToJSONObj(dirListStr, dlj) == false)
        {
            exit(1);
        }

        // remove filePrefix from the list
        auto it = std::find(dlj["list"].begin(), dlj["list"].end(), filePrefix);
        if (it != dlj["list"].end())
        {
            dlj["list"].erase(it);
        }

        std::string dljStr = dlj.dump();
        delete dljPtr;

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_DIR_LIST_KEY), std::string(FDB_DIR_LIST_KEY).size(), reinterpret_cast<const uint8_t *>(dljStr.c_str()), dljStr.size());
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return true;
}

bool FDBMetaStore::renameMeta(File &sf, File &df)
{
    // src and dst file keys
    char srcFileKey[PATH_MAX], dstFileKey[PATH_MAX];
    int srcFileKeyLength = genFileKey(sf.namespaceId, sf.name, sf.nameLength, srcFileKey);
    int dstFileKeyLength = genFileKey(df.namespaceId, df.name, df.nameLength, dstFileKey);

    // file uuids
    char srcFileUuidKey[FDB_MAX_KEY_SIZE + 64], dstFileUuidKey[FDB_MAX_KEY_SIZE + 64];
    memset(srcFileUuidKey, 0, FDB_MAX_KEY_SIZE + 64);
    memset(dstFileUuidKey, 0, FDB_MAX_KEY_SIZE + 64);
    sf.genUUID();
    df.genUUID();
    if (!genFileUuidKey(sf.namespaceId, sf.uuid, srcFileUuidKey))
        return false;
    if (!genFileUuidKey(df.namespaceId, df.uuid, dstFileUuidKey))
        return false;

    // update file names
    std::lock_guard<std::mutex> lk(_lock);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // rename file key from srcFileKey to dstFileKey

    // obtain file metadata
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, std::string(srcFileKey, srcFileKeyLength), fileMetaStr);
    if (fileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::renameMeta() Error reading metadata for file" << sf.name;

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    // parse fileMeta as JSON object
    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;
    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    // update uuid-to-file-name mapping for the latest file version
    std::string lastVerFileKey = fmj["verName"].back().get<std::string>();
    // find metadata for current file version
    std::string verFileMetaStr;
    bool verFileMetaExist = getValueInTX(tx, lastVerFileKey, verFileMetaStr);
    if (verFileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::renameMeta() failed to find latest version in MetaStore, file: " << sf.name;
        exit(1);
    }

    nlohmann::json *verFmjPtr = new nlohmann::json();
    auto &verFmj = *verFmjPtr;
    if (parseStrToJSONObj(verFileMetaStr, verFmj) == false)
    {
        exit(1);
    }

    verFmj["uuid"] = boost::uuids::to_string(df.uuid);

    // update verFileKey
    std::string verFmjStr = verFmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(lastVerFileKey.c_str()), lastVerFileKey.size(), reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());
    delete verFmjPtr;

    // insert dstFileKey
    std::string fmjStr = fmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileKey), dstFileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());
    delete fmjPtr;

    // remove srcFileKey
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFileKey), srcFileKeyLength);

    // set dstFileUuidKey; remove the original sfidKey
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileUuidKey), FDB_MAX_KEY_SIZE + 64, reinterpret_cast<const uint8_t *>(df.name), df.nameLength);
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFileUuidKey), FDB_MAX_KEY_SIZE + 64);

    // DLOG(INFO) << "FDBMetaStore::renameMeta() Add reverse mapping (" << std::string(dstFileUuidKey, FDB_MAX_KEY_SIZE) << ") for file " << std::string(dstFileKey, dstFileKeyLength);
    // DLOG(INFO) << "FDBMetaStore::renameMeta() remove reverse mapping (" << std::string(srcFileUuidKey, FDB_MAX_KEY_SIZE) << ") for file " << std::string(srcFileKey, srcFileKeyLength);

    // update the src file prefix set
    std::string srcFilePrefix = getFilePrefix(srcFileKey);

    std::string srcFilePrefixListStr;
    bool srcFilePrefixSetExist = getValueInTX(tx, srcFilePrefix, srcFilePrefixListStr);
    if (srcFilePrefixSetExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::renameMeta() Error finding file prefix set";
        exit(1);
    }

    nlohmann::json *srcFPljPtr = new nlohmann::json();
    auto &srcFPlj = *srcFPljPtr;
    if (parseStrToJSONObj(srcFilePrefixListStr, srcFPlj) == false)
    {
        exit(1);
    }

    // remove srcFileKey
    srcFPlj["list"].erase(std::remove(srcFPlj["list"].begin(), srcFPlj["list"].end(), std::string(srcFileKey, srcFileKeyLength)), srcFPlj["list"].end());

    int numFilesInSrcPrefixList = srcFPlj["list"].size();

    if (numFilesInSrcPrefixList > 0)
    { // update prefix list
        std::string srcFPljStr = srcFPlj.dump();
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(srcFilePrefix.c_str()), srcFilePrefix.size(), reinterpret_cast<const uint8_t *>(srcFPljStr.c_str()), srcFPljStr.size());
    }
    else
    { // the prefix list has no file, remove the prefix list
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFilePrefix.c_str()), srcFilePrefix.size());
    }
    delete srcFPljPtr;

    // update the dst file prefix set
    std::string dstFilePrefix = getFilePrefix(dstFileKey);
    std::string dstFilePrefixListStr;
    bool dstFilePrefixSetExist = getValueInTX(tx, dstFilePrefix, dstFilePrefixListStr);

    nlohmann::json *dstFPljPtr = new nlohmann::json();
    auto &dstFPlj = *dstFPljPtr;
    if (dstFilePrefixSetExist == false)
    {
        dstFPlj["list"] = nlohmann::json::array();
    }
    else
    {
        if (parseStrToJSONObj(dstFilePrefixListStr, dstFPlj) == false)
        {
            exit(1);
        }
    }
    dstFPlj["list"].push_back(std::string(dstFileKey, dstFileKeyLength));

    std::string dstFPljStr = dstFPlj.dump();
    delete dstFPljPtr;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFilePrefix.c_str()), dstFilePrefix.size(), reinterpret_cast<const uint8_t *>(dstFPljStr.c_str()), dstFPljStr.size());

    // update DIR_LIST
    std::string dirListStr;
    bool dirListExist = getValueInTX(tx, std::string(FDB_DIR_LIST_KEY), dirListStr);
    if (dirListExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::deleteMeta() Error finding directory list";
        exit(1);
    }

    nlohmann::json *dljPtr = new nlohmann::json();
    auto &dlj = *dljPtr;
    if (parseStrToJSONObj(dirListStr, dlj) == false)
    {
        exit(1);
    }

    // remove src file prefix from directory set
    if (numFilesInSrcPrefixList == 0)
    {
        // remove filePrefix from the list
        auto it = std::find(dlj["list"].begin(), dlj["list"].end(), srcFilePrefix);
        if (it != dlj["list"].end())
        {
            dlj["list"].erase(it);
        }
    }
    // add dst file prefix from directory set
    auto it = std::find(dlj["list"].begin(), dlj["list"].end(), dstFilePrefix);
    if (it == dlj["list"].end())
    {
        dlj["list"].push_back(dstFilePrefix);
    }

    std::string dljStr = dlj.dump();
    delete dljPtr;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_DIR_LIST_KEY), std::string(FDB_DIR_LIST_KEY).size(), reinterpret_cast<const uint8_t *>(dljStr.c_str()), dljStr.size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));

    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: renameMeta() finished";

    return true;
}

bool FDBMetaStore::updateTimestamps(const File &f)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, std::string(fileKey, fileKeyLength), fileMetaStr);
    if (fileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateTimestamps() failed to get metadata for file " << f.name;

        // commit transaction and return
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;
    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    std::string verFileKey = fmj["verName"].back().get<std::string>();

    // find metadata for current file version
    std::string verFileMetaStr;
    bool verFileMetaExist = getValueInTX(tx, verFileKey, verFileMetaStr);
    if (verFileMetaExist == false)
    {
        // commit transaction and return
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    nlohmann::json *verFmjPtr = new nlohmann::json();
    auto &verFmj = *verFmjPtr;
    if (parseStrToJSONObj(verFileMetaStr, verFmj) == false)
    {
        exit(1);
    }

    verFmj["atime"] = f.atime;
    verFmj["mtime"] = f.mtime;
    verFmj["ctime"] = f.ctime;
    verFmj["tctime"] = f.tctime;

    // serialize json to string and store in FDB
    std::string verFmjStr = verFmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey.c_str()), verFileKey.size(), reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());
    delete verFmjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: updateTimestamps() finished";

    return true;
}

int FDBMetaStore::updateChunks(const File &f, int version)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    DLOG(INFO) << fileKeyLength;

    // update the latest version

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, fileKey, fileMetaStr);
    if (fileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateTimestamps() failed to get metadata for file " << f.name;

        // commit transaction and return
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    // current metadata format: key: filename; value: {"verList": [v0, v1, v2,
    // ...]}; for non-versioned system, verList only stores v0
    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;
    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    std::string verFileKey = fmj["verName"].back().get<std::string>();

    // find metadata for current file version
    std::string verFileMetaStr;
    bool verFileMetaExist = getValueInTX(tx, verFileKey, verFileMetaStr);
    if (verFileMetaExist == false)
    {
        // commit transaction and return
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    nlohmann::json *vfmjPtr = new nlohmann::json();
    auto &vfmj = *vfmjPtr;
    if (parseStrToJSONObj(verFileMetaStr, vfmj) == false)
    {
        exit(1);
    }

    // update Chunk information
    char chunkName[FDB_MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++)
    {
        genChunkKeyPrefix(f.chunks[i].getChunkId(), chunkName);

        std::string cidKey = std::string(chunkName) + std::string("-cid");
        vfmj[cidKey.c_str()] = std::to_string(f.containerIds[i]);
        std::string csizeKey = std::string(chunkName) + std::string("-size");
        vfmj[csizeKey.c_str()] = std::to_string(f.chunks[i].size);
    }

    // serialize json to string and store in FDB
    std::string vfmjStr = vfmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey.size()), verFileKey.size(), reinterpret_cast<const uint8_t *>(vfmjStr.c_str()), vfmjStr.size());
    delete vfmjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    LOG(INFO) << "FDBMetaStore:: updateChunks() finished";

    return true;
}

bool FDBMetaStore::getFileName(boost::uuids::uuid fuuid, File &f)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileUuidKey[FDB_MAX_KEY_SIZE + 64];
    memset(fileUuidKey, 0, FDB_MAX_KEY_SIZE + 64);
    if (!genFileUuidKey(f.namespaceId, fuuid, fileUuidKey))
        return false;
    return getFileName(fileUuidKey, f);

    return true;
}

unsigned int FDBMetaStore::getFileList(FileInfo **list, unsigned char namespaceId, bool withSize, bool withTime, bool withVersions, std::string prefix)
{
    std::lock_guard<std::mutex> lk(_lock);

    if (namespaceId == INVALID_NAMESPACE_ID)
        namespaceId = Config::getInstance().getProxyNamespaceId();

    // candidate fileKeys for listing
    std::vector<std::string> candidateFileKeys;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    DLOG(INFO) << "FDBMetaStore::getFileList() " << "prefix = " << prefix;

    if (prefix == "" || prefix.back() != '/')
    { // get keys started with prefix
        // get all keys started with prefixWithNS
        std::string prefixWithNS = std::to_string(namespaceId) + "_" + prefix;

        FDBFuture *keyRangeFut = fdb_transaction_get_key(tx, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>(prefixWithNS.c_str()), prefixWithNS.size()), 0); // not set snapshot
        exitOnError(fdb_future_block_until_ready(keyRangeFut));

        FDBKey *keyArray = nullptr;
        int keyCount;

        exitOnError(fdb_future_get_key_array(keyRangeFut, &keyArray, &keyCount));
        fdb_future_destroy(keyRangeFut);
        keyRangeFut = nullptr;

        for (int idx = 0; idx < keyCount; idx++)
        {
            std::string keyStr(reinterpret_cast<const char *>(keyArray[idx].key), keyArray[idx].key_length);
            candidateFileKeys.push_back(keyStr);
        }
    }
    else
    {
        std::string sprefix;
        sprefix.append(std::to_string(namespaceId)).append("_").append(prefix);
        sprefix = getFilePrefix(sprefix.c_str());
        DLOG(INFO) << "FDBMetaStore::getFileList() sprefix = " << sprefix;

        std::string filePrefixListStr;
        bool filePrefixSetExist = getValueInTX(tx, sprefix, filePrefixListStr);
        if (filePrefixSetExist == false)
        {
            LOG(ERROR) << "FDBMetaStore::getFileList() Error finding file prefix set";
            exit(1);
        }

        nlohmann::json *fpljPtr = new nlohmann::json();
        auto &fplj = *fpljPtr;
        if (parseStrToJSONObj(filePrefixListStr, fplj) == false)
        {
            exit(1);
        }

        // get all candidate fileKeys
        if (fplj["list"].size() > 0)
        {
            // get all fileKeys started with prefix
            for (auto &fileKey : fplj["list"])
            {
                candidateFileKeys.push_back(fileKey.get<std::string>());
            }
        }
        delete fpljPtr;
    }

    // init file list
    if (candidateFileKeys.size() > 0)
    {
        *list = new FileInfo[candidateFileKeys.size()];
    }

    // count number of files
    int numFiles = 0;

    for (auto &fileKey : candidateFileKeys)
    {
        if (isSystemKey(fileKey.c_str()))
        {
            continue;
        }
        // full name in form of "namespaceId_filename"
        if (!getNameFromFileKey(
                fileKey.c_str(), fileKey.size(),
                &list[0][numFiles].name,
                list[0][numFiles].nameLength,
                list[0][numFiles].namespaceId))
        {
            continue;
        }

        // store in FileInfo
        FileInfo &cur = list[0][numFiles];

        std::string fileMetaStr;
        bool fileMetaExist = getValueInTX(tx, fileKey, fileMetaStr);
        if (fileMetaExist == false)
        {
            LOG(ERROR) << "FDBMetaStore::getFileList() failed to get metadata for file " << cur.name;
            continue;
        }
        nlohmann::json *fmjPtr = new nlohmann::json();
        auto &fmj = *fmjPtr;
        if (parseStrToJSONObj(fileMetaStr, fmj) == false)
        {
            exit(1);
        }

        if (withSize || withTime || withVersions)
        {

            // get file size and time if requested

            // get latest version
            std::string verFileKey = fmj["verName"].back().get<std::string>();
            std::string verFileMetaStr;
            bool verFileMetaExist = getValueInTX(tx, verFileKey, verFileMetaStr);
            if (verFileMetaExist == false)
            {
                LOG(ERROR) << "FDBMetaStore::getFileList() failed to get metadata for file " << cur.name;
                continue;
            }
            nlohmann::json *vfmjPtr = new nlohmann::json();
            auto &vfmj = *vfmjPtr;
            if (parseStrToJSONObj(verFileMetaStr, vfmj) == false)
            {
                exit(1);
            }

            cur.size = vfmj["size"].get<unsigned long int>();
            cur.ctime = vfmj["ctime"].get<time_t>();
            cur.atime = vfmj["atime"].get<time_t>();
            cur.mtime = vfmj["mtime"].get<time_t>();
            cur.version = vfmj["ver"].get<int>();
            cur.isDeleted = vfmj["dm"].get<int>();
            ChecksumCalculator::unHex(vfmj["md5"].get<std::string>(), cur.md5, MD5_DIGEST_LENGTH);
            cur.numChunks = vfmj["numChunks"].get<int>();
            // staged last modified time
            time_t staged_mtime = vfmj["sg_mtime"].get<time_t>();
            if (mtime > cur.mtime)
            {
                cur.mtime = staged_mtime;
                cur.atime = staged_mtime;
                cur.size = vfmj["sg_size"].get<unsigned long int>();
            }
            cur.storageClass = vfmj["sc"].get<std::string>();

            delete vfmjPtr;
        }
        // do not add delete marker to the list unless for queries on versions
        if (!withVersions && cur.isDeleted)
        {
            continue;
        }
        if (withVersions && cur.version > 0)
        {
            // get all version information
            cur.numVersions = fmj["verId"].size();
            try
            {
                cur.versions = new VersionInfo[cur.numVersions];
                for (size_t vi = 0; vi < cur.numVersions; vi++)
                {
                    cur.versions[vi].version = fmj["verId"][vi].get<int>();
                    std::string verSummary = fmj["verSummary"][vi].get<std::string>();
                    std::stringstream ss(verSummary);
                    string item;
                    std::vector<string> items;
                    while (ss >> item)
                    {
                        items.push_back(item);
                    }
                    cur.versions[vi].size = strtoul(items[0].c_str(), nullptr, 0);
                    cur.versions[vi].mtime = strtol(items[1].c_str(), nullptr, 0);
                    ChecksumCalculator::unHex(items[2].c_str(), cur.versions[vi].md5, MD5_DIGEST_LENGTH);
                    cur.versions[vi].isDeleted = atoi(items[3].c_str());
                    cur.versions[vi].numChunks = atoi(items[4].c_str());

                    DLOG(INFO) << "Add version " << cur.versions[vi].version << " size " << cur.versions[vi].size << " mtime " << cur.versions[vi].mtime << " deleted " << cur.versions[vi].isDeleted << " to version list of file " << cur.name;
                }
            }
            catch (std::exception &e)
            {
                LOG(ERROR) << "Cannot allocate memory for " << cur.numVersions << " version records";
                cur.versions = 0;
            }
        }

        delete fmjPtr;
        numFiles++;
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    DLOG(INFO) << "FDBMetaStore::getFileList() finished";

    return numFiles;
}

unsigned int FDBMetaStore::getFolderList(std::vector<std::string> &list, unsigned char namespaceId, std::string prefix, bool skipSubfolders)
{
    std::lock_guard<std::mutex> lk(_lock);

    // generate the prefix for pattern-based directory searching
    prefix.append("a");
    char filename[PATH_MAX];
    genFileKey(namespaceId, prefix.c_str(), prefix.size(), filename);
    std::string pattern = getFilePrefix(filename, /* no ending slash */ true).append("*");
    ssize_t pfSize = pattern.size();
    unsigned long int count = 0;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // get all keys started with prefix in FDB_DIR_LIST_KEY
    std::string dirListStr;
    bool dirListExist = getValueInTX(tx, std::string(FDB_DIR_LIST_KEY), dirListStr);
    if (dirListExist == false)
    {
        LOG(INFO) << "FDBMetaStore::getFolderList() directory list not exist";
        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return 0;
    }

    nlohmann::json *dljPtr = new nlohmann::json();
    auto &dlj = *dljPtr;
    if (parseStrToJSONObj(dirListStr, dlj) == false)
    {
        exit(1);
    }

    for (auto &dir : dlj["list"])
    {
        std::string dirStr = dir.get<std::string>();
        // filter invalid strings
        if (dirStr.size() < pfSize - 1)
        {
            continue;
        }
        // check if pattern exists in dirStr
        if (dirStr.compare(0, pfSize, pattern) != 0)
        {
            continue;
        }
        // skip subfolders
        if (skipSubfolders && strchr(dirStr.c_str() + pfSize - 1, '/') != 0)
        {
            continue;
        }
        list.push_back(dirStr.substr(pfSize - 1));
        count++;

        DLOG(INFO) << "Add " << dirStr << " to the result of " << pattern;
    }

    delete dljPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return 0;
}

unsigned long int FDBMetaStore::getMaxNumKeysSupported()
{
    // max = (1 << 32) - 1 - FDB_NUM_RESERVED_SYSTEM_KEYS, but we store also uuid for each file
    return (unsigned long int)(1 << 31) - FDB_NUM_RESERVED_SYSTEM_KEYS / 2 - (FDB_NUM_RESERVED_SYSTEM_KEYS % 2);
}

unsigned long int FDBMetaStore::getNumFiles()
{
    std::lock_guard<std::mutex> lk(_lock);
    unsigned long int count = 0;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // TBD!!!

    return 0;
}

unsigned long int FDBMetaStore::getNumFilesToRepair()
{
    std::lock_guard<std::mutex> lk(_lock);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::string fileRepairStr;
    bool fileRepairExist = getValueInTX(tx, std::string(FDB_FILE_REPAIR_KEY), fileRepairStr);

    if (fileRepairExist == false)
    {
        LOG(WARNING) << "FDBMetaStore::getNumFilesToRepair() Error finding file repair list";
        return -1; // same as Redis-based MetaStore
    }

    nlohmann::json *frjPtr = new nlohmann::json();
    auto &frj = *frjPtr;
    if (parseStrToJSONObj(fileRepairStr, frj) == false)
    {
        exit(1);
    }
    unsigned long int count = frj["list"].size();
    delete frjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return count;
}

int FDBMetaStore::getFilesToRepair(int numFiles, File files[])
{
    std::lock_guard<std::mutex> lk(_lock);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::string fileRepairStr;
    bool fileRepairExist = getValueInTX(tx, std::string(FDB_FILE_REPAIR_KEY), fileRepairStr);

    if (fileRepairExist == false)
    {
        LOG(WARNING) << "FDBMetaStore::getNumFilesToRepair() Error finding file repair list";
        return -1; // same as Redis-based MetaStore
    }

    nlohmann::json *frjPtr = new nlohmann::json();
    auto &frj = *frjPtr;
    if (parseStrToJSONObj(fileRepairStr, frj) == false)
    {
        exit(1);
    }
    unsigned long int count = frj["list"].size();

    if (count < numFiles)
    {
        LOG(ERROR) << "FDBMetaStore::getFilesToRepair() insufficient number of files " << count << " files";

        delete frjPtr;
        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return -1;
    }

    int numFilesToRepair = 0;
    for (int i = 0; i < numFiles; i++)
    {
        // remove the last element
        std::string fileName = frj["list"].back().get<std::string>();
        free(files[numFilesToRepair].name);
        if (!getNameFromFileKey(fileName.c_str(), fileName.size(), &files[numFilesToRepair].name, files[numFilesToRepair].nameLength, files[numFilesToRepair].namespaceId, &files[numFilesToRepair].version))
        {
            continue;
        }
        frj["list"].erase(frj["list"].end());
        numFilesToRepair++;
    }

    delete frjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return 0;
}

bool FDBMetaStore::markFileAsRepaired(const File &file)
{
    return markFileRepairStatus(file, true);
}

bool FDBMetaStore::markFileAsNeedsRepair(const File &file)
{
    return markFileRepairStatus(file, false);
}

bool FDBMetaStore::markFileRepairStatus(const File &file, bool needsRepair)
{
    return markFileStatus(file, FDB_FILE_REPAIR_KEY, needsRepair, "repair");
}

bool FDBMetaStore::markFileAsPendingWriteToCloud(const File &file)
{
    return markFileStatus(file, FDB_FILE_PENDING_WRITE_KEY, true, "pending write to cloud");
}

bool FDBMetaStore::markFileAsWrittenToCloud(const File &file, bool removePending)
{
    return markFileStatus(file, FDB_FILE_PENDING_WRITE_COMP_KEY, false, "pending completing write to cloud") &&
           (!removePending || markFileStatus(file, FDB_FILE_PENDING_WRITE_KEY, false, "pending write to cloud"));
}

bool FDBMetaStore::markFileStatus(const File &file, const char *listName, bool set, const char *opName)
{
    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX];
    int verFileKeyLength = genFileKey(file.namespaceId, file.name, file.nameLength, fileKey);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check listName
    std::string listStr;
    bool listExist = getValueInTX(tx, std::string(listName), listStr);
    if (listExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::markFileStatus() Error finding list " << listName;

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    nlohmann::json *ljPtr = new nlohmann::json();
    auto &lj = *ljPtr;
    if (parseStrToJSONObj(listStr, lj) == false)
    {
        LOG(ERROR) << "FDBMetaStore::markFileStatus() Error finding file in list " << listName;

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    if (set == false)
    {
        // remove the file from the list
        lj["list"].erase(std::remove(lj["list"].begin(), lj["list"].end(), std::string(fileKey, verFileKeyLength)), lj["list"].end());
    }
    else
    {
        // add the file to the list
        if (std::find(lj["list"].begin(), lj["list"].end(), std::string(fileKey, verFileKeyLength)) == lj["list"].end())
        {
            lj["list"].push_back(std::string(fileKey, verFileKeyLength));
        }
    }

    std::string ljStr = lj.dump();
    delete ljPtr;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(listName), strlen(listName), reinterpret_cast<const uint8_t *>(ljStr.c_str()), ljStr.size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    DLOG(INFO) << "File " << file.name << "(" << std::string(fileKey) << ")" << (set ? " added to" : " removed from") << " the " << opName << " list";

    return false;
}

int FDBMetaStore::getFilesPendingWriteToCloud(int numFiles, File files[])
{
    // TODO: this implementation is not checked correctness for now

    std::lock_guard<std::mutex> lk(_lock);

    int retVal = 0;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::string filePendingWriteStr;
    std::string filePendingWriteCopyKey = std::string(FDB_FILE_PENDING_WRITE_KEY) + std::string("_copy");
    bool filePendingWriteExist = getValueInTX(tx, filePendingWriteCopyKey, filePendingWriteStr);

    if (filePendingWriteExist == false)
    {
        LOG(WARNING) << "FDBMetaStore::getFilesPendingWriteToCloud() Error finding file pending write list";

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return retVal; // same as Redis-based MetaStore
    }

    nlohmann::json *fpwjPtr = new nlohmann::json();
    auto &fpwj = *fpwjPtr;
    if (parseStrToJSONObj(filePendingWriteStr, fpwj) == false)
    {
        exit(1);
    }

    int numFilesToRepair = 0;
    numFilesToRepair = fpwj["list"].size();

    if (numFilesToRepair == 0 && !_endOfPendingWriteSet)
    {
        _endOfPendingWriteSet = true;

        delete fpwjPtr;
        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);
        return retVal;
    }

    // refill the set for scan
    if (numFilesToRepair == 0)
    {

        delete fpwjPtr;
        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);
        return retVal;
    }

    // mark the set scanning is in-progress
    _endOfPendingWriteSet = false;

    std::fileKey = fpwj["list"].back().get<std::string>();
    fpwj.erase(fpwj["list"].end());

    std::string fpwjStr = fpwj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePendingWriteCopyKey.c_str()), filePendingWriteCopyKey.size(), reinterpret_cast<const uint8_t *>(fpwjStr.c_str()), fpwjStr.size());

    delete fpwjPtr;

    // add the key to filePendingWriteCompleteKey
    std::string filePendingWriteCompStr;
    bool filePendingWriteCompExist = getValueInTX(tx, std::string(FDB_FILE_PENDING_WRITE_COMP_KEY), filePendingWriteCompStr);

    nlohmann::json *fpwcPtr = new nlohmann::json();
    auto &fpwc = *fpwcPtr;

    if (filePendingWriteCompExist == false)
    {
        fpwc["list"] = nlohhmann::json::array();
    }
    else
    {
        if (parseStrToJSONObj(filePendingWriteCompStr, fpwc) == false)
        {
            exit(1);
        }
    }

    bool addToList = false;

    if (std::find(fpwc["list"].begin(), fpwc["list"].end(), fileKey) == fpwc["list"].end())
    {
        fpwc["list"].push_back(fileKey);
        addToList = true;
    }

    if (addToList && getNameFromFileKey(fileKey.data(), fileKey.length(), &files[0].name, files[0].nameLength, files[0].namespaceId, &files[0].version))
    {
        retVal = 1;
    }

    delete fpwcPtr;

    std::string fpwcStr = fpwc.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_FILE_PENDING_WRITE_COMP_KEY), strlen(FDB_FILE_PENDING_WRITE_COMP_KEY), reinterpret_cast<const uint8_t *>(fpwcStr.c_str()), fpwcStr.size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return retVal;
}

bool FDBMetaStore::updateFileStatus(const File &file)
{
    // TODO: this implementation is not checked correctness for now
    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(file.namespaceId, file.name, file.nameLength, fileKey);
    bool ret = false;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    if (file.status == FileStatus::PART_BG_TASK_COMPLETED)
    {
        // decrement number of task by 1, and remove the file is the number of pending task drops to 0
    }
    else if (file.status == FileStatus::BG_TASK_PENDING)
    {
        // increment number of task by 1
    }
    else if (file.status == FileStatus::ALL_BG_TASKS_COMPLETED)
    {
        // remove the file from the list
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return false;
}

bool FDBMetaStore::getNextFileForTaskCheck(File &file)
{
    std::lock_guard<std::mutex> lk(_lock);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::string bgTaskPendingStr;
    bool bgTaskPendingExist = getValueInTX(tx, std::string(FDB_BG_TASK_PENDING_KEY), bgTaskPendingStr);

    if (bgTaskPendingExist == false)
    {
        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false; // same as Redis-based MetaStore
    }

    nlohmann::json *btpjPtr = new nlohmann::json();
    auto &btpj = *btpjPtr;
    if (parseStrToJSONObj(bgTaskPendingStr, btpj) == false)
    {
        exit(1);
    }

    std::string fileKey = btpj["list"].back().get<std::string>();
    char *d = strchr(fileKey.c_str(), '_');
    if (d != nullptr)
    {
        file.nameLength = fileKey.size() - (d - fileKey.c_str() + 1);
        file.name = (char *)malloc(file.nameLength + 1);
        memcpy(file.name, d + 1, file.nameLength);
        file.name[file.nameLength] = '\0';
        file.namespaceId = atoi(fileKey.c_str());
        DLOG(INFO) << "Next file to check: " << file.name << ", " << (int)file.namespaceId;
    }

    delete btpjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return true;
}

bool FDBMetaStore::lockFile(const File &file)
{
    std::lock_guard<std::mutex> lk(_lock);
    return getLockOnFile(file, true);
}

bool FDBMetaStore::unlockFile(const File &file)
{
    std::lock_guard<std::mutex> lk(_lock);
    return getLockOnFile(file, false);
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

bool FDBMetaStore::addChunkToJournal(const File &file, const Chunk &chunk, int containerId, bool isWrite)
{
    extractJournalFieldKeyParts("", 0);
    return false;
}

bool FDBMetaStore::updateChunkInJournal(const File &file, const Chunk &chunk, bool isWrite, bool deleteRecord, int containerId)
{
    // TBD
    return false;
}

void FDBMetaStore::getFileJournal(const FileInfo &file, std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> &records)
{
    // TBD
}

int FDBMetaStore::getFilesWithJounal(FileInfo **list)
{
    // TBD
    return 0;
}

bool FDBMetaStore::fileHasJournal(const File &file)
{
    // TBD
    return false;
}

int FDBMetaStore::genFileKey(unsigned char namespaceId, const char *name, int nameLength, char key[])
{
    return snprintf(key, PATH_MAX, "%d_%*s", namespaceId, nameLength, name);
}

int FDBMetaStore::genVersionedFileKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[])
{
    return snprintf(key, PATH_MAX, "/%d_%*s\n%d", namespaceId, nameLength, name, version);
}

bool FDBMetaStore::genFileUuidKey(unsigned char namespaceId, boost::uuids::uuid uuid, char key[])
{
    return snprintf(key, FDB_MAX_KEY_SIZE + 64, "//fu%d-%s", namespaceId, boost::uuids::to_string(uuid).c_str()) <= FDB_MAX_KEY_SIZE;
}

int FDBMetaStore::genChunkKeyPrefix(int chunkId, char prefix[])
{
    return snprintf(prefix, FDB_MAX_KEY_SIZE, "c%d", chunkId);
}

int FDBMetaStore::genFileJournalKeyPrefix(char key[], unsigned char namespaceId)
{
    if (namespaceId == 0)
    {
        return snprintf(key, FDB_MAX_KEY_SIZE, "//jl");
    }
    return snprintf(key, FDB_MAX_KEY_SIZE, "//jl_%d", namespaceId);
}

int FDBMetaStore::genFileJournalKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[])
{
    int prefixLength = genFileJournalKeyPrefix(key, namespaceId);
    return snprintf(key + prefixLength, FDB_MAX_KEY_SIZE - prefixLength, "_%*s_%d", nameLength, name, version) + prefixLength;
}

const char *FDBMetaStore::getBlockKeyPrefix(bool unique)
{
    return unique ? "ub" : "db";
}

int FDBMetaStore::genBlockKey(int blockId, char prefix[], bool unique)
{
    return snprintf(prefix, FDB_MAX_KEY_SIZE, "%s%d", getBlockKeyPrefix(unique), blockId);
}

bool FDBMetaStore::getNameFromFileKey(const char *str, size_t len, char **name, int &nameLength, unsigned char &namespaceId, int *version)
{
    // full name in form of "namespaceId_filename"
    int ofs = isVersionedFileKey(str) ? 1 : 0;
    std::string fullname(str + ofs, len - ofs);
    size_t dpos = fullname.find_first_of("_");
    if (dpos == std::string::npos)
        return false;
    size_t epos = fullname.find_first_of("\n");
    if (epos == std::string::npos)
    {
        epos = len;
    }
    else if (version)
    {
        *version = atoi(str + ofs + epos + 1);
    }

    // fill in the namespace id, file name length and file name
    std::string namespaceIdStr(fullname, 0, dpos);
    namespaceId = strtol(namespaceIdStr.c_str(), NULL, 10) % 256;
    *name = (char *)malloc(epos - dpos);
    nameLength = epos - dpos - 1;
    memcpy(*name, str + ofs + dpos + 1, nameLength);
    (*name)[nameLength] = 0;

    return true;
}

bool FDBMetaStore::getFileName(char fileUuidKey[], File &f)
{
    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileNameStr;
    bool fileNameExist = getValueInTX(tx, std::string(fileUuidKey, FDB_MAX_KEY_SIZE + 64), fileNameStr);
    if (fileNameExist == false)
    {
        LOG(WARNING) << "FDBMetaStore::getFileName() failed to get filename from File UUID Key " << std::string(fileUuidKey, FDB_MAX_KEY_SIZE + 64);

        // commit transaction and return
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    // copy to file name
    f.nameLength = fileNameStr.size();
    f.name = (char *)malloc(f.nameLength + 1);
    strncpy(f.name, fileNameStr.c_str(), f.nameLength);
    f.name[f.nameLength] = 0;

    // commit transaction and return
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return true;
}

bool FDBMetaStore::isSystemKey(const char *key)
{
    return (
        strncmp("//", key, 2) == 0 ||
        false);
}

bool FDBMetaStore::isVersionedFileKey(const char *key)
{
    return (
        strncmp("/", key, 1) == 0 ||
        false);
}

std::string FDBMetaStore::getFilePrefix(const char name[], bool noEndingSlash)
{
    // the location of sub-dirs
    const char *slash = strrchr(name, '/');

    // the location of first '_' (to locate namespace)
    const char *us = strchr(name, '_');

    std::string filePrefix(FDB_FILE_PREFIX);
    // file on root directory (namespace_filename), or root directory (ends
    // with one '/' (namespace_/))
    if (slash == NULL || us + 1 == slash)
    {
        // set filePrefix to "FDB_FILE_PREFIX<namesapce>_"
        filePrefix.append(name, us - name + 1);
        // append a slash at the end
        return noEndingSlash ? filePrefix : filePrefix.append("/");
    }
    // set filePrefix to //pf_dirname
    return filePrefix.append(name, slash - name);
}

bool FDBMetaStore::getLockOnFile(const File &file, bool lock)
{
    return lockFile(file, lock, FDB_FILE_LOCK_KEY, "lock");
}

bool FDBMetaStore::pinStagedFile(const File &file, bool lock)
{
    return lockFile(file, lock, FDB_FILE_PIN_STAGED_KEY, "pin");
}

bool FDBMetaStore::lockFile(const File &file, bool lock, const char *type, const char *name)
{
    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(file.namespaceId, file.name, file.nameLength, fileKey);

    std::string lockListName(type);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // add filename to file Prefix Set
    std::string lockListStr;
    bool lockListExist = getValueInTX(tx, lockListName, lockListStr);

    // unlock but lock list not exist
    if (lock == false && lockListExist == false)
    {
        // commit transaction and return
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    bool retVal = false;
    nlohmann::json *lljPtr = new nlohmann::json();
    auto &llj = *lljPtr;
    if (lockListExist == false)
    {
        // create the set and add the fileKey
        llj["list"] = nlohmann::json::array();
        llj["list"].push_back(std::string(fileKey, fileKeyLength));
        retVal = true;
    }
    else
    {
        if (parseStrToJSONObj(lockListStr, llj) == false)
        {
            exit(1);
        }

        bool lockExist = (std::find(llj["list"].begin(), llj["list"].end(), std::string(fileKey, fileKeyLength)) != llj["list"].end());

        if (lock == true)
        { // lock
            if (lockExist == false)
            {
                llj["list"].push_back(std::string(fileKey, fileKeyLength));
                retVal = true;
            }
        }
        else
        { // unlock
            if (lockExist == true)
            {
                llj["list"].erase(std::remove(llj["list"].begin(), llj["list"].end(), std::string(fileKey, fileKeyLength)), llj["list"].end());
                retVal = true;
            }
        }
    }
    std::string lljStr = llj.dump();
    delete lljPtr;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(lockListName.c_str()), lockListName.size(), reinterpret_cast<const uint8_t *>(lljStr.c_str()), lljStr.size());

    // commit transaction and return
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return retVal;
}

void FDBMetaStore::exitOnError(fdb_error_t err)
{
    if (err)
    {
        LOG(ERROR) << "FoundationDB MetaStore error: " << fdb_get_error(err);
        exit(1);
    }
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

bool FDBMetaStore::getValueInTX(FDBTransaction *tx, const std::string &key, std::string &value)
{
    if (tx == NULL)
    {
        LOG(ERROR) << "FDBMetaStore:: getValueInTX() invalid Transaction";
        return false;
    }

    // Create future
    FDBFuture *getFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(key.c_str()), key.size(), 0); // not set snapshot
    exitOnError(fdb_future_block_until_ready(getFut));

    fdb_bool_t isKeyPresent;
    const uint8_t *valueRaw = nullptr;
    int valueRawLength;

    // block future and get raw value
    exitOnError(fdb_future_get_value(getFut, &isKeyPresent, &valueRaw, &valueRawLength));
    fdb_future_destroy(getFut);
    getFut = nullptr;

    // check whether the key presents
    if (isKeyPresent)
    {
        // parse result as string
        value = std::string(reinterpret_cast<const char *>(valueRaw), valueRawLength);
        // LOG(INFO) << "FDBMetaStore::getValueInTX() key " << key << " found: value " << value;
        // DLOG(INFO) << "FDBMetaStore::getValueInTX() key " << key << " found, value has size: " << value.size();
        return true;
    }
    else
    {
        // DLOG(INFO) << "FDBMetaStore:: getValueInTX() key " << key << " not found";
        return false;
    }
}

bool FDBMetaStore::parseStrToJSONObj(const std::string &str, nlohmann::json &j)
{
    try
    {
        j = nlohmann::json::parse(str);
    }
    catch (const nlohmann::json::parse_error &e)
    {
        LOG(ERROR) << "FDBMetaStore::parseStrToJSONObj() Error parsing JSON string: " << e.what() << ", exception id: " << e.id << '\n'
                   << "byte position of error: " << e.byte << std::endl;
        ;
        return false;
    }
    return true;
}
