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
#define FDB_COMMON_KEY_PREFIX "//sncc"
#define FDB_FILE_LOCK_KEY "//snccFLock"
#define FDB_FILE_PIN_STAGED_KEY "//snccFPinStaged"
#define FDB_FILE_REPAIR_KEY "//snccFRepair"
#define FDB_FILE_PENDING_WRITE_KEY "//snccFPendingWrite"
#define FDB_FILE_PENDING_WRITE_COMP_KEY "//snccFPendingWriteComp"
#define FDB_BG_TASK_PENDING_KEY "//snccFBgTask"
#define FDB_DIR_LIST_KEY "//snccDirList"
#define FDB_JL_LIST_KEY "//snccJournalFSet"
#define FDB_FILE_PREFIX_KEY "//pf_"

#define FDB_MAX_KEY_SIZE (64)
#define FDB_NUM_REQ_FIELDS (10)

static std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength);

FDBMetaStore::FDBMetaStore()
{
    // didn't use for now
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

    // TODO: check the params here
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

    // create a file version summary
    // format: "size mtime md5 isDeleted numChunks"
    std::string fVerSummary;
    fVerSummary.append(std::to_string(f.size).append(" "));
    fVerSummary.append(std::to_string(f.mtime).append(" "));
    fVerSummary.append(std::string(f.md5, MD5_DIGEST_LENGTH).append(" "));
    fVerSummary.append(std::to_string(((f.size == 0) ? f.isDeleted : 0)).append(" "));
    fVerSummary.append(std::to_string(f.numChunks));

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, fileKey, fileMetaStr);

    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;

    if (fileMetaExist == false)
    { // No file meta: init the new version
        // version id: (TODO: check correctness when f.version == -1)
        fmj["verId"] = nlohmann::json::array();
        fmj["verId"].push_back(f.version);
        // version name
        fmj["verName"] = nlohmann::json::array();
        fmj["verName"].push_back(verFileKey);
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
                fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(prevVerFileKey.c_str()), prevVerFileKey.size());
            }

            // only keep the input file version
            fmj["verId"].clear();
            fmj["verId"].push_back(f.version);
            fmj["verName"].clear();
            fmj["verName"].push_back(verFileKey);
            fmj["verSummary"].clear();
            fmj["verSummary"].push_back(fVerSummary);
        }
        else
        {
            // if the version is not stored, insert it in ascending order
            if (fmj["verId"].find(verFileKey) == fmj["verId"].end())
            {
                int pos;
                for (pos = 0; pos < fmj["verId"].size(); pos++)
                {
                    if (f.version < fmj["verId"].get<int>(i))
                    {
                        break;
                    }
                }
                fmj["verId"].insert(fmj["verId"].begin() + pos, f.version);
                fmj["verName"].insert(fmj["verName"].begin() + pos, verFileKey);
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
    verFmj["size"] = std::to_string(f.size);
    verFmj["numC"] = std::to_string(f.numChunks);
    verFmj["sc"] = f.storageClass;
    verFmj["cs"] = std::string(1, f.codingMeta.coding);
    verFmj["n"] = std::to_string(f.codingMeta.n);
    verFmj["k"] = std::to_string(f.codingMeta.k);
    verFmj["f"] = std::to_string(f.codingMeta.f);
    verFmj["maxCS"] = std::to_string(f.codingMeta.maxChunkSize);
    verFmj["codingStateS"] = std::to_string(f.codingMeta.codingStateSize);
    verFmj["codingState"] = std::string(reinterpret_cast<char *>(codingState));
    verFmj["numS"] = std::to_string(f.numStripes);
    verFmj["ver"] = std::to_string(f.version);
    verFmj["ctime"] = std::to_string(f.ctime);
    verFmj["atime"] = std::to_string(f.atime);
    verFmj["mtime"] = std::to_string(f.mtime);
    verFmj["tctime"] = std::to_string(f.tctime);
    verFmj["md5"] = std::string(f.md5, MD5_DIGEST_LENGTH);
    verFmj["sg_size"] = std::to_string(f.staged.size);
    verFmj["sg_sc"] = f.staged.storageClass;
    verFmj["sg_cs"] = std::to_string(f.staged.codingMeta.coding);
    verFmj["sg_n"] = std::to_string(f.staged.codingMeta.n);
    verFmj["sg_k"] = std::to_string(f.staged.codingMeta.k);
    verFmj["sg_f"] = std::to_string(f.staged.codingMeta.f);
    verFmj["sg_maxCS"] = std::to_string(f.staged.codingMeta.maxChunkSize);
    verFmj["sg_mtime"] = std::to_string(f.staged.mtime);
    verFmj["dm"] = std::to_string(deleted);
    verFmj["numUB"] = std::to_string(numUniqueBlocks);
    verFmj["numDB"] = std::to_string(numDuplicateBlocks);

    // container ids
    char chunkName[FDB_MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++)
    {
        genChunkKeyPrefix(f.chunks[i].getChunkId(), chunkName);
        std::string cidKey = std::string(chunkName) + std::string("-cid");
        verFmj[cidKey.c_str()] = std::to_string(f.containerIds[i]);
        std::string csizeKey = std::string(chunkName) + std::string("-size");
        verFmj[csizeKey.c_str()] = std::to_string(f.chunks[i].size);
        std::string cmd5Key = std::string(chunkName) + std::string("-md5");
        verFmj[cmd5Key.c_str()] = std::string(f.chunks[i].md5, MD5_DIGEST_LENGTH);
        std::string cmd5Bad = std::string(chunkName) + std::string("-bad");
        verFmj[cmd5Bad.c_str()] = std::to_string((f.chunksCorrupted ? f.chunksCorrupted[i] : 0));
    }

    // deduplication fingerprints and block mapping
    char blockName[FDB_MAX_KEY_SIZE];
    size_t blockId = 0;
    for (auto it = f.uniqueBlocks.begin(); it != f.uniqueBlocks.end(); it++, blockId++)
    { // deduplication fingerprints
        genBlockKey(blockId, blockName, /* is unique */ true);
        std::string fp = it->second.first.get();
        // logical offset, length, fingerprint, physical offset
        verFmj[std::string(blockName).c_str()] = std::to_string(it->first._offset) + std::to_string(it->first._length) + fp.data() + std::to_string(it->second.second);
    }
    blockId = 0;
    for (auto it = f.duplicateBlocks.begin(); it != f.duplicateBlocks.end(); it++, blockId++)
    {
        genBlockKey(blockId, blockName, /* is unique */ false);
        std::string fp = it->second.get();
        // logical offset, length, fingerprint
        verFmj[std::string(blockName).c_str()] = std::to_string(it->first._offset) + std::to_string(it->first._length) + fp.data();
    }

    std::string verFmjStr = verFmj.dump();
    // Store versioned file meta into FDB
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());
    delete verFmjPtr;

    // add uuid-to-file-name maping
    char fUUIDKey[FDB_MAX_KEY_SIZE + 64];
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
    bool fPrefixSetExist = getValueInTX(tx, filePrefix, fPrefixListStr);

    nlohmann::json *fpljPtr = new nlohmann::json();
    auto &fplj = *fpljPtr;
    if (fPrefixSetExist == false)
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
        if (fplj["list"].find(fileKey) == fplj["list"].end())
        {
            fplj["list"].push_back(fileKey);
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
        if (dlj["list"].find(filePrefix) == dlj["list"].end())
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

    LOG(INFO) << "FDBMetaStore:: putMeta() finished";

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
    bool fileMetaExist = getValueInTX(tx, fileKey, fileMetaStr);

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

    if (f.version == -1)
    { // version not specified: retrieved the latest version
        std::string lastVerFileKey = fmj["verName"].back().get<std::string>();
        memcpy(verFileKey, lastVerFileKey.c_str(), lastVerFileKey.size());
    }
    else
    { // versioned file
        // check whether the file version exists
        if (fmj["verName"].find(verFileKey) == fmj["verName"].end())
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
    std::string verFmjStr;
    bool verFmjExist = getValueInTX(tx, verFileKey, verFmjStr);
    if (verFileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::getMeta() failed to find version in MetaStore, file: " << f.name << ", version: " << f.version;
        exit(1);
    }

    nlohmann::json *verFmjPtr = new nlohmann::json();
    auto &verFmj = *verFmjPtr;
    if (parseStrToJSONObj(verFmjStr, verFmj) == false)
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
    f.staged.mtime = verFmj["sg_mtime"].get<time_t>();
    f.isDeleted = verFmj["dm"].get<bool>();
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
        genChunkKeyPrefix(chunkId, chunkName);
        std::string cidKey = std::string(chunkName) + std::string("-cid");
        std::string csizeKey = std::string(chunkName) + std::string("-size");
        std::string cmd5Key = std::string(chunkName) + std::string("-md5");
        std::string cbadKey = std::string(chunkName) + std::string("-bad");

        f.containerIds[chunkId] = std::stoi(verFmj[cidKey.c_str()].get<std::string>());
        f.chunks[chunkId].size = std::stoi(verFmj[csizeKey.c_str()].get<std::string>());
        f.chunks[chunkId].md5 = std::string(verFmj[cmd5Key.c_str()].get<std::string>().c_str(), MD5_DIGEST_LENGTH);
        f.chunksCorrupted[chunkId] = std::stoi(verFmj[cbadKey.c_str()].get<std::string>()); // TODO: double check this field; make sure it's correct
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
        int pOffset = 0;
        int noFpOfs = sizeof(unsigned long int) + sizeof(unsigned int);
        int hasFpOfs = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH;
        int lengthWithFp = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH + sizeof(int);
        for (size_t blockId = 0; blockId < numUniqueBlocks; blockId++)
        {
            genBlockKey(blockId, blockName, /* is unique */ true);
            std::string blockStr = verFmj[std::string(blockName).c_str()].get<std::string>();
            loc._offset = std::stoull(blockStr.substr(pOffset, sizeof(unsigned long int)));
            loc._length = std::stoull(blockStr.substr(pOffset + sizeof(unsigned long int), sizeof(unsigned int)));
            if (verFmj[std::string(blockName).c_str()].size() >= lengthWithFp)
            {
                fp.set(blockStr.substr(pOffset + hasFpOfs, SHA256_DIGEST_LENGTH).c_str());
            }
            else
            {
                fp.set(blockStr.substr(pOffset + noFpOfs, SHA256_DIGEST_LENGTH).c_str());
            }

            auto followIt = f.uniqueBlocks.end(); // hint is the item after the element to insert for c++11, and before the element for c++98
            f.uniqueBlocks.emplace_hint(followIt, std::make_pair(loc, std::make_pair(fp, pOffset)));
        }
    }

    if (getBlocks == 2 || getBlocks == 3)
    { // duplicate blocks
        int noFpOfs = sizeof(unsigned long int) + sizeof(unsigned int);
        int lengthWithFp = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH;

        for (size_t blockId = 0; blockId < numDuplicateBlocks; blockId++)
        {
            genBlockKey(blockId, blockName, /* is unique */ false);

            loc._offset = std::stoull(verFmj[std::string(blockName).c_str()].substr(0, sizeof(unsigned long int)));
            loc._length = std::stoull(verFmj[std::string(blockName).c_str()].substr(sizeof(unsigned long int), sizeof(unsigned int)));

            if (verFmj[std::string(blockName).c_str()].size() >= lengthWithFp)
            {
                fp.set(verFmj[std::string(blockName).c_str()].substr(noFpOfs, SHA256_DIGEST_LENGTH).c_str());
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

    LOG(INFO) << "FDBMetaStore::getMeta() finished";

    return true;
}

bool FDBMetaStore::deleteMeta(File &f)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileKey[PATH_MAX], verFileKey[PATH_MAX];

    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);
    int verFileKeyLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, verFileKey);

    int versionToDelete = f.version;
    std::string filePrefix = getFilePrefix(fileKey);

    bool isVersioned = !config.overwriteFiles();
    bool lazyDeletion = false;
    bool ret = true;

    DLOG(INFO) << "FDBMetaStore::deleteMeta() start to delete file " << f.name << ", version: " << f.version;

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
    bool fileMetaExist = getValueInTX(tx, fileKey, fileMetaStr);

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

    // delete a specific version
    if (isVersioned && versionToDelete != -1)
    {
        int curVersion = fmj["verId"].back();
        int numVersions = fmj["verId"].size();

        if (numVersions == 0)
        {
            LOG(WARNING) << "FDBMetaStore::deleteMeta() No version found for file " << f.name;
            return false;
        }

        // check whether the version exists in version list
        auto it = fmj["verId"].find(versionToDelete);
        if (it == fmj["verId"].end())
        {
            // handle error
            LOG(WARNING) << "FDBMetaStore::deleteMeta() version not found for file " << f.name << ", version: " << versionToDelete;
            return false;
        }

        // remove a specific version
        int idx = std::distance(fmj["verId"].begin(), it);
        fmj["verId"].erase(fmj["verId"].begin() + idx);
        fmj["verName"].erase(fmj["verName"].begin() + idx);
        fmj["verSummary"].erase(fmj["verSummary"].begin() + idx);

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

    // TODO remove workaround for renamed file
    f.genUUID();

    // remove file uuid key
    if (!genFileUuidKey(f.namespaceId, f.uuid, fidKey))
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
    auto it = fplj["list"].find(fileKey);
    if (it != fplj["list"].end())
    {
        fplj["list"].erase(it);
    }

    int numFilesInDir = fplj["list"].size();

    std::string fpljStr = fplj.dump();
    delete fpljPtr;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size(), reinterpret_cast<const uint8_t *>(fpljStr.c_str()), fpljStr.size());

    // remove file prefix from directory set
    if (numFilesInDir == 0)
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
        auto it = dlj["list"].find(filePrefix);
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
    bool fileMetaExist = getValueInTX(tx, srcFileKey, fileMetaStr);
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
    try
    {
        std::string filMetaStr(reinterpret_cast<const char *>(fileMetaRaw));
        fmj = nlohmann::json::parse(filMetaStr);
    }
    catch (std::exception e)
    {
        LOG(ERROR) << "FDBMetaStore::putMeta() Error parsing JSON string: " << e.what();
        exit(1);
    }

    // update uuid-to-file-name mapping
    fmj["uuid"] = boost::uuids::to_string(df.uuid).c_str();

    // store in dstFileKey
    std::string fmjStr = fmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileKey), dstFileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());
    delete fmjPtr;

    // remove srcFileKey
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFileKey), srcFileKeyLength);

    // set dstFileUuidKey; remove the original sfidKey (check putMeta impl)
    // insert new metadata
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileUuidKey), FDB_MAX_KEY_SIZE + 64, dstFileKey, dstFileKeyLength);

    DLOG(INFO) << "FDBMetaStore::renameMeta() Add reverse mapping (" << dstFileUuidKey << ") for file " << dstFileKey;

    // remove srcFileUuidKey
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFileUuidKey), FDB_MAX_KEY_SIZE + 64);

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
    srcFPlj["list"].erase(std::remove(srcFPlj["list"].begin(), srcFPlj["list"].end(), srcFileKey), srcFPlj["list"].end());

    std::string srcFPljStr = srcFPlj.dump();
    delete srcFPljPtr;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(srcFilePrefix.c_str()), srcFilePrefix.size(), reinterpret_cast<const uint8_t *>(srcFPljStr.c_str()), srcFPljStr.size());

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
    dstFPlj["list"].push_back(dstFileKey);

    std::string dstFPljStr = dstFPlj.dump();
    delete dstFPljPtr;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFilePrefix.c_str()), dstFilePrefix.size(), reinterpret_cast<const uint8_t *>(dstFPljStr.c_str()), dstFPljStr.size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));

    fdb_future_destroy(cmt);

    LOG(INFO) << "FDBMetaStore:: renameMeta() finished";

    return true;
}

bool FDBMetaStore::updateTimestamps(const File &f)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    // update the latest version

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    FDBFuture *fileMetaFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength, 0 /** not set snapshot */);
    exitOnError(fdb_future_block_until_ready(fileMetaFut));
    fdb_bool_t fileMetaExist;
    const uint8_t *fileMetaRaw = NULL;
    int fileMetaRawLength;
    exitOnError(fdb_future_get_value(fileMetaFut, &fileMetaExist, &fileMetaRaw, &fileMetaRawLength));
    fdb_future_destroy(fileMetaFut);
    fileMetaFut = nullptr;

    // current metadata format: key: filename; value: {"verList": [v0, v1, v2,
    // ...]}; for non-versioned system, verList only stores v0
    nlohmann::json *fmjptr = new nlohmann::json();
    auto &fmj = *fmjptr;

    std::String verFileKey;

    if (fileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateTimestamps() failed to get metadata for file " << f.name;

        // commit transaction and return
        delete fmjptr;
        FDBFuture *fcmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(fcmt));
        fdb_future_destroy(fcmt);

        return false;
    }
    else
    {
        // parse fileMeta as JSON object
        try
        {
            std::string filMetaRawStr(reinterpret_cast<const char *>(fileMetaRaw));
            fmj = nlohmann::json::parse(filMetaRawStr);
        }
        catch (std::exception e)
        {
            LOG(ERROR) << "FDBMetaStore::updateTimestamps() Error parsing JSON string: " << e.what();
            exit(1);
        }

        delete fmjptr;

        verFileKey = fmj["verList"].back().get<std::string>();
    }

    // find metadata for current file version
    FDBFuture *verFileMetaFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, 0 /** not set snapshot */);
    exitOnError(fdb_future_block_until_ready(verFileMetaFut));
    fdb_bool_t verFileMetaExist;
    const uint8_t *verFileMetaRaw = NULL;
    int verFileMetaRawLength;
    exitOnError(fdb_future_get_value(verFileMetaFut, &verFileMetaExist, &verFileMetaRaw, &verFileMetaRawLength));
    fdb_future_destroy(verFileMetaFut);
    verFileMetaFut = nullptr;

    nlohmann::json *vfmjptr = new nlohmann::json();
    auto &vfmj = *vfmjptr;

    if (verFileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateTimestamps() failed to get versioned metadata for file " << f.name;

        // commit transaction and return
        delete vfmjptr;
        FDBFuture *fcmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(fcmt));
        fdb_future_destroy(fcmt);

        return false;
    }
    else
    {
        // parse fileMeta as JSON object
        try
        {
            std::string verFileMetaRawStr(reinterpret_cast<const char *>(verFileMetaRaw));
            vfmj = nlohmann::json::parse(verFileMetaRawStr);
        }
        catch (std::exception e)
        {
            LOG(ERROR) << "FDBMetaStore::updateTimestamps() Error parsing JSON string: " << e.what();
            exit(1);
        }

        vfmj["atime"] = std::to_string(f.atime);
        vfmj["mtime"] = std::to_string(f.mtime);
        vfmj["ctime"] = std::to_string(f.ctime);

        // serialize json to string and store in FDB
        std::string vfmjStr = vfmj.dump();
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, reinterpret_cast<const uint8_t *>(vfmjStr.c_str()), vfmjStr.size());
        delete vfmjptr;
    }

    LOG(INFO) << "FDBMetaStore:: updateTimestamps() finished";

    return true;
}

int FDBMetaStore::updateChunks(const File &f, int version)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    // update the latest version

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    FDBFuture *fileMetaFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength, 0 /** not set snapshot */);
    exitOnError(fdb_future_block_until_ready(fileMetaFut));
    fdb_bool_t fileMetaExist;
    const uint8_t *fileMetaRaw = NULL;
    int fileMetaRawLength;
    exitOnError(fdb_future_get_value(fileMetaFut, &fileMetaExist, &fileMetaRaw, &fileMetaRawLength));
    fdb_future_destroy(fileMetaFut);
    fileMetaFut = nullptr;

    // current metadata format: key: filename; value: {"verList": [v0, v1, v2,
    // ...]}; for non-versioned system, verList only stores v0
    nlohmann::json *fmjptr = new nlohmann::json();
    auto &fmj = *fmjptr;

    std::String verFileKey;

    if (fileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateTimestamps() failed to get metadata for file " << f.name;

        // commit transaction and return
        delete fmjptr;
        FDBFuture *fcmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(fcmt));
        fdb_future_destroy(fcmt);

        return false;
    }
    else
    {
        // parse fileMeta as JSON object
        try
        {
            std::string filMetaRawStr(reinterpret_cast<const char *>(fileMetaRaw));
            fmj = nlohmann::json::parse(filMetaRawStr);
        }
        catch (std::exception e)
        {
            LOG(ERROR) << "FDBMetaStore::updateTimestamps() Error parsing JSON string: " << e.what();
            exit(1);
        }

        delete fmjptr;

        verFileKey = fmj["verList"].back().get<std::string>();
    }

    // find metadata for current file version
    FDBFuture *verFileMetaFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, 0 /** not set snapshot */);
    exitOnError(fdb_future_block_until_ready(verFileMetaFut));
    fdb_bool_t verFileMetaExist;
    const uint8_t *verFileMetaRaw = NULL;
    int verFileMetaRawLength;
    exitOnError(fdb_future_get_value(verFileMetaFut, &verFileMetaExist, &verFileMetaRaw, &verFileMetaRawLength));
    fdb_future_destroy(verFileMetaFut);
    verFileMetaFut = nullptr;

    nlohmann::json *vfmjptr = new nlohmann::json();
    auto &vfmj = *vfmjptr;

    if (verFileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateTimestamps() failed to get versioned metadata for file " << f.name;

        // commit transaction and return
        delete vfmjptr;
        FDBFuture *fcmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(fcmt));
        fdb_future_destroy(fcmt);

        return false;
    }
    else
    {
        // parse fileMeta as JSON object
        try
        {
            std::string verFileMetaRawStr(reinterpret_cast<const char *>(verFileMetaRaw));
            vfmj = nlohmann::json::parse(verFileMetaRawStr);
        }
        catch (std::exception e)
        {
            LOG(ERROR) << "FDBMetaStore::updateTimestamps() Error parsing JSON string: " << e.what();
            exit(1);
        }

        // update Chunk information
        char chunkName[MAX_KEY_SIZE];
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
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, reinterpret_cast<const uint8_t *>(vfmjStr.c_str()), vfmjStr.size());
        delete vfmjptr;
    }

    LOG(INFO) << "FDBMetaStore:: updateChunks() finished";

    return true;
}

bool FDBMetaStore::getFileName(boost::uuids::uuid fuuid, File &f)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileUuidKey[FDB_MAX_KEY_SIZE + 64];
    if (!genFileUuidKey(f.namespaceId, fuuid, fileUuidKey))
        return false;
    return getFileName(fileUuidKey, f);

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
    // max = (1 << 32) - 1 - FDB_NUM_RESERVED_SYSTEM_KEYS, but we store also uuid for each file
    return (unsigned long int)(1 << 31) - FDB_NUM_RESERVED_SYSTEM_KEYS / 2 - (FDB_NUM_RESERVED_SYSTEM_KEYS % 2);
}

unsigned long int FDBMetaStore::getNumFiles()
{
    // TODO: decide how to count the number of files

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
    return markFileRepairStatus(file, false);
}

bool FDBMetaStore::markFileAsRepaired(const File &file)
{
    return markFileRepairStatus(file, true);
}

bool RedisMetaStore::markFileRepairStatus(const File &file, bool needsRepair)
{
    return markFileStatus(file, FDB_FILE_REPAIR_KEY, needsRepair, "repair");
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

bool FDBMetaStore::getValueInTX(const FDBTransaction *tx, const std::string &key, std::string &value)
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
        LOG(INFO) << "FDBMetaStore::getValueInTX() key " << key << " found: value " << value;
        return true;
    }
    else
    {
        LOG(INFO) << "FDBMetaStore:: getValueInTX() key " << key << "not found";
        return false;
    }
}

bool FDBMetaStore::parseStrToJSONObj(const std::string &str, nlohmann::json &j)
{
    try
    {
        j = nlohmann::json::parse(str);
    }
    catch (std::exception e)
    {
        LOG(ERROR) << "FDBMetaStore::parseStrToJSONObj() Error parsing JSON string: " << e.what();
        return false;
    }
    return true;
}

bool FDBMetaStore::getFileName(char fileUuidKey[], File &f)
{
    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    FDBFuture *fileKeyFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(fileUuidKey), FDB_MAX_KEY_SIZE + 64, 0 /** not set snapshot */);
    exitOnError(fdb_future_block_until_ready(fileKeyFut));
    fdb_bool_t fileKeyExist;
    const uint8_t *fileKeyRaw = NULL;
    int fileKeyRawLength;
    exitOnError(fdb_future_get_value(fileKeyFut, &fileKeyExist, &fileKeyRaw, &fileKeyRawLength));
    fdb_future_destroy(fileKeyFut);
    fileKeyFut = nullptr;

    if (fileKeyFut == false)
    {
        LOG(ERROR) << "FDBMetaStore::getFileName() failed to get filename from fileUuidKey " << fileUuidKey;
        return false;
    }
    else
    {
        // copy to file name
        std::string fileKeyRawStr(reinterpret_cast<const char *>(fileKeyRaw));
        f.nameLength = fileKeyRawStr.size();
        f.name = (char *)malloc(f.nameLength + 1);
        strncpy(f.name, fileKeyRawStr.c_str(), f.nameLength);
        f.name[f.nameLength] = 0;

        // commit transaction and return
        FDBFuture *fcmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(fcmt));
        fdb_future_destroy(fcmt);

        return true;
    }
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

std::string FDBMetaStore::getFilePrefix(const char name[], bool noEndingSlash)
{
    // the location of sub-dirs
    const char *slash = strrchr(name, '/');

    // the location of first '_' (to locate namespace)
    const char *us = strchr(name, '_');

    std::string filePrefix(FDB_FILE_PREFIX_KEY);
    // file on root directory (namespace_filename), or root directory (ends
    // with one '/' (namespace_/))
    if (slash == NULL || us + 1 == slash)
    {
        // set filePrefix to "FDB_FILE_PREFIX_KEY<namesapce>_"
        filePrefix.append(name, us - name + 1);
        // append a slash at the end
        return noEndingSlash ? filePrefix : filePrefix.append("/");
    }
    // set filePrefix to //pf_dirname
    return filePrefix.append(name, slash - name);
}

int FDBMetaStore::genChunkKeyPrefix(int chunkId, char prefix[])
{
    return snprintf(prefix, FDB_MAX_KEY_SIZE, "c%d", chunkId);
}

int FDBMetaStore::genBlockKey(int blockId, char prefix[], bool unique)
{
    return snprintf(prefix, FDB_MAX_KEY_SIZE, "%s%d", getBlockKeyPrefix(unique), blockId);
}

const char *FDBMetaStore::getBlockKeyPrefix(bool unique)
{
    return unique ? "ub" : "db";
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