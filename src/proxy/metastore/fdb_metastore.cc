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

#include <boost/timer/timer.hpp>

// change defs's prefix to FDB_
#define FDB_FILE_PREFIX "//pf_"
#define FDB_FILE_LOCK_KEY_PREFIX "//snccFLock"
#define FDB_FILE_PIN_STAGED_KEY_PREFIX "//snccFPinStaged"
#define FDB_FILE_REPAIR_KEY_PREFIX "//snccFRepair"
#define FDB_FILE_PENDING_WRITE_KEY_PREFIX "//snccFPendingWrite"
#define FDB_FILE_PENDING_WRITE_COMP_KEY_PREFIX "//snccFPendingWriteComp"
#define FDB_BG_TASK_PENDING_KEY_PREFIX "//snccFBgTask"
#define FDB_DIR_LIST_KEY_PREFIX "//snccDirList"
#define FDB_JL_LIST_KEY_PREFIX "//snccJournalFSet"

// FDB system keys
#define FDB_NUM_RESERVED_SYSTEM_KEYS (1)
#define FDB_NUM_FILES_KEY "//snccFileNum"

#define FDB_MAX_KEY_SIZE (64)
#define FDB_NUM_REQ_FIELDS (10)

static std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength);

FDBMetaStore::FDBMetaStore()
{
    // Config &config = Config::getInstance();

    // select API version
    fdb_select_api_version(FDB_API_VERSION);

    // init network
    exitOnError(fdb_setup_network());
    if (pthread_create(&_fdb_network_thread, NULL, FDBMetaStore::runNetworkThread, NULL) != 0)
    {
        LOG(ERROR) << "FDBMetaStore::FDBMetaStore() failed to create network thread";
        exit(1);
    }

    // init database
    _db = getDatabase(_FDBClusterFile);

    _taskScanIt = "0";
    _endOfPendingWriteSet = true;

    LOG(INFO) << "FDBMetaStore::FDBMetaStore() initialized, API version: " << FDB_API_VERSION << ", clusterFile: " << _FDBClusterFile;
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
    // benchmark time (init)
    double overallTimeSec = 0, parsingTimeSec = 0;
    boost::timer::cpu_timer overallTimer, parsingTimer;
    boost::timer::nanosecond_type duration;

    // benchmark time (start)
    overallTimer.start();

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
    std::string fileMd5 = ChecksumCalculator::toHex(f.md5, MD5_DIGEST_LENGTH);
    fVerSummary.append(fileMd5 + std::string(" "));
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
    { // no file meta: init the new version
        fmj["verId"] = nlohmann::json::array();
        fmj["verId"].push_back(f.version);
        fmj["verName"] = nlohmann::json::array();
        fmj["verName"].push_back(std::string(verFileKey, verFileKeyLength));
        fmj["verSummary"] = nlohmann::json::array();
        fmj["verSummary"].push_back(fVerSummary);
    }
    else
    { // file meta exists: update
        // benchmark time (start)
        parsingTimer.start();

        if (parseStrToJSONObj(fileMetaStr, fmj) == false)
        {
            exit(1);
        }

        // benchmark time (end)
        duration = parsingTimer.elapsed().wall;
        parsingTimeSec += duration / 1e9;

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
            // check whether the version exists
            if (std::find(fmj["verId"].begin(), fmj["verId"].end(), f.version) == fmj["verId"].end())
            {
                size_t pos;
                for (pos = 0; pos < fmj["verId"].size(); pos++)
                {
                    if (f.version < fmj["verId"][pos].get<int>())
                    {
                        break;
                    }
                }
                // insert the version in ascending order
                fmj["verId"].insert(fmj["verId"].begin() + pos, f.version);
                fmj["verName"].insert(fmj["verName"].begin() + pos, std::string(verFileKey, verFileKeyLength));
                fmj["verSummary"].insert(fmj["verSummary"].begin() + pos, fVerSummary);
            }
        }
    }

    // benchmark time (start)
    parsingTimer.start();

    // serialize file metadata to JSON string
    std::string fmjStr = fmj.dump();

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());

    delete fmjPtr;

    // create metadata for current file version
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

    // benchmark time (start)
    parsingTimer.start();

    // serialize file metadata to JSON string
    std::string verFmjStr = verFmj.dump();

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());

    delete verFmjPtr;

    // add uuid-to-file-name maping
    char fUUIDKey[FDB_MAX_KEY_SIZE + 64];
    memset(fUUIDKey, 0, FDB_MAX_KEY_SIZE + 64);
    if (genFileUuidKey(f.namespaceId, f.uuid, fUUIDKey) == false)
    {
        LOG(WARNING) << "FDBMetaStore::putMeta() File uuid " << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    }
    else
    {
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fUUIDKey), FDB_MAX_KEY_SIZE + 64, reinterpret_cast<const uint8_t *>(f.name), f.nameLength);
    }

    // file prefix set: add filePrefixKey (filePrefix_filename)
    std::string filePrefix = getFilePrefix(fileKey);
    std::string filePrefixKey = filePrefix + std::string("_") + std::string(f.name, f.nameLength);
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePrefixKey.c_str()), filePrefixKey.size(), reinterpret_cast<const uint8_t *>(f.name), f.nameLength);

    // directory set: add dirKey (FDB_DIR_LIST_KEY_PREFIX_filePrefix)
    std::string dirKey = std::string(FDB_DIR_LIST_KEY_PREFIX) + std::string("_") + filePrefix;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dirKey.c_str()), dirKey.size(), reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size());

    // update file count
    if (fileMetaExist == false)
    { // creating a new file
        std::string numFilesStr;
        bool numFilesExist = getValueInTX(tx, std::string(FDB_NUM_FILES_KEY), numFilesStr);
        if (numFilesExist == false)
        {
            fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_NUM_FILES_KEY), std::string(FDB_NUM_FILES_KEY).size(), reinterpret_cast<const uint8_t *>("1"), 1);
        }
        else
        {
            int numFiles = std::stoi(numFilesStr);
            fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_NUM_FILES_KEY), std::string(FDB_NUM_FILES_KEY).size(), reinterpret_cast<const uint8_t *>(std::to_string(numFiles + 1).c_str()), std::to_string(numFiles + 1).size());
        }
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: putMeta() finished";

    // benchmark time (end)
    duration = overallTimer.elapsed().wall;
    overallTimeSec += duration / 1e9;

    DLOG(INFO) << "FDBMetaStore::putMeta() finished, overall time(s): " << overallTimeSec << ", parsing time(s): " << parsingTimeSec << ", percentage: " << (parsingTimeSec / overallTimeSec) * 100 << "%";

    return true;
}

bool FDBMetaStore::getMeta(File &f, int getBlocks)
{
    // benchmark time (init)
    double overallTimeSec = 0, parsingTimeSec = 0;
    boost::timer::cpu_timer overallTimer, parsingTimer;
    boost::timer::nanosecond_type duration;

    // benchmark time (start)
    overallTimer.start();

    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX], verFileKey[PATH_MAX];

    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);
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

    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;

    // benchmark time (start)
    parsingTimer.start();

    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    // check file version
    if (f.version == -1)
    { // version not specified: retrieve the latest version (version at the back of the list)
        std::string lastVerFileKey = fmj["verName"].back().get<std::string>();
        memcpy(verFileKey, lastVerFileKey.c_str(), lastVerFileKey.size());
        verFileKeyLength = lastVerFileKey.size();
    }
    else
    { // retrieve a specific version
        // check whether the specified version exists
        if (std::find(fmj["verId"].begin(), fmj["verId"].end(), f.version) == fmj["verId"].end())
        {
            LOG(WARNING) << "FDBMetaStore::getMeta() file version not exists, file: " << f.name << ", version: " << f.version;

            delete fmjPtr;

            // commit transaction
            FDBFuture *cmt = fdb_transaction_commit(tx);
            exitOnError(fdb_future_block_until_ready(cmt));
            fdb_future_destroy(cmt);

            return false;
        }
    }

    delete fmjPtr;

    size_t numUniqueBlocks = 0, numDuplicateBlocks = 0;

    // find metadata for the specified file version
    std::string verFileMetaStr;
    bool verFileMetaExist = getValueInTX(tx, std::string(verFileKey, verFileKeyLength), verFileMetaStr);
    if (verFileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::getMeta() file metadata for specified version not exists, file: " << f.name << ", version: " << f.version;
        exit(1);
    }

    nlohmann::json *verFmjPtr = new nlohmann::json();
    auto &verFmj = *verFmjPtr;

    // benchmark time (start)
    parsingTimer.start();

    if (parseStrToJSONObj(verFileMetaStr, verFmj) == false)
    {
        exit(1);
    }

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    // parse fields
    f.size = verFmj["size"].get<unsigned long>();
    f.numChunks = verFmj["numC"].get<int>();
    f.numStripes = verFmj["numS"].get<int>();
    std::string retrievedUUID = verFmj["uuid"].get<std::string>();
    if (f.setUUID(retrievedUUID) == false)
    {
        LOG(WARNING) << "FDBMetaStore::getMeta() invalid UUID: " << retrievedUUID;

        delete verFmjPtr;

        // commit transaction
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

        delete verFmjPtr;

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

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
        std::string chunkMd5 = verFmj[cmd5Key.c_str()].get<std::string>();
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

    delete verFmjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore::getMeta() finished";

    // benchmark time (end)
    duration = overallTimer.elapsed().wall;
    overallTimeSec += duration / 1e9;

    DLOG(INFO) << "FDBMetaStore::getMeta() finished, overall time(s): " << overallTimeSec << ", parsing time(s): " << parsingTimeSec << ", percentage: " << (parsingTimeSec / overallTimeSec) * 100 << "%";

    return true;
}

bool FDBMetaStore::deleteMeta(File &f)
{
    // benchmark time (init)
    double overallTimeSec = 0, parsingTimeSec = 0;
    boost::timer::cpu_timer overallTimer, parsingTimer;
    boost::timer::nanosecond_type duration;

    // benchmark time (start)
    overallTimer.start();

    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX], verFileKey[PATH_MAX];

    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    Config &config = Config::getInstance();

    bool isVersioned = !config.overwriteFiles();
    int versionToDelete = f.version;
    bool lazyDeletion = false;

    bool ret = true;

    // DLOG(INFO) << "FDBMetaStore::deleteMeta() start to delete file " << f.name << ", version: " << f.version << ", fileKey: " << std::string(fileKey, fileKeyLength);

    if ((isVersioned || lazyDeletion) && versionToDelete == -1)
    { // versioning enabled but without a specified version: mark as deleted
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

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, std::string(fileKey, fileKeyLength), fileMetaStr);

    if (fileMetaExist == false)
    { // file metadata not exist: report and return
        LOG(WARNING) << "FDBMetaStore::deleteMeta() failed to get file metadata, file: " << f.name << ", version: " << f.version;

        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    // parse fileMeta as JSON object
    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;

    // benchmark time (start)
    parsingTimer.start();

    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    if (isVersioned == false)
    { // non-versioned: remove the only versioned file metadata
        std::string verFileKeyStr = fmj["verName"].back().get<std::string>();
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(verFileKeyStr.c_str()), verFileKeyStr.size());
        fmj["verId"].clear();
        fmj["verName"].clear();
        fmj["verSummary"].clear();
    }

    if (isVersioned == true && versionToDelete != -1)
    { // versioned: delete the specified versioned file metadata
        int numVersions = fmj["verId"].size();

        if (numVersions == 0)
        {
            LOG(WARNING) << "FDBMetaStore::deleteMeta() No version found, file: " << f.name;

            delete fmjPtr;

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

            delete fmjPtr;

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

    // remove file metadata
    bool removeFileMeta = (isVersioned && fmj["verId"].size() == 0) || !isVersioned;
    if (!removeFileMeta)
    { // update file meta (only remove a specified version)
        // benchmark time (start)
        parsingTimer.start();

        std::string fmjStr = fmj.dump();

        // benchmark time (end)
        duration = parsingTimer.elapsed().wall;
        parsingTimeSec += duration / 1e9;

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());

        delete fmjPtr;

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return true;
    }

    delete fmjPtr;

    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength);

    char fUuidKey[FDB_MAX_KEY_SIZE + 64];
    memset(fUuidKey, 0, FDB_MAX_KEY_SIZE + 64);
    f.genUUID();

    // remove file uuid key
    if (!genFileUuidKey(f.namespaceId, f.uuid, fUuidKey))
    {
        LOG(WARNING) << "FDBMetaStore::deleteMeta() File uuid" << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    }
    else
    {
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(fUuidKey), FDB_MAX_KEY_SIZE + 64);
    }

    // file prefix set: remove filePrefixKey (filePrefix_filename)
    std::string filePrefix = getFilePrefix(fileKey);
    std::string filePrefixKey = filePrefix + std::string("_") + std::string(f.name, f.nameLength);
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(filePrefixKey.c_str()), filePrefixKey.size());

    // directory set: check whether file prefix set has any files
    // if no files: remove dirKey (FDB_DIR_LIST_KEY_PREFIX_filePrefix)
    std::vector<std::pair<std::string, std::string>> fileNamesWithPrefix;
    if (getKVPairsWithKeyPrefixInTX(tx, std::string(filePrefix + "_"), fileNamesWithPrefix) == 0)
    {
        exit(1);
    }

    if (fileNamesWithPrefix.size() == 0)
    {
        std::string dirKey = std::string(FDB_DIR_LIST_KEY_PREFIX) + std::string("_") + filePrefix;
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(dirKey.c_str()), dirKey.size());
    }

    // update file count
    std::string numFilesStr;
    bool numFilesExist = getValueInTX(tx, std::string(FDB_NUM_FILES_KEY), numFilesStr);
    if (numFilesExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::deleteMeta() Error finding file count";
        exit(1);
    }
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_NUM_FILES_KEY), std::string(FDB_NUM_FILES_KEY).size(), reinterpret_cast<const uint8_t *>(std::to_string(std::stoi(numFilesStr) - 1).c_str()), std::to_string(std::stoi(numFilesStr) - 1).size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: deleteMeta() finished";

    // benchmark time (end)
    duration = overallTimer.elapsed().wall;
    overallTimeSec += duration / 1e9;

    DLOG(INFO) << "FDBMetaStore::deleteMeta() finished, overall time(s): " << overallTimeSec << ", parsing time(s): " << parsingTimeSec << ", percentage: " << (parsingTimeSec / overallTimeSec) * 100 << "%";

    return true;
}

bool FDBMetaStore::renameMeta(File &sf, File &df)
{
    // benchmark time (init)
    double overallTimeSec = 0, parsingTimeSec = 0;
    boost::timer::cpu_timer overallTimer, parsingTimer;
    boost::timer::nanosecond_type duration;

    // benchmark time (start)
    overallTimer.start();

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
        LOG(ERROR) << "FDBMetaStore::renameMeta() failed to get metadata, file: " << sf.name;

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    // parse fileMeta as JSON object
    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;

    // benchmark time (start)
    parsingTimer.start();

    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

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

    // benchmark time (start)
    parsingTimer.start();

    if (parseStrToJSONObj(verFileMetaStr, verFmj) == false)
    {
        exit(1);
    }

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    verFmj["uuid"] = boost::uuids::to_string(df.uuid);

    // benchmark time (start)
    parsingTimer.start();

    std::string verFmjStr = verFmj.dump();

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(lastVerFileKey.c_str()), lastVerFileKey.size(), reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());

    delete verFmjPtr;

    // benchmark time (start)
    parsingTimer.start();
    std::string fmjStr = fmj.dump();

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    // insert dstFileKey, remove srcFileKey
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileKey), dstFileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFileKey), srcFileKeyLength);

    delete fmjPtr;

    // set dstFileUuidKey; remove the original sfidKey
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileUuidKey), FDB_MAX_KEY_SIZE + 64, reinterpret_cast<const uint8_t *>(df.name), df.nameLength);
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFileUuidKey), FDB_MAX_KEY_SIZE + 64);

    // DLOG(INFO) << "FDBMetaStore::renameMeta() Add reverse mapping (" << std::string(dstFileUuidKey, FDB_MAX_KEY_SIZE) << ") for file " << std::string(dstFileKey, dstFileKeyLength);
    // DLOG(INFO) << "FDBMetaStore::renameMeta() remove reverse mapping (" << std::string(srcFileUuidKey, FDB_MAX_KEY_SIZE) << ") for file " << std::string(srcFileKey, srcFileKeyLength);

    // file prefix set: remove srcFilePrefixKey, add dstFilePrefixKey
    std::string srcFilePrefix = getFilePrefix(srcFileKey);
    std::string srcFilePrefixKey = srcFilePrefix + std::string("_") + std::string(sf.name, sf.nameLength);
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFilePrefixKey.c_str()), srcFilePrefixKey.size());

    std::string dstFilePrefix = getFilePrefix(dstFileKey);
    std::string dstFilePrefixKey = dstFilePrefix + std::string("_") + std::string(df.name, df.nameLength);
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFilePrefixKey.c_str()), dstFilePrefixKey.size(), reinterpret_cast<const uint8_t *>(std::string(df.name, df.nameLength).c_str()), df.nameLength);

    // directory set: check whether src file prefix set has any files
    // if no files: remove srcDirKey (FDB_DIR_LIST_KEY_PREFIX_filePrefix)
    // add dstDirKey
    std::vector<std::pair<std::string, std::string>> srcFileNamesWithPrefix;
    if (getKVPairsWithKeyPrefixInTX(tx, std::string(srcFilePrefix + "_"), srcFileNamesWithPrefix) == 0)
    {
        exit(1);
    }

    if (srcFileNamesWithPrefix.size() == 0)
    {
        std::string srcDirKey = std::string(FDB_DIR_LIST_KEY_PREFIX) + std::string("_") + srcFilePrefix;
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcDirKey.c_str()), srcDirKey.size());
    }

    std::string dstDirKey = std::string(FDB_DIR_LIST_KEY_PREFIX) + std::string("_") + dstFilePrefix;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstDirKey.c_str()), dstDirKey.size(), reinterpret_cast<const uint8_t *>(dstFilePrefix.c_str()), dstFilePrefix.size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: renameMeta() finished";

    // benchmark time (end)
    duration = overallTimer.elapsed().wall;
    overallTimeSec += duration / 1e9;

    DLOG(INFO) << "FDBMetaStore::renameMeta() finished, overall time(s): " << overallTimeSec << ", parsing time(s): " << parsingTimeSec << ", percentage: " << (parsingTimeSec / overallTimeSec) * 100 << "%";

    return true;
}

bool FDBMetaStore::updateTimestamps(const File &f)
{
    // benchmark time (init)
    double overallTimeSec = 0, parsingTimeSec = 0;
    boost::timer::cpu_timer overallTimer, parsingTimer;
    boost::timer::nanosecond_type duration;

    // benchmark time (start)
    overallTimer.start();

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
        LOG(ERROR) << "FDBMetaStore::updateTimestamps() failed to get metadata, file: " << f.name;

        // commit transaction and return
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return false;
    }

    nlohmann::json *fmjPtr = new nlohmann::json();
    auto &fmj = *fmjPtr;

    // benchmark time (start)
    parsingTimer.start();

    if (parseStrToJSONObj(fileMetaStr, fmj) == false)
    {
        exit(1);
    }

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    std::string verFileKey = fmj["verName"].back().get<std::string>();
    delete fmjPtr;

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

    // benchmark time (start)
    parsingTimer.start();

    if (parseStrToJSONObj(verFileMetaStr, verFmj) == false)
    {
        exit(1);
    }

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    verFmj["atime"] = f.atime;
    verFmj["mtime"] = f.mtime;
    verFmj["ctime"] = f.ctime;
    verFmj["tctime"] = f.tctime;

    // benchmark time (start)
    parsingTimer.start();

    std::string verFmjStr = verFmj.dump();

    // benchmark time (end)
    duration = parsingTimer.elapsed().wall;
    parsingTimeSec += duration / 1e9;

    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey.c_str()), verFileKey.size(), reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());

    delete verFmjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: updateTimestamps() finished";

    // benchmark time (end)
    duration = overallTimer.elapsed().wall;
    overallTimeSec += duration / 1e9;

    DLOG(INFO) << "FDBMetaStore::updateTimestamps() finished, overall time(s): " << overallTimeSec << ", parsing time(s): " << parsingTimeSec << ", percentage: " << (parsingTimeSec / overallTimeSec) * 100 << "%";

    return true;
}

int FDBMetaStore::updateChunks(const File &f, int version)
{
    std::lock_guard<std::mutex> lk(_lock);

    char fileKey[PATH_MAX];
    genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check whether the file metadata exists
    std::string fileMetaStr;
    bool fileMetaExist = getValueInTX(tx, fileKey, fileMetaStr);
    if (fileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateChunks() failed to get metadata, file: " << f.name;

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
    delete fmjPtr;

    // find metadata for current file version
    std::string verFileMetaStr;
    bool verFileMetaExist = getValueInTX(tx, verFileKey, verFileMetaStr);
    if (verFileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::updateChunks() failed to get latest versioned metadata, file: " << f.name;

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

    // update Chunk information
    char chunkName[FDB_MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++)
    {
        genChunkKeyPrefix(f.chunks[i].getChunkId(), chunkName);

        std::string cidKey = std::string(chunkName) + std::string("-cid");
        verFmj[cidKey.c_str()] = std::to_string(f.containerIds[i]);
        std::string csizeKey = std::string(chunkName) + std::string("-size");
        verFmj[csizeKey.c_str()] = std::to_string(f.chunks[i].size);
    }

    std::string verFmjStr = verFmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey.size()), verFileKey.size(), reinterpret_cast<const uint8_t *>(verFmjStr.c_str()), verFmjStr.size());

    delete verFmjPtr;

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    // DLOG(INFO) << "FDBMetaStore:: updateChunks() finished";

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
    // benchmark time (init)
    double overallTimeSec = 0, parsingTimeSec = 0;
    boost::timer::cpu_timer overallTimer, parsingTimer;
    boost::timer::nanosecond_type duration;

    // benchmark time (start)
    overallTimer.start();

    std::lock_guard<std::mutex> lk(_lock);

    if (namespaceId == INVALID_NAMESPACE_ID)
        namespaceId = Config::getInstance().getProxyNamespaceId();

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    DLOG(INFO) << "FDBMetaStore::getFileList() " << "prefix = " << prefix;

    // candidate file key and metadata for listing
    std::vector<std::pair<std::string, std::string>> candidateFileMetas;
    std::string prefixWithNS;
    if (prefix == "" || prefix.back() != '/')
    {
        prefixWithNS = std::to_string(namespaceId) + "_" + prefix;
    }
    else
    {
        std::string sprefix = std::to_string(namespaceId) + std::string("_") + prefix;
        prefixWithNS = getFilePrefix(sprefix.c_str());
    }

    // get file keys with prefix
    if (getKVPairsWithKeyPrefixInTX(tx, prefixWithNS, candidateFileMetas) == false)
    {
        exit(1);
    }

    // init file metadata list
    if (candidateFileMetas.size() > 0)
    {
        *list = new FileInfo[candidateFileMetas.size()];
    }

    // count number of files
    int numFiles = 0;

    for (size_t i = 0; i < candidateFileMetas.size(); i++)
    {
        std::string &fileKey = candidateFileMetas[i].first;
        std::string &fileMetaStr = candidateFileMetas[i].second;

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

        nlohmann::json *fmjPtr = new nlohmann::json();
        auto &fmj = *fmjPtr;

        // benchmark time (start)
        parsingTimer.start();

        if (parseStrToJSONObj(fileMetaStr, fmj) == false)
        {
            exit(1);
        }

        // benchmark time (end)
        duration = parsingTimer.elapsed().wall;
        parsingTimeSec += duration / 1e9;

        if (withSize || withTime || withVersions)
        { // get file size and time if requested
            // get latest version
            std::string verFileKey = fmj["verName"].back().get<std::string>();
            std::string verFileMetaStr;
            bool verFileMetaExist = getValueInTX(tx, verFileKey, verFileMetaStr);
            if (verFileMetaExist == false)
            {
                LOG(ERROR) << "FDBMetaStore::getFileList() failed to get metadata for file " << cur.name;
                continue;
            }
            nlohmann::json *verFmjPtr = new nlohmann::json();
            auto &verFmj = *verFmjPtr;

            // benchmark time (start)
            parsingTimer.start();

            if (parseStrToJSONObj(verFileMetaStr, verFmj) == false)
            {
                exit(1);
            }

            // benchmark time (end)
            duration = parsingTimer.elapsed().wall;
            parsingTimeSec += duration / 1e9;

            cur.size = verFmj["size"].get<unsigned long int>();
            cur.ctime = verFmj["ctime"].get<time_t>();
            cur.atime = verFmj["atime"].get<time_t>();
            cur.mtime = verFmj["mtime"].get<time_t>();
            cur.version = verFmj["ver"].get<int>();
            cur.isDeleted = verFmj["dm"].get<int>();
            ChecksumCalculator::unHex(verFmj["md5"].get<std::string>(), cur.md5, MD5_DIGEST_LENGTH);
            cur.numChunks = verFmj["numC"].get<int>();
            // staged last modified time
            time_t staged_mtime = verFmj["sg_mtime"].get<time_t>();
            if (staged_mtime > cur.mtime)
            {
                cur.mtime = staged_mtime;
                cur.atime = staged_mtime;
                cur.size = verFmj["sg_size"].get<unsigned long int>();
            }
            cur.storageClass = verFmj["sc"].get<std::string>();

            delete verFmjPtr;
        }
        // do not add delete marker to the list unless for queries on versions
        if (!withVersions && cur.isDeleted)
        {
            delete fmjPtr;

            continue;
        }
        if (withVersions && cur.version > 0)
        { // get all versions information
            cur.numVersions = fmj["verId"].size();
            try
            {
                cur.versions = new VersionInfo[cur.numVersions];
                for (int vi = 0; vi < cur.numVersions; vi++)
                {
                    cur.versions[vi].version = fmj["verId"][vi].get<int>();
                    std::string verSummary = fmj["verSummary"][vi].get<std::string>();
                    std::stringstream ss(verSummary);
                    std::vector<std::string> items;
                    std::string item;
                    while (ss >> item)
                    {
                        items.push_back(item);
                    }
                    cur.versions[vi].size = strtoul(items[0].c_str(), nullptr, 0);
                    cur.versions[vi].mtime = strtol(items[1].c_str(), nullptr, 0);
                    ChecksumCalculator::unHex(items[2].c_str(), cur.versions[vi].md5, MD5_DIGEST_LENGTH);
                    cur.versions[vi].isDeleted = atoi(items[3].c_str());
                    cur.versions[vi].numChunks = atoi(items[4].c_str());

                    DLOG(INFO) << "FDBMetaStore::getFileList() Add version " << cur.versions[vi].version << " size " << cur.versions[vi].size << " mtime " << cur.versions[vi].mtime << " deleted " << cur.versions[vi].isDeleted << " to version list of file " << cur.name;
                }
            }
            catch (std::exception &e)
            {
                LOG(ERROR) << "FDBMetaStore::getFileList() Cannot allocate memory for " << cur.numVersions << " version records";
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

    // DLOG(INFO) << "FDBMetaStore::getFileList() finished";

    // benchmark time (end)
    duration = overallTimer.elapsed().wall;
    overallTimeSec += duration / 1e9;

    DLOG(INFO) << "FDBMetaStore::getFileList() finished, overall time(s): " << overallTimeSec << ", parsing time(s): " << parsingTimeSec << ", percentage: " << (parsingTimeSec / overallTimeSec) * 100 << "%";

    return numFiles;
}

unsigned int FDBMetaStore::getFolderList(std::vector<std::string> &list, unsigned char namespaceId, std::string prefix, bool skipSubfolders)
{
    std::lock_guard<std::mutex> lk(_lock);

    // get all directories started with prefix
    char fileKey[PATH_MAX];
    genFileKey(namespaceId, prefix.c_str(), prefix.size(), fileKey);
    std::string filePrefix = getFilePrefix(fileKey, /* no ending slash */ true);
    size_t pfSize = filePrefix.size();

    std::string dirKey = std::string(FDB_DIR_LIST_KEY_PREFIX) + std::string("_") + filePrefix;
    unsigned long int count = 0;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::vector<std::pair<std::string, std::string>> filePrefixes;
    if (getKVPairsWithKeyPrefixInTX(tx, dirKey, filePrefixes) == 0)
    {
        exit(1);
    }

    for (size_t i = 0; i < filePrefixes.size(); i++)
    {
        std::string &dirStr = filePrefixes[i].second;

        // skip sub-folders
        if (skipSubfolders && strchr(dirStr.c_str() + pfSize, '/') != 0)
        {
            continue;
        }
        list.push_back(dirStr.substr(pfSize));
        count++;

        DLOG(INFO) << "FDBMetaStore::getFolderList() Add " << dirStr << " to the result of " << filePrefix;
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return count;
}

unsigned long int FDBMetaStore::getMaxNumKeysSupported()
{
    // max = (1 << 32) - 1 - FDB_NUM_RESERVED_SYSTEM_KEYS, but we store also uuid for each file
    return (unsigned long int)(1 << 31) - FDB_NUM_RESERVED_SYSTEM_KEYS / 2 - (FDB_NUM_RESERVED_SYSTEM_KEYS % 2);
}

unsigned long int FDBMetaStore::getNumFiles()
{
    std::lock_guard<std::mutex> lk(_lock);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    int numFiles = 0;

    std::string numFilesStr;
    bool numFilesExist = getValueInTX(tx, std::string(FDB_NUM_FILES_KEY), numFilesStr);
    if (numFilesExist == true)
    {
        numFiles = std::stoi(numFilesStr);
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return numFiles;
}

unsigned long int FDBMetaStore::getNumFilesToRepair()
{
    std::lock_guard<std::mutex> lk(_lock);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::vector<std::pair<std::string, std::string>> fileKeys;
    std::string fileRepairKey = std::string(FDB_FILE_REPAIR_KEY_PREFIX) + "_";
    if (getKVPairsWithKeyPrefixInTX(tx, fileRepairKey, fileKeys) == 0)
    {
        exit(1);
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return fileKeys.size();
}

int FDBMetaStore::getFilesToRepair(int numFiles, File files[])
{
    std::lock_guard<std::mutex> lk(_lock);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::vector<std::pair<std::string, std::string>> fileKeys;
    std::string fileRepairKey = std::string(FDB_FILE_REPAIR_KEY_PREFIX) + "_";
    if (getKVPairsWithKeyPrefixInTX(tx, fileRepairKey, fileKeys) == 0)
    {
        exit(1);
    }

    int count = fileKeys.size();
    if (count < numFiles)
    {
        LOG(ERROR) << "FDBMetaStore::getFilesToRepair() insufficient number of files " << count << " to repair, requested " << numFiles;

        // commit transaction
        FDBFuture *cmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(cmt));
        fdb_future_destroy(cmt);

        return -1;
    }

    int numFilesToRepair = 0;
    for (int i = 0; i < numFiles; i++)
    { // remove the elements in the list (in lexigraphically sorted order)
        std::string fileToRepairKey = fileKeys[i].first;
        std::string fileName = fileKeys[i].second;

        free(files[numFilesToRepair].name);
        if (!getNameFromFileKey(fileName.c_str(), fileName.size(), &files[numFilesToRepair].name, files[numFilesToRepair].nameLength, files[numFilesToRepair].namespaceId, &files[numFilesToRepair].version))
        {
            continue;
        }

        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(fileToRepairKey.c_str()), fileToRepairKey.size());

        numFilesToRepair++;
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return numFilesToRepair;
}

bool FDBMetaStore::markFileAsRepaired(const File &file)
{
    return markFileRepairStatus(file, false);
}

bool FDBMetaStore::markFileAsNeedsRepair(const File &file)
{
    return markFileRepairStatus(file, true);
}

bool FDBMetaStore::markFileRepairStatus(const File &file, bool needsRepair)
{
    return markFileStatus(file, FDB_FILE_REPAIR_KEY_PREFIX, needsRepair, "repair");
}

bool FDBMetaStore::markFileAsPendingWriteToCloud(const File &file)
{
    return markFileStatus(file, FDB_FILE_PENDING_WRITE_KEY_PREFIX, true, "pending write to cloud");
}

bool FDBMetaStore::markFileAsWrittenToCloud(const File &file, bool removePending)
{
    return markFileStatus(file, FDB_FILE_PENDING_WRITE_COMP_KEY_PREFIX, false, "pending completing write to cloud") &&
           (!removePending || markFileStatus(file, FDB_FILE_PENDING_WRITE_KEY_PREFIX, false, "pending write to cloud"));
}

bool FDBMetaStore::markFileStatus(const File &file, const char *listName, bool set, const char *opName)
{
    std::lock_guard<std::mutex> lk(_lock);
    char verFileKey[PATH_MAX];
    int verFileKeyLength = genVersionedFileKey(file.namespaceId, file.name, file.nameLength, file.version, verFileKey);

    int retVal = 0;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // check listName
    std::string listKey = std::string(listName) + std::string("_") + std::string(verFileKey, verFileKeyLength);

    std::string verFileKeyStr;
    bool found = getValueInTX(tx, listKey, verFileKeyStr);

    if (set == false)
    {
        if (found == true)
        { // remove the file from the list
            fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(listKey.c_str()), listKey.size());
            retVal = 1;
        }
    }
    else
    { // add the file to the list
        if (found == false)
        {
            fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(listKey.c_str()), listKey.size(), reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength);
            retVal = 1;
        }
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    DLOG(INFO) << "FDBMetaStore::markFileStatus() File " << file.name << "(" << std::string(verFileKey) << ")" << (set ? " added to" : " removed from") << " the " << opName << " list";

    return retVal;
}

int FDBMetaStore::getFilesPendingWriteToCloud(int numFiles, File files[])
{
    // TODO: this implementation is not checked correctness for now

    std::lock_guard<std::mutex> lk(_lock);

    int retVal = 0;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // std::string filePendingWriteStr;
    // std::string filePendingWriteCopyKey = std::string(FDB_FILE_PENDING_WRITE_KEY) + std::string("_copy");
    // bool filePendingWriteExist = getValueInTX(tx, filePendingWriteCopyKey, filePendingWriteStr);

    // if (filePendingWriteExist == false)
    // {
    //     LOG(WARNING) << "FDBMetaStore::getFilesPendingWriteToCloud() Error finding file pending write list";

    //     // commit transaction
    //     FDBFuture *cmt = fdb_transaction_commit(tx);
    //     exitOnError(fdb_future_block_until_ready(cmt));
    //     fdb_future_destroy(cmt);

    //     return retVal; // same as Redis-based MetaStore
    // }

    // nlohmann::json *fpwjPtr = new nlohmann::json();
    // auto &fpwj = *fpwjPtr;
    // if (parseStrToJSONObj(filePendingWriteStr, fpwj) == false)
    // {
    //     exit(1);
    // }

    // int numFilesToRepair = 0;
    // numFilesToRepair = fpwj["list"].size();

    // if (numFilesToRepair == 0 && !_endOfPendingWriteSet)
    // {
    //     _endOfPendingWriteSet = true;

    //     delete fpwjPtr;
    //     // commit transaction
    //     FDBFuture *cmt = fdb_transaction_commit(tx);
    //     exitOnError(fdb_future_block_until_ready(cmt));
    //     fdb_future_destroy(cmt);
    //     return retVal;
    // }

    // // refill the set for scan
    // if (numFilesToRepair == 0)
    // {

    //     delete fpwjPtr;
    //     // commit transaction
    //     FDBFuture *cmt = fdb_transaction_commit(tx);
    //     exitOnError(fdb_future_block_until_ready(cmt));
    //     fdb_future_destroy(cmt);
    //     return retVal;
    // }

    // // mark the set scanning is in-progress
    // _endOfPendingWriteSet = false;

    // std::string fileKey = fpwj["list"].back().get<std::string>();
    // fpwj.erase(fpwj["list"].begin() + fpwj["list"].size() - 1);

    // std::string fpwjStr = fpwj.dump();
    // fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePendingWriteCopyKey.c_str()), filePendingWriteCopyKey.size(), reinterpret_cast<const uint8_t *>(fpwjStr.c_str()), fpwjStr.size());

    // delete fpwjPtr;

    // // add the key to filePendingWriteCompleteKey
    // std::string filePendingWriteCompStr;
    // bool filePendingWriteCompExist = getValueInTX(tx, std::string(FDB_FILE_PENDING_WRITE_COMP_KEY), filePendingWriteCompStr);

    // nlohmann::json *fpwcPtr = new nlohmann::json();
    // auto &fpwc = *fpwcPtr;

    // if (filePendingWriteCompExist == false)
    // {
    //     fpwc["list"] = nlohmann::json::array();
    // }
    // else
    // {
    //     if (parseStrToJSONObj(filePendingWriteCompStr, fpwc) == false)
    //     {
    //         exit(1);
    //     }
    // }

    // bool addToList = false;

    // if (std::find(fpwc["list"].begin(), fpwc["list"].end(), fileKey) == fpwc["list"].end())
    // {
    //     fpwc["list"].push_back(fileKey);
    //     addToList = true;
    // }

    // if (addToList && getNameFromFileKey(fileKey.data(), fileKey.length(), &files[0].name, files[0].nameLength, files[0].namespaceId, &files[0].version))
    // {
    //     retVal = 1;
    // }

    // delete fpwcPtr;

    // std::string fpwcStr = fpwc.dump();
    // fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_FILE_PENDING_WRITE_COMP_KEY), strlen(FDB_FILE_PENDING_WRITE_COMP_KEY), reinterpret_cast<const uint8_t *>(fpwcStr.c_str()), fpwcStr.size());

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return retVal;
}

bool FDBMetaStore::updateFileStatus(const File &file)
{
    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(file.namespaceId, file.name, file.nameLength, fileKey);

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::string bgTaskKey = std::string(FDB_BG_TASK_PENDING_KEY_PREFIX) + std::string("_") + std::string(fileKey, fileKeyLength);
    std::string bgTaskCountStr;
    bool bgTaskCountExist = getValueInTX(tx, bgTaskKey, bgTaskCountStr);
    int bgTaskCount = 0;
    if (bgTaskCountExist == true) {
        bgTaskCount = std::stoi(bgTaskCountStr);
    }

    if (file.status == FileStatus::PART_BG_TASK_COMPLETED)
    {
        // decrement number of task by 1, and remove the file is the number of
        // pending task drops to 0
        bgTaskCount--;
        if (bgTaskCount > 0) {
            fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(bgTaskKey.c_str()), bgTaskKey.size(), reinterpret_cast<const uint8_t *>(std::to_string(bgTaskCount).c_str()), std::to_string(bgTaskCount).size());
        } else {
            fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(bgTaskKey.c_str()), bgTaskKey.size());
        }
    }
    else if (file.status == FileStatus::BG_TASK_PENDING)
    { // increment number of task by 1
        bgTaskCount++;
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(bgTaskKey.c_str()), bgTaskKey.size(), reinterpret_cast<const uint8_t *>(std::to_string(bgTaskCount).c_str()), std::to_string(bgTaskCount).size());
        DLOG(INFO) << "File (task pending) " << file.name << " status updated bg task, count: " << bgTaskCount;
    }
    else if (file.status == FileStatus::ALL_BG_TASKS_COMPLETED)
    { // remove the file from the list
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(bgTaskKey.c_str()), bgTaskKey.size());
        DLOG(INFO) << "File (all tasks completed) " << file.name << " status updated";
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return ret;
}

bool FDBMetaStore::getNextFileForTaskCheck(File &file)
{
    std::lock_guard<std::mutex> lk(_lock);

    bool retVal = false;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    std::string taskCheckKey = std::string(FDB_BG_TASK_PENDING_KEY_PREFIX) + std::string("_");
    std::vector<std::pair<std::string, std::string>> fileKeys;
    if (getKVPairsWithKeyPrefixInTX(tx, taskCheckKey, fileKeys) == 0)
    {
        exit(1);
    }

    if (fileKeys.size() > 0)
    { // retrieve the first key (in lexigraphically sorted order)
        std::string tcKey = fileKeys[0].first;
        std::string fileKey = fileKeys[0].second;

        const char *d = strchr(fileKey.c_str(), '_');
        if (d != nullptr)
        {
            file.nameLength = fileKey.size() - (d - fileKey.c_str() + 1);
            file.name = (char *)malloc(file.nameLength + 1);
            memcpy(file.name, d + 1, file.nameLength);
            file.name[file.nameLength] = '\0';
            file.namespaceId = atoi(fileKey.c_str());
            DLOG(INFO) << "FDBMetaStore::getNextFileForTaskCheck() Next file to check: " << file.name << ", " << (int)file.namespaceId;
        }

        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(tcKey.c_str()), tcKey.size());

        retVal = true;
    }

    // commit transaction
    FDBFuture *cmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(cmt));
    fdb_future_destroy(cmt);

    return retVal;
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
    return lockFile(file, lock, FDB_FILE_LOCK_KEY_PREFIX, "lock");
}

bool FDBMetaStore::pinStagedFile(const File &file, bool lock)
{
    return lockFile(file, lock, FDB_FILE_PIN_STAGED_KEY_PREFIX, "pin");
}

bool FDBMetaStore::lockFile(const File &file, bool lock, const char *type, const char *name)
{
    char fileKey[PATH_MAX];
    int fileKeyLength = genFileKey(file.namespaceId, file.name, file.nameLength, fileKey);

    bool retVal = false;

    // create transaction
    FDBTransaction *tx;
    exitOnError(fdb_database_create_transaction(_db, &tx));

    // add filename to file Prefix Set
    std::string lockName(type);
    std::string lockKey = lockName + std::string("_") + std::string(fileKey, fileKeyLength);
    std::string lockStr;
    bool lockExist = getValueInTX(tx, lockKey, lockStr);

    if (lockExist == false && lock == true)
    { // add the fileKey
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(lockKey.c_str()), lockKey.size(), reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength);
        retVal = true;
    }
    else if (lockExist == true && lock == false)
    { // remove the fileKey
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(lockKey.c_str()), lockKey.size());
        retVal = true;
    }

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

void *FDBMetaStore::runNetworkThread(void *args)
{
    fdb_error_t err = fdb_run_network();
    if (err)
    {
        LOG(ERROR) << "FDBMetaStore::runNetworkThread fdb_run_network() error";
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
        // LOG(INFO) << "FDBMetaStore::getValueInTX() key " << key << " found: value (size : " << value.size() << ") " << value;
        return true;
    }
    else
    {
        // DLOG(INFO) << "FDBMetaStore:: getValueInTX() key " << key << " not found";
        return false;
    }
}

bool FDBMetaStore::getKVPairsWithKeyPrefixInTX(FDBTransaction *tx, const std::string &prefix, std::vector<std::pair<std::string, std::string>> &kvs)
{
    if (tx == NULL)
    {
        LOG(ERROR) << "FDBMetaStore:: getValueInTX() invalid Transaction";
        return false;
    }
    kvs.clear();

    // prefix end size
    size_t prefixSize = prefix.size();
    unsigned char *prefixEnd = (unsigned char *)malloc(prefixSize * sizeof(unsigned char));
    strncpy(reinterpret_cast<char *>(prefixEnd), prefix.c_str(), prefixSize);
    prefixEnd[prefixSize - 1]++;

    const FDBKeyValue *kvArray = nullptr;
    int totalKVCount = 0;
    int curKVCount = 0;
    fdb_bool_t outMore = 1;
    int iteration = 0;

    FDBFuture *kvRangeFut = fdb_transaction_get_range(tx, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefixSize), FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>(prefixEnd), prefixSize), 0, 0, FDB_STREAMING_MODE_WANT_ALL, ++iteration, 0 /* No snapshot*/, 0);

    while (outMore)
    {
        exitOnError(fdb_future_block_until_ready(kvRangeFut));

        exitOnError(fdb_future_get_keyvalue_array(kvRangeFut, &kvArray, &curKVCount, &outMore));

        // process the results
        for (int i = 0; i < curKVCount; i++)
        {
            std::string key(reinterpret_cast<const char *>(kvArray[i].key), kvArray[i].key_length);
            std::string value(reinterpret_cast<const char *>(kvArray[i].value), kvArray[i].value_length);
            kvs.push_back(std::make_pair(key, value));
        }

        totalKVCount += curKVCount;

        if (outMore)
        {
            FDBFuture *kvRangeFutNext = fdb_transaction_get_range(tx, FDB_KEYSEL_FIRST_GREATER_THAN(kvArray[curKVCount - 1].key, kvArray[curKVCount - 1].key_length), FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(reinterpret_cast<const uint8_t *>(prefixEnd), prefixSize), 0, 0, FDB_STREAMING_MODE_WANT_ALL, ++iteration, 0 /* No snapshot*/, 0);
            fdb_future_destroy(kvRangeFut);
            kvRangeFut = kvRangeFutNext;
        }
    }
    fdb_future_destroy(kvRangeFut);
    kvRangeFut = nullptr;

    // DLOG(INFO) << "FDBMetaStore:: getKVPairsWithKeyPrefixInTX() prefix: " << prefix << " total KV pairs found: " << totalKVCount << " in " << iteration << " iterations";

    free(prefixEnd);

    return true;
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
