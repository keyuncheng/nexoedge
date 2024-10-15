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
    char fileKey[PATH_MAX], verFileKey[PATH_MAX], verListKey[PATH_MAX];

    // file key (format: namespace_filename)
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);
    // versioned file key (format: namespace_filename_version)
    int verFileKeyLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, verFileKey);
    // // version list (format: vlnamespace_filename) (currently removed)
    // int vlnameLength = genFileVersionListKey(f.namespaceId, f.name, f.nameLength, verListKey);

    // pre-create a file version summary
    std::string fvsummary;
    fvsummary.append(std::to_string(f.size).append(" "));                            // file size
    fvsummary.append(std::to_string(f.mtime).append(" "));                           // modify time
    fvsummary.append(std::string(f.md5, f.md5 + MD5_DIGEST_LENGTH).append(" "));     // md5
    fvsummary.append(std::to_string(((f.size == 0) ? f.isDeleted : 0)).append(" ")); // is deleted
    fvsummary.append(std::to_string(f.numChunks));                                   // number of chunks

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

    if (fileMetaExist == false)
    {
        // init the version list with the new version
        fmj["verList"] = nlohmann::json::array();
        fmj["verList"].push_back(verFileKey);
        fmj["verNum"] = nlohmann::json::array();
        fmj["verNum"].push_back(f.version);
        fmj["verSummary"] = nlohmann::json::array();
        fmj["verSummary"].push_back(fvsummary);
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
            LOG(ERROR) << "FDBMetaStore::putMeta() Error parsing JSON string: " << e.what();
            exit(1);
        }

        // check if versioning is enabled
        bool keepVersion = !config.overwriteFiles();
        if (keepVersion == false)
        {
            // remove all previous versions (verFilekey) in FDB
            for (auto prevVerFileKey : fmj["verList"])
            {
                fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(prevVerFileKey.c_str()), prevVerFileKey.size());
            }

            // only keep the latest version
            fmj["verList"].clear();
            fmj["verList"].push_back(verFileKey);
            fmj["verNum"].clear();
            fmj["verNum"].push_back(f.version);
            fmj["verSummary"].clear();
            fmj["verSummary"].push_back(fvsummary);
        }
        else
        {
            // if the version is not stored in the list, insert the version
            if (fmj["verList"].find(verFileKey) == fmj["verList"].end())
            {
                bool found_pos = false;
                for (int i = 0; i < fmj["verNum"].size(); i++)
                {
                    if (f.version < fmj["verNum"].get<int>(i))
                    {
                        found_pos = true;
                        fmj["verList"].insert(fmj["verList"].begin() + i, verFileKey);
                        fmj["verNum"].insert(fmj["verNum"].begin() + i, f.version);
                        fmj["verSummary"].insert(fmj["verSummary"].begin() + i, fvsummary);
                        break;
                    }
                }
                if (found_pos == false)
                {
                    fmj["verList"].push_back(verFileKey);
                    fmj["verNum"].push_back(f.version);
                    fmj["verSummary"].push_back(fvsummary);
                }
            }
        }
    }

    // serialize json to string and store in FDB
    std::string fmjStr = fmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fileKey), fileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());
    delete fmjptr;

    // create metadata for current file version, format: serialized JSON string
    bool isEmptyFile = f.size == 0;
    unsigned char *codingState = isEmptyFile || f.codingMeta.codingState == NULL ? (unsigned char *)"" : f.codingMeta.codingState;
    int deleted = isEmptyFile ? f.isDeleted : 0;
    size_t numUniqueBlocks = f.uniqueBlocks.size();
    size_t numDuplicateBlocks = f.duplicateBlocks.size();

    nlohmann::json *vfmjptr = new nlohmann::json();
    auto &vfmj = *vfmjptr;
    vfmj["name"] = std::string(f.name, f.name + f.nameLength);
    vfmj["uuid"] = boost::uuids::to_string(f.uuid);
    vfmj["size"] = std::to_string(f.size);
    vfmj["numC"] = std::to_string(f.numChunks);
    vfmj["sc"] = f.storageClass;
    vfmj["cs"] = std::string(1, f.codingMeta.coding);
    vfmj["n"] = std::to_string(f.codingMeta.n);
    vfmj["k"] = std::to_string(f.codingMeta.k);
    vfmj["f"] = std::to_string(f.codingMeta.f);
    vfmj["maxCS"] = std::to_string(f.codingMeta.maxChunkSize);
    vfmj["codingStateS"] = std::to_string(f.codingMeta.codingStateSize);
    vfmj["codingState"] = std::string(reinterpret_cast<char *>(codingState));
    vfmj["numS"] = std::to_string(f.numStripes);
    vfmj["ver"] = std::to_string(f.version);
    vfmj["ctime"] = std::to_string(f.ctime);
    vfmj["atime"] = std::to_string(f.atime);
    vfmj["mtime"] = std::to_string(f.mtime);
    vfmj["tctime"] = std::to_string(f.tctime);
    vfmj["md5"] = std::string(f.md5, f.md5 + MD5_DIGEST_LENGTH);
    vfmj["sg_size"] = std::to_string(f.staged.size);
    vfmj["sg_sc"] = f.staged.storageClass;
    vfmj["sg_cs"] = std::to_string(f.staged.codingMeta.coding);
    vfmj["sg_n"] = std::to_string(f.staged.codingMeta.n);
    vfmj["sg_k"] = std::to_string(f.staged.codingMeta.k);
    vfmj["sg_f"] = std::to_string(f.staged.codingMeta.f);
    vfmj["sg_maxCS"] = std::to_string(f.staged.codingMeta.maxChunkSize);
    vfmj["sg_mtime"] = std::to_string(f.staged.mtime);
    vfmj["dm"] = std::to_string(deleted);
    vfmj["numUB"] = std::to_string(numUniqueBlocks);
    vfmj["numDB"] = std::to_string(numDuplicateBlocks);

    // container ids
    char chunkName[FDB_MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++)
    {
        genChunkKeyPrefix(f.chunks[i].getChunkId(), chunkName);
        std::string cidKey = std::string(chunkName) + std::string("-cid");
        vfmj[cidKey.c_str()] = std::to_string(f.containerIds[i]);
        std::string csizeKey = std::string(chunkName) + std::string("-size");
        vfmj[csizeKey.c_str()] = std::to_string(f.chunks[i].size);
        std::string cmd5Key = std::string(chunkName) + std::string("-md5");
        vfmj[cmd5Key.c_str()] = std::string(f.chunks[i].md5, f.chunks[i].md5 + MD5_DIGEST_LENGTH);
        std::string cmd5Bad = std::string(chunkName) + std::string("-bad");
        vfmj[cmd5Bad.c_str()] = std::to_string((f.chunksCorrupted ? f.chunksCorrupted[i] : 0));
    }

    // deduplication fingerprints and block mapping
    char blockName[FDB_MAX_KEY_SIZE];
    size_t blockId = 0;
    for (auto it = f.uniqueBlocks.begin(); it != f.uniqueBlocks.end(); it++, blockId++)
    { // deduplication fingerprints
        genBlockKey(blockId, blockName, /* is unique */ true);
        std::string fp = it->second.first.get();
        // logical offset, length, fingerprint, physical offset
        vfmj[std::string(blockName).c_str()] = std::to_string(it->first._offset) + std::to_string(it->first._length) + fp.data() + std::to_string(it->second.second);
    }
    blockId = 0;
    for (auto it = f.duplicateBlocks.begin(); it != f.duplicateBlocks.end(); it++, blockId++)
    {
        genBlockKey(blockId, blockName, /* is unique */ false);
        std::string fp = it->second.get();
        // logical offset, length, fingerprint
        vfmj[std::string(blockName).c_str()] = std::to_string(it->first._offset) + std::to_string(it->first._length) + fp.data();
    }

    // serialize json to string and store in FDB
    std::string vfmjStr = vfmj.dump();
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(verFileKey), verFileKeyLength, reinterpret_cast<const uint8_t *>(vfmjStr.c_str()), vfmjStr.size());
    delete vfmjptr;

    // add uuid-to-file-name maping
    char fidKey[FDB_MAX_KEY_SIZE + 64];
    if (genFileUuidKey(f.namespaceId, f.uuid, fidKey) == false)
    {
        LOG(WARNING) << "File uuid " << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    }
    else
    {
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(fidKey), FDB_MAX_KEY_SIZE + 64, reinterpret_cast<const uint8_t *>(f.name), f.nameLength);
    }

    // update the corresponding directory prefix set of this file
    std::string filePrefix = getFilePrefix(fileKey);
    FDBFuture *filePrefixFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size(), 0); // not set snapshot
    exitOnError(fdb_future_block_until_ready(filePrefixFut));
    fdb_bool_t filePrefixExist;
    const uint8_t *filePrefixRaw = NULL;
    int filePrefixRawLength;
    exitOnError(fdb_future_get_value(filePrefixFut, &filePrefixExist, &filePrefixRaw, &filePrefixRawLength));
    fdb_future_destroy(filePrefixFut);
    filePrefixFut = nullptr;
    if (!filePrefixExist)
    {
        // create the list and add to the list
        nlohmann::json *dljptr = new nlohmann::json();
        auto &dlj = *dljptr;
        dlj["list"] = nlohmann::json::array();
        dlj["list"].push_back(fileKey);
        std::string dljstr = dlj.dump();
        delete dljptr;

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size(), reinterpret_cast<const uint8_t *>(dljstr.c_str()), dljstr.size());
    }
    else
    {
        std::string dljstr(reinterpret_cast<const char *>(filePrefixRaw), filePrefixRawLength);
        // add fileKey to the list (avoid duplication)
        nlohmann::json *dljptr = new nlohmann::json();
        auto &dlj = *dljptr;
        dlj.parse(dljstr);
        if (dlj["list"].find(fileKey) == dlj["list"].end())
        {
            dlj["list"].push_back(fileKey);
        }
        std::string dljstr = dlj.dump();
        delete dljptr;

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size(), reinterpret_cast<const uint8_t *>(dljstr.c_str()), dljstr.size());
    }

    // update the global directory prefix set of this file
    FDBFuture *GDLFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(FDB_DIR_LIST_KEY), std::string(FDB_DIR_LIST_KEY).size(), 0); // not set snapshot
    exitOnError(fdb_future_block_until_ready(GDLFut));
    fdb_bool_t GDLExist;
    const uint8_t *GDLRaw = NULL;
    int GDLRawLength;
    exitOnError(fdb_future_get_value(GDLFut, &GDLExist, &GDLRaw, &GDLRawLength));
    fdb_future_destroy(GDLFut);
    GDLFut = nullptr;
    if (!GDLExist)
    {
        // create the list and add filePrefix to the list
        nlohmann::json *gdljptr = new nlohmann::json();
        auto &gdlj = *gdljptr;
        gdlj["list"] = nlohmann::json::array();
        gdlj["list"].push_back(filePrefix.c_str());
        std::string gdljstr = gdlj.dump();
        delete gdljptr;

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_DIR_LIST_KEY), std::string(FDB_DIR_LIST_KEY).size(), reinterpret_cast<const uint8_t *>(gdljstr.c_str()), gdljstr.size());
    }
    else
    {
        std::string gdljstr(reinterpret_cast<const char *>(GDLRaw), GDLRawLength);
        // add filePrefix to the list (avoid duplication)
        nlohmann::json *gdljptr = new nlohmann::json();
        auto &gdlj = *gdljptr;
        gdlj.parse(gdljstr);
        if (gdlj["list"].find(filePrefix) == gdlj["list"].end())
        {
            gdlj["list"].push_back(filePrefix);
        }
        std::string gdljstr = gdlj.dump();
        delete gdljptr;

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(FDB_DIR_LIST_KEY), std::string(FDB_DIR_LIST_KEY).size(), reinterpret_cast<const uint8_t *>(gdljstr.c_str()), gdljstr.size());
    }

    // commit transaction
    FDBFuture *fcmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(fcmt));

    fdb_future_destroy(fcmt);

    LOG(INFO) << "FDBMetaStore:: putMeta() finished";

    return true;
}

bool FDBMetaStore::getMeta(File &f, int getBlocks)
{
    std::lock_guard<std::mutex> lk(_lock);
    char fileKey[PATH_MAX];

    // file key (format: namespace_filename)
    int fileKeyLength = genFileKey(f.namespaceId, f.name, f.nameLength, fileKey);
    // versioned file key (format: namespace_filename_version)
    int verFileKeyLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, verFileKey);

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

    nlohmann::json *fmjptr = new nlohmann::json();
    auto &fmj = *fmjptr;

    if (fileMetaExist == false)
    { // file metadata not exist: report and return
        LOG(WARNING) << "FDBMetaStore::putMeta() failed to get metadata for file " << f.name;

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
            LOG(ERROR) << "FDBMetaStore::getMeta() Error parsing JSON string: " << e.what();
            exit(1);
        }
    }

    // For non-versioned file: get the only version; for versioned file: get
    // metadata of f.version
    if (f.version == -1)
    { // non-versioned file
        // update verFileKey as the only element in verList
        memcpy(verFileKey, fmj["verList"][0].get<std::string>().c_str(), fmj["verList"][0].get<std::string>().size());
    }
    else
    { // versioned file: make sure that the current version exists in the file metadata
        if (fmj["verList"].find(verFileKey) == fmj["verList"].end())
        {
            LOG(WARNING) << "FDBMetaStore::getMeta() failed to find version " << f.version << " for file " << f.name;

            // commit transaction and return
            delete fmjptr;
            FDBFuture *fcmt = fdb_transaction_commit(tx);
            exitOnError(fdb_future_block_until_ready(fcmt));
            fdb_future_destroy(fcmt);

            return false;
        }
    }
    delete fmjptr;

    size_t numUniqueBlocks = 0, numDuplicateBlocks = 0;

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
    { // versioned file metadata not exist: report and return
        LOG(WARNING) << "FDBMetaStore::getMeta() failed to get versioned metadata for file " << f.name;

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
            LOG(ERROR) << "FDBMetaStore::getMeta() Error parsing JSON string: " << e.what();
            exit(1);
        }
    }
    delete vfmjptr;

    // parse fields
    f.size = std::stoull(vfmj["size"].get<std::string>());
    f.numChunks = std::stoi(vfmj["numC"].get<std::string>());
    f.numStripes = std::stoi(vfmj["numS"].get<std::string>());
    std::string retrievedUUID = vfmj["uuid"].get < std::string().c_str();
    if (f.setUUID(retrievedUUID) == false)
    {
        LOG(WARNING) << "FDBMetaStore::getMeta() invalid UUID in metadata: " << retrievedUUID;

        // commit transaction and return
        delete vfmjptr;
        FDBFuture *fcmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(fcmt));
        fdb_future_destroy(fcmt);

        return false;
    }

    f.storageClass = vfmj["sc"].get<std::string>().c_str();
    f.codingMeta.coding = vfmj["cs"].get < std::string().c_str()[0];
    f.codingMeta.n = std::stoi(vfmj["n"].get<std::string>());
    f.codingMeta.k = std::stoi(vfmj["k"].get<std::string>());
    f.codingMeta.f = std::stoi(vfmj["f"].get<std::string>());
    f.codingMeta.maxChunkSize = std::stoi(vfmj["maxCS"].get<std::string>());
    f.staged.mtime = std::stoull(vfmj["sg_mtime"].get<std::string>());
    f.isDeleted = std::stoi(vfmj["dm"].get<std::string>());
    f.numUniqueBlocks = std::stoi(vfmj["numUB"].get<std::string>());
    f.numDuplicateBlocks = std::stoi(vfmj["numDB"].get<std::string>());

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

        f.containerIds[chunkId] = std::stoi(vfmj[cidKey.c_str()].get<std::string>());
        f.chunks[chunkId].size = std::stoi(vfmj[csizeKey.c_str()].get<std::string>());
        f.chunks[chunkId].md5 = std::string(vfmj[cmd5Key.c_str()].get<std::string>().c_str(), MD5_DIGEST_LENGTH);
        f.chunksCorrupted[chunkId] = std::stoi(vfmj[cbadKey.c_str()].get<std::string>()); // TODO: double check this field; make sure it's correct
        f.chunks[chunkId].setId(f.namespaceId, f.uuid, chunkId);
        f.chunks[chunkId].data = 0;
        f.chunks[chunkId].freeData = true;
        f.chunks[chunkId].fileVersion = f.version;
    }

    // get block attributes for deduplication
    BlockLocation::InObjectLocation loc;
    Fingerprint fp;
    char blockName[FDB_MAX_KEY_SIZE];

    int noFpOfs = sizeof(unsigned long int) + sizeof(unsigned int);
    int hasFpOfs = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH;
    int lengthWithFp = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH + sizeof(int);

    if (getBlocks == 1 || getBlocks == 3)
    { // unique blocks
        int pOffset = 0;
        for (size_t blockId = 0; blockId < numUniqueBlocks; blockId++)
        {
            genBlockKey(blockId, blockName, /* is unique */ true);
            std::string blockStr = vfmj[std::string(blockName).c_str()].get<std::string>();
            loc._offset = std::stoull(blockStr.substr(pOffset, sizeof(unsigned long int)));
            loc._length = std::stoull(blockStr.substr(pOffset + sizeof(unsigned long int), sizeof(unsigned int)));
            if (vfmj[std::string(blockName).c_str()].size() <= lengthWithFp)
            {
                fp.set(blockStr.substr(pOffset + noFpOfs, SHA256_DIGEST_LENGTH).c_str());
            }
            else
            {
                fp.set(blockStr.substr(pOffset + hasFpOfs, SHA256_DIGEST_LENGTH).c_str());
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

            loc._offset = std::stoull(vfmj[std::string(blockName).c_str()].substr(0, sizeof(unsigned long int)));
            loc._length = std::stoull(vfmj[std::string(blockName).c_str()].substr(sizeof(unsigned long int), sizeof(unsigned int)));

            if (vfmj[std::string(blockName).c_str()].size() >= lengthWithFp)
            {
                fp.set(vfmj[std::string(blockName).c_str()].substr(noFpOfs, SHA256_DIGEST_LENGTH).c_str());
            }

            auto followIt = f.duplicateBlocks.end(); // hint is the item after the element to insert for c++11, and before the element for c++98
            f.duplicateBlocks.emplace_hint(followIt, std::make_pair(loc, fp));
        }
    }

    // commit transaction and return
    delete vfmjptr;
    FDBFuture *fcmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(fcmt));
    fdb_future_destroy(fcmt);

    return true;
}

bool FDBMetaStore::deleteMeta(File &f)
{
    return true;
}

bool FDBMetaStore::renameMeta(File &sf, File &df)
{
    // file names
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

    // retrieve the original metadata
    FDBFuture *fileMetaFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(srcfileKey), srcFileKeyLength, 0 /** not set snapshot */);
    exitOnError(fdb_future_block_until_ready(fileMetaFut));
    fdb_bool_t fileMetaExist;
    const uint8_t *fileMetaRaw = NULL;
    int fileMetaRawLength;
    exitOnError(fdb_future_get_value(fileMetaFut, &fileMetaExist, &fileMetaRaw, &fileMetaRawLength));
    fdb_future_destroy(fileMetaFut);
    fileMetaFut = nullptr;

    if (fileMetaExist == false)
    {
        LOG(ERROR) << "FDBMetaStore::renameMeta() Error reading metadata for file" << sf.name;

        // commit transaction
        FDBFuture *fcmt = fdb_transaction_commit(tx);
        exitOnError(fdb_future_block_until_ready(fcmt));
        fdb_future_destroy(fcmt);

        return false;
    }
    else
    {
        // parse fileMeta as JSON object
        nlohmann::json *fmjptr = new nlohmann::json();
        auto &fmj = *fmjptr;
        try
        {
            std::string filMetaRawStr(reinterpret_cast<const char *>(fileMetaRaw));
            fmj = nlohmann::json::parse(filMetaRawStr);
        }
        catch (std::exception e)
        {
            LOG(ERROR) << "FDBMetaStore::putMeta() Error parsing JSON string: " << e.what();
            exit(1);
        }

        // update uuid-to-file-name mapping
        fmj["uuid"] = boost::uuids::to_string(df.uuid).c_str();

        // serialize json to string and store in FDB
        std::string fmjStr = fmj.dump();
        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileKey), dstFileKeyLength, reinterpret_cast<const uint8_t *>(fmjStr.c_str()), fmjStr.size());
        delete fmjptr;

        // remove original metadata
        fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcfileKey), srcFileKeyLength);
    }

    // set dfidKey; remove the original sfidKey (check putMeta impl)
    // insert new metadata
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFileUuidKey), FDB_MAX_KEY_SIZE + 64, dstFileKey, dstFileKeyLength);

    DLOG(INFO) << "FDBMetaStore::renameMeta() Add reverse mapping (" << dstFileUuidKey << ") for file " << dstFileKey;

    // remove original mapping
    fdb_transaction_clear(tx, reinterpret_cast<const uint8_t *>(srcFileUuidKey), FDB_MAX_KEY_SIZE + 64);

    std::string dstFilePrefix = getFilePrefix(dstFileKey);

    // update the src file prefix set
    std::string srcFilePrefix = getFilePrefix(srcFileKey);
    FDBFuture *srcFilePrefixFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(srcFilePrefix.c_str()), srcFilePrefix.size(), 0); // not set snapshot
    exitOnError(fdb_future_block_until_ready(srcFilePrefixFut));
    fdb_bool_t srcFilePrefixExist;
    const uint8_t *srcFilePrefixRaw = NULL;
    int srcFilePrefixRawLength;
    exitOnError(fdb_future_get_value(srcFilePrefixFut, &srcFilePrefixExist, &srcfilePrefixRaw, &srcFilePrefixRawLength));
    fdb_future_destroy(srcFilePrefixFut);
    srcFilePrefixFut = nullptr;
    if (!srcFilePrefixExist)
    {
        LOG(ERROR) << "FDBMetaStore::putMeta() Error finding src prefix set: " << e.what();
        exit(1);
    }
    else
    {
        std::string dljstr(reinterpret_cast<const char *>(srcFilePrefixRaw), srcFilePrefixRawLength);
        // add srcFileKey to the list (avoid duplication)
        nlohmann::json *dljptr = new nlohmann::json();
        auto &dlj = *dljptr;
        dlj.parse(dljstr);
        if (dlj["list"].find(srcFileKey) != dlj["list"].end())
        {
            dlj["list"].erase(srcFileKey);
        }
        std::string dljstr = dlj.dump();
        delete dljptr;

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(srcFilePrefix.c_str()), srcFilePrefix.size(), reinterpret_cast<const uint8_t *>(dljstr.c_str()), dljstr.size());
    }

    // update the dst file prefix set
    std::string dstFilePrefix = getFilePrefix(dstFileKey);
    FDBFuture *dstFilePrefixFut = fdb_transaction_get(tx, reinterpret_cast<const uint8_t *>(dstFilePrefix.c_str()), dstFilePrefix.size(), 0); // not set snapshot
    exitOnError(fdb_future_block_until_ready(dstFilePrefixFut));
    fdb_bool_t dstFilePrefixExist;
    const uint8_t *dstFilePrefixRaw = NULL;
    int dstFilePrefixRawLength;
    exitOnError(fdb_future_get_value(dstFilePrefixFut, &dstFilePrefixExist, &dstfilePrefixRaw, &dstFilePrefixRawLength));
    fdb_future_destroy(dstFilePrefixFut);
    dstFilePrefixFut = nullptr;

    nlohmann::json *dljptr = new nlohmann::json();
    auto &dlj = *dljptr;
    if (!dstFilePrefixExist)
    {
        // create the list and add to the list
        dlj["list"] = nlohmann::json::array();
        dlj["list"].push_back(dstFileKey);

        fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(filePrefix.c_str()), filePrefix.size(), reinterpret_cast<const uint8_t *>(dljstr.c_str()), dljstr.size());
    }
    else
    {
        // parse from existing prefix set
        std::string dljstr(reinterpret_cast<const char *>(dstFilePrefixRaw), dstFilePrefixRawLength);
        dlj.parse(dljstr);
        // add dstFileKey to the list (avoid duplication)
        if (dlj["list"].find(dstFileKey) == dlj["list"].end())
        {
            dlj["list"].push_back(dstFileKey);
        }
        std::string dljstr = dlj.dump();
        delete dljptr;
    }

    std::string dljstr = dlj.dump();
    delete dljptr;
    fdb_transaction_set(tx, reinterpret_cast<const uint8_t *>(dstFilePrefix.c_str()), dstFilePrefix.size(), reinterpret_cast<const uint8_t *>(dljstr.c_str()), dljstr.size());

    // commit transaction
    FDBFuture *fcmt = fdb_transaction_commit(tx);
    exitOnError(fdb_future_block_until_ready(fcmt));

    fdb_future_destroy(fcmt);

    LOG(INFO) << "FDBMetaStore:: renameMeta() finished";

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

int FDBMetaStore::genVersionedFileKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[])
{
    return snprintf(key, PATH_MAX, "/%d_%*s\n%d", namespaceId, nameLength, name, version);
}

int FDBMetaStore::genFileVersionListKey(unsigned char namespaceId, const char *name, int nameLength, char key[])
{
    return snprintf(key, PATH_MAX, "//vl%d_%*s", namespaceId, nameLength, name);
}

bool FDBMetaStore::genFileUuidKey(unsigned char namespaceId, boost::uuids::uuid uuid, char key[])
{
    return snprintf(key, FDB_MAX_KEY_SIZE + 64, "//fu%d-%s", namespaceId, boost::uuids::to_string(uuid).c_str()) <= FDB_MAX_KEY_SIZE;
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