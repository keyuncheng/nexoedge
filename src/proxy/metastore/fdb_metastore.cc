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
    // char filename[PATH_MAX], vfilename[PATH_MAX], vlname[PATH_MAX];
    // int nameLength = genFileKey(f.namespaceId, f.name, f.nameLength, filename);
    // int vlnameLength = 0;
    // std::string prefix = getFilePrefix(filename);
    // int curVersion = -1;

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

bool FDBMetaStore::updateTimestamps(const File &f) {
    return true;
}

int FDBMetaStore::updateChunks(const File &f, int version) {
    return true;
}

bool FDBMetaStore::getFileName(boost::uuids::uuid fuuid, File &f) {
    return true;
}
    
unsigned int FDBMetaStore::getFileList(FileInfo **list, unsigned char namespaceId, bool withSize, bool withTime, bool withVersions, std::string prefix) {
    return 0;
}

unsigned int FDBMetaStore::getFolderList(std::vector<std::string> &list, unsigned char namespaceId, std::string prefix, bool skipSubfolders) {
    return 0;
}

unsigned long int FDBMetaStore::getMaxNumKeysSupported() {
    return 0;
}

unsigned long int FDBMetaStore::getNumFiles() {
    return 0;
}

unsigned long int FDBMetaStore::getNumFilesToRepair() {
    return 0;
}

int FDBMetaStore::getFilesToRepair(int numFiles, File files[]) {
    return 0;
}

bool FDBMetaStore::markFileAsNeedsRepair(const File &file) {
    return false;
}

bool FDBMetaStore::markFileAsRepaired(const File &file) {
    return false;
}

bool FDBMetaStore::markFileAsPendingWriteToCloud(const File &file) {
    return false;
}

bool FDBMetaStore::markFileAsWrittenToCloud(const File &file, bool removePending) {
    return false;
}

int FDBMetaStore::getFilesPendingWriteToCloud(int numFiles, File files[]) {
    return false;
}

bool FDBMetaStore::updateFileStatus(const File &file) {
    return true;
}

bool FDBMetaStore::getNextFileForTaskCheck(File &file) {
    return true;
}

bool FDBMetaStore::lockFile(const File &file) {
    return true;
}

bool FDBMetaStore::unlockFile(const File &file) {
    return true;
}

bool FDBMetaStore::addChunkToJournal(const File &file, const Chunk &chunk, int containerId, bool isWrite) {
    extractJournalFieldKeyParts("", 0);
    return true;
}

bool FDBMetaStore::updateChunkInJournal(const File &file, const Chunk &chunk, bool isWrite, bool deleteRecord, int containerId) {
    return true;
}

void FDBMetaStore::getFileJournal(const FileInfo &file, std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> &records) {
    
}

int FDBMetaStore::getFilesWithJounal(FileInfo **list) {
    return 0;
}

bool FDBMetaStore::fileHasJournal(const File &file) {
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
    if (err) {
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

std::string FDBMetaStore::getValue(std::string key)
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

    std::string ret_val;
    // DEBUG
    if (key_present)
    {
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

    return ret_val;
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

std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength) {
    std::string fieldKey(field, fieldLength);

    // expected format 'c<chunk_id>-<type>-<container_id>', e.g., c00-op-1
    size_t delimiter1 = fieldKey.find("-");
    size_t delimiter2 = fieldKey.find("-", delimiter1 + 1);
    if (delimiter1 == std::string::npos || delimiter2 == std::string::npos) {
        return std::make_tuple(INVALID_CHUNK_ID, "", INVALID_CONTAINER_ID);
    }

    int chunkId, containerId;
    std::string type;
    // first part is 'c[0-9]+'
    chunkId = strtol(fieldKey.substr(1, delimiter1-1).c_str(), NULL, 10);
    // second part is a string
    type = fieldKey.substr(delimiter1 + 1, delimiter2-delimiter1-1);
    // third part is a '[0-9]+'
    containerId = strtol(fieldKey.substr(delimiter2 + 1).c_str(), NULL, 10);

    return std::make_tuple(chunkId, type, containerId);
}