# Metadata Format

## Redis

### Metadata Data Structure

* File
    * FileKey
    * (verFileKey0, verFileKey1, ...)
    * FileUUID
    * FileVersionList

* File prefix set

* DIR_LIST_KEY -> File prefix set

### Functions

#### extractJournalFieldKeyParts()

* TBD

#### RedisMetaStore()

* Connect to Redis server through IP and port
* Init _taskScanIt and _endOfPendingWriteSet (Todo: usage?)

#### ~RedisMetaStore()

* Free Redis context

#### putMeta()

* Add lock_guard()
* Generate FileKey
* Generate FilePrefix from FileKey
* Obtain current file version (if it exists in the metastore)
    * Set to curVersion
* Now assume versioning is enabled
    * If the input file has higher version than the currently stored version, backup the current version
        * Rename current FileKey to versionedFileKey, with input file version - 1
        * Backup some fields from versionedFileKey (size mtime md5 dm numC) to FileVersionList
            * Get FileVersionListKey
            * Create a file version summary and append to version list
                * Format: version-1 size mtime md5 dm numC
            * Append summary to FileVersionList with version-1
    * If the incoming File has older version than the current stored version
        * Find the current previous version, make sure it exists in the
          current metadata store
            * obtain by genFileVersionListKey
            * Find the current version with ZRANGEBYSCORE 
        * Set filename as versionedFilename (in f.version), otherwise no need to update filename
* Set fields to filename
    * filename (versioned/non-versioned): 
        * Bind with filename: Check redis_metastore.cc:180-188
        * Bind with containers: Check redis_metastore.cc:213-219
        * Bind with deduplication: Check redis_metastore.cc:221-242
    * Set file uuid to filename mapping
    * Add directory prefix set (file prefix set -> filename)
    * Add directory prefix set (DIR_LIST_KEY -> file prefix)


#### getMeta()

* add lock_guard()
* set filename as fileKey
* If enabled versioning, if retrieved version is different from f.version
    * set filename as versionedFileKey with the specified version
* Get fields
    * filename (versioned/non-versioned): 
        * Check redis_metastore.cc:313-321
* define macros for parsing redis replies
    * check_and_copy_string(field, idx, len): set field as string
    * check_and_copy_field(field, idx, len): memory copy field from reply element
    * check_and_copy_field_at_offset(field, idx, ofs, len): memory copy field from reply element starting from a given offset
    * check_and_copy_or_set_field(field, idx, len, default_val): if have result, then check_and_copy_string(field, idx, len); otherwise set as default_val
    * check_and_convert_or_set_field(field, idx, len, conv, default_val): if not have result: set as default_val; otherwise convert and set as field
* Parse replies
    * Check redis_metastore.cc:400-447
* Get chunk and container ids
    * Get chunk prefix and get fields
    * Parse replieds
* Get block attributes for deduplication
    * Handle for different dedup attributes

#### deleteMeta()

* genFileKey()
* genFileVersionListKey()
* getFilePrefix()
* check versioning
* If the file metadata does not exist, cancel delete file metadata
* If versioning enabled, lazy deletion is enabled, versionToDelete, no
  specified version to delete, enable lazy deletion
  * Call putMeta(f) to update metadata, to tell the caller not to remove the
    data
* If a specified version is to be deleted
    * Obtain the current version (make sure it exists)
    * Find all versions (sorted in order through ZCARD)
    * If modifying the current version, set the second latest version as the
      current version
    * If no version to remove, return false (it's an invalid operation)
    * Otherwise, remove a version
* If no version is to be deleted, delete the current file
* Delete fuuidKey mapping
* Delete dirkey and filekey mapping

#### renameMeta()

* sfname, dfname: src/dst filename
* sprefix, dprefix: src/dst file prefix
* sfidKey, dfidKey: src/dst file uuid key
* add lock_guard()
* Rename the sfname to dfname (RENAMENX)
* Create dst fuuid to filename mapping
* Set filename fuuid
* Remove src file prefix
* Add dst file prefix

#### updateTimestamps()

* add lock_guard()
* update atime, mtime and ctime
* Verify version
* Set chunks (cid; size)

#### updateChunks()

* add lock_guard()
* Update chunk information

#### getFileName(fuuid, f)

* Use getFileName(fuuidKey, f) to get the filename


#### getFileList()

* Generate sprefix: "//pf_namespace_prefix"

* If no prefix or not directory prefix at the end
    * Directly search "namespace_prefix"
* Otherwise, search all members of sprefix

### getFolderList()

* add lock_guard()
* getFilePrefix
* SSCAN to match directory prefix by cursor
* List out all the directories

#### genFileKey()

* FileKey format: namespaceId_filename

#### genVersionedFileKey()

* Versioned FileKey format: "/namespace_filename'\n'version"

#### genFileVersionListKey()

* FileVersionListKey format: "//vlnamespaceid_name"

#### getFilePrefix()

* slash: the last occurence of position of '/'
* us: the first position of '_', usually between namespace and filename

* prefix starts with "//pf_"

* If the file is on root directory ("namespace_filename", without '/'), or the
  file itself is the root: directory 'namespace_/'
    * Set file prefix to "//pf_namespace_" (without the filename or '/')
    * If noEndingSlash == false, append a '/' at the end of the prefix
* Otherwise, the file is stored in a sub-directory
    * Set the file prefix to "pf_namespace_subdirs/"

## FDB

### Metadata Data Structure

### Workflow

#### putMeta()

* Filename (assume filename is "abc")
* genFileKey(File) -> filename = "namespaceId_abc"
* genVersionedFileKey(File) -> filename = "namespaceId_abc_ver"
* genFileVersionListKey(File) -> filename = verListKey (the same as
    Redis), currently not used
* Create record for filename
    * Key: filename
    * Value: JSON string {}
* Check file version
    * If not enabled, only keep version 0
    * If enabled
        * When incoming a new version, append to the current metadata list
        * When incoming an old version than the latest version in
            metastore
            * Check existence of the old version
            * Directly operate on that version
    * Update versionedFileKey
        * Format: JSON string
        * Workflow: the same as Redis-based metastore

#### getMeta()

* The flow of getMeta() is very similar to Redis-based metastore; will update
  details after impl

#### renameMeta()

* The flow of renameMeta() is very similar to Redis-based metastore; will
  update details after impl