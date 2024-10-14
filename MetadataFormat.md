# Metadata Format

## Redis

### Metadata Data Structure

* Filename (assume filename is "abc")
    * genFileKey(File) -> filename = "namespaceId_abc"
    * getFilePrefix(filename) ->
        * Directory: "//pf_(dirname)"
        * File: "//pf_(everything-before-slash)"
    * FileKey
        * without versioning: through genFileKey()
        * with versioning
            * the latest one: through genFileKey()
            * Previous versions: through genVersionedFileKeys()
    * getFilePrefix(filename) ->
        * Directory: "//pf_(dirname)"
        * File: "//pf_(everything-before-slash)"
    * FileKey
        * wi/wo versioning: through genFileKey()
        * Without versioning: only keep the latest version (through genVersFileKeys())
        * With versioning: keep all versions (through genVersFileKeys())

### Workflow

#### putMeta()

* Add lock_guard()
* Check file versioning (now assume versioning is enabled)
    * If the incoming request has higher version than the current version, backup the current version
        * Rename current hash key (filename) to versionedFileKey ( current version - 1)
        * Backup some fields from versionedFileKey (size mtime md5 dm numC) to versionedList
            * Get version list key ("//vl_filename)
            * Create a file version summary and append to version list
                * Format: version mtime md5 dm numC
    * If the incoming File has older version than the current stored version
        * Find the current previous version, make sure it exists in the current metadata store
        * Update filename to versionedFilename (in f.version)
* Set fields
    * filename (versioned/non-versioned): 
        * Bind with filename: Check redis_metastore.cc:180-188
        * Bind with containers: Check redis_metastore.cc:213-219
        * Bind with deduplication: Check redis_metastore.cc:221-242
    * Set file uuid to filename mapping
    * Set add directory prefix set (file prefix -> filename)
    * Set add global directory prefix set (DIR_LIST_KEY -> file prefix)


#### getMeta()

* Input: assume that f.version is set (-1: non-versioned; otherwise versioned)
* add lock_guard()
* set filename as fileKey
* If enabled versioning
    * get current version
    * if retrieved version is different from f.version
        * set filename as versionedFileKey
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

TBD

#### renameMeta()

* sfname: source filename; dfname: destination filename
* Generate FileKeys, FilePrefix and FileUUIDKey for sfname and dfname
* add lock_guard()
* rename the sfname to dfname
* Set dfidKey to dfname (reverse mapping); remove the original sfidKey; if
  wrong, should reverse the update
* Set UUIDKey to dfname: if unsuccessful, should reverse the update
* Remove the previous filePrefix from prefix set
* Add the new prefix filePrevix to prefix set


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