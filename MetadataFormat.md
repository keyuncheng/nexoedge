# Metadata Format

## Redis

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


## Redis putMeta() workflow
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

# FDB

# putMeta() workflow

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
