# Metadata Format

## Redis

* File lock:
    * (lockguard -> putMeta())

* Filename (assume filename is "abc")
    * genFileKey(File) -> filename = "namespaceId_abc"
    * getFilePrefix(filename) ->
        * Directory: "//pf_(dirname)"
        * File: "//pf_(everything-before-slash)"


## Redis putMeta() workflow
    * genFileKey and genFilePrefix
    * Work on file versioning
        * versioning is enabled:
            * If the incoming File has larger version than the current stored version, backup the current version
                * Rename previous hashkey (filename) -> versioned filename (with cur-version - 1)
                * Get some fields from the previous hashkey (size mtime md5 dm numC)
                * Get version list key ("//vl_filename)
                * Create a file version summary and append to version list
                    * Format: version mtime md5 dm numC
                * add record to version list: vlist [version: summary]
            * If the incoming File has older version than the current stored version, reset the filename to the versioned filename with old version
    * Set fields
        * filename (versioned/non-versioned): 
            * Bind with filename: Check redis_metastore.cc:180-188
            * Bind with containers: Check redis_metastore.cc:213-219
            * Bind with deduplication: Check redis_metastore.cc:221-242
        * Update other metadata (uuid, previx, dir): Check redis_metastore.cc:247-264
