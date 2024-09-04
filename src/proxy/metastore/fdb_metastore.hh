// SPDX-License-Identifier: Apache-2.0

/**
 * @brief FoundationDB-based metadata store
 *
 */

#ifndef __FDB_METASTORE_HH__
#define __FDB_METASTORE_HH__

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// See also: https://apple.github.io/foundationdb/api-c.html
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#endif // define __FDB_METASTORE_HH__
