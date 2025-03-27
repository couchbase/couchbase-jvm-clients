/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsExtension;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions;

@Stability.Internal
public class TransactionsSupportedExtensionsUtil {
    public static final CoreTransactionsSupportedExtensions SUPPORTED = CoreTransactionsSupportedExtensions.from(
        /* @since 3.3.0 */
        CoreTransactionsExtension.EXT_TRANSACTION_ID,
        CoreTransactionsExtension.EXT_DEFERRED_COMMIT,
        CoreTransactionsExtension.EXT_TIME_OPT_UNSTAGING,
        CoreTransactionsExtension.EXT_BINARY_METADATA,
        CoreTransactionsExtension.EXT_CUSTOM_METADATA_COLLECTION,
        CoreTransactionsExtension.EXT_QUERY,
        CoreTransactionsExtension.EXT_STORE_DURABILITY,
        CoreTransactionsExtension.BF_CBD_3838,
        CoreTransactionsExtension.BF_CBD_3787,
        CoreTransactionsExtension.BF_CBD_3705,
        CoreTransactionsExtension.BF_CBD_3794,
        CoreTransactionsExtension.EXT_REMOVE_COMPLETED,
        CoreTransactionsExtension.EXT_ALL_KV_COMBINATIONS,
        CoreTransactionsExtension.EXT_UNKNOWN_ATR_STATES,
        CoreTransactionsExtension.BF_CBD_3791,
        CoreTransactionsExtension.EXT_THREAD_SAFE,
        CoreTransactionsExtension.EXT_SERIALIZATION,
        CoreTransactionsExtension.EXT_SDK_INTEGRATION,
        CoreTransactionsExtension.EXT_MOBILE_INTEROP,

        /* @since 3.3.4 */
        CoreTransactionsExtension.EXT_REPLACE_BODY_WITH_XATTR,

        /* @since 3.4.0 */
        CoreTransactionsExtension.EXT_INSERT_EXISTING,
        CoreTransactionsExtension.EXT_OBSERVABILITY,

        /* @since 3.4.1 */
        CoreTransactionsExtension.EXT_QUERY_CONTEXT,

        /* @since 3.7.0 */
        CoreTransactionsExtension.EXT_BINARY_SUPPORT,

        /* @since 3.7.3 */
        CoreTransactionsExtension.EXT_PARALLEL_UNSTAGING,

        /* @since 3.7.4 */
        CoreTransactionsExtension.EXT_REPLICA_FROM_PREFERRED_GROUP,

        /* @since 3.8.0 */
        CoreTransactionsExtension.EXT_GET_MULTI
    );
}
