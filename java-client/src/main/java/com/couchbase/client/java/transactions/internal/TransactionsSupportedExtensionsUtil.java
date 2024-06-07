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
    public static final CoreTransactionsSupportedExtensions SUPPORTED = new CoreTransactionsSupportedExtensions();

    static {
        /* @since 3.3.0 */
        SUPPORTED.add(CoreTransactionsExtension.EXT_TRANSACTION_ID);
        SUPPORTED.add(CoreTransactionsExtension.EXT_DEFERRED_COMMIT);
        SUPPORTED.add(CoreTransactionsExtension.EXT_TIME_OPT_UNSTAGING);
        SUPPORTED.add(CoreTransactionsExtension.EXT_BINARY_METADATA);
        SUPPORTED.add(CoreTransactionsExtension.EXT_CUSTOM_METADATA_COLLECTION);
        SUPPORTED.add(CoreTransactionsExtension.EXT_QUERY);
        SUPPORTED.add(CoreTransactionsExtension.EXT_STORE_DURABILITY);
        SUPPORTED.add(CoreTransactionsExtension.BF_CBD_3838);
        SUPPORTED.add(CoreTransactionsExtension.BF_CBD_3787);
        SUPPORTED.add(CoreTransactionsExtension.BF_CBD_3705);
        SUPPORTED.add(CoreTransactionsExtension.BF_CBD_3794);
        SUPPORTED.add(CoreTransactionsExtension.EXT_REMOVE_COMPLETED);
        SUPPORTED.add(CoreTransactionsExtension.EXT_ALL_KV_COMBINATIONS);
        SUPPORTED.add(CoreTransactionsExtension.EXT_UNKNOWN_ATR_STATES);
        SUPPORTED.add(CoreTransactionsExtension.BF_CBD_3791);
        SUPPORTED.add(CoreTransactionsExtension.EXT_THREAD_SAFE);
        SUPPORTED.add(CoreTransactionsExtension.EXT_SERIALIZATION);
        SUPPORTED.add(CoreTransactionsExtension.EXT_SDK_INTEGRATION);
        SUPPORTED.add(CoreTransactionsExtension.EXT_MOBILE_INTEROP);

        /* @since 3.3.4 */
        SUPPORTED.add(CoreTransactionsExtension.EXT_REPLACE_BODY_WITH_XATTR);

        /* @since 3.4.0 */
        SUPPORTED.add(CoreTransactionsExtension.EXT_INSERT_EXISTING);
        SUPPORTED.add(CoreTransactionsExtension.EXT_OBSERVABILITY);

        /* @since 3.4.1 */
        SUPPORTED.add(CoreTransactionsExtension.EXT_QUERY_CONTEXT);

        /* @since 3.7.0 */
        SUPPORTED.add(CoreTransactionsExtension.EXT_BINARY_SUPPORT);
    }
}
