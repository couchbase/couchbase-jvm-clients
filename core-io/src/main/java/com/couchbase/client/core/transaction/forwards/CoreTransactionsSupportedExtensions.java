/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.forwards;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.CbCollections;

import java.util.Arrays;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.newEnumSet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

/**
 * Defines what is support by this implementation (extensions and protocol version).
 */
@Stability.Internal
public class CoreTransactionsSupportedExtensions {
    public static final CoreTransactionsSupportedExtensions NONE = from();
    public static final CoreTransactionsSupportedExtensions ALL = from(CoreTransactionsExtension.values());
    private static final Set<CoreTransactionsExtension> ALL_2_1 = CbCollections.setOf(CoreTransactionsExtension.EXT_TRANSACTION_ID,
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
            CoreTransactionsExtension.EXT_SINGLE_QUERY);

    public final Set<CoreTransactionsExtension> extensions;
    private final Set<String> extensionIds;

    private final int protocolMajor = 2;
    private final int protocolMinor;

    private CoreTransactionsSupportedExtensions(Iterable<CoreTransactionsExtension> extensions) {
        this.extensions = unmodifiableSet(newEnumSet(CoreTransactionsExtension.class, extensions));
        this.extensionIds = unmodifiableSet(
                this.extensions.stream()
                        .map(CoreTransactionsExtension::value)
                        .collect(toSet())
        );
        this.protocolMinor = this.extensions.equals(ALL_2_1) ? 1 : 0;
    }

    public int protocolMajor() {
        return protocolMajor;
    }

    public int protocolMinor() {
        return protocolMinor;
    }

    public static CoreTransactionsSupportedExtensions from(CoreTransactionsExtension... extensions) {
        return new CoreTransactionsSupportedExtensions(Arrays.asList(extensions));
    }

    public boolean has(String extensionId) {
        return extensionIds.contains(extensionId);
    }

    @Override
    public String toString() {
        return "Supported {extensions=" + extensions + ", protocol=" + protocolMajor + "." + protocolMinor + "}";
    }
}
