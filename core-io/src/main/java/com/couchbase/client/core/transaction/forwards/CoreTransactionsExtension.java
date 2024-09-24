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

import java.util.Objects;

/**
 * All known protocol extensions.
 * <p>
 * The implementations that use this core implementation may support different subsets of this, which is represented
 * with {@link CoreTransactionsSupportedExtensions}.
 */
@Stability.Internal
public enum CoreTransactionsExtension {
    EXT_TRANSACTION_ID("TI"),
    EXT_DEFERRED_COMMIT("DC"),
    EXT_TIME_OPT_UNSTAGING("TO"),
    EXT_BINARY_METADATA("BM"),
    EXT_CUSTOM_METADATA_COLLECTION("CM"),
    EXT_QUERY("QU"),
    EXT_STORE_DURABILITY("SD"),
    BF_CBD_3838("BF3838"),
    BF_CBD_3787("BF3787"),
    BF_CBD_3705("BF3705"),
    BF_CBD_3794("BF3794"),
    EXT_REMOVE_COMPLETED("RC"),
    EXT_ALL_KV_COMBINATIONS("CO"),
    EXT_UNKNOWN_ATR_STATES("UA"),
    BF_CBD_3791("BF3791"),
    EXT_SINGLE_QUERY("SQ"),
    EXT_THREAD_SAFE("TS"),
    EXT_SERIALIZATION("SZ"),
    EXT_SDK_INTEGRATION("SI"),
    EXT_MOBILE_INTEROP("MI"),
    EXT_REPLACE_BODY_WITH_XATTR("RX"),
    EXT_INSERT_EXISTING("IX"),
    EXT_OBSERVABILITY("OB"),
    EXT_QUERY_CONTEXT("QC"),
    EXT_BINARY_SUPPORT("BS"),
    EXT_PARALLEL_UNSTAGING("PU"),
    EXT_REPLICA_FROM_PREFERRED_GROUP("RP"),
    ;

    private final String value;

    CoreTransactionsExtension(String value) {
        this.value = Objects.requireNonNull(value);
    }

    public String value() {
        return value;
    }
}
