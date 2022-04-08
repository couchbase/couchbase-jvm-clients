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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * All protocol extensions known about by this implementation.
 */
@Stability.Internal
public enum Extension {
    /**
     * @since 3.3.0
     */
    EXT_TRANSACTION_ID("TI"),

    /**
     * @since 3.3.0
     */
    EXT_DEFERRED_COMMIT("DC"),

    /**
     * @since 3.3.0
     */
    EXT_TIME_OPT_UNSTAGING("TO"),

    /**
     * @since 3.3.0
     */
    EXT_BINARY_METADATA("BM"),

    /**
     * @since 3.3.0
     */
    EXT_CUSTOM_METADATA_COLLECTION("CM"),

    /**
     * @since 3.3.0
     */
    EXT_QUERY("QU"),

    /**
     * @since 3.3.0
     */
    EXT_STORE_DURABILITY("SD"),

    /**
     * @since 3.3.0
     */
    BF_CBD_3838("BF3838"),

    /**
     * @since 3.3.0
     */
    BF_CBD_3787("BF3787"),

    /**
     * @since 3.3.0
     */
    BF_CBD_3705("BF3705"),

    /**
     * @since 3.3.0
     */
    BF_CBD_3794("BF3794"),

    /**
     * @since 3.3.0
     */
    EXT_REMOVE_COMPLETED("RC"),

    /**
     * @since 3.3.0
     */
    EXT_ALL_KV_COMBINATIONS("CO"),

    /**
     * @since 3.3.0
     */
    EXT_UNKNOWN_ATR_STATES("UA"),

    /**
     * @since 3.3.0
     */
    BF_CBD_3791("BF3791"),

    /**
     * @since 3.3.0
     */
     EXT_SINGLE_QUERY("SQ"),

    /**
     * @since 3.3.0
     */
    EXT_THREAD_SAFE("TS"),

    /**
     * @since 3.3.0
     */
    EXT_SERIALIZATION("SZ"),

    /**
     * @since 3.3.0
     */
    EXT_SDK_INTEGRATION("SI"),

    /**
     * @since 3.3.0
     */
    EXT_MOBILE_INTEROP("MI"),
    ;

    private String value;

    Extension(String value) {
        this.value = Objects.requireNonNull(value);
    }

    public String value() {
        return value;
    }

    /**
     * All protocol extensions supported by this implementation.
     */
    public static final Set<Extension> SUPPORTED = Collections.unmodifiableSet(EnumSet.allOf(Extension.class));
}
