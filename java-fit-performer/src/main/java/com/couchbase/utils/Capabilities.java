/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.utils;

import com.couchbase.client.protocol.sdk.Caps;

import java.util.List;

public class Capabilities {
    public static List<Caps> sdkImplementationCaps() {
        return List.of(
            Caps.SDK_PRESERVE_EXPIRY,
            Caps.SDK_BUCKET_MANAGEMENT,
            Caps.SDK_COLLECTION_MANAGEMENT,
            Caps.SDK_MANAGEMENT_HISTORY_RETENTION,
            Caps.SDK_KV_RANGE_SCAN,
            Caps.SDK_QUERY_INDEX_MANAGEMENT,
            Caps.SDK_COLLECTION_QUERY_INDEX_MANAGEMENT,
            Caps.SDK_SEARCH,
            Caps.SDK_SEARCH_INDEX_MANAGEMENT,
            Caps.SDK_SCOPE_SEARCH,
            Caps.SDK_SCOPE_SEARCH_INDEX_MANAGEMENT,
            Caps.SDK_EVENTING_FUNCTION_MANAGER, //TODO: fully implement eventing function manager - Java performer only supports getFunction currently
            Caps.SDK_QUERY,
            Caps.SDK_QUERY_READ_FROM_REPLICA,
            Caps.SDK_LOOKUP_IN,
            Caps.SDK_LOOKUP_IN_REPLICAS,
            Caps.SDK_OBSERVABILITY_RFC_REV_24,
            Caps.SDK_KV,
            Caps.SDK_DOCUMENT_NOT_LOCKED,
            Caps.SDK_CIRCUIT_BREAKERS,
            Caps.SDK_VECTOR_SEARCH,
            Caps.SDK_VECTOR_SEARCH_BASE64,
            Caps.SDK_ZONE_AWARE_READ_FROM_REPLICA,
            Caps.SDK_OBSERVABILITY_CLUSTER_LABELS,
            Caps.SDK_APP_TELEMETRY,
            Caps.SDK_BUCKET_SETTINGS_NUM_VBUCKETS,
            Caps.SDK_PREFILTER_VECTOR_SEARCH,
            Caps.SUPPORTS_AUTHENTICATOR,
            Caps.SDK_SET_AUTHENTICATOR,
            Caps.SDK_JWT,
            Caps.SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS,
            Caps.SDK_GET_OR_NULL,
            Caps.SDK_QUERY_2120,
            Caps.SDK_SEARCH_SCORE_FUSION
        );
    }
}
