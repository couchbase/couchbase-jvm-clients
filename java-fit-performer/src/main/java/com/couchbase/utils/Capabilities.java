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

import java.util.ArrayList;
import java.util.List;

public class Capabilities {
    public static List<Caps> sdkImplementationCaps() {
        var out = new ArrayList<Caps>();

        // [if:3.1.5]
        out.add(Caps.SDK_PRESERVE_EXPIRY);
        // [end]

      // [if:3.4.12]
      out.add(Caps.SDK_BUCKET_MANAGEMENT);
      out.add(Caps.SDK_COLLECTION_MANAGEMENT);
      out.add(Caps.SDK_MANAGEMENT_HISTORY_RETENTION);
      // [end]


        // [if:3.4.1]
        out.add(Caps.SDK_KV_RANGE_SCAN);
        // [end]

        // We're not performance testing query index management, so not worth the complexity
        // of keeping them compiling.
        // [if:3.4.3]
        out.add(Caps.SDK_QUERY_INDEX_MANAGEMENT);
        out.add(Caps.SDK_COLLECTION_QUERY_INDEX_MANAGEMENT);
        // [end]

        // Actually the SDK has had various forms of FTS since 3.0.0, but we're not performance testing it currently
        // so it's not worth trying to keep the various minor API additions compiling.
        // [if:3.4.5]
        out.add(Caps.SDK_SEARCH);
        out.add(Caps.SDK_SEARCH_INDEX_MANAGEMENT);
        out.add(Caps.SDK_SCOPE_SEARCH);
        out.add(Caps.SDK_SCOPE_SEARCH_INDEX_MANAGEMENT);
        // [end]

        //TODO: fully implement eventing function manager - Java performer only supports getFunction currently
        out.add(Caps.SDK_EVENTING_FUNCTION_MANAGER);

        out.add(Caps.SDK_QUERY);
        out.add(Caps.SDK_QUERY_READ_FROM_REPLICA);
        out.add(Caps.SDK_LOOKUP_IN);
        out.add(Caps.SDK_LOOKUP_IN_REPLICAS);

        // We supported observability before this, but as we have to fix the version of tracing-opentelemetry module used by the performer, we hit
        // issues related to not having GrpcAwareRequestTracer.
        // [if:3.5.0]
        out.add(Caps.SDK_OBSERVABILITY_RFC_REV_24);
        // [end]

        out.add(Caps.SDK_KV);
        // [if:3.5.1]
        out.add(Caps.SDK_DOCUMENT_NOT_LOCKED);
        // This was added long before this release, but we are not performance testing it.
        out.add(Caps.SDK_CIRCUIT_BREAKERS);
        // [end]

        // [if:3.6.0]
        out.add(Caps.SDK_VECTOR_SEARCH);
        // [end]

        // [if:3.7.0]
        out.add(Caps.SDK_VECTOR_SEARCH_BASE64);
        // [end]

        return out;
    }
}
