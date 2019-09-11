/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.query;

import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic smoke test for query options.
 */
class QueryOptionsSmokeTest {

    @Test
    void testScanConsistency() {
        QueryOptions options = QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        QueryOptions.Built opts = options.build();
        JsonObject queryJson = JsonObject.create();
        opts.injectParams(queryJson);
        assertEquals(queryJson.get("scan_consistency"), "request_plus");
    }

    @Test
    void testProfile() {
        QueryOptions options = QueryOptions.queryOptions().profile(QueryProfile.TIMINGS);
        QueryOptions.Built opts = options.build();
        JsonObject queryJson = JsonObject.create();
        opts.injectParams(queryJson);
        assertEquals(queryJson.get("profile"), "timings");
    }

    @Test
    void testClientContextId() {
        String randomId = UUID.randomUUID().toString();
        QueryOptions options = QueryOptions.queryOptions().clientContextId(randomId);
        QueryOptions.Built opts = options.build();
        JsonObject queryJson = JsonObject.create();
        opts.injectParams(queryJson);
        assertEquals(queryJson.get("client_context_id"), randomId);
    }

}
