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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

/**
 * Basic smoke test for query options
 */
public class QueryOptionsSmokeTest {

    @Test
    public void testCredentials() {
        QueryOptions options = QueryOptions.queryOptions().withCredentials("johndoe", "secret");
        QueryOptions.BuiltQueryOptions opts = options.build();
        JsonObject queryJson = JsonObject.create();
        opts.injectParams(queryJson);
        assertEquals(queryJson.getArray("creds").getObject(0).get("user"), "johndoe");
        assertEquals(queryJson.getArray("creds").getObject(0).get("pass"), "secret");
    }

    @Test
    public void testScanConsistency() {
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        QueryOptions.BuiltQueryOptions opts = options.build();
        JsonObject queryJson = JsonObject.create();
        opts.injectParams(queryJson);
        assertEquals(queryJson.get("scan_consistency"), "request_plus");
    }

    @Test
    public void testProfile() {
        QueryOptions options = QueryOptions.queryOptions().withProfile(QueryProfile.TIMINGS);
        QueryOptions.BuiltQueryOptions opts = options.build();
        JsonObject queryJson = JsonObject.create();
        opts.injectParams(queryJson);
        assertEquals(queryJson.get("profile"), "timings");
    }

    @Test
    public void testClientContextId() {
        String randomId = UUID.randomUUID().toString();
        QueryOptions options = QueryOptions.queryOptions().withClientContextId(randomId);
        QueryOptions.BuiltQueryOptions opts = options.build();
        JsonObject queryJson = JsonObject.create();
        opts.injectParams(queryJson);
        assertEquals(queryJson.get("client_context_id"), randomId);
    }
}