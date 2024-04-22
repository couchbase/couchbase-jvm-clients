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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static com.couchbase.client.core.classic.query.ClassicCoreQueryOps.convertOptions;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Basic smoke test for query options.
 */
class QueryOptionsSmokeTest {

    @Test
    void testScanConsistency() {
        QueryOptions options = QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        ObjectNode queryJson = toObjectNode(options);
        assertEquals(queryJson.get("scan_consistency").textValue(), "request_plus");
    }

    @Test
    void testProfile() {
        QueryOptions options = QueryOptions.queryOptions().profile(QueryProfile.TIMINGS);
        ObjectNode queryJson = toObjectNode(options);
        assertEquals(queryJson.get("profile").textValue(), "timings");
    }

    @Test
    void testClientContextId() {
        String randomId = UUID.randomUUID().toString();
        QueryOptions options = QueryOptions.queryOptions().clientContextId(randomId);
        ObjectNode queryJson = toObjectNode(options);
        assertEquals(queryJson.get("client_context_id").textValue(), randomId);
    }

    @Test
    void canUseNestedJsonObjectsAsRawValue() {
        QueryOptions options = QueryOptions.queryOptions().raw("foo", JsonObject.create().put("bar", JsonObject.create().put("magicWord", "xyzzy")));
        ObjectNode queryJson = toObjectNode(options);
        assertEquals("xyzzy", queryJson.path("foo").path("bar").path("magicWord").textValue());
    }

    @Test
    void canUseNestedMapsAsRawValue() {
        QueryOptions options = QueryOptions.queryOptions().raw("foo", mapOf("bar", mapOf("magicWord", "xyzzy")));
        ObjectNode queryJson = toObjectNode(options);
        assertEquals("xyzzy", queryJson.path("foo").path("bar").path("magicWord").textValue());
    }

    @Test
    void canUseNestedJsonArraysAsRawValue() {
        QueryOptions options = QueryOptions.queryOptions().raw("foo", JsonArray.from("bar", JsonArray.from("xyzzy")));
        ObjectNode queryJson = toObjectNode(options);
        assertEquals("xyzzy", queryJson.path("foo").path(1).path(0).textValue());
    }

    @Test
    void canUseNestedListsAsRawValue() {
        QueryOptions options = QueryOptions.queryOptions().raw("foo", listOf("bar", listOf("xyzzy")));
        ObjectNode queryJson = toObjectNode(options);
        assertEquals("xyzzy", queryJson.path("foo").path(1).path(0).textValue());
    }

    @Test
    void rejectsPojoAsRawValue() {
        QueryOptions options = QueryOptions.queryOptions().raw("foo", new Object());
        assertThrows(InvalidArgumentException.class, () -> toObjectNode(options));
    }

    private static ObjectNode toObjectNode(QueryOptions options) {
        QueryOptions.Built opts = options.build();
        return convertOptions(opts);
    }
}
