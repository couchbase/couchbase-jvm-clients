/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.java.analytics.AnalyticsMetaData;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.analytics.AnalyticsStatus;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the basic functionality of analytics queries in an end-to-end fashion.
 */
@IgnoreWhen( missesCapabilities = { Capabilities.ANALYTICS })
class AnalyticsIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;

    @BeforeAll
    static void setup() {
        cluster = Cluster.connect(seedNodes(), clusterOptions());
        cluster.bucket(config().bucketname());
    }

    @AfterAll
    static void tearDown() {
        cluster.disconnect();
    }

    @Test
    void performsDataverseQuery() {
        AnalyticsResult result = cluster.analyticsQuery("SELECT DataverseName FROM Metadata.`Dataverse`");

        List<JsonObject> rows = result.rowsAs(JsonObject.class);
        assertFalse(rows.isEmpty());
        for (JsonObject row : rows) {
            assertNotNull(row.get("DataverseName"));
        }

        AnalyticsMetaData meta = result.metaData();
        assertFalse(meta.clientContextId().isEmpty());
        assertTrue(meta.signature().isPresent());
        assertFalse(meta.requestId().isEmpty());
        assertEquals(AnalyticsStatus.SUCCESS, meta.status());

        assertFalse(meta.metrics().elapsedTime().isZero());
        assertFalse(meta.metrics().executionTime().isZero());
        assertEquals(rows.size(), meta.metrics().resultCount());
        assertEquals(rows.size(), meta.metrics().processedObjects());
        assertTrue(meta.metrics().resultSize() > 0);
        assertTrue(meta.warnings().isEmpty());

        assertEquals(0, meta.metrics().errorCount());
    }

    @Test
    void failsOnError() {
        assertThrows(ParsingFailureException.class, () -> cluster.analyticsQuery("SELECT 1="));
    }

    @Test
    void canSetCustomContextId() {
        String contextId = "mycontextid";
        AnalyticsResult result = cluster.analyticsQuery(
                "SELECT DataverseName FROM Metadata.`Dataverse`",
                analyticsOptions().clientContextId(contextId)
        );
        assertEquals(result.metaData().clientContextId(), contextId);
    }

}
