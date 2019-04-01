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

import com.couchbase.client.core.error.AnalyticsServiceException;
import com.couchbase.client.java.analytics.AnalyticsMeta;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.analytics.AnalyticsStatus;
import com.couchbase.client.java.env.ClusterEnvironment;
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
    private static ClusterEnvironment environment;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        Bucket bucket = cluster.bucket(config().bucketname());
    }

    @AfterAll
    static void tearDown() {
        cluster.shutdown();
        environment.shutdown();
    }

    @Test
    void performsDataverseQuery() {
        AnalyticsResult result = cluster.analyticsQuery("SELECT DataverseName FROM Metadata.`Dataverse`");

        List<JsonObject> rows = result.allRowsAs(JsonObject.class);
        assertFalse(rows.isEmpty());
        for (JsonObject row : rows) {
            assertNotNull(row.get("DataverseName"));
        }

        AnalyticsMeta meta = result.meta();
        assertFalse(meta.clientContextId().isEmpty());
        assertTrue(meta.signatureAs(JsonObject.class).isPresent());
        assertFalse(meta.requestId().isEmpty());
        assertEquals(AnalyticsStatus.SUCCESS, meta.status());

        assertFalse(meta.metrics().elapsedTime().isEmpty());
        assertFalse(meta.metrics().executionTime().isEmpty());
        assertEquals(rows.size(), meta.metrics().resultCount());
        assertEquals(rows.size(), meta.metrics().processedObjects());
        assertTrue(meta.metrics().resultSize() > 0);
        assertFalse(meta.warnings().isPresent());
        assertFalse(meta.errors().isPresent());

        assertEquals(0, meta.metrics().errorCount());
        assertEquals(0, meta.metrics().warningCount());
        assertEquals(0, meta.metrics().mutationCount());
    }

    @Test
    void failsOnError() {
        assertThrows(AnalyticsServiceException.class, () -> cluster.analyticsQuery("SELECT 1="));
    }

    @Test
    void canSetCustomContextId() {
        String contextId = "mycontextid";
        AnalyticsResult result = cluster.analyticsQuery(
                "SELECT DataverseName FROM Metadata.`Dataverse`",
                analyticsOptions().clientContextId(contextId)
        );
        assertEquals(result.meta().clientContextId(), contextId);
    }

}
