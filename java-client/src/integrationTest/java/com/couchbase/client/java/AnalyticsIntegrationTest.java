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

import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.analytics.AnalyticsMetaData;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.analytics.AnalyticsStatus;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the basic functionality of analytics queries in an end-to-end fashion.
 */
@IgnoreWhen(
  missesCapabilities = Capabilities.ANALYTICS,
  clusterTypes = ClusterType.CAVES,
  isProtostellar = true
)
class AnalyticsIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;

    @BeforeAll
    static void setup() {
        cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());

        cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
        waitForService(bucket, ServiceType.ANALYTICS);
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


    /**
     * We need to make sure that if a scope-level analytics query is performed on an older cluster a proper exception
     * is thrown and not an "unknown query error".
     */
    @Test
    @IgnoreWhen(hasCapabilities = Capabilities.COLLECTIONS)
    void failsIfScopeLevelIsNotAvailable() {
        Scope scope = cluster.bucket(config().bucketname()).scope("myscope");
        assertThrows(FeatureNotAvailableException.class, () ->  scope.analyticsQuery("select * from mycollection"));
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
