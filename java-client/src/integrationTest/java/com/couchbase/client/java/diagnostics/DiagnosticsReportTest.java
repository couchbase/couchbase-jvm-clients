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
package com.couchbase.client.java.diagnostics;

import com.couchbase.client.core.diag.DiagnosticsResult;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simple verification of the {@link DiagnosticsResult}.
 *
 * @since 1.5.0
 */
public class DiagnosticsReportTest extends JavaIntegrationTest {

    private static Cluster cluster;

    @BeforeAll
    static void setup() {
        cluster = Cluster.connect(connectionString(), clusterOptions());
        cluster.bucket(config().bucketname());
    }

    @AfterAll
    static void tearDown() {
        cluster.diagnostics();
    }

    @Test
    void basicKVCheck() {
        DiagnosticsResult response = cluster.diagnostics();

        String json = response.exportToJson(true);
        assertFalse(json.isEmpty());

        // One for config().bucketname(), one for BUCKET_GLOBAL_SCOPE
        assertTrue(response.endpoints(ServiceType.KV).size() >= 2);
    }

    @IgnoreWhen( missesCapabilities = { Capabilities.QUERY }, clusterTypes = ClusterType.MOCKED)
    @Test
    void noInfoOnQueryUntilRunStatement() {
        assertTrue(cluster.diagnostics().endpoints(ServiceType.QUERY).isEmpty());

        cluster.query("select 'hello' as greeting");

        assertEquals(1, cluster.diagnostics().endpoints(ServiceType.QUERY).size());
    }

    @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
    @Test
    void shouldExposeHealthInfoAfterConnect() {
        cluster.query("select 'hello' as greeting");

        DiagnosticsResult response = cluster.diagnostics();

        String json = response.exportToJson(true);
        assertFalse(json.isEmpty());

        // One for config().bucketname(), one for BUCKET_GLOBAL_SCOPE
        assertTrue(response.endpoints(ServiceType.KV).size() >= 2);
    }
}
