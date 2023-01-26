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

import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Simple verification of the {@link DiagnosticsResult}.
 */
@IgnoreWhen(isProtostellarWillWorkLater = true)
public class DiagnosticsReportTest extends JavaIntegrationTest {

    private static Cluster cluster;

    @BeforeAll
    static void setup() {
        cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());
        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    }

    @AfterAll
    static void tearDown() {
        cluster.diagnostics();
    }

    @Test
    void containsKeyValueEndpointsWithoutOp() {
        DiagnosticsResult response = cluster.diagnostics();

        String json = response.exportToJson();
        assertFalse(json.isEmpty());

        assertFalse(response.endpoints().get(ServiceType.KV).isEmpty());
    }

}
