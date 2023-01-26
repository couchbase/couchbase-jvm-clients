/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.Test;

import java.util.UUID;

@IgnoreWhen(clusterTypes = {ClusterType.MOCKED},
  missesCapabilities = {Capabilities.CREATE_AS_DELETED}
)
public class TransactionsShutdownIntegrationTest extends JavaIntegrationTest {
    /**
     * Sanity test that a basic app still quits if cluster.disconnect() not called.
     * Essentially checking that the schedulers are using daemon threads.
     */
    @Test
    void withoutDisconnect() {
        createClusterAndRunTransaction();
    }

    /**
     * The app should certainly end if cluster.disconnect() is called.
     */
    @Test
    void withDisconnect() {
        Cluster cluster = createClusterAndRunTransaction();
        cluster.disconnect();
    }

    /**
     * Should be able to create multiple Clusters and resources should not be shared.
     */
    @Test
    void clustersDoNotShareResources() {
        Cluster cluster1 = createClusterAndRunTransaction();
        cluster1.disconnect();

        Cluster cluster2 = createClusterAndRunTransaction();
        cluster2.disconnect();
    }

    private Cluster createClusterAndRunTransaction() {
        Cluster cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());
        Collection collection = bucket.defaultCollection();

        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

        String docId = UUID.randomUUID().toString();
        JsonObject content = JsonObject.create().put("foo", "bar");

        cluster.transactions().run((ctx) -> {
            ctx.insert(collection, docId, content);
        });

        return cluster;
    }
}
