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

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.transactions.config.SingleQueryTransactionOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Transactions are heavily tested by FIT, these are some basic sanity tests for single query transactions.
 */
@IgnoreWhen(clusterTypes = {ClusterType.MOCKED},
  // Using COLLECTIONS as a proxy for 7.0+, which is when query added support for transactions
  missesCapabilities = {Capabilities.CREATE_AS_DELETED, Capabilities.COLLECTIONS, Capabilities.QUERY},
  isProtostellarWillWorkLater = true
)
public class TransactionsSingleQueryIntegrationTest extends JavaIntegrationTest {

    static private Cluster cluster;

    @BeforeAll
    static void beforeAll() {
        cluster = createCluster();
    }

    @AfterAll
    static void afterAll() {
        cluster.disconnect();
    }

    @Test
    void basic() {
        QueryResult qr = cluster.query("SELECT 'Hello World' AS Greeting", QueryOptions.queryOptions().asTransaction());
        assertEquals(1, qr.rowsAsObject().size());
    }

    @Test
    void options() {
        QueryResult qr = cluster.query("SELECT 'Hello World' AS Greeting",
                QueryOptions.queryOptions().asTransaction(SingleQueryTransactionOptions.singleQueryTransactionOptions()
                        .durabilityLevel(DurabilityLevel.NONE)));
        assertEquals(1, qr.rowsAsObject().size());
    }

}
