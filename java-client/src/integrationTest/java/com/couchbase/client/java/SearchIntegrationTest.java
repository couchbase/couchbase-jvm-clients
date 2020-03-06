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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static com.couchbase.client.java.search.SearchOptions.searchOptions;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the basic functionality of analytics queries in an end-to-end fashion.
 */
@IgnoreWhen( missesCapabilities = { Capabilities.SEARCH })
@Disabled("to be fixed in JCBC-1557")
class SearchIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection collection;

    @BeforeAll
    static void setup() {
        cluster = Cluster.connect(seedNodes(), clusterOptions());
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        bucket.waitUntilReady(Duration.ofSeconds(5));
        cluster.searchIndexes().upsertIndex(new SearchIndex("idx-" + config().bucketname(), config().bucketname()));
    }

    @AfterAll
    static void tearDown() {
        cluster.searchIndexes().dropIndex("idx-" + config().bucketname());
        cluster.disconnect();
    }

    @Test
    void simpleSearch() throws Exception {
       String docId = UUID.randomUUID().toString();
       MutationResult insertResult = collection.insert(docId, "{\"name\": \"michael\"}");

       int maxTries = 20;
       for (int i = 0; i < maxTries; i++) {
        try {
            SearchResult result = cluster.searchQuery(
              "idx-" + config().bucketname(),
              SearchQuery.queryString("michael"),
              searchOptions().consistentWith(MutationState.from(insertResult.mutationToken().get()))
            );

            assertFalse(result.rows().isEmpty());
            boolean foundInserted = false;
            for (SearchRow row : result.rows()) {
                if (row.id().equals(docId)) {
                    foundInserted = true;
                }
            }
            assertEquals(1, result.metaData().metrics().totalRows());
            assertTrue(foundInserted);
            break;
        } catch (CouchbaseException ex) {
            // this is a pretty dirty hack to avoid a race where we don't know if the index
            // is ready yet
            if (ex.getMessage().contains("no planPIndexes for indexName")
              || ex.getMessage().contains("pindex_consistency mismatched partition")) {
                Thread.sleep(500);
                continue;
            }
            throw ex;
        }
       }
    }

}
