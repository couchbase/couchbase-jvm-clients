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

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the basic functionality of analytics queries in an end-to-end fashion.
 */
@IgnoreWhen( missesCapabilities = { Capabilities.SEARCH })
class SearchIntegrationTest extends JavaIntegrationTest {

    private static final String INDEX_DEF = "{\n" +
      "  \"type\": \"fulltext-index\",\n" +
      "  \"name\": \"$NAME$\",\n" +
      "  \"sourceType\": \"couchbase\",\n" +
      "  \"sourceName\": \"$BUCKET$\",\n" +
      "  \"planParams\": {\n" +
      "    \"maxPartitionsPerPIndex\": 171\n" +
      "  },\n" +
      "  \"params\": {\n" +
      "    \"doc_config\": {\n" +
      "      \"docid_prefix_delim\": \"\",\n" +
      "      \"docid_regexp\": \"\",\n" +
      "      \"mode\": \"type_field\",\n" +
      "      \"type_field\": \"type\"\n" +
      "    },\n" +
      "    \"mapping\": {\n" +
      "      \"analysis\": {},\n" +
      "      \"default_analyzer\": \"standard\",\n" +
      "      \"default_datetime_parser\": \"dateTimeOptional\",\n" +
      "      \"default_field\": \"_all\",\n" +
      "      \"default_mapping\": {\n" +
      "        \"dynamic\": true,\n" +
      "        \"enabled\": true\n" +
      "      },\n" +
      "      \"default_type\": \"_default\",\n" +
      "      \"docvalues_dynamic\": true,\n" +
      "      \"index_dynamic\": true,\n" +
      "      \"store_dynamic\": false,\n" +
      "      \"type_field\": \"_type\"\n" +
      "    },\n" +
      "    \"store\": {\n" +
      "      \"indexType\": \"scorch\",\n" +
      "      \"kvStoreName\": \"\"\n" +
      "    }\n" +
      "  },\n" +
      "  \"sourceParams\": {}\n" +
      "}";

    private static Cluster cluster;
    private static ClusterEnvironment environment;
    private static Collection collection;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        String indexDef = INDEX_DEF
          .replace("$BUCKET$", config().bucketname())
          .replace("$NAME$", "idx-" + config().bucketname());

        // cluster.searchIndexes().upsertIndex(SearchIndex.fromJson(indexDef.getBytes(StandardCharsets.UTF_8)));
    }

    @AfterAll
    static void tearDown() {
        cluster.shutdown();
        environment.shutdown();
    }

    @Test
    void simpleSearch() throws Exception {
        String docId = UUID.randomUUID().toString();
        collection.insert(docId, "{\"name\": \"michael\"}");

        for (int i = 0; i < 20; i++) {
            try {
                SearchResult result = cluster.searchQuery(new SearchQuery(
                  "idx-" + config().bucketname(),
                  SearchQuery.queryString("michael")
                ));

                if (result.rows().size() >= 1) {
                    assertEquals(docId, result.rows().get(0).id());
                }
                return;
            } catch (Exception ex) {
                // TODO: we need to figure out a better way to make sure an index
                // TODO: is properly created to avoid race conditions in unit tests
                // System.err.println(ex);
                // ex.printStackTrace();
                Thread.sleep(1000);
            }
        }

        // index didn't come up after 10 seconds :/
        fail();
    }

}
