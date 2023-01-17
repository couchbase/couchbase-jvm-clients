/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;


public class KeyValueReactiveCollectionIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection collection;
    private static ReactiveCollection reactiveCollection;
    private static int numDocs = 3;
    private static Flux<String> docIds;

    @BeforeAll
    static void beforeAll() {
        cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
        reactiveCollection = collection.reactive();
    }

    @AfterAll
    static void afterAll() {
        cluster.disconnect();
    }

    @BeforeEach
    void beforeEach(){
        docIds = getDocsIds();
    }

    /**
     * Generate document keys flux
     */
    private static Flux<String> getDocsIds() {
        int seed  = (int) System.currentTimeMillis();
        return Flux.range(seed, numDocs).map(k -> "key" + k);
    }

    @Test
    void reactiveInsert() {
        // Insert docs and create iterator for verifier
        Iterator<MutationResult> mutationResultsIt = docIds
          .concatMap(key -> reactiveCollection.insert(key, JsonObject.create().put("key", key)))
          .collectList()
          .block()
          .iterator();

        // Check get results have equal cas to insert cas, and key value
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .thenConsumeWhile(res -> res.cas() == mutationResultsIt.next().cas() && res.contentAsObject().get("key").toString().matches("key.*"))
          .verifyComplete();

        // Insert on same ids fail
        StepVerifier
          .create(docIds.concatMap(key -> reactiveCollection.insert(key, "foo")))
          .expectError(DocumentExistsException.class)
          .verify();
    }

    @IgnoreWhen(isProtostellarWillWorkLater = true)
    @Test
    void reactiveUpsert(){
        // Upsert docs
        List<MutationResult> mutationResults = docIds
          .concatMap(key -> reactiveCollection.upsert(key, JsonObject.create().put("key", key)))
          .collectList()
          .block();

        // Check get results equal to upsert results
        Iterator<MutationResult> mutationResultsIt = mutationResults.iterator();
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .thenConsumeWhile(res -> res.cas() == mutationResultsIt.next().cas() && res.contentAsObject().get("key").toString().matches("key.*"))
          .verifyComplete();

        // Upsert with same doc ids, check overwritten
        Iterator<MutationResult> mutationResultsIt2 = mutationResults.iterator();
        docIds.concatMap(key -> reactiveCollection.upsert(key, "foo")).collectList().block();
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .thenConsumeWhile(res -> res.cas() != mutationResultsIt2.next().cas() && res.contentAs(String.class).equals("foo"))
          .verifyComplete();
    }

    @IgnoreWhen(isProtostellarWillWorkLater = true)
    @Test
    void reactiveReplace(){
        // Insert docs
        docIds
          .concatMap(key -> reactiveCollection.insert(key, JsonObject.create().put("key", key)))
          .collectList()
          .block();

        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .thenConsumeWhile(res -> res.contentAsObject().get("key").toString().matches("key.*"))
          .verifyComplete();

        // Replace docs with new data
        Iterator<MutationResult> replaceResultIt = docIds
          .concatMap(doc -> reactiveCollection.replace(doc, JsonObject.create().put("new", "data")))
          .collectList()
          .block()
          .iterator();

        // Check GetResults equal to new data
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .thenConsumeWhile(res -> res.cas() == replaceResultIt.next().cas() && res.contentAsObject().get("new").equals("data"))
          .verifyComplete();

        // Check other docs
        StepVerifier
          .create(getDocsIds().concatMap(reactiveCollection::get))
          .expectError(DocumentNotFoundException.class)
          .verify();
    }

    @Test
    void reactiveRemove(){
        // Insert docs
        docIds
          .concatMap(key -> reactiveCollection.insert(key, JsonObject.create().put("key", key)))
          .collectList()
          .block();

        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .thenConsumeWhile(res -> res.contentAsObject().get("key").toString().matches("key.*"))
          .verifyComplete();

        // Remove docs
        docIds
          .concatMap(reactiveCollection::remove)
          .collectList()
          .block();

        // Check Get attempt reports exception
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .expectError(DocumentNotFoundException.class)
          .verify();

        StepVerifier
          .create(docIds.concatMap(reactiveCollection::remove))
          .expectError(DocumentNotFoundException.class)
          .verify();
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)
    void reactiveExists(){
        // Insert docs
        docIds
          .concatMap(key -> reactiveCollection.insert(key, JsonObject.create().put("key", key)))
          .collectList()
          .block();
        
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .thenConsumeWhile(res -> res.contentAsObject().get("key").toString().matches("key.*"))
          .verifyComplete();

        // Check inserted docs exist
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::exists))
          .thenConsumeWhile(ExistsResult::exists)
          .verifyComplete();

        // Remove docs
        docIds
          .concatMap(reactiveCollection::remove)
          .collectList()
          .block();

        // Check results don't exist
        StepVerifier
          .create(docIds.concatMap(reactiveCollection::exists))
          .thenConsumeWhile(res -> !res.exists())
          .verifyComplete();

        StepVerifier
          .create(docIds.concatMap(reactiveCollection::get))
          .expectError(DocumentNotFoundException.class)
          .verify();
     }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)
    void reactiveTouch(){
        StepVerifier
          .create(docIds.concatMap(key -> reactiveCollection.touch(key, Duration.ofSeconds(1))))
          .expectError(DocumentNotFoundException.class)
          .verify();

        // Insert docs with expiry of 10s
        InsertOptions firstOpts = InsertOptions.insertOptions().expiry(Duration.ofSeconds(10));
        List<MutationResult> insertResults = docIds
          .concatMap(key -> reactiveCollection.insert(key, JsonObject.create().put("key", key), firstOpts))
          .collectList()
          .block();

        // Touch docs and update the expiry to 1s
        List<MutationResult> touchResults = docIds
          .concatMap(key -> reactiveCollection.touch(key, Duration.ofSeconds(1)))
          .collectList()
          .block();

        // Assert cas of insert and touch results differ
        StepVerifier
          .create(Flux.range(0, numDocs))
          .thenConsumeWhile(i -> insertResults.get(i).cas() != touchResults.get(i).cas())
          .verifyComplete();

        List<GetResult> getResults = docIds
          .concatMap(reactiveCollection::get)
          .collectList()
          .block();

        StepVerifier
          .create(Flux.range(0, numDocs))
          .thenConsumeWhile(i -> getResults.get(i).cas() == touchResults.get(i).cas())
          .verifyComplete();
    }

}
