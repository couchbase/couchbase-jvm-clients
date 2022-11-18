/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.error.*;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = { ClusterType.MOCKED, ClusterType.CAVES , ClusterType.CAPELLA},
        missesCapabilities = {Capabilities.COLLECTIONS, Capabilities.EVENTING},
        clusterVersionIsBelow = "7.1.2") // MB-52649
public class EventingFunctionManagerIntegrationTest extends JavaIntegrationTest {
  private static Logger LOGGER = LoggerFactory.getLogger(EventingFunctionManagerIntegrationTest.class);

  private static Cluster cluster;
  private static Collection sourceCollection;
  private static Collection metaCollection;
  private static EventingFunctionManager functions;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    functions = cluster.eventingFunctions();
    cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

    Bucket bucket = cluster.bucket(config().bucketname());

    bucket.collections().createScope("eventing");
    ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), "eventing");
    bucket.collections().createCollection(CollectionSpec.create("source", "eventing"));
    bucket.collections().createCollection(CollectionSpec.create("meta", "eventing"));
    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), "source", "eventing");
    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), "meta", "eventing");

    sourceCollection = bucket.scope("eventing").collection("source");
    metaCollection = bucket.scope("eventing").collection("meta");

    waitUntilCondition(() -> bucket.collections().getAllScopes().stream().anyMatch(s -> {
        if (s.name().equals("eventing")) {
          boolean sourceFound = false;
          boolean metaFound = false;
          for (CollectionSpec c : s.collections()) {
            if (c.name().equals("source")) {
              sourceFound = true;
            } else if (c.name().equals("meta")) {
              metaFound = true;
            }
          }
          return sourceFound && metaFound;
        }
        return false;
      }));

    // On CI seeing CollectionNotFoundException intermittently.  Wait until we can successfully create a function
    // on the newly-created collection.
    waitUntilCanCreateFunction();
  }

  private static void waitUntilCanCreateFunction() {
    waitUntilCondition(() -> {
      String funcName = UUID.randomUUID().toString();
      EventingFunction function = EventingFunction.create(
              funcName,
              "function OnUpdate(doc, meta) {}",
              EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name()),
              EventingFunctionKeyspace.create(metaCollection.bucketName(), metaCollection.scopeName(), metaCollection.name())
      );
      try {
        functions.upsertFunction(function);
      }
      catch (RuntimeException err) {
        LOGGER.info("Waiting until can create function, got error {}", err.toString());
        return false;
      }
      return true;
    });
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void upsertGetAndDropFunction() {
    String funcName = UUID.randomUUID().toString();
    EventingFunction function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name()),
      EventingFunctionKeyspace.create(metaCollection.bucketName(), metaCollection.scopeName(), metaCollection.name())
    );
    functions.upsertFunction(function);

    EventingFunction read = functions.getFunction(funcName);
    assertEquals("function OnUpdate(doc, meta) {}", read.code());
    assertTrue(functions.getAllFunctions().stream().anyMatch(f -> f.name().equals(funcName)));

    functions.dropFunction(funcName);
    assertTrue(functions.getAllFunctions().stream().noneMatch(f -> f.name().equals(funcName)));
  }

  @Test
  void failsWithUnknownFunctionName() {
    String funcName = UUID.randomUUID().toString();

    assertThrows(EventingFunctionNotFoundException.class, () -> functions.getFunction(funcName));
    assertThrows(EventingFunctionNotFoundException.class, () -> functions.deployFunction(funcName));
    assertThrows(EventingFunctionNotFoundException.class, () -> functions.pauseFunction(funcName));

    // see MB-47840 on why those are not EventingFunctionNotFoundException
    try {
      assertThrows(EventingFunctionNotDeployedException.class, () -> functions.dropFunction(funcName));
      assertThrows(EventingFunctionNotDeployedException.class, () -> functions.undeployFunction(funcName));
      assertThrows(EventingFunctionNotDeployedException.class, () -> functions.resumeFunction(funcName));
    } catch (AssertionFailedError err) {
      if (err.getCause() instanceof EventingFunctionNotFoundException) {
        assertThrows(EventingFunctionNotFoundException.class, () -> functions.dropFunction(funcName));
        assertThrows(EventingFunctionNotFoundException.class, () -> functions.undeployFunction(funcName));
        assertThrows(EventingFunctionNotFoundException.class, () -> functions.resumeFunction(funcName));
      } else {
        throw err;
      }
    }
  }

  @Test
  void failsIfCodeIsInvalid() {
    String funcName = UUID.randomUUID().toString();
    EventingFunction function = EventingFunction.create(
      funcName,
      "someInvalidFunc",
      EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name()),
      EventingFunctionKeyspace.create(metaCollection.bucketName(), metaCollection.scopeName(), metaCollection.name())
    );
    assertThrows(EventingFunctionCompilationFailureException.class, () -> functions.upsertFunction(function));
  }

  @Test
  void failsIfCollectionNotFound() {
    String funcName = UUID.randomUUID().toString();
    EventingFunction function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name()),
      EventingFunctionKeyspace.create(metaCollection.bucketName(), metaCollection.scopeName(), "noIdeaWhatThisIs")
    );
    assertThrows(CollectionNotFoundException.class, () -> functions.upsertFunction(function));
  }

  @Test
  void failsIfSourceAndMetaSame() {
    String funcName = UUID.randomUUID().toString();
    EventingFunction function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name()),
      EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name())
    );
    assertThrows(EventingFunctionIdenticalKeyspaceException.class, () -> functions.upsertFunction(function));
  }

  @Test
  void failsIfBucketDoesNotExist() {
    String funcName = UUID.randomUUID().toString();
    EventingFunction function = EventingFunction.create(
            funcName,
            "function OnUpdate(doc, meta) {}",
            EventingFunctionKeyspace.create("foo", sourceCollection.scopeName(), sourceCollection.name()),
            EventingFunctionKeyspace.create("bar", sourceCollection.scopeName(), sourceCollection.name())
    );
    assertThrows(BucketNotFoundException.class, () -> functions.upsertFunction(function));
  }

  @Test
  void deploysAndUndeploysFunction() {
    String funcName = UUID.randomUUID().toString();
    EventingFunction function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name()),
      EventingFunctionKeyspace.create(metaCollection.bucketName(), metaCollection.scopeName(), metaCollection.name())
    );
    functions.upsertFunction(function);

    EventingFunction read = functions.getFunction(funcName);
    assertEquals(EventingFunctionDeploymentStatus.UNDEPLOYED, read.settings().deploymentStatus());

    assertThrows(EventingFunctionNotDeployedException.class, () -> functions.undeployFunction(funcName));
    functions.deployFunction(funcName);

    waitUntilCondition(() -> isState(funcName, EventingFunctionStatus.DEPLOYED));

    read = functions.getFunction(funcName);
    assertEquals(EventingFunctionDeploymentStatus.DEPLOYED, read.settings().deploymentStatus());

    functions.undeployFunction(funcName);

    waitUntilCondition(() -> isState(funcName, EventingFunctionStatus.UNDEPLOYED));

    read = functions.getFunction(funcName);
    assertEquals(EventingFunctionDeploymentStatus.UNDEPLOYED, read.settings().deploymentStatus());

    functions.dropFunction(funcName);
  }

  @Test
  void pausesAndResumesFunction() {
    String funcName = UUID.randomUUID().toString();
    EventingFunction function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.create(sourceCollection.bucketName(), sourceCollection.scopeName(), sourceCollection.name()),
      EventingFunctionKeyspace.create(metaCollection.bucketName(), metaCollection.scopeName(), metaCollection.name())
    );
    functions.upsertFunction(function);

    EventingFunction read = functions.getFunction(funcName);
    assertEquals(EventingFunctionProcessingStatus.PAUSED, read.settings().processingStatus());

    assertThrows(EventingFunctionNotBootstrappedException.class, () -> functions.pauseFunction(funcName));
    assertThrows(EventingFunctionNotDeployedException.class, () -> functions.resumeFunction(funcName));

    functions.deployFunction(funcName);

    waitUntilCondition(() -> isState(funcName, EventingFunctionStatus.DEPLOYED));

    read = functions.getFunction(funcName);
    assertEquals(EventingFunctionProcessingStatus.RUNNING, read.settings().processingStatus());

    functions.pauseFunction(funcName);

    waitUntilCondition(() -> isState(funcName, EventingFunctionStatus.PAUSED));

    read = functions.getFunction(funcName);
    assertEquals(EventingFunctionProcessingStatus.PAUSED, read.settings().processingStatus());

    functions.undeployFunction(funcName);

    waitUntilCondition(() -> isState(funcName, EventingFunctionStatus.UNDEPLOYED));
    functions.dropFunction(funcName);
  }

  private boolean isState(String funcName, EventingFunctionStatus status) {
    EventingStatus stat = functions.functionsStatus();

    // All these requireNonNull because see intermittent NPE on CI
    Objects.requireNonNull(stat);
    Objects.requireNonNull(stat.functions());

    return stat.functions().stream().anyMatch(state -> {
      Objects.requireNonNull(state);
      Objects.requireNonNull(state.name());
      Objects.requireNonNull(state.status());

      LOGGER.info("Function {} waiting for {}:{}", state, funcName, status);

      return state.name().equals(funcName) && state.status() == status;
    });
  }

}
