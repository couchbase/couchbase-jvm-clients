/*
 * Copyright (c) 2021 Couchbase, Inc.
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
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static com.couchbase.client.core.util.ConnectionStringUtil.INCOMPATIBLE_CONNECTION_STRING_PARAMS;
import static com.couchbase.client.core.util.ConnectionStringUtil.INCOMPATIBLE_CONNECTION_STRING_SCHEME;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedClusterEnvironmentIntegrationTest extends JavaIntegrationTest {

  @Test
  void throwsOnIncompatibleConnectionString() {
    assertIncompatibleConnectionString("couchbases://example.com", INCOMPATIBLE_CONNECTION_STRING_SCHEME);
  }

  @Test
  void throwsOnIncompatibleConnectionStringParams() {
    assertIncompatibleConnectionString("couchbase://example.com?foo=bar", INCOMPATIBLE_CONNECTION_STRING_PARAMS);
  }

  private void assertIncompatibleConnectionString(String connectionString, String expectedErrorMessage) {
    try (ClusterEnvironment env = ClusterEnvironment.builder().build()) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
        Cluster.connect(
          connectionString,
          ClusterOptions.clusterOptions("username", "password")
            .environment(env)
        )
      );
      assertEquals(e.getMessage(), expectedErrorMessage);
    }
  }

  @Test
  void canShareClusterEnvironment() {
    ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder();
    environmentCustomizer().accept(envBuilder);
    ClusterEnvironment env = envBuilder.build();

    ClusterOptions options = ClusterOptions.clusterOptions(authenticator()).environment(env);
    Cluster cluster1 = Cluster.connect(connectionString(), options);
    Cluster cluster2 = Cluster.connect(connectionString(), options);

    try {
      assertSame(
          cluster1.core().context().environment(),
          cluster2.core().context().environment()
      );

      Bucket bucket1 = cluster1.bucket(config().bucketname());
      Bucket bucket2 = cluster2.bucket(config().bucketname());

      bucket1.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
      bucket2.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      String documentId = UUID.randomUUID().toString();

      bucket1.defaultCollection().upsert(documentId, "foo", upsertOptions().expiry(Duration.ofSeconds(1)));
      cluster1.disconnect();

      // environment should remain active for the other cluster to use
      bucket2.defaultCollection().upsert(documentId, "foo", upsertOptions().expiry(Duration.ofSeconds(1)));

    } finally {
      cluster1.disconnect();
      cluster2.disconnect();
      env.shutdown();
    }
  }
}
