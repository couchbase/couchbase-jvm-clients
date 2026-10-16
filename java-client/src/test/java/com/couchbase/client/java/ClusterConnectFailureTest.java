/*
 * Copyright 2026 Couchbase, Inc.
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

import com.couchbase.client.core.CoreLimiter;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.OwnedOrExternal;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.env.OwnedOrExternal.external;
import static com.couchbase.client.core.env.OwnedOrExternal.owned;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies a failed {@code connect} doesn't leak the SDK-created environment
 * or a {@link CoreLimiter} slot (JVMCBC-1789).
 */
class ClusterConnectFailureTest {

  // SDK doesn't like KV port to be 8091 or 18091
  private static final ConnectionString MANAGEMENT_PORT = ConnectionString.create("couchbase://127.0.0.1:8091");

  @Test
  void managementPortFailureShutsDownOwnedEnvironment() {
    ClusterEnvironment env = ClusterEnvironment.builder().build();
    try {
      assertThrows(InvalidArgumentException.class, () -> newAsyncCluster(owned(env)));
      assertTrue(env.scheduler().isDisposed(), "owned environment should be shut down");
    } finally {
      // A second shutdown times out, so only shut down if the fix didn't.
      if (!env.scheduler().isDisposed()) {
        env.shutdown();
      }
    }
  }

  @Test
  void managementPortFailureLeavesExternalEnvironmentRunning() {
    ClusterEnvironment env = ClusterEnvironment.builder().build();
    try {
      assertThrows(InvalidArgumentException.class, () -> newAsyncCluster(external(env)));
      assertFalse(env.scheduler().isDisposed(), "external environment belongs to the caller");
    } finally {
      env.shutdown();
    }
  }

  @Test
  void tlsRequiredAuthenticatorFailureReleasesLimiterSlot() {
    int instancesBefore = CoreLimiter.numInstances();

    // The default Authenticator.requiresTls() is true.
    Authenticator requiresTls = new Authenticator() {};
    assertThrows(IllegalArgumentException.class, () ->
      Cluster.connect("couchbase://127.0.0.1", clusterOptions(requiresTls)));

    assertEquals(instancesBefore, CoreLimiter.numInstances());
  }

  private static AsyncCluster newAsyncCluster(OwnedOrExternal<ClusterEnvironment> env) {
    return new AsyncCluster(env, PasswordAuthenticator.create("Administrator", "password"), MANAGEMENT_PORT);
  }
}
