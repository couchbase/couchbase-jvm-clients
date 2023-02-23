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

package com.couchbase.client.core.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Extends the {@link ClusterAwareIntegrationTest} with core-io specific code.
 *
 * @since 2.0.0
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES) // Safety timer so tests can't block CI executors
public class CoreIntegrationTest extends ClusterAwareIntegrationTest {

  static {
    // Make sure that every core integration test records and spits out buffer leaks.
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  protected static final Duration kvTimeout = Duration.ofSeconds(5);
  protected static final Duration managementTimeout = Duration.ofSeconds(5);

  protected static void waitUntilReady(Core core) {
    try {
      core.waitUntilReady(
              Collections.emptySet(),
              Duration.ofSeconds(15),
              ClusterState.ONLINE,
              config().bucketname())
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates an environment builder that already has all the needed properties to bootstrap
   * set.
   */
  protected static CoreEnvironment.Builder environment() {
    return CoreEnvironment.builder();
  }

  protected static Set<SeedNode> seedNodes() {
    return config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.ofNullable(cfg.ports().get(Services.KV)),
      Optional.ofNullable(cfg.ports().get(Services.MANAGER))
    )).collect(Collectors.toSet());
  }

  protected static Authenticator authenticator() {
    return PasswordAuthenticator.create(config().adminUsername(), config().adminPassword());
  }

}
