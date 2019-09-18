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

package com.couchbase.client.core.env;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConnectionStringPropertyLoaderTest {

  private static final String USER = "user";
  private static final String PASS = "pass";

  @Test
  void shouldApplySeedNodesFromJustHostname() {
    parse("127.0.0.1", env -> {
      assertEquals(1, env.seedNodes().size());
      SeedNode node = env.seedNodes().iterator().next();
      assertEquals("127.0.0.1", node.address());
      assertFalse(node.kvPort().isPresent());
      assertFalse(node.httpPort().isPresent());
    });
  }

  @Test
  void shouldApplySeedNodesWithCustomKvPortAndNoScheme() {
    parse("127.0.0.1:1234", env -> {
      assertEquals(1, env.seedNodes().size());
      SeedNode node = env.seedNodes().iterator().next();
      assertEquals("127.0.0.1", node.address());
      assertEquals(1234, node.kvPort().get());
      assertFalse(node.httpPort().isPresent());
    });
  }

  @Test
  void shouldApplySeedNodesWithCustomKvPortAndCouchbaseScheme() {
    parse("couchbase://127.0.0.1:1234", env -> {
      assertEquals(1, env.seedNodes().size());
      SeedNode node = env.seedNodes().iterator().next();
      assertEquals("127.0.0.1", node.address());
      assertEquals(1234, node.kvPort().get());
      assertFalse(node.httpPort().isPresent());
    });

    parse("couchbases://127.0.0.1:1234", env -> {
      assertEquals(1, env.seedNodes().size());
      SeedNode node = env.seedNodes().iterator().next();
      assertEquals("127.0.0.1", node.address());
      assertEquals(1234, node.kvPort().get());
      assertFalse(node.httpPort().isPresent());
    });
  }

  @Test
  void shouldApplyMultipleSeedNodesWithCustomKvPorts() {
    Map<String, Integer> expected = new HashMap<>();
    expected.put("10.0.0.1", 11210);
    expected.put("10.0.0.2", 11211);
    expected.put("10.0.0.3", 11212);

    List<String> nodes = expected
      .entrySet()
      .stream()
      .map(e -> e.getKey() + ":" + e.getValue())
      .collect(Collectors.toList());

    parse("couchbase://" + String.join(",", nodes), env -> {
      assertEquals(3, env.seedNodes().size());

      int matched = 0;
      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        for (SeedNode node : env.seedNodes()) {
          if (node.address().equals(entry.getKey())) {
            assertEquals(node.kvPort().get(), entry.getValue());
            matched++;
          }
          assertFalse(node.httpPort().isPresent());
        }
      }
      assertEquals(3, matched);
    });
  }

  @Test
  void shouldApplyPropertiesFromConnectionString() {
    parse("couchbase://127.0.0.1?service.queryService.minEndpoints=23&io.configPollInterval=2m", env -> {
      assertEquals(23, env.serviceConfig().queryServiceConfig().minEndpoints());
      assertEquals(Duration.ofMinutes(2), env.ioConfig().configPollInterval());
    });
  }

  /**
   * Helper method to parse a connection string into the env and clean it up afterwards.
   *
   * @param connectionString the connection string to parse.
   * @param consumer the consumer which will get the full env to assert against.
   */
  private void parse(final String connectionString, final Consumer<CoreEnvironment> consumer) {
    CoreEnvironment.Builder builder = CoreEnvironment.builder();
    ConnectionStringPropertyLoader loader = new ConnectionStringPropertyLoader(connectionString);
    loader.load(builder);
    CoreEnvironment built = builder.build();
    try {
      consumer.accept(built);
    } finally {
      built.shutdown();
    }
  }

}