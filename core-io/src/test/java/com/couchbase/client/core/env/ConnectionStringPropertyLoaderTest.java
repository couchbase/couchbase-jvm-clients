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