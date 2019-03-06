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

package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.util.Utils;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the surrounding code in the {@link BaseLoader}.
 *
 * @since 2.0.0
 */
class BaseLoaderTest {

  private static final NetworkAddress SEED = NetworkAddress.localhost();
  private static final String BUCKET = "bucket";
  private static final int PORT = 1234;
  private static final ServiceType SERVICE = ServiceType.KV;

  private Core core;

  @BeforeEach
  void setup() {
    CoreEnvironment env = mock(CoreEnvironment.class);
    core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, env);
    when(core.context()).thenReturn(ctx);
  }

  @Test
  void loadsAndParsesConfig() {
    Loader loader = new BaseLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NetworkAddress seed, String bucket) {
        return Mono.just(Utils.readResource(
          "../config_with_external.json",
          BaseLoaderTest.class
        ).getBytes(CharsetUtil.UTF_8));
      }
    };

    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.empty());

    BucketConfig config = loader.load(SEED, PORT, BUCKET).block();
    assertEquals("default", config.name());
    assertEquals(1073, config.rev());
  }

  @Test
  void failsWhenConfigNotParsable() {
    Loader loader = new BaseLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NetworkAddress seed, String bucket) {
        return Mono.just("invalid".getBytes(CharsetUtil.UTF_8));
      }
    };

    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.empty());

    assertThrows(ConfigException.class, () -> loader.load(SEED, PORT, BUCKET).block());
  }

  @Test
  void failsWhenServiceCannotBeEnabled() {
    Loader loader = new BaseLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NetworkAddress seed, String bucket) {
        return Mono.error(new IllegalStateException("Not expected to be called!"));
      }
    };
    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.error(new CouchbaseException("Some error during service ensure")));

    assertThrows(ConfigException.class, () -> loader.load(SEED, PORT, BUCKET).block());
  }

  @Test
  void failsWhenChildDiscoverFails() {
    Loader loader = new BaseLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NetworkAddress seed, String bucket) {
        return Mono.error(new CouchbaseException("Failed discovering for some reason"));
      }
    };

    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.empty());

    assertThrows(ConfigException.class, () -> loader.load(SEED, PORT, BUCKET).block());
  }

}