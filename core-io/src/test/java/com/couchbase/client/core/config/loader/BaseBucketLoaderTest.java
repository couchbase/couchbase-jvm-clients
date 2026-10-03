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
import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.SeedNodeOutdatedException;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceState;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.util.SingleStateful;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.topology.TopologyTestUtils.nodeId;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.readResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the surrounding code in the {@link BaseBucketLoader}.
 *
 * @since 2.0.0
 */
class BaseBucketLoaderTest {

  private static final NodeIdentifier SEED = nodeId("127.0.0.1", 8091);
  private static final String BUCKET = "bucket";
  private static final int PORT = 1234;
  private static final ServiceType SERVICE = ServiceType.KV;

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void setup() {
    env = mock(CoreEnvironment.class);
    when(env.scheduler()).thenReturn(Schedulers.immediate());
    core = mockCore();
    CoreContext ctx = new CoreContext(core, 1, env, mock(Authenticator.class));
    when(core.context()).thenReturn(ctx);
  }

  @Test
  void loadsAndParsesConfig() {
    BucketLoader loader = new BaseBucketLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NodeIdentifier seed, String bucket) {
        return Mono.just(readResource(
          "../config_with_external.json",
          BaseBucketLoaderTest.class
        ).getBytes(UTF_8));
      }
    };

    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.empty());

    when(core.serviceState(eq(SEED), eq(SERVICE), eq(Optional.of(BUCKET)))).thenReturn(Optional.of(Flux.just(ServiceState.CONNECTED)));

    ProposedBucketConfigContext ctx = loader.load(SEED, PORT, BUCKET).block();
    BucketConfig config = BucketConfigParser.parse(ctx.config(), StandardMemcachedHashingStrategy.INSTANCE, ctx.origin());
    assertEquals("default", config.name());
    assertEquals(1073, config.rev());
  }

  @Test
  void failsWhenServiceCannotBeEnabled() {
    BucketLoader loader = new BaseBucketLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NodeIdentifier seed, String bucket) {
        return Mono.error(new IllegalStateException("Not expected to be called!"));
      }
    };
    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.error(new CouchbaseException("Some error during service ensure")));

    assertThrows(ConfigException.class, () -> loader.load(SEED, PORT, BUCKET).block());
  }

  @Test
  void failsWhenChildDiscoverFails() {
    BucketLoader loader = new BaseBucketLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NodeIdentifier seed, String bucket) {
        return Mono.error(new CouchbaseException("Failed discovering for some reason"));
      }
    };

    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.empty());

    assertThrows(ConfigException.class, () -> loader.load(SEED, PORT, BUCKET).block());
  }

  @Test
  void failsWhenServiceRemovedBeforeConnecting() {
    BucketLoader loader = new BaseBucketLoader(core, SERVICE) {
      @Override
      protected Mono<byte[]> discoverConfig(NodeIdentifier seed, String bucket) {
        return Mono.error(new IllegalStateException("Not expected to be called!"));
      }
    };

    when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
      .thenReturn(Mono.empty());

    // A service's state stream completes when the service is disconnected.
    when(core.serviceState(eq(SEED), eq(SERVICE), eq(Optional.of(BUCKET))))
      .thenReturn(Optional.of(Flux.just(ServiceState.CONNECTING)));

    ConfigException e = assertThrows(ConfigException.class, () -> loader.load(SEED, PORT, BUCKET).block());
    assertInstanceOf(SeedNodeOutdatedException.class, e);
  }

  /**
   * The service state is emitted while holding the state locks of the service (and its endpoint).
   * Sending the config request on that thread could deadlock, because sending might need the service's lock
   * (to open a new endpoint), while another thread holding the service's lock waits for a state lock.
   */
  @Test
  void discoversConfigOffTheThreadThatEmittedTheServiceState() throws Exception {
    Scheduler scheduler = Schedulers.newSingle("bucket-loader-test");
    try {
      when(env.scheduler()).thenReturn(scheduler);

      SingleStateful<ServiceState> serviceState = SingleStateful.fromInitial(ServiceState.CONNECTING);
      when(core.ensureServiceAt(eq(SEED), eq(SERVICE), eq(PORT), eq(Optional.of(BUCKET))))
        .thenReturn(Mono.empty());
      when(core.serviceState(eq(SEED), eq(SERVICE), eq(Optional.of(BUCKET))))
        .thenReturn(Optional.of(serviceState.states()));

      AtomicReference<Thread> discoverThread = new AtomicReference<>();
      AtomicBoolean discoverHeldStateLock = new AtomicBoolean();
      BucketLoader loader = new BaseBucketLoader(core, SERVICE) {
        @Override
        protected Mono<byte[]> discoverConfig(NodeIdentifier seed, String bucket) {
          return Mono.defer(() -> {
            discoverThread.set(Thread.currentThread());
            discoverHeldStateLock.set(Thread.holdsLock(serviceState));
            return Mono.just(readResource(
              "../config_with_external.json",
              BaseBucketLoaderTest.class
            ).getBytes(UTF_8));
          });
        }
      };

      CompletableFuture<ProposedBucketConfigContext> result = loader.load(SEED, PORT, BUCKET).toFuture();
      assertFalse(result.isDone(), "load should wait for the service to connect");

      // Emits CONNECTED on this thread, while holding the state's lock.
      serviceState.transition(ServiceState.CONNECTED);

      assertNotNull(result.get(10, TimeUnit.SECONDS));
      assertFalse(discoverHeldStateLock.get(), "config discovery ran while holding the service state lock");
      assertNotSame(Thread.currentThread(), discoverThread.get(), "config discovery ran on the emitting thread");
    } finally {
      scheduler.dispose();
    }
  }

}
