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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigRequest;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigResponse;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link KeyValueLoader}.
 */
class KeyValueLoaderTest {

  private static final NodeIdentifier SEED = mock(NodeIdentifier.class);
  private static final String BUCKET = "bucket";

  private KeyValueLoader loader;
  private Core core;

  @BeforeEach
  void setup() {
    CoreEnvironment env = mock(CoreEnvironment.class);
    when(env.timeoutConfig()).thenReturn(TimeoutConfig.create());
    when(env.retryStrategy()).thenReturn(BestEffortRetryStrategy.INSTANCE);

    core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, env);
    when(core.context()).thenReturn(ctx);
    loader = new KeyValueLoader(core);
  }

  @Test
  void loadsConfigSuccessfully() {
    byte[] expectedConfig = "config".getBytes(UTF_8);

    CarrierBucketConfigResponse response = mock(CarrierBucketConfigResponse.class);
    when(response.status()).thenReturn(ResponseStatus.SUCCESS);
    when(response.content()).thenReturn(expectedConfig);

    doAnswer(i -> {
      ((CarrierBucketConfigRequest) i.getArgument(0)).succeed(response);
      return null;
    }).when(core).send(any(CarrierBucketConfigRequest.class));

    byte[] config = loader.discoverConfig(SEED, BUCKET).block();
    assertArrayEquals(expectedConfig, config);
  }

  @Test
  void errorsIfNonSuccessful() {
    CarrierBucketConfigResponse response = mock(CarrierBucketConfigResponse.class);
    when(response.status()).thenReturn(ResponseStatus.UNKNOWN);

    doAnswer(i -> {
      ((CarrierBucketConfigRequest) i.getArgument(0)).succeed(response);
      return null;
    }).when(core).send(any(CarrierBucketConfigRequest.class));

    assertThrows(ConfigException.class, () -> loader.discoverConfig(SEED, BUCKET).block());
  }

  @Test
  void errorsIfUnsupported() {
    CarrierBucketConfigResponse response = mock(CarrierBucketConfigResponse.class);
    when(response.status()).thenReturn(ResponseStatus.UNSUPPORTED);

    doAnswer(i -> {
      ((CarrierBucketConfigRequest) i.getArgument(0)).succeed(response);
      return null;
    }).when(core).send(any(CarrierBucketConfigRequest.class));

    assertThrows(
      UnsupportedConfigMechanismException.class,
      () -> loader.discoverConfig(SEED, BUCKET).block()
    );
  }

  @Test
  void errorsIfFailedRequest() {
    doAnswer(i -> {
      ((CarrierBucketConfigRequest) i.getArgument(0))
        .fail(new UnsupportedOperationException());
      return null;
    }).when(core).send(any(CarrierBucketConfigRequest.class));

    assertThrows(
      UnsupportedOperationException.class,
      () -> loader.discoverConfig(SEED, BUCKET).block()
    );
  }

  /**
   * Since the client may run many loaders in parallel, once a good config is found the other
   * attempts will be stopped.
   *
   * <p>This test makes sure that if an operation is ongoing but the downstream listener
   * unsubscribes, it gets cancelled so we are not performing any loader ops that are not needed
   * anymore.</p>
   */
  @Test
  void cancelRequestOnceUnsubscribed() {
    final AtomicReference<CarrierBucketConfigRequest> request = new AtomicReference<>();
    doAnswer(i -> {
      request.set(i.getArgument(0));
      return null;
    }).when(core).send(any(CarrierBucketConfigRequest.class));

    Disposable disposable = loader.discoverConfig(SEED, BUCKET).subscribe();
    disposable.dispose();

    assertTrue(request.get().completed());
    assertTrue(request.get().cancelled());
    assertEquals(CancellationReason.STOPPED_LISTENING, request.get().cancellationReason());
  }

}
