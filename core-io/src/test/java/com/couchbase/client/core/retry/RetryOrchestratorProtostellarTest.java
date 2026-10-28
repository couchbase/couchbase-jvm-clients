/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.retry;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.protostellar.ProtostellarContext;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RetryOrchestratorProtostellarTest {
  private CoreEnvironment env;
  private CoreProtostellar core;

  @BeforeEach
  void setUp() {
    env = CoreEnvironment.create();
    core = mock(CoreProtostellar.class);
    when(core.context()).thenReturn(new ProtostellarContext(
      env,
      PasswordAuthenticator.create("Administrator", "password"),
      mock(CoreResources.class)
    ));
  }

  @AfterEach
  void tearDown() {
    env.shutdown();
  }

  @Test
  void backsOffExponentiallyWithTheDefaultRetryStrategy() {
    ProtostellarRequest<String> request = new ProtostellarRequest<>(
      "request",
      core,
      ServiceType.KV,
      "get",
      mock(RequestSpan.class),
      Duration.ofMinutes(1),
      true,
      BestEffortRetryStrategy.INSTANCE,
      new HashMap<>(),
      0,
      null
    );

    List<Duration> delays = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      delays.add(RetryOrchestratorProtostellar.shouldRetry(core, request, RetryReason.SERVICE_NOT_AVAILABLE).retryDuration());
    }

    // Like the retries of other requests: immediately, then doubling from 1 ms up to 500 ms.
    assertEquals(
      Arrays.asList(0L, 1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L, 500L, 500L),
      delays.stream().map(Duration::toMillis).collect(toList())
    );
    assertEquals(12, request.retryAttempts());
  }
}
