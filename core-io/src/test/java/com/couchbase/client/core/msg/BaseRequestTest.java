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

package com.couchbase.client.core.msg;

import com.couchbase.client.core.deps.io.netty.util.Timeout;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class BaseRequestTest {

  @Test
  void cancelsTimeoutRegistrationOnSuccess() {
    DummyRequest request = new DummyRequest();

    Timeout registration = mock(Timeout.class);
    request.timeoutRegistration(registration);

    request.succeed(mock(Response.class));
    verify(registration, times(1)).cancel();
  }

  @Test
  void cancelsTimeoutRegistrationOnFailure() {
    DummyRequest request = new DummyRequest();

    Timeout registration = mock(Timeout.class);
    request.timeoutRegistration(registration);

    request.fail(new RuntimeException());
    verify(registration, times(1)).cancel();
  }

  @Test
  void cancelsTimeoutRegistrationOnCancel() {
    DummyRequest request = new DummyRequest();

    Timeout registration = mock(Timeout.class);
    request.timeoutRegistration(registration);

    request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY);
    verify(registration, times(1)).cancel();
  }

  private static class DummyRequest extends BaseRequest<Response> {

    public DummyRequest() {
      super(Duration.ofSeconds(10), mockCore().context(), BestEffortRetryStrategy.INSTANCE);
    }

    @Override
    public ServiceType serviceType() {
      return ServiceType.KV;
    }

  }

}
