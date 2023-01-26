/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.io.netty.util.Timeout;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CbCollections;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Where the public API requires a {@link Request}, create one dynamically.
 */
@Stability.Volatile
public class ProtostellarBaseRequest implements Request<ProtostellarBaseRequest.ProtostellarResponse> {
  private final ProtostellarRequest<?> request;
  private final Core core;

  @Stability.Internal
  public ProtostellarBaseRequest(Core core, ProtostellarRequest<?> request) {
    this.request = request;
    this.core = core;
  }

  /**
   * Because the underlying {@link ProtostellarRequest> does not implement the {@link Request} interface, it cannot support all operations.
   * <p>
   * Ideally the Request interface, part of the public API, would be much smaller and would only contain getters; it contains the methods it does for legacy reasons.
   * <p>
   * We have aimed to provide all the methods and fields that would be useful to users of the interface, particular from {@link RetryStrategy}.
   */
  private static UnsupportedOperationException unsupported(String message) {
    return new UnsupportedOperationException(message);
  }

  @Override
  public long id() {
    throw unsupported("Protostellar requests do not have unique identifiers");
  }

  @Override
  public CompletableFuture<ProtostellarResponse> response() {
    throw unsupported("Protostellar requests do not contain their responses");
  }

  @Override
  public void succeed(ProtostellarResponse result) {
    throw unsupported("Protostellar requests cannot be succeeded this way");
  }

  @Override
  public void fail(Throwable error) {
    throw unsupported("Protostellar requests cannot be failed this way");
  }

  @Override
  public void cancel(CancellationReason reason, Function<Throwable, Throwable> exceptionTranslator) {
    throw unsupported("Protostellar requests cannot be cancelled this way");
  }

  @Override
  public void timeoutRegistration(Timeout registration) {
    throw unsupported("Protostellar requests cannot have their timeouts configured in this way");
  }

  @Override
  public RequestContext context() {
    return new RequestContext(core.context(), this);
  }

  @Override
  public Duration timeout() {
    return request.timeout();
  }

  @Override
  public boolean timeoutElapsed() {
    return request.timeoutElapsed();
  }

  @Override
  public boolean completed() {
    return request.completed();
  }

  @Override
  public boolean succeeded() {
    return request.succeeded();
  }

  @Override
  public boolean failed() {
    return request.failed();
  }

  @Override
  public boolean cancelled() {
    // We don't track cancellation separately for ProtostellarRequest.  It's just another form of failure.
    return failed();
  }

  @Override
  public CancellationReason cancellationReason() {
    return request.cancellationReason();
  }

  @Override
  public ServiceType serviceType() {
    return request.serviceType();
  }

  @Override
  public Map<String, Object> serviceContext() {
    return CbCollections.mapOf();
  }

  @Override
  public RetryStrategy retryStrategy() {
    return request.retryStrategy();
  }

  @Override
  public RequestSpan requestSpan() {
    return request.span();
  }

  @Override
  public long createdAt() {
    return request.createdAt();
  }

  @Override
  public long absoluteTimeout() {
    return request.absoluteTimeout();
  }

  static class ProtostellarResponse implements Response {
    @Override
    public ResponseStatus status() {
      throw unsupported("Protostellar requests do not have a status field");
    }
  }
}
