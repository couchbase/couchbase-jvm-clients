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

package com.couchbase.client.core.msg.query;

import static com.couchbase.client.core.env.CoreEnvironment.*;
import static java.time.temporal.ChronoUnit.*;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.QueryStreamException;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Query response which returns the reactor processors which emit parts of the response
 * as requested
 *
 * @since 2.0.0
 */
public class QueryResponse extends BaseResponse {

  private EmitterProcessor<byte[]> rowsProcessor;
  private MonoProcessor<QueryAdditional> additional;
  private byte[] signature;
  private String requestId;
  private String clientContextId;
  private volatile Instant lastRequestTimeStamp;
  private final Channel channel;
  private final AtomicLong rowRequestSize = new AtomicLong(0);
  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final CoreEnvironment environment;

  @Stability.Internal
  public QueryResponse(ResponseStatus status, Channel channel, CoreEnvironment environment) {
    super(status);
    this.channel = channel;
    this.environment = environment;
    this.rowsProcessor = EmitterProcessor.create(DEFAULT_QUERY_BACKPRESSURE_QUEUE_SIZE);
    this.errorsProcessor = EmitterProcessor.create(DEFAULT_QUERY_BACKPRESSURE_QUEUE_SIZE);
    this.additional = MonoProcessor.create();

    FluxSink<byte[]> rowsSink = this.rowsProcessor.sink();
    rowsSink.onRequest(n -> {
        if(this.rowRequestSize.addAndGet(n) > 0) {
          this.lastRequestTimeStamp = Instant.now();
          this.channel.config().setAutoRead(true);
        }
    });
    scheduleQueryStreamTimeout();
  }

  private void scheduleQueryStreamTimeout() {
    this.environment.timer().schedule(() -> {
      long timeoutConfigured = this.environment.timeoutConfig().queryStreamReleaseTimeout().getSeconds();
      if ((lastRequestTimeStamp == null && this.rowRequestSize.get() == 0) ||
              (lastRequestTimeStamp != null && lastRequestTimeStamp.until(Instant.now(), SECONDS) >= timeoutConfigured)) {
        this.completeExceptionally(new QueryStreamException("No rows were requested within " +
                this.environment.timeoutConfig().queryStreamReleaseTimeout().getSeconds() + "s"));
      }
    }, this.environment.timeoutConfig().queryStreamReleaseTimeout());
  }

  @Stability.Internal
  public void rowRequestCompleted() {
    if(this.rowRequestSize.decrementAndGet() == 0) {
      this.channel.config().setAutoRead(false);
      scheduleQueryStreamTimeout();
    }
  }

  @Stability.Internal
  public void completeSuccessfully() {
    this.rowsProcessor.onComplete();
    this.additional.onComplete();
    this.complete();
  }

  @Stability.Internal
  public void completeExceptionally(Throwable t) {
    this.rowsProcessor.onError(t);
    this.additional.onError(t);
    this.complete();
  }

  @Stability.Internal
  public void complete() {
    this.completed.set(true);
    this.channel.config().setAutoRead(true);
  }

  @Stability.Internal
  public boolean isCompleted() {
    return this.completed.get();
  }

  @Stability.Internal
  public long rowRequestSize() {
    return this.rowRequestSize.get();
  }

  public String requestId() {
    return this.requestId;
  }

  public String clientContextId() {
    return this.clientContextId;
  }

  public byte[] signature() { return this.signature; }

  public Mono<QueryAdditional> additional() { return this.additional; }

  public EmitterProcessor<byte[]> rows() { return this.rowsProcessor; }
}

