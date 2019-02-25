package com.couchbase.client.core.msg.query;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.channel.Channel;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.MonoProcessor;

/**
 * Query response which returns the reactor processors which emit parts of the response
 * as requested
 *
 * @since 2.0.0
 */
public class QueryResponse extends BaseResponse {

  private EmitterProcessor<byte[]> rowsProcessor;
  private EmitterProcessor<byte[]> errorsProcessor;
  private EmitterProcessor<byte[]> warningsProcessor;
  private MonoProcessor<byte[]> metricsProcessor;
  private MonoProcessor<String> requestIdProcessor;
  private MonoProcessor<String> clientContextIdProcessor;
  private MonoProcessor<String> queryStatusProcessor;

  private final Channel channel;
  private final AtomicLong rowRequestSize = new AtomicLong(0);
  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final CoreEnvironment environment;

  public QueryResponse(ResponseStatus status, Channel channel, CoreEnvironment environment) {
    super(status);
    this.channel = channel;
    this.environment = environment;
    this.rowsProcessor = EmitterProcessor.create(1);
    this.errorsProcessor = EmitterProcessor.create(1);
    this.warningsProcessor = EmitterProcessor.create(1);
    this.requestIdProcessor = MonoProcessor.create();
    this.clientContextIdProcessor = MonoProcessor.create();
    this.metricsProcessor = MonoProcessor.create();
    this.queryStatusProcessor = MonoProcessor.create();

    FluxSink<byte[]> rowsSink = this.rowsProcessor.sink();
    rowsSink.onRequest(n -> {
        if(this.rowRequestSize.addAndGet(n) > 0) {
          this.channel.config().setAutoRead(true);
        }
    });
    this.environment.timer().schedule(() -> {
      if (this.rowRequestSize.get() == 0) {
        this.complete();
      }
    }, TimeoutConfig.DEFAULT_STREAM_RELEASE_TIMEOUT);
  }

  @Stability.Internal
  public void rowRequestCompleted() {
    if(this.rowRequestSize.decrementAndGet() == 0) {
      this.channel.config().setAutoRead(false);
      this.environment.timer().schedule(() -> {
        if (this.rowRequestSize.get() == 0) {
          this.complete();
        }
      }, TimeoutConfig.DEFAULT_STREAM_RELEASE_TIMEOUT);
    }
  }

  @Stability.Internal
  public void complete() {
    this.requestIdProcessor.onComplete();
    this.clientContextIdProcessor.onComplete();
    this.metricsProcessor.onComplete();
    this.queryStatusProcessor.onComplete();
    this.rowsProcessor.onComplete();
    this.errorsProcessor.onComplete();
    this.warningsProcessor.onComplete();
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

  public MonoProcessor<String> requestId() {
    return this.requestIdProcessor;
  }

  public MonoProcessor<String> clientContextId() {
    return this.clientContextIdProcessor;
  }

  public MonoProcessor<byte[]> metrics() {
    return this.metricsProcessor;
  }

  public MonoProcessor<String> queryStatus() {
    return this.queryStatusProcessor;
  }

  public EmitterProcessor<byte[]> rows() {
    return this.rowsProcessor;
  }

  public EmitterProcessor<byte[]> errors() {
    return this.errorsProcessor;
  }

  public EmitterProcessor<byte[]> warnings() {
    return this.warningsProcessor;
  }
}