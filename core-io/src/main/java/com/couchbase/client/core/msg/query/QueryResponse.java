package com.couchbase.client.core.msg.query;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.channel.Channel;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;

public class QueryResponse extends BaseResponse {
  private EmitterProcessor<byte[]> rowsProcessor;
  private EmitterProcessor<byte[]> errorsProcessor;
  private EmitterProcessor<byte[]> warningsProcessor;
  private MonoProcessor<byte[]> metricsProcessor;
  private MonoProcessor<String> requestIdProcessor;
  private MonoProcessor<String> clientContextIdProcessor;
  private MonoProcessor<String> queryStatusProcessor;
  private boolean parsingSuccess;
  private final Channel channel;
  private final AtomicLong requestSize = new AtomicLong(1);

  public QueryResponse(ResponseStatus status, Channel channel) {
    super(status);
    this.channel = channel;
    this.rowsProcessor = EmitterProcessor.create(1);
    this.errorsProcessor = EmitterProcessor.create(1);
    this.warningsProcessor = EmitterProcessor.create(1);
    this.requestIdProcessor = MonoProcessor.create();
    this.clientContextIdProcessor = MonoProcessor.create();
    this.metricsProcessor = MonoProcessor.create();
    this.queryStatusProcessor = MonoProcessor.create();

    FluxSink<byte[]> rowsSink = this.rowsProcessor.sink();
    rowsSink.onRequest(n -> {
        if(this.requestSize.addAndGet(n) > 0) {
          this.channel.config().setAutoRead(true);
        }
    });
    //TODO: buffer eviction handling
    this.rowsProcessor.onBackpressureBuffer(Duration.ofSeconds(1), 1, (b) -> { this.rowsProcessor.onComplete();});
    this.errorsProcessor.onBackpressureBuffer(Duration.ofSeconds(1), 1, (b) -> {});
    this.warningsProcessor.onBackpressureBuffer(Duration.ofSeconds(1), 1, (b) -> {});
  }

  public void rowRequestCompleted() {
    if(this.requestSize.decrementAndGet() == 0) {
      this.channel.config().setAutoRead(false);
    }
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

  public void complete() {
    this.rowsProcessor.onComplete();
    this.errorsProcessor.onComplete();
    this.warningsProcessor.onComplete();
  }
}
