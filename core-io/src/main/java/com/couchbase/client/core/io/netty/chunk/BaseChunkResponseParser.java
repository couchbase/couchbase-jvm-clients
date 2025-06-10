/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.chunk;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.channel.ChannelConfig;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.chunk.ChunkHeader;
import com.couchbase.client.core.msg.chunk.ChunkRow;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.nio.ByteBuffer;
import java.util.Optional;

import static com.couchbase.client.core.Reactor.emitFailureHandler;

/**
 * Provides a common, abstract implementation of the {@link ChunkResponseParser} interface.
 */
public abstract class BaseChunkResponseParser<H extends ChunkHeader, ROW extends ChunkRow,
  T extends ChunkTrailer> implements ChunkResponseParser<H, ROW, T> {

  /**
   * Holds the current stream parser created by the child.
   */
  private JsonStreamParser parser;

  /**
   * Remember if the stream parser threw an exception so we can skip the rest of the stream.
   */
  private CouchbaseException decodingFailure;

  /**
   * Whether the subclass has marked the header as complete.
   */
  private boolean headerComplete;

  /**
   * Holds the current channel config.
   */
  private ChannelConfig channelConfig;

  /**
   * Holds the current associated trailer.
   */
  private Sinks.One<T> trailer;

  /**
   * Holds the current associated rows.
   */
  private Flux<ROW> rows;

  /**
   * Holds the current associated row sink.
   */
  private Sinks.Many<ROW> rowSink;

  /**
   * For backpressure, a running total of the number of rows requested by the subscriber,
   * minus the number of rows emitted. A negative value indicates a surplus of available rows.
   * <p>
   * A value of {@code Long.MAX_VALUE} indicates backpressure is disabled.
   */
  private long demand = 0;

  /**
   * Guards access to `demand` and channel autoRead setting.
   */
  private final Object autoReadLock = new Object();

  /**
   * Subclass implements this to return the "meat" of the decoding, the chunk parser.
   */
  protected abstract JsonStreamParser.Builder parserBuilder();

  private volatile RequestContext requestContext;

  private volatile HttpResponse responseHeader;

  public final void cleanup() {
    if (parser != null) {
      parser.close();
    }
    parser = null;
    decodingFailure = null;
    headerComplete = false;
    doCleanup();
  }

  @Override
  public void updateRequestContext(final RequestContext requestContext) {
    this.requestContext = requestContext;
  }

  protected RequestContext requestContext() {
    return requestContext;
  }

  @Override
  public void updateResponseHeader(final HttpResponse responseHeader) {
    this.responseHeader = responseHeader;
  }

  protected HttpResponse responseHeader() {
    return responseHeader;
  }

  protected void markHeaderComplete() {
    headerComplete = true;
  }

  /**
   * Only for use by subclasses. External collaborators should call
   * {@link ChunkResponseParser#header(boolean)} to see if the header is ready.
   */
  protected boolean isHeaderComplete() {
    return headerComplete;
  }

  /**
   * Give subclasses a chance to reset their state.
   */
  protected abstract void doCleanup();

  @Override
  public void feed(ByteBuf input) {
    try {
      if (decodingFailure != null) {
        return;
      }

      if (input.nioBufferCount() != -1) {
        for (ByteBuffer nioBuffer : input.nioBuffers()) {
          parser.feed(nioBuffer);
        }
      } else {
        // Shouldn't end up here, but just in case...
        parser.feed(ByteBuffer.wrap(ByteBufUtil.getBytes(input)));
      }

    } catch (Exception e) {
      decodingFailure = new DecodingFailureException(e);
      failRows(decodingFailure);
      failTrailer(decodingFailure);

    } finally {
      input.release();
    }
  }

  /**
   * Initializes the parser to a fresh state.
   *
   * @param channelConfig the channel config used for backpressure auto-read.
   */
  @Override
  public void initialize(final ChannelConfig channelConfig) {
    cleanup();
    parser = parserBuilder().build();
    this.channelConfig = channelConfig;
    this.trailer = Sinks.one();
    synchronized (autoReadLock) {
      this.demand = 0;
    }

    this.rowSink = Sinks.many().unicast().onBackpressureBuffer();
    this.rows = rowSink
      .asFlux()
      .doOnRequest(v -> {
        synchronized (autoReadLock) {
          if (demand == Long.MAX_VALUE) {
            return;
          }

          try {
            demand = Math.addExact(demand, v);

          } catch (ArithmeticException e) {
            // They asked for it, so open the floodgates.
            demand = Long.MAX_VALUE;
            channelConfig.setAutoRead(true);
            return;
          }

          if (demand > 0 && !channelConfig.isAutoRead()) {
            channelConfig.setAutoRead(true);
          }
        }
      })
      .doOnTerminate(this::readRemainingRowsWithoutBackpressure)
      .doOnCancel(this::readRemainingRowsWithoutBackpressure)
      .publish()
      .refCount();
  }

  /**
   * Allow the remaining result rows to be consumed, even in the absence of demand.
   * Must not be called before the row subscription is terminated or cancelled.
   * <p>
   * From a memory usage perspective, this is safe because future attempts to emit
   * rows into the sink fail instead of buffering the items.
   * <p>
   * Do this because the current API contract requires publishing metadata
   * even if the row sink terminates or is cancelled. Otherwise, we would
   * probably be better off simply closing the HTTP connection.
   */
  private void readRemainingRowsWithoutBackpressure() {
    synchronized (autoReadLock) {
      demand = Long.MAX_VALUE;
      channelConfig.setAutoRead(true);
    }
  }

  @Override
  public Flux<ROW> rows() {
    return rows;
  }

  @Override
  public Mono<T> trailer() {
    return trailer.asMono();
  }

  @Override
  public void endOfInput() {
    if (decodingFailure != null) {
      return;
    }

    try {
      parser.endOfInput();

    } catch (DecodingFailureException e) {
      decodingFailure = e;
      failRows(e);
      failTrailer(e);
      return;
    }

    signalComplete();
  }

  @Override
  public Optional<CouchbaseException> decodingFailure() {
    return Optional.ofNullable(decodingFailure);
  }

  /**
   * Called when the JSON stream has been parsed completely and successfully.
   */
  protected abstract void signalComplete();

  /**
   * Emits a single row into the rows flux.
   *
   * <p>Note that this method also handles the backpressure stalling side. If we find that someone
   * is subscribed to this flux but has not requested any further rows, the channel auto-read
   * is going to be paused until further rows are requested or the subscriber unsubscribes.</p>
   *
   * @param row the row to emit.
   */
  protected void emitRow(final ROW row) {
    rowSink.emitNext(row, emitFailureHandler());

    synchronized (autoReadLock) {
      if (demand == Long.MAX_VALUE) {
        return;
      }

      demand--;

      if (demand <= 0 && channelConfig.isAutoRead()) {
        channelConfig.setAutoRead(false);
      }
    }
  }

  /**
   * Fails the row flux with the given message.
   *
   * @param t the throwable with which to fail the rows.
   */
  protected void failRows(Throwable t) {
    rowSink.emitError(t, emitFailureHandler());
  }

  /**
   * Completes the row flux.
   */
  protected void completeRows() {
    rowSink.emitComplete(emitFailureHandler());
  }

  /**
   * Fails the trailer mono with the given message.
   */
  private void failTrailer(Throwable t) {
    this.trailer.emitError(t, emitFailureHandler());
  }

  /**
   * Called from the child implementation to complete the trailing bits.
   *
   * @param trailer the trailer value to be fed into the mono.
   */
  protected void completeTrailer(T trailer) {
    this.trailer.emitValue(trailer, emitFailureHandler());
  }

}
