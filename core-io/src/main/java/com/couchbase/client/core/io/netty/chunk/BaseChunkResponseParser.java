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
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelConfig;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.json.stream.CopyingStreamWindow;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.chunk.ChunkHeader;
import com.couchbase.client.core.msg.chunk.ChunkRow;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

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
   * Scratch buffer reused by each stream parser.
   */
  private final ByteBuf scratchBuffer = Unpooled.unreleasableBuffer(Unpooled.buffer());

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
  private FluxProcessor<ROW, ROW> rows;

  /**
   * Holds the current associated row sink.
   */
  private FluxSink<ROW> rowSink;

  /**
   * Holds the currently user-requested rows for backpressure handling.
   */
  private final AtomicLong requested = new AtomicLong(0);

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
   * {@link ChunkResponseParser#header()} to see if the header is ready.
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
    if (decodingFailure != null) {
      return;
    }

    try {
      parser.feed(input);

    } catch (DecodingFailureException e) {
      decodingFailure = e;
      failRows(e);
      failTrailer(e);
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
    parser = parserBuilder().build(scratchBuffer, new CopyingStreamWindow(channelConfig.getAllocator()));
    this.channelConfig = channelConfig;
    this.trailer = Sinks.one();
    this.requested.set(0);
    this.rows = EmitterProcessor.create();
    this.rowSink = this.rows
      .sink(FluxSink.OverflowStrategy.BUFFER)
      .onRequest(v -> {
        requested.addAndGet(v);
        if (!channelConfig.isAutoRead()) {
          channelConfig.setAutoRead(true);
        }
      })
      .onDispose(() -> channelConfig.setAutoRead(true));
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
    rowSink.next(row);
    requested.decrementAndGet();
    if (requested.get() <= 0 && channelConfig.isAutoRead() && rows.downstreamCount() > 0) {
      channelConfig.setAutoRead(false);
    }
  }

  /**
   * Fails the row flux with the given message.
   *
   * @param t the throwable with which to fail the rows.
   */
  protected void failRows(Throwable t) {
    rowSink.error(t);
  }

  /**
   * Completes the row flux.
   */
  protected void completeRows() {
    rowSink.complete();
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
