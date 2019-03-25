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
import com.couchbase.client.core.deps.io.netty.channel.ChannelConfig;
import com.couchbase.client.core.msg.chunk.ChunkHeader;
import com.couchbase.client.core.msg.chunk.ChunkRow;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.EOFException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a common, abstract implementation of the {@link ChunkResponseParser} interface.
 */
public abstract class BaseChunkResponseParser<H extends ChunkHeader, ROW extends ChunkRow,
  T extends ChunkTrailer> implements ChunkResponseParser<H, ROW, T> {

  /**
   * Holds the parser created by the child.
   */
  private final ByteBufJsonParser parser;

  /**
   * Holds the current channel config.
   */
  private ChannelConfig channelConfig;

  /**
   * Holds the current associated trailer.
   */
  private MonoProcessor<T> trailer;

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
   * Creates a new {@link BaseChunkResponseParser}.
   */
  protected BaseChunkResponseParser() {
    this.parser = initParser();
  }

  /**
   * Child needs to implement this to return the "meat" of the decoding, the chunk parser.
   */
  protected abstract ByteBufJsonParser initParser();

  /**
   * Child can implement this to reset any state variables.
   */
  protected abstract void resetState();

  /**
   * Initializes the parser to a fresh state.
   *
   * @param content the content slice to use on parsing.
   * @param channelConfig the channel config used for backpressure auto-read.
   */
  @Override
  public void initialize(final ByteBuf content, final ChannelConfig channelConfig) {
    resetState();
    parser.initialize(content);
    this.channelConfig = channelConfig;
    this.trailer = MonoProcessor.create();
    this.requested.set(0);
    this.rows = EmitterProcessor.create();
    final Disposable disposable = () -> channelConfig.setAutoRead(true);
    this.rowSink = this.rows
      .sink(FluxSink.OverflowStrategy.BUFFER)
      .onRequest(v -> {
        requested.addAndGet(v);
        if (!channelConfig.isAutoRead()) {
          channelConfig.setAutoRead(true);
        }
      })
      .onCancel(disposable)
      .onDispose(disposable);
  }

  @Override
  public boolean parse() {
    try {
      parser.parse();
      return true;
    } catch (final EOFException ex) {
      return false;
    }
  }

  @Override
  public Flux<ROW> rows() {
    return rows;
  }

  @Override
  public Mono<T> trailer() {
    return trailer;
  }

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
   * Called from the child implementation to complete the trailing bits.
   *
   * @param trailer the trailver value to be fed into the mono.
   */
  protected void completeTrailer(T trailer) {
    this.trailer.onNext(trailer);
    this.trailer.onComplete();
  }

}
