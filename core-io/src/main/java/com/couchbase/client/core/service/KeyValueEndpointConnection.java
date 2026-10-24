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

package com.couchbase.client.core.service;

import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.SortedMap;
import java.util.concurrent.TimeoutException;

/**
 * A KV connection, as {@link SocketKeyValueEndpoint} sees it: either a {@link KeyValueConnection} (a reader thread
 * and a writer thread per connection), or a {@link NioKeyValueConnection} (on a shared event loop).
 * <p>
 * Thread-safe.
 */
@NullMarked
interface KeyValueEndpointConnection {

  /**
   * What the connection tells its endpoint.
   */
  interface Listener {
    /**
     * Called for every response the connection reads.
     */
    void responseReceived();

    /**
     * Called for each request that was queued, but never written, when the connection closed.
     */
    void writeFailed(KeyValueRequest<? extends Response> request, Throwable cause);

    /**
     * Called once, after a connection that was {@linkplain #start started} closes, and its requests were retried.
     *
     * @param requestsInFlight how many requests were written, and waiting for their responses, when it closed.
     */
    void closed(KeyValueEndpointConnection connection, int requestsInFlight);
  }

  /**
   * One attempt to open a connection: connects, and does the handshake.
   * {@link #open} blocks the caller; {@link #close} (from another thread) stops the attempt.
   */
  interface Opener {
    /**
     * Connects, and does the handshake. Blocks until it's done. Call once.
     *
     * @param deadlineNanos when to give up, in {@link System#nanoTime()} terms.
     * @return the open connection, ready to {@linkplain KeyValueEndpointConnection#start start}.
     * @throws TimeoutException if the deadline passes.
     */
    KeyValueEndpointConnection open(long deadlineNanos, Listener listener) throws IOException, TimeoutException;

    /**
     * Stops the attempt (if it's still going).
     */
    void close();

    /**
     * How long each connect step took so far, keyed like the Netty implementation's connect timings.
     */
    SortedMap<String, Duration> timings();
  }

  /**
   * The connection's ID, as sent in the HELLO request.
   */
  String channelId();

  /**
   * The connection's ID in short form (hex digits, without a "0x" prefix), like a Netty channel ID's short form.
   */
  String shortChannelId();

  /**
   * How long each connect step took, keyed like the Netty implementation's connect timings.
   */
  SortedMap<String, Duration> timings();

  SocketAddress localAddress();

  SocketAddress remoteAddress();

  boolean isClosed();

  /**
   * Returns false if the connection has too many bytes waiting to be written (like Netty's channel writability,
   * with the same high and low water marks).
   */
  boolean isWritable();

  /**
   * The number of requests written that are waiting for their responses.
   */
  int inFlightCount();

  /**
   * Starts reading and writing. Call once.
   */
  void start();

  /**
   * Queues the request to be written.
   *
   * @return false if the connection is closed, so the request wasn't sent (the caller should retry it).
   */
  boolean send(KeyValueRequest<? extends Response> request);

  /**
   * Closes the connection (if it isn't already). The requests it was sending are retried, then the listener is told.
   */
  void close();
}
