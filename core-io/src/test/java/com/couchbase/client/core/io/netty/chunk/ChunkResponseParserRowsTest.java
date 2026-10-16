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

package com.couchbase.client.core.io.netty.chunk;

import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelConfig;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.io.netty.query.QueryChunkResponseParser;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rows flux of a chunked (streaming) response.
 */
class ChunkResponseParserRowsTest {

  private final EmbeddedChannel channel = new EmbeddedChannel();
  private final ChannelConfig channelConfig = channel.config();
  private final QueryChunkResponseParser parser = new QueryChunkResponseParser();

  @BeforeEach
  void setup() {
    parser.initialize(channelConfig);
  }

  @AfterEach
  void teardown() {
    parser.cleanup();
    channel.finishAndReleaseAll();
  }

  private void feed(String json) {
    parser.feed(Unpooled.copiedBuffer(json, UTF_8));
  }

  private void feedRestAndEnd() {
    feed("{\"n\":3}],\"status\":\"success\"}");
    parser.endOfInput();
  }

  @Test
  void secondSubscriberIsRejectedWhileRowsAreStreaming() {
    feed("{\"requestID\":\"abc\",\"results\":[{\"n\":1},");

    Recorder first = new Recorder(Long.MAX_VALUE);
    parser.rows().subscribe(first);
    feed("{\"n\":2},");

    Recorder second = new Recorder(Long.MAX_VALUE);
    parser.rows().subscribe(second);
    feedRestAndEnd();

    assertRejected(second);
    assertEquals(listOf("{\"n\":1}", "{\"n\":2}", "{\"n\":3}"), first.rows);
    assertTrue(first.completed);
  }

  @Test
  void secondSubscriberIsRejectedAfterTheFirstFinished() {
    feed("{\"requestID\":\"abc\",\"results\":[{\"n\":1},{\"n\":2},");
    feedRestAndEnd();

    Recorder first = new Recorder(Long.MAX_VALUE);
    parser.rows().subscribe(first);
    assertTrue(first.completed);

    Recorder second = new Recorder(Long.MAX_VALUE);
    parser.rows().subscribe(second);
    assertRejected(second);
  }

  @Test
  void rejectedSubscriberDoesNotTurnOffBackpressure() {
    feed("{\"requestID\":\"abc\",\"results\":[");

    Recorder first = new Recorder(1);
    parser.rows().subscribe(first);

    // Requests without limit, then fails; neither should affect the first subscriber's backpressure.
    Recorder second = new Recorder(Long.MAX_VALUE);
    parser.rows().subscribe(second);
    assertRejected(second);

    feed("{\"n\":1},{\"n\":2},");
    assertEquals(listOf("{\"n\":1}"), first.rows);
    assertFalse(channelConfig.isAutoRead(), "should stop reading once the first subscriber's demand is met");
  }

  private static void assertRejected(Recorder subscriber) {
    assertEquals(listOf(), subscriber.rows);
    assertInstanceOf(IllegalStateException.class, subscriber.error);
    assertTrue(subscriber.error.getMessage().contains("only once"), subscriber.error.getMessage());
  }

  private static final class Recorder extends BaseSubscriber<QueryChunkRow> {
    final long initialRequest;
    final List<String> rows = new CopyOnWriteArrayList<>();
    volatile boolean completed;
    volatile Throwable error;

    Recorder(long initialRequest) {
      this.initialRequest = initialRequest;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(initialRequest);
    }

    @Override
    protected void hookOnNext(QueryChunkRow row) {
      rows.add(new String(row.data(), UTF_8));
    }

    @Override
    protected void hookOnComplete() {
      completed = true;
    }

    @Override
    protected void hookOnError(Throwable t) {
      error = t;
    }
  }
}
