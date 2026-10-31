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

package com.couchbase.client.core.io.kv;

import com.couchbase.client.core.io.kv.KeyValueOutboundQueue.Outbound;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class KeyValueOutboundQueueTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  @SuppressWarnings("unchecked")
  private static Outbound outbound(int size) {
    return new Outbound((KeyValueRequest<Response>) mock(KeyValueRequest.class), size);
  }

  @Test
  void isNotWritableAboveTheHighWaterMarkUntilBelowTheLowWaterMark() {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    Outbound first = outbound(60);
    Outbound second = outbound(60);

    assertTrue(queue.add(first));
    assertTrue(queue.isWritable());
    assertTrue(queue.add(second)); // 120 bytes
    assertFalse(queue.isWritable());

    queue.written(queue.poll()); // 60: not below the low water mark yet
    assertFalse(queue.isWritable());
    queue.written(queue.poll()); // 0
    assertTrue(queue.isWritable());
  }

  /**
   * Once everything is written, the queue is writable, however senders and the writer interleave. (Otherwise the
   * endpoint would refuse every request. It holds because a sender makes the queue unwritable before queueing its
   * own request, so writing that request comes later, and makes it writable again.)
   */
  @Test
  void isWritableOnceEverythingIsWrittenEvenWhenSendersRaceTheWriter() throws Exception {
    for (int round = 0; round < 200; round++) {
      KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
      int senders = 4;
      CountDownLatch start = new CountDownLatch(1);
      Thread[] threads = new Thread[senders];
      for (int i = 0; i < senders; i++) {
        threads[i] = new Thread(() -> {
          try {
            start.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          for (int n = 0; n < 1_000; n++) {
            Outbound outbound = outbound(60);
            queue.add(outbound);
            queue.written(outbound); // as if the writer wrote it at once
          }
        });
        threads[i].start();
      }
      start.countDown();
      for (Thread t : threads) {
        t.join(TIMEOUT.toMillis());
      }
      assertTrue(queue.isWritable(), "not writable with nothing left to write, in round " + round);
    }
  }

  @Test
  void drainReturnsWhatWasNeverWrittenAndThenRefusesRequests() {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    Outbound unwritten = outbound(10);
    queue.add(unwritten);

    assertEquals(singletonList(unwritten), queue.drain());

    Outbound late = outbound(10);
    assertFalse(queue.add(late));
    assertNull(queue.poll(), "a refused request isn't left in the queue");
  }
}
