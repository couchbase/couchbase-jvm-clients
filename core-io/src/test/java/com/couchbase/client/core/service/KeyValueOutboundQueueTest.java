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

import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.service.KeyValueOutboundQueue.Outbound;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueOutboundQueueTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static Outbound outbound(int size) {
    return new Outbound(null, Unpooled.buffer(size).writeZero(size));
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
    queue.close();

    assertEquals(singletonList(unwritten), queue.drain()); // not CLOSE

    Outbound late = outbound(10);
    assertFalse(queue.add(late));
    assertNull(queue.poll(), "a refused request isn't left in the queue");
  }

  @Test
  void writerTakesCloseOnceItHasTakenWhatsAheadOfIt() {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    Outbound first = outbound(10);
    queue.add(first);
    queue.close();

    assertSame(first, queue.poll());
    assertSame(KeyValueOutboundQueue.CLOSE, queue.poll());
  }

  @Test
  void addingWakesTheParkedWriter() throws Exception {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    Thread writer = startWriter(queue);

    queue.add(outbound(10));

    writer.join(TIMEOUT.toMillis());
    assertFalse(writer.isAlive(), "writer should have woken");
  }

  @Test
  void closingWakesTheParkedWriter() throws Exception {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    Thread writer = startWriter(queue);

    queue.close();

    writer.join(TIMEOUT.toMillis());
    assertFalse(writer.isAlive(), "writer should have woken");
  }

  @Test
  @Timeout(10) // interrupts the test thread, so a park returns (and the test fails)
  void writerDoesNotWaitWhenThereIsSomethingToWrite() {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    queue.writerStarted();
    queue.add(outbound(10));

    queue.awaitRequests(); // returns at once: there's a request to write
  }

  @Test
  void requestsSentByTheReaderDuringABatchWakeTheWriterWhenTheBatchEnds() throws Exception {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    Thread writer = startWriter(queue);

    CountDownLatch added = new CountDownLatch(1);
    CountDownLatch endBatch = new CountDownLatch(1);
    Thread reader = new Thread(() -> {
      queue.readerStarted();
      queue.beginReadBatch();
      queue.add(outbound(10)); // from a response callback, say
      added.countDown();
      try {
        endBatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      queue.endReadBatch();
    });
    reader.start();

    assertTrue(added.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    Thread.sleep(100);
    assertTrue(writer.isAlive(), "writer shouldn't wake until the batch ends");

    endBatch.countDown();
    writer.join(TIMEOUT.toMillis());
    assertFalse(writer.isAlive(), "writer should have woken");
    reader.join(TIMEOUT.toMillis());
  }

  @Test
  void requestsSentByOtherThreadsDuringTheReadersBatchWakeTheWriter() throws Exception {
    KeyValueOutboundQueue queue = new KeyValueOutboundQueue(100, 50);
    Thread writer = startWriter(queue);

    CountDownLatch inBatch = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(1);
    Thread reader = new Thread(() -> {
      queue.readerStarted();
      queue.beginReadBatch();
      inBatch.countDown();
      try {
        done.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    reader.start();
    assertTrue(inBatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

    queue.add(outbound(10)); // not the reader thread

    writer.join(TIMEOUT.toMillis());
    assertFalse(writer.isAlive(), "writer should have woken");
    done.countDown();
    reader.join(TIMEOUT.toMillis());
  }

  /**
   * Starts a thread that waits for requests (with no time limit), and returns once it's waiting.
   */
  private static Thread startWriter(KeyValueOutboundQueue queue) throws InterruptedException {
    Thread writer = new Thread(() -> {
      queue.writerStarted();
      queue.awaitRequests();
    });
    writer.setDaemon(true);
    writer.start();
    waitUntil(() -> writer.getState() == Thread.State.WAITING);
    return writer;
  }

  private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Condition not met within " + TIMEOUT);
      }
      Thread.sleep(1);
    }
  }
}
