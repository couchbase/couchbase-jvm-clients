/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BlockingWaitGroupTest {

  private static BlockingWaitGroup createWaitGroup(
    BlockingWaitGroup.TimeoutExceptionFactory factory,
    BlockingWaitGroup.WaitGroupLogger logger,
    boolean debugMode
  ) {
    return new BlockingWaitGroup(factory, logger, debugMode);
  }

  private static BlockingWaitGroup createWaitGroup(
    BlockingWaitGroup.TimeoutExceptionFactory factory,
    BlockingWaitGroup.WaitGroupLogger logger
  ) {
    return createWaitGroup(factory, logger, false);
  }

  private static BlockingWaitGroup createWaitGroup() {
    return createWaitGroup(
      (msg, cause) -> new RuntimeException(msg, cause),
      null
    );
  }

  @Test
  void awaitingWithNoWaitingOperationsReturnsImmediately() {
    BlockingWaitGroup wg = createWaitGroup();
    wg.await(Duration.ofSeconds(1));
  }

  @Test
  void waitingCountReturnsZeroWhenNothingWaiting() {
    BlockingWaitGroup wg = createWaitGroup();
    assertEquals(0, wg.waitingCount());
  }

  @Test
  void addIncrementsWaitingCount() {
    BlockingWaitGroup wg = createWaitGroup();
    assertTrue(wg.add("operation1"));
    assertEquals(1, wg.waitingCount());
  }

  @Test
  void doneDecrementsWaitingCount() {
    BlockingWaitGroup wg = createWaitGroup();
    assertTrue(wg.add("operation1"));
    wg.done();
    assertEquals(0, wg.waitingCount());
  }

  @Test
  void doubleDoneThrows() {
    BlockingWaitGroup wg = createWaitGroup();
    assertTrue(wg.add("operation1"));
    wg.done();
    assertThrows(NoSuchElementException.class, () -> wg.done());
    assertEquals(0, wg.waitingCount());
  }

  @Test
  void multipleAddsIncrementWaitingCount() {
    BlockingWaitGroup wg = createWaitGroup();
    assertTrue(wg.add("operation1"));
    assertTrue(wg.add("operation2"));
    assertTrue(wg.add("operation3"));
    assertEquals(3, wg.waitingCount());
  }

  @Test
  void multipleDonesDecrementWaitingCount() {
    BlockingWaitGroup wg = createWaitGroup();
    assertTrue(wg.add("operation1"));
    assertTrue(wg.add("operation2"));
    assertTrue(wg.add("operation3"));
    wg.done();
    wg.done();
    assertEquals(1, wg.waitingCount());
  }

  @Test
  void doneWhenNothingWaitingThrows() {
    BlockingWaitGroup wg = createWaitGroup();
    assertThrows(NoSuchElementException.class, () -> wg.done());
  }

  @Test
  void closeAndAwaitRejectsNewOperations() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();
    assertTrue(wg.add("operation1"));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> closeFuture = executor.submit(() -> wg.closeAndAwait(Duration.ofSeconds(1)));

    Thread.sleep(50);
    assertFalse(wg.add("operation2"));

    wg.done();
    closeFuture.get(1, TimeUnit.SECONDS);
    executor.shutdown();
  }

  @Test
  void closeAndAwaitTimesOutAndStaysClosed() {
    BlockingWaitGroup wg = createWaitGroup();
    assertTrue(wg.add("operation1"));

    assertThrows(RuntimeException.class, () -> wg.closeAndAwait(Duration.ofMillis(50)));

    assertFalse(wg.add("operation2"));

    wg.done();
    assertEquals(0, wg.waitingCount());
  }

  @Test
  void awaitWaitsForSingleOperationToComplete() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();
    wg.add("operation1");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future = executor.submit(() -> {
      try {
        Thread.sleep(100);
        wg.done();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    wg.await(Duration.ofSeconds(1));
    future.get();
    executor.shutdown();
  }

  @Test
  void awaitWaitsForMultipleOperationsToComplete() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();
    wg.add("operation1");
    wg.add("operation2");
    wg.add("operation3");

    ExecutorService executor = Executors.newFixedThreadPool(3);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      futures.add(executor.submit(() -> {
        try {
          Thread.sleep(50);
          wg.done();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }));
    }

    wg.await(Duration.ofSeconds(1));

    for (Future<?> future : futures) {
      future.get();
    }
    executor.shutdown();
  }

  @Test
  void awaitBlocksUntilAllOperationsDone() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();
    wg.add("operation1");
    wg.add("operation2");

    AtomicBoolean awaitCompleted = new AtomicBoolean(false);
    Thread awaitThread = new Thread(() -> {
      wg.await(Duration.ofSeconds(5));
      awaitCompleted.set(true);
    });
    awaitThread.start();

    // Give awaitThread time to start blocking
    Thread.sleep(100);

    // await should not be completed yet
    assertFalse(awaitCompleted.get());

    // Complete operations
    wg.done();
    Thread.sleep(50);
    assertFalse(awaitCompleted.get());

    wg.done();
    awaitThread.join(1000);

    // Now await should be completed
    assertTrue(awaitCompleted.get());
  }

  @Test
  void awaitTimesOutWhenOperationsNotCompleted() {
    BlockingWaitGroup wg = createWaitGroup();
    wg.add("slow-operation");

    assertThrows(RuntimeException.class, () -> {
      wg.await(Duration.ofMillis(100));
    });
  }

  @Test
  void awaitInterruptedWhenThreadInterrupted() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();
    wg.add("operation");

    Thread awaitThread = new Thread(() -> {
      wg.await(Duration.ofSeconds(10));
    });
    awaitThread.start();

    // Give awaitThread time to start blocking
    Thread.sleep(100);

    // Interrupt the thread
    awaitThread.interrupt();
    awaitThread.join(1000);

    // Await thread should have completed due to interrupt
    assertFalse(awaitThread.isAlive());
  }

  @Test
  void concurrentAddsAndDones() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();
    int numOperations = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numOperations);

    ExecutorService executor = Executors.newFixedThreadPool(numOperations);

    // Create concurrent operations
    for (int i = 0; i < numOperations; i++) {
      final int opNum = i;
      executor.submit(() -> {
        try {
          // Wait until all operations are told to kick-off
          startLatch.await();
          wg.add("operation-" + opNum);
          Thread.sleep(10);
          wg.done();
          doneLatch.countDown();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    // Start off all operation
    startLatch.countDown();
    // Wait for them all to finish
    doneLatch.await(5, TimeUnit.SECONDS);

    // All operations should have unregistered and the await should return instantly
    assertEquals(0, wg.waitingCount());
    wg.closeAndAwait(Duration.ofSeconds(1));
    assertTrue(wg.isClosed());

    executor.shutdown();
  }

  @Test
  void timeoutExceptionFactoryIsInvokedOnTimeout() {
    List<String> capturedMessages = new ArrayList<>();
    List<TimeoutException> capturedCauses = new ArrayList<>();

    BlockingWaitGroup wg = createWaitGroup(
      (msg, cause) -> {
        capturedMessages.add(msg);
        capturedCauses.add(cause);
        return new CustomTimeoutException(msg, cause);
      },
      null
    );

    wg.add("operation1");

    CustomTimeoutException exception = assertThrows(CustomTimeoutException.class, () -> {
      wg.await(Duration.ofMillis(50));
    });

    assertEquals(1, capturedMessages.size());
    assertEquals(1, capturedCauses.size());
  }

  @Test
  void loggingIsCalledWhenDebugModeEnabled() {
    List<String> loggedMessages = new ArrayList<>();

    BlockingWaitGroup wg = createWaitGroup(
      (msg, cause) -> new RuntimeException(msg, cause),
      (format, args) -> loggedMessages.add(String.format(format, args)),
      true
    );

    wg.add("operation1");
    wg.done();

    assertFalse(loggedMessages.isEmpty(), "Should have logged messages in debug mode");
  }

  @Test
  void loggingNotCalledWhenDebugModeDisabled() {
    List<String> loggedMessages = new ArrayList<>();

    BlockingWaitGroup wg = createWaitGroup(
      (msg, cause) -> new RuntimeException(msg, cause),
      (format, args) -> loggedMessages.add(String.format(format, args)),
      false
    );

    wg.add("operation1");
    wg.done();

    assertEquals(0, loggedMessages.size(), "Should not log when debug mode is disabled");
  }

  @Test
  void awaitWithZeroTimeoutIsValid() {
    BlockingWaitGroup wg = createWaitGroup();
    wg.add("operation");

    assertThrows(RuntimeException.class, () -> wg.await(Duration.ZERO));
  }

  @Test
  void multipleAwaitCycles() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();

    // First cycle
    wg.add("op1");
    wg.add("op2");
    Thread t1 = new Thread(() -> {
      try {
        Thread.sleep(50);
        wg.done();
        wg.done();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    t1.start();
    wg.await(Duration.ofSeconds(1));
    t1.join();

    assertEquals(0, wg.waitingCount());

    // Second cycle
    wg.add("op3");
    wg.add("op4");
    Thread t2 = new Thread(() -> {
      try {
        Thread.sleep(50);
        wg.done();
        wg.done();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    t2.start();
    wg.await(Duration.ofSeconds(1));
    t2.join();

    assertEquals(0, wg.waitingCount());
  }

  @Test
  void stressTestConcurrentOperations() throws Exception {
    BlockingWaitGroup wg = createWaitGroup();
    int numThreads = 20;
    int operationsPerThread = 50;
    AtomicInteger completedCount = new AtomicInteger(0);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();

          for (int j = 0; j < operationsPerThread; j++) {
            wg.add("op-" + Thread.currentThread().getId() + "-" + j);
            // Simulate some work
            Thread.sleep(1);
            wg.done();
            completedCount.incrementAndGet();
          }

          completionLatch.countDown();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    startLatch.countDown();
    completionLatch.await(10, TimeUnit.SECONDS);

    // Ensure all operations completed
    assertEquals(numThreads * operationsPerThread, completedCount.get());

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  static class CustomTimeoutException extends RuntimeException {
    public CustomTimeoutException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
