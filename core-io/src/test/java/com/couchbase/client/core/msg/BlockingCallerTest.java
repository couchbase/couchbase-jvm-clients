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

package com.couchbase.client.core.msg;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(10)
class BlockingCallerTest {

  private final ScheduledExecutorService otherThread = Executors.newSingleThreadScheduledExecutor();
  private final Request<?> request = mock(Request.class);

  BlockingCallerTest() {
    // A real request is done by the time its caller stops waiting.
    when(request.completed()).thenReturn(true);
  }

  @AfterEach
  void teardown() {
    otherThread.shutdownNow();
    Thread.interrupted(); // in case a test left this thread interrupted
  }

  /**
   * Records where it ran, or that it was dropped.
   */
  private static final class RecordingRetry implements BlockingCaller.Retry {
    final List<Thread> ranOn = new CopyOnWriteArrayList<>();
    final Runnable action;
    volatile boolean dropped;

    RecordingRetry(Runnable action) {
      this.action = action;
    }

    @Override
    public void run() {
      ranOn.add(Thread.currentThread());
      action.run();
    }

    @Override
    public void drop() {
      dropped = true;
    }
  }

  @Test
  void returnsTheResultCompletedByAnotherThread() {
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    otherThread.schedule(() -> result.complete("done"), 50, TimeUnit.MILLISECONDS);

    assertEquals("done", caller.await(request, result));
  }

  @Test
  void throwsTheFailureLikeABlockingCall() {
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    otherThread.schedule(() -> result.completeExceptionally(new IllegalStateException("oops")), 50, TimeUnit.MILLISECONDS);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> caller.await(request, result));
    assertEquals("oops", e.getMessage());
  }

  @Test
  void runsARetryFromAnotherThreadOnTheCallersThreadOnceItsDelayHasElapsed() {
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    RecordingRetry retry = new RecordingRetry(() -> result.complete("retried"));
    AtomicBoolean accepted = new AtomicBoolean();

    long start = System.nanoTime();
    otherThread.execute(() -> accepted.set(caller.offerRetry(retry, Duration.ofMillis(50))));

    assertEquals("retried", caller.await(request, result));
    assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(50));
    assertTrue(accepted.get());
    assertEquals(singletonList(Thread.currentThread()), retry.ranOn);
    assertFalse(retry.dropped);
  }

  @Test
  void runsRetriesTheCallerOffersItself() {
    // Like a request sent on the caller's thread that has to be retried, twice.
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    RecordingRetry second = new RecordingRetry(() -> result.complete("retried twice"));
    RecordingRetry first = new RecordingRetry(() -> assertTrue(caller.offerRetry(second, Duration.ofMillis(1))));
    assertTrue(caller.offerRetry(first, Duration.ofMillis(1)));

    assertEquals("retried twice", caller.await(request, result));
    assertEquals(singletonList(Thread.currentThread()), first.ranOn);
    assertEquals(singletonList(Thread.currentThread()), second.ranOn);
  }

  @Test
  void takesOneRetryAtATime() {
    BlockingCaller caller = new BlockingCaller();
    assertTrue(caller.offerRetry(new RecordingRetry(() -> {
    }), Duration.ofSeconds(1)));
    assertFalse(caller.offerRetry(new RecordingRetry(() -> {
    }), Duration.ofSeconds(1)));
  }

  @Test
  void dropsAPendingRetryIfTheResultArrivesFirst() {
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    RecordingRetry retry = new RecordingRetry(() -> {
    });
    assertTrue(caller.offerRetry(retry, Duration.ofSeconds(30)));
    otherThread.schedule(() -> result.complete("done"), 50, TimeUnit.MILLISECONDS);

    assertEquals("done", caller.await(request, result));
    assertTrue(retry.ranOn.isEmpty());
    assertTrue(retry.dropped);

    // The caller isn't waiting anymore, so retries must be scheduled some other way.
    assertFalse(caller.offerRetry(new RecordingRetry(() -> {
    }), Duration.ZERO));
  }

  @Test
  void interruptWhileWaitingCancelsTheRequest() {
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    RecordingRetry retry = new RecordingRetry(() -> {
    });
    assertTrue(caller.offerRetry(retry, Duration.ofSeconds(30)));
    Thread callerThread = Thread.currentThread();
    otherThread.schedule(callerThread::interrupt, 50, TimeUnit.MILLISECONDS);

    RuntimeException e = assertThrows(RuntimeException.class, () -> caller.await(request, result));

    // Like CoreAsyncUtils.block, which also consumes the interrupt.
    InterruptedException interrupted = assertInstanceOf(InterruptedException.class, e.getCause());
    assertEquals(0, interrupted.getSuppressed().length);
    assertFalse(Thread.currentThread().isInterrupted());

    verify(request).cancel(CancellationReason.STOPPED_LISTENING);
    assertTrue(retry.ranOn.isEmpty());
    assertTrue(retry.dropped);
  }

  @Test
  void interruptDuringTheRequestsWorkCancelsTheRequestAndReportsTheFailureItCaused() {
    // For example, the interrupt closed the connection a virtual thread was reading the response from.
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    Thread.currentThread().interrupt();
    result.completeExceptionally(new IllegalStateException("connection closed"));

    RuntimeException e = assertThrows(RuntimeException.class, () -> caller.await(request, result));

    InterruptedException interrupted = assertInstanceOf(InterruptedException.class, e.getCause());
    assertEquals(1, interrupted.getSuppressed().length);
    assertEquals("connection closed", interrupted.getSuppressed()[0].getMessage());
    assertFalse(Thread.currentThread().isInterrupted());
    verify(request).cancel(CancellationReason.STOPPED_LISTENING);
  }

  @Test
  void interruptDuringARetryCancelsTheRequest() {
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    RecordingRetry retry = new RecordingRetry(() -> Thread.currentThread().interrupt()); // the result is still pending
    assertTrue(caller.offerRetry(retry, Duration.ofMillis(1)));

    RuntimeException e = assertThrows(RuntimeException.class, () -> caller.await(request, result));

    assertInstanceOf(InterruptedException.class, e.getCause());
    assertEquals(singletonList(Thread.currentThread()), retry.ranOn);
    verify(request).cancel(CancellationReason.STOPPED_LISTENING);
  }

  @Test
  void interruptAfterTheResultSucceededReturnsTheResult() {
    BlockingCaller caller = new BlockingCaller();
    Thread.currentThread().interrupt();

    assertEquals("done", caller.await(request, CompletableFuture.completedFuture("done")));

    // Like future.get() on a completed future.
    assertTrue(Thread.interrupted(), "should leave the thread interrupted");
    verify(request, never()).cancel(any(CancellationReason.class));
  }

  @Test
  void isCurrentThreadOnlyOnTheCallersThreadWhileItsWaiting() throws Exception {
    BlockingCaller caller = new BlockingCaller();
    assertTrue(caller.isCurrentThread());

    ExecutorService another = Executors.newSingleThreadExecutor();
    try {
      assertFalse(another.submit(caller::isCurrentThread).get());
    } finally {
      another.shutdownNow();
    }

    caller.await(request, CompletableFuture.completedFuture("done"));
    assertFalse(caller.isCurrentThread());
  }

  @Test
  void awaitMustBeCalledOnTheCallersThread() throws Exception {
    BlockingCaller caller = new BlockingCaller();
    ExecutorService another = Executors.newSingleThreadExecutor();
    try {
      Throwable t = assertThrows(Exception.class,
        () -> another.submit(() -> caller.await(request, CompletableFuture.completedFuture("done"))).get()).getCause();
      assertInstanceOf(IllegalStateException.class, t);
    } finally {
      another.shutdownNow();
    }
  }

  @Test
  void retryThatThrowsFailsTheRequest() {
    // Sending a request reports failures by completing it, not by throwing; throwing is a bug.
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    doAnswer(invocation -> result.completeExceptionally(invocation.getArgument(0))).when(request).fail(any());
    IllegalStateException bug = new IllegalStateException("bug");
    assertTrue(caller.offerRetry(new RecordingRetry(() -> {
      throw bug;
    }), Duration.ofMillis(1)));

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> caller.await(request, result));
    assertEquals("bug", e.getMessage());
    verify(request).fail(bug);
  }

  @Test
  void sendAndAwaitReturnsTheResult() {
    BlockingCaller caller = new BlockingCaller();
    assertEquals("done", caller.sendAndAwait(request, () -> CompletableFuture.completedFuture("done")));
    assertFalse(caller.isCurrentThread());
  }

  @Test
  void sendThatThrowsFailsTheRequestAndDropsARetry() {
    BlockingCaller caller = new BlockingCaller();
    RecordingRetry retry = new RecordingRetry(() -> {
    });
    IllegalStateException bug = new IllegalStateException("bug");

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> caller.sendAndAwait(request, () -> {
      assertTrue(caller.offerRetry(retry, Duration.ofSeconds(30))); // like a request that had to be retried
      throw bug;
    }));

    assertSame(bug, e);
    verify(request).fail(bug);
    assertTrue(retry.dropped);
    assertFalse(caller.offerRetry(new RecordingRetry(() -> {
    }), Duration.ZERO), "should stop waiting");
  }

  @Test
  void stopsWaitingWithAnErrorByCancellingTheRequestAndDroppingAPendingRetry() {
    Request<?> unfinished = mock(Request.class);
    BlockingCaller caller = new BlockingCaller();
    CompletableFuture<String> result = new CompletableFuture<>();
    RecordingRetry pending = new RecordingRetry(() -> {
    });
    RecordingRetry fatal = new RecordingRetry(() -> {
      assertTrue(caller.offerRetry(pending, Duration.ofSeconds(30)));
      throw new StackOverflowError("simulated");
    });
    assertTrue(caller.offerRetry(fatal, Duration.ofMillis(1)));

    assertThrows(StackOverflowError.class, () -> caller.await(unfinished, result));

    // Nothing carries on: no one would see what became of it.
    verify(unfinished).cancel(CancellationReason.STOPPED_LISTENING);
    assertTrue(pending.ranOn.isEmpty());
    assertTrue(pending.dropped);
  }
}
