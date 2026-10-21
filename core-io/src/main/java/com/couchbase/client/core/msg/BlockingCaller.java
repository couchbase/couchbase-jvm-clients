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

import com.couchbase.client.core.Timer;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.CoreAsyncUtils;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * A thread blocked waiting for a request's result, which does the request's work itself
 * instead of waiting for other threads to do it.
 * <p>
 * Without one, a blocking call starts a request and waits: another thread sends the request and reads
 * the response (for example, an OkHttp dispatcher thread), and if the request has to be retried,
 * the {@link Timer} waits out the retry delay and sends it again from its own thread. With one:
 * <ul>
 *   <li>A service that can sends the request and reads the response on the caller's thread
 *       ({@link #isCurrentThread}).
 *   <li>The timer hands retries to the caller ({@link #offerRetry}), which waits out the delay
 *       and sends the request again, in {@link #await}.
 * </ul>
 * Nothing else changes: the retry strategy still decides whether and when to retry, retries still count
 * towards the limit on requests in retry, and the timer still enforces the request timeout
 * (cancelling the request completes the result, which wakes the caller).
 * <p>
 * The result can take more than one request: for example, a prepared query sends a request to prepare
 * (or execute) the statement, made from the original request, which isn't sent itself. Each request this caller does
 * the work of is {@linkplain #attach attached} to it, and the latest is its current request: the one whose work
 * it's doing, or waiting for.
 * <p>
 * Interrupting the caller stops the work: the current request is cancelled
 * ({@link CancellationReason#STOPPED_LISTENING}), so it isn't retried, and the caller gets the same exception
 * as an interrupted {@link CoreAsyncUtils#block}. The caller notices the interrupt while it waits (for a retry delay
 * to elapse, or for a result another thread completes), and when the request's work it was doing returns. That work
 * stops soon after the interrupt: interrupting a virtual thread closes the socket it's reading, and for a platform
 * thread, whose socket reads can't be interrupted, the service has a watcher cancel the request,
 * which closes the connection.
 * <p>
 * Nothing carries on after the caller stops waiting: the current request is done by then (if it isn't, for example
 * because the caller is leaving with an {@link Error}, it's cancelled), a retry still pending is dropped, and a request
 * attached afterwards (for example, by a retry of a prepared query running on another thread) is cancelled.
 * <p>
 * A caller that processes a streamed result's rows as they arrive can also give the service a
 * {@linkplain #rowAction row action}. A service reading the response on the caller's thread passes each row
 * straight to it, instead of handing the rows to the caller through a stream (which, with the caller as the only
 * thread, would wait forever for the caller to take them). So the action runs while the response is being read:
 * reading pauses while it runs.
 */
@Stability.Internal
@NullMarked
public final class BlockingCaller {

  /**
   * A retry handed to the caller.
   */
  public interface Retry {
    /**
     * Sends the request again. Called on the caller's thread, once the retry delay has elapsed.
     */
    void run();

    /**
     * Called instead of {@link #run} if the caller stops waiting first (when the request is done):
     * forgets the retry.
     */
    void drop();
  }

  /**
   * Thrown by {@link #acceptRow} when the row action throws, to stop reading the response.
   * The action's exception is {@linkplain #rowActionFailure kept by the caller}.
   */
  public static final class RowActionException extends RuntimeException {
    private RowActionException() {
      super("The row action threw an exception", null, false, false);
    }
  }

  private final Thread thread = Thread.currentThread();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();

  /**
   * Set to false when the caller stops waiting, before it checks {@link #current}; {@link #attach} sets
   * {@link #current} before checking this. So a request attached as the caller stops waiting
   * is cancelled by one of them (or both, which is harmless).
   */
  private volatile boolean waiting = true;

  /**
   * The latest request attached to this caller, or null if none is.
   */
  private volatile @Nullable Request<?> current;

  // Guarded by lock.
  private @Nullable Retry retry;
  private long retryAtNanos;

  // Set before the request is sent, and otherwise only used on the caller's thread.
  private @Nullable Consumer<Object> rowAction;
  private @Nullable Throwable rowActionFailure;

  /**
   * Whether the rows of the latest response that arrived went to the row action. Set by whichever thread reads
   * the response, before it completes the request, so the caller sees it once it has the result.
   */
  private volatile boolean rowsWentToAction;

  /**
   * Sets the action a service reading a streamed response on the caller's thread passes the rows to.
   * Call before sending the request.
   *
   * @param action accepts rows of the type the request's response streams
   */
  @SuppressWarnings("unchecked")
  public void rowAction(Consumer<?> action) {
    this.rowAction = (Consumer<Object>) action;
  }

  /**
   * Returns true if a service reading a streamed response on this thread should pass the rows to
   * {@link #acceptRow}: there's a row action, this is the caller's thread, and the caller is still waiting.
   */
  public boolean takesRowsOnCurrentThread() {
    return rowAction != null && isCurrentThread();
  }

  /**
   * Passes a row to the row action. Only call if {@link #takesRowsOnCurrentThread} returned true.
   *
   * @throws RowActionException if the action threw (which the caller keeps; see {@link #rowActionFailure}).
   * Stop reading the response, and end the rows with this exception.
   */
  public void acceptRow(Object row) {
    try {
      requireNonNull(rowAction).accept(row);
    } catch (Throwable t) {
      rowActionFailure = t;
      throw new RowActionException();
    }
  }

  /**
   * Returns the exception the row action threw, if it threw one.
   */
  public @Nullable Throwable rowActionFailure() {
    return rowActionFailure;
  }

  /**
   * Records whether a response's rows go to the row action (true), or arrive through the response's stream of rows
   * (false, because another thread is reading the response). A service reading a streamed response calls this
   * when the response arrives, before completing the request with it.
   */
  public void rowsWentToAction(boolean wentToAction) {
    this.rowsWentToAction = wentToAction;
  }

  /**
   * Returns true if the rows of the response the caller got went to the row action. Then the response's stream
   * of rows had already ended (without any rows) when the caller stopped waiting, since the caller's thread
   * read the whole response first. Call once the caller stopped waiting.
   */
  public boolean rowsWentToAction() {
    return rowsWentToAction;
  }

  /**
   * Makes the request this caller's current request: the one whose work it does
   * (for example, a request to execute a prepared statement, made from the original request).
   * {@link BaseHttpRequest#blockingCaller(BlockingCaller)} calls this. Call before sending the request.
   * <p>
   * If the caller already stopped waiting, cancels the request ({@link CancellationReason#STOPPED_LISTENING}),
   * since no one would see what became of it.
   */
  public void attach(Request<?> request) {
    current = request;
    if (!waiting) {
      request.cancel(CancellationReason.STOPPED_LISTENING);
    }
  }

  /**
   * Returns true if called on the caller's thread while it's waiting for the result.
   */
  public boolean isCurrentThread() {
    return Thread.currentThread() == thread && waiting;
  }

  /**
   * Hands a retry to the caller, to run once the delay has elapsed.
   *
   * @return false if the caller isn't waiting anymore (or already has a retry to run),
   * in which case the retry must be scheduled some other way.
   */
  public boolean offerRetry(Retry retry, Duration delay) {
    lock.lock();
    try {
      if (!waiting || this.retry != null) {
        return false;
      }
      this.retry = retry;
      this.retryAtNanos = System.nanoTime() + delay.toNanos();
      changed.signal();
      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Sends the request, then {@linkplain #await waits} for the result.
   * <p>
   * If sending throws, fails the current request with the exception (so it isn't left running unobserved)
   * and rethrows it, after dropping any retry the caller was given in the meantime, as {@link #await} would.
   *
   * @param send sends the request, and returns its result
   */
  public <T> T sendAndAwait(Supplier<CompletableFuture<T>> send) {
    CompletableFuture<T> result;
    try {
      result = send.get();
    } catch (RuntimeException | Error e) {
      Request<?> request = current;
      if (request != null) {
        request.fail(e);
      }
      stopWaiting();
      throw e;
    }
    return await(result);
  }

  /**
   * Waits for the result, running the retries handed to the caller in the meantime.
   * Must be called on the thread that created this caller, after it sent the request.
   * Prefer {@link #sendAndAwait}, which makes sure the caller stops waiting even if sending throws.
   * <p>
   * If the thread is interrupted (now, or while waiting), cancels the current request and throws
   * {@code RuntimeException(InterruptedException)}, like {@link CoreAsyncUtils#block}, with any failure
   * the result already had added as a suppressed exception. Unless the result had already succeeded:
   * then returns it, and leaves the thread interrupted.
   *
   * @return the result, or throws, like {@link CoreAsyncUtils#block}
   */
  public <T> T await(CompletableFuture<T> result) {
    if (Thread.currentThread() != thread) {
      throw new IllegalStateException("Must be called on the thread that created this caller");
    }
    result.whenComplete((value, error) -> wake());
    try {
      runRetriesUntilDone(result);
    } catch (InterruptedException e) {
      if (!result.isDone() || result.isCompletedExceptionally()) {
        throw cancelBecauseInterrupted(result, e);
      }
      Thread.currentThread().interrupt(); // Nothing to stop. Keep the interrupt, like future.get() would.
    } finally {
      stopWaiting();
    }
    return CoreAsyncUtils.block(result);
  }

  /**
   * Cancels the current request, and returns the exception to throw:
   * the same as {@link CoreAsyncUtils#block} throws when interrupted.
   */
  private RuntimeException cancelBecauseInterrupted(CompletableFuture<?> result, InterruptedException e) {
    // Todo: Thread.currentThread().interrupt(), along with the related change in CoreAsyncUtils.block()

    // Whatever the result failed with before the request was cancelled. Possibly caused by the interrupt:
    // for example, closing the connection a virtual thread was reading from, or failing the stream of rows.
    Throwable failure = null;
    if (result.isCompletedExceptionally()) {
      try {
        result.join();
      } catch (CompletionException | CancellationException t) {
        failure = t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
      }
    }

    Request<?> request = current;
    if (request != null) {
      request.cancel(CancellationReason.STOPPED_LISTENING);
    }

    if (failure != null) {
      e.addSuppressed(failure);
    }
    return new RuntimeException(e);
  }

  private void runRetriesUntilDone(CompletableFuture<?> result) throws InterruptedException {
    // Also checks for an interrupt that arrived while this thread was doing the request's work
    // (sending it the first time, before this method, or retrying it, below).
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    lock.lockInterruptibly();
    try {
      while (!result.isDone()) {
        Retry r = retry;
        if (r == null) {
          changed.await();
          continue;
        }
        long remaining = retryAtNanos - System.nanoTime();
        if (remaining > 0) {
          changed.awaitNanos(remaining);
          continue;
        }
        retry = null;
        lock.unlock();
        try {
          r.run(); // might hand another retry to this caller
        } catch (RuntimeException e) {
          // A bug (sending a request reports failures by completing it, not by throwing). Don't leave the request
          // running unobserved until it times out: fail it, so the caller gets the exception the usual way.
          Request<?> request = current;
          if (request != null) {
            request.fail(e);
          }
        } finally {
          lock.lock();
        }
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void wake() {
    lock.lock();
    try {
      changed.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Stops taking retries, and makes sure nothing carries on: cancels the current request if it isn't done
   * (and any request attached later), and drops a retry still pending.
   */
  private void stopWaiting() {
    Retry r;
    lock.lock();
    try {
      waiting = false;
      r = retry;
      retry = null;
    } finally {
      lock.unlock();
    }

    // Normally it's done by now (the caller waits until it is, or cancels or fails it before it stops waiting).
    // If it isn't (for example, the caller is leaving with an Error), no one would see what became of it.
    Request<?> request = current;
    if (request != null && !request.completed()) {
      request.cancel(CancellationReason.STOPPED_LISTENING);
    }
    if (r != null) {
      r.drop();
    }
  }

  @Override
  public String toString() {
    return "BlockingCaller{" +
      "thread=" + thread.getName() +
      ", waiting=" + waiting +
      '}';
  }
}
