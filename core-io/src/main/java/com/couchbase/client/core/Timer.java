/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.TimerConfig;
import com.couchbase.client.core.msg.BaseHttpRequest;
import com.couchbase.client.core.msg.BlockingCaller;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link Timer} acts as the main timing facility for various operations, for
 * example to track and time out requests if they run for too long or for rescheduling needs.
 *
 * @since 2.0.0
 */
@NullMarked
@Stability.Internal
public class Timer {

  /**
   * A task scheduled to run later, which can be cancelled until it runs.
   */
  @FunctionalInterface
  public interface Registration {
    /**
     * Cancels the task, unless it already ran (or started running), or was cancelled.
     *
     * @return true if this cancelled the task
     */
    boolean cancel();
  }

  /**
   * The internal timers.
   */
  private final List<TimerWheel> wheelTimers;
  private final AtomicLong currentTimer = new AtomicLong();

  /**
   * Set to true once stopped.
   */
  private volatile boolean stopped = false;

  /**
   * Number of requests currently outstanding for retry.
   */
  private final AtomicLong outstandingForRetry = new AtomicLong(0);

  /**
   * The maximum number of outstanding operations until backpressure kicks in.
   */
  private final long maxNumRequestsInRetry;

  /**
   * Creates a new {@link Timer} with default values.
   *
   * @param maxNumRequestsInRetry the maximum number of requests in retry allowed before backpressure hits.
   * @return the created timer.
   */
  public static Timer create(final long maxNumRequestsInRetry, TimerConfig timerConfig) {
    return new Timer(maxNumRequestsInRetry, timerConfig);
  }

  /**
   * Creates and starts a timer with default values.
   *
   * @param maxNumRequestsInRetry the maximum number of requests in retry allowed before backpressure hits.
   * @return the created and started timer.
   */
  public static Timer createAndStart(final long maxNumRequestsInRetry) {
    Timer timer = create(maxNumRequestsInRetry, TimerConfig.create());
    timer.start();
    return timer;
  }

  public static Timer createAndStart(final long maxNumRequestsInRetry, TimerConfig timerConfig) {
    Timer timer = create(maxNumRequestsInRetry, timerConfig);
    timer.start();
    return timer;
  }

  /**
   * Internal timer constructor.
   *
   * @param maxNumRequestsInRetry the maximum number of requests in retry allowed before backpressure hits.
   */
  private Timer(final long maxNumRequestsInRetry, TimerConfig timerConfig) {
    this.maxNumRequestsInRetry = maxNumRequestsInRetry;
    wheelTimers = new ArrayList<>();
    for (int i = 0; i < timerConfig.numTimers(); i ++) {
      TimerWheel wheelTimer = new TimerWheel(
        timerConfig.tickDuration(),
        timerConfig.numBuckets(),
        // Preserve original "cb-timer" name if only using default single timer.
        "cb-timer" + (timerConfig.numTimers() == 1 ? "" : "-" + i)
      );
      wheelTimers.add(wheelTimer);
    }
  }

  /**
   * Schedules a request to be retried after the given duration.
   * <p>
   * Note that this operation performs backpressure handling for the SDK by doing account towards a maximum outstanding
   * request limit!
   * <p>
   * If a thread is blocked waiting for the request and does the request's work itself
   * ({@link BaseHttpRequest#blockingCaller()}), that thread waits out the delay and retries, instead of this timer.
   *
   * @param core the core to eventually retry against.
   * @param request the request to retry.
   * @param runAfter the duration after which to retry.
   */
  public void scheduleForRetry(final Core core, final Request<? extends Response> request, final Duration runAfter) {
    if (stopped) {
      request.cancel(CancellationReason.SHUTDOWN);
      return;
    }

    if (outstandingForRetry.get() >= maxNumRequestsInRetry) {
      request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY);
      return;
    }

    outstandingForRetry.incrementAndGet();
    RetryTask task = new RetryTask(core, request);
    BlockingCaller caller = request instanceof BaseHttpRequest ? ((BaseHttpRequest<?>) request).blockingCaller() : null;
    if (caller != null && caller.offerRetry(task, runAfter)) {
      return;
    }
    task.schedule(runAfter);
  }

  public @Nullable Registration schedule(final Runnable callback, final Duration runAfter) {
    return schedule(callback, runAfter, false);
  }

  /**
   * Schedule an arbitrary task for this timer.
   *
   * @param respectMax whether maxNumRequestsInRetry should be respected.  Will return null if the operation was not scheduled for this reason.
   * @return the scheduled timeout, or null if the task was not scheduled because the timer is stopped
   * or (when {@code respectMax} is true) too many requests are in retry.
   */
  public @Nullable Registration schedule(final Runnable callback, final Duration runAfter, boolean respectMax) {
    if (stopped) {
      return null;
    }

    if (outstandingForRetry.incrementAndGet() >= maxNumRequestsInRetry) {
      if (respectMax) {
        outstandingForRetry.getAndDecrement();
        return null;
      }
    }

    return timer().newTimeout(() -> {
      outstandingForRetry.decrementAndGet();
      callback.run();
    }, runAfter.toNanos(), TimeUnit.NANOSECONDS);
  }

  private TimerWheel timer() {
    // Using a long for currentTimer permits 100,000 timeouts/retries per second for 9.223372e+13 seconds, or
    // nearly 3 million years.
    return wheelTimers.get((int) (currentTimer.getAndIncrement() % wheelTimers.size()));
  }

  /**
   * Registers the given request to be tracked with its timeout value.
   *
   * @param request the request to track.
   */
  public  void register(final Request<Response> request) {
    if (stopped) {
      request.cancel(CancellationReason.SHUTDOWN);
      return;
    }

    final Registration registration = scheduleRequestTask(new TimeoutTask(request), request.timeout());
    if (registration != null) {
      request.timeoutRegistration(registration);
    }
  }

  /**
   * Schedules a task on behalf of a request, making sure the request is cancelled
   * if the timer is stopped before the task can run.
   *
   * @return the scheduled timeout, or null if the timer was already stopped.
   */
  private @Nullable Registration scheduleRequestTask(final RequestTask task, final Duration runAfter) {
    final Registration timeout;
    try {
      timeout = timer().newTimeout(task, runAfter.toNanos(), TimeUnit.NANOSECONDS);
    } catch (IllegalStateException e) {
      // The wheel timer refuses new tasks once stopped.
      if (!stopped) {
        throw e;
      }
      task.request().cancel(CancellationReason.SHUTDOWN);
      return null;
    }

    // If stop() raced with scheduling, the wheel timer might not have returned this task
    // as unprocessed, so cancel the request here. Cancelling twice is harmless.
    if (stopped) {
      task.request().cancel(CancellationReason.SHUTDOWN);
    }
    return timeout;
  }

  /**
   * Starts this timer.
   */
  public void start() {
    wheelTimers.forEach(TimerWheel::start);
  }

  /**
   * Stops this timer.
   * <p>
   * Requests with pending timeout or retry tasks are cancelled with {@link CancellationReason#SHUTDOWN}.
   */
  public void stop() {
    stopped = true;
    for (TimerWheel wheelTimer : wheelTimers) {
      for (TimerWheel.Entry unprocessed : wheelTimer.stop()) {
        Runnable task = unprocessed.task(); // null if cancelled since the wheel stopped
        if (task instanceof RequestTask) {
          ((RequestTask) task).request().cancel(CancellationReason.SHUTDOWN);
        }
      }
    }
  }


  /**
   * Returns the number of requests currently outstanding for retry.
   */
  public long outstandingForRetry() {
    return outstandingForRetry.get();
  }

  /**
   * A timer task that acts on behalf of a request, so the request can be cancelled
   * if the timer is stopped before the task runs.
   */
  private interface RequestTask extends Runnable {
    Request<? extends Response> request();
  }

  /**
   * Cancels the request when its timeout expires.
   */
  private static final class TimeoutTask implements RequestTask {
    private final Request<? extends Response> request;

    TimeoutTask(final Request<? extends Response> request) {
      this.request = request;
    }

    @Override
    public Request<? extends Response> request() {
      return request;
    }

    @Override
    public void run() {
      request.cancel(CancellationReason.TIMEOUT);
    }
  }

  /**
   * Sends the request again once its retry delay has elapsed,
   * on this timer's thread, or on the request's blocking caller's thread.
   */
  private final class RetryTask implements RequestTask, BlockingCaller.Retry {
    private final Core core;
    private final Request<? extends Response> request;

    RetryTask(final Core core, final Request<? extends Response> request) {
      this.core = core;
      this.request = request;
    }

    void schedule(final Duration runAfter) {
      if (scheduleRequestTask(this, runAfter) == null) {
        outstandingForRetry.decrementAndGet();
      }
    }

    @Override
    public Request<? extends Response> request() {
      return request;
    }

    @Override
    public void run() {
      outstandingForRetry.decrementAndGet();
      if (!request.completed()) {
        core.send(request, false);
      }
    }

    @Override
    public void drop() {
      outstandingForRetry.decrementAndGet();
    }
  }

  @Override
  public String toString() {
    return "Timer{" +
      "wheelTimer=" + wheelTimers +
      ", stopped=" + stopped +
      ", outstandingForRetry=" + outstandingForRetry +
      ", maxNumRequestsInRetry=" + maxNumRequestsInRetry +
      '}';
  }
}
