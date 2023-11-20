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
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.deps.io.netty.util.HashedWheelTimer;
import com.couchbase.client.core.deps.io.netty.util.Timeout;
import com.couchbase.client.core.deps.io.netty.util.concurrent.DefaultThreadFactory;

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
@Stability.Internal
public class Timer {

  /**
   * The internal timer.
   */
  private final List<HashedWheelTimer> wheelTimers;
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
      HashedWheelTimer wheelTimer = new HashedWheelTimer(
        // Preserve original "cb-timer" name if only using default single timer.
        new DefaultThreadFactory("cb-timer" + (timerConfig.numTimers() == 1 ? "" : "-" + i), true),
        timerConfig.tickDuration().toMillis(),
        TimeUnit.MILLISECONDS,
        timerConfig.numBuckets()
      );
      wheelTimers.add(wheelTimer);
    }
  }

  /**
   * Schedules a request to be retried after the given duration.
   * <p>
   * Note that this operation performs backpressure handling for the SDK by doing account towards a maximum outstanding
   * request limit!
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

    schedule(() -> {
      if (!request.completed()) {
        core.send(request, false);
      }
    }, runAfter);
  }

  public Timeout schedule(final Runnable callback, final Duration runAfter) {
    return schedule(callback, runAfter, false);
  }

  /**
   * Schedule an arbitrary task for this timer.
   *
   * @param respectMax whether maxNumRequestsInRetry should be respected.  Will return null if the operation was not scheduled for this reason.
   */
  public Timeout schedule(final Runnable callback, final Duration runAfter, boolean respectMax) {
    if (stopped) {
      return null;
    }

    if (outstandingForRetry.incrementAndGet() >= maxNumRequestsInRetry) {
      if (respectMax) {
        outstandingForRetry.getAndDecrement();
        return null;
      }
    }

    return timer().newTimeout(timeout -> {
      outstandingForRetry.decrementAndGet();
      callback.run();
    }, runAfter.toNanos(), TimeUnit.NANOSECONDS);
  }

  private HashedWheelTimer timer() {
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

    final Timeout registration = timer().newTimeout(
      timeout -> request.cancel(CancellationReason.TIMEOUT),
      request.timeout().toNanos(),
      TimeUnit.NANOSECONDS
    );
    request.timeoutRegistration(registration);
  }

  /**
   * Starts this timer.
   */
  public void start() {
    wheelTimers.forEach(HashedWheelTimer::start);
  }

  /**
   * Stops this timer.
   */
  public void stop() {
    stopped = true;
    wheelTimers.forEach(HashedWheelTimer::stop);
  }


  /**
   * Returns the number of requests currently outstanding for retry.
   */
  public long outstandingForRetry() {
    return outstandingForRetry.get();
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
