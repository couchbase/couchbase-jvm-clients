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
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.deps.io.netty.util.HashedWheelTimer;
import com.couchbase.client.core.deps.io.netty.util.Timeout;
import com.couchbase.client.core.deps.io.netty.util.concurrent.DefaultThreadFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The {@link Timer} acts as the main timing facility for various operations, for
 * example to track and time out requests if they run for too long or for rescheduling needs.
 *
 * @since 2.0.0
 */
@Stability.Internal
public class Timer {

  /**
   * We are using a default tick duration of 10ms instead of the netty-default 100ms because we are also
   * retrying operations on this timer that have much lower resolution.
   * <p>
   * Based on our testing 10ms strikes a good balance, 1ms seems to be too noisy and 100ms has too much loss
   * in accuracy.
   */
  private static final Duration DEFAULT_TICK_DURATION = Duration.ofMillis(10);

  /**
   * The internal timer.
   */
  private HashedWheelTimer wheelTimer;

  /**
   * Set to true once stopped.
   */
  private volatile boolean stopped = false;

  /**
   * Creates a new {@link Timer} with default values.
   *
   * @return the created timer.
   */
  public static Timer create() {
    return new Timer();
  }

  /**
   * Creates and starts a timer with default values.
   *
   * @return the created and started timer.
   */
  public static Timer createAndStart() {
    Timer timer = create();
    timer.start();
    return timer;
  }

  /**
   * Internal timer constructor.
   */
  private Timer() {
    wheelTimer = new HashedWheelTimer(
      new DefaultThreadFactory("cb-timer", true),
      DEFAULT_TICK_DURATION.toMillis(),
      TimeUnit.MILLISECONDS
    );
  }

  /**
   * Schedule an arbitrary task for this timer.
   */
  public Timeout schedule(Runnable callback, Duration runAfter) {
    if (stopped) {
      return null;
    }
    return wheelTimer.newTimeout(timeout -> callback.run(), runAfter.toNanos(), TimeUnit.NANOSECONDS);
  }

  /**
   * Registers the given request to be tracked with its timeout value.
   *
   * @param request the request to track.
   */
  public void register(final Request<Response> request) {
    if (stopped) {
      request.cancel(CancellationReason.SHUTDOWN);
      return;
    }

    final Timeout registration = wheelTimer.newTimeout(
      timeout -> request.cancel(CancellationReason.TIMEOUT),
      request.timeout().toNanos(),
      TimeUnit.NANOSECONDS
    );
    request.response().whenComplete((r, throwable) -> registration.cancel());
  }

  /**
   * Starts this timer.
   */
  public void start() {
    wheelTimer.start();
  }

  /**
   * Stops this timer.
   */
  public void stop() {
    stopped = true;
    wheelTimer.stop();
  }

}
