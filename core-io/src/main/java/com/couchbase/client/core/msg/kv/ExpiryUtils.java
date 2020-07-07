/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.request.SuspiciousExpiryDurationEvent;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.DAYS;

@Stability.Internal
public class ExpiryUtils {
  private ExpiryUtils() {
    throw new AssertionError("not instantiable");
  }

  private static final int RELATIVE_EXPIRY_CUTOFF_SECONDS = (int) DAYS.toSeconds(30);
  private static final int WORKAROUND_EXPIRY_CUTOFF_SECONDS = (int) DAYS.toSeconds(365) * 50;

  /**
   * If the duration is less than 30 days, returns the number of seconds
   * in the duration, otherwise returns the current time in Unix epoch seconds
   * plus the number of seconds in the duration.
   */
  public static long getAdjustedExpirySeconds(Duration duration, EventBus eventBus) {
    long seconds = duration.getSeconds();
    if (seconds < RELATIVE_EXPIRY_CUTOFF_SECONDS) {
      return seconds;
    }

    // Some users may have worked around JCBC-1645 by stuffing the absolute timestamp
    // into the duration. Accommodate them by passing very large values through
    // unmodified.
    if (seconds > WORKAROUND_EXPIRY_CUTOFF_SECONDS) {
      eventBus.publish(new SuspiciousExpiryDurationEvent(duration));
      return seconds;
    }

    return (System.currentTimeMillis() / 1000) + seconds;
  }
}
