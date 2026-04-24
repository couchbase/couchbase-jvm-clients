/*
 * Copyright (c) 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.time.Duration;

@Stability.Internal
public final class CbDurations {
  private CbDurations() {

  }

  /**
   * Converts a Duration to seconds, rounding up if there are any partial seconds.
   * @param duration the duration to convert
   * @return the duration in seconds, rounded up if there are any partial seconds
   */
  public static long getSecondsCeil(final Duration duration) {
    return duration.getNano() == 0
      ? duration.getSeconds()
      : Math.addExact(duration.getSeconds(), 1L);
  }
}
