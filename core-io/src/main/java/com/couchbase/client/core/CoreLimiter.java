/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.core.TooManyInstancesDetectedEvent;
import com.couchbase.client.core.error.TooManyInstancesException;

@Stability.Internal
public class CoreLimiter {
  private CoreLimiter() {
  }

  /**
   * Number of max allowed Core instances initialized at any point in time.
   */
  private static int maxAllowedInstances = 1;

  /**
   * Number of currently initialized Core instances.
   */
  private static int numInstances = 0;

  /**
   * If core creation should fail or warn if the configured Core instance limit is reached.
   */
  private static boolean failIfInstanceLimitReached = false;

  /**
   * Configures the maximum allowed core instances before warning/failing.
   *
   * @param maxAllowedInstances the number of max allowed core instances. Must be greater than zero.
   */
  public static synchronized void setMaxAllowedInstances(final int maxAllowedInstances) {
    if (maxAllowedInstances < 1) {
      throw new IllegalArgumentException("maxAllowedInstances must be > 0, but was " + maxAllowedInstances);
    }
    CoreLimiter.maxAllowedInstances = maxAllowedInstances;
  }

  public static synchronized int getMaxAllowedInstances() {
    return maxAllowedInstances;
  }

  /**
   * Configures if the SDK should fail to create instead of warn if the instance limit is reached.
   *
   * @param failIfInstanceLimitReached true if it should throw an exception instead of warn.
   */
  public static synchronized void setFailIfInstanceLimitReached(final boolean failIfInstanceLimitReached) {
    CoreLimiter.failIfInstanceLimitReached = failIfInstanceLimitReached;
  }

  public static boolean getFailIfInstanceLimitReached() {
    return failIfInstanceLimitReached;
  }

  public static synchronized int numInstances() {
    return numInstances;
  }

  /**
   * Checks that the number of running instances never gets over the configured limit and takes action if needed.
   */
  static synchronized void incrementAndVerifyNumInstances(final EventBus eventBus) {
    if (numInstances >= maxAllowedInstances) {
      String msg = "The number of connected Cluster instances (" + (numInstances + 1) + ") exceeds " +
        "the configured limit (" + maxAllowedInstances + "). It is recommended to create only " +
        "one instance and reuse it across the application lifetime. Also, make sure to disconnect Clusters if they " +
        "are not used anymore.";

      if (failIfInstanceLimitReached) {
        throw new TooManyInstancesException(msg);
      } else {
        eventBus.publish(new TooManyInstancesDetectedEvent(msg));
      }
    }
    numInstances++;
  }

  static synchronized void decrement() {
    numInstances--;
  }
}
