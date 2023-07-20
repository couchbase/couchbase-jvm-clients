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
      int newNumInstances = numInstances + 1;
      String msg = "The number of simultaneously connected Cluster instances (" + newNumInstances + ") exceeds" +
        " the configurable warning threshold of " + maxAllowedInstances + "." +
        " This is a diagnostic message to help detect potential resource leaks and inefficient usage patterns." +
        " If you actually intended to create this many instances, please ignore this warning," +
        " or increase the warning threshold by calling Cluster.maxAllowedInstances(int) on startup." +
        " However, if you did not intend to have " + newNumInstances + " Cluster instances" +
        " connected at the same time, this warning may indicate a resource leak." +
        " In that case, please make sure to call cluster.disconnect() after a Cluster" +
        " and its associated Buckets, Scopes, Collections, etc. are no longer required by your application." +
        " Also note that Cluster, Bucket, Scope, and Collection instances are thread-safe and reusable" +
        " until the Cluster is disconnected; for best performance, reuse the same instances throughout your application's lifetime.";

      if (failIfInstanceLimitReached) {
        msg += " This warning was upgraded to an exception because of an earlier call to Cluster.failIfInstanceLimitReached(true).";
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
