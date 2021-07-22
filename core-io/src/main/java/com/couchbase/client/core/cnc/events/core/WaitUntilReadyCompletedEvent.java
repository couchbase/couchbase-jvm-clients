/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

public class WaitUntilReadyCompletedEvent extends AbstractEvent {

  private final Reason reason;

  public WaitUntilReadyCompletedEvent(final Context context, final Reason reason) {
    super(reason.severity, Category.CORE, Duration.ZERO, context);
    this.reason = reason;
  }

  @Override
  public String description() {
    if (reason == Reason.SUCCESS) {
      return "WaitUntilReady completed successfully.";
    } else if (reason == Reason.CLUSTER_LEVEL_NOT_SUPPORTED) {
      return "Cluster-Level WaitUntilReady completed without action, because it was run against a Couchbase Server " +
        "version which does not support it (only supported with 6.5 and later). Please execute " +
        "WaitUntilReady at the bucket level and open at least one bucket to perform your operations.";
    } else {
      return "WaitUntilReady completed.";
    }
  }

  public enum Reason {
    /**
     * Wait until ready has been used at the cluster level against a server which
     * does not support global configurations (only supported with  6.5 and later).
     */
    CLUSTER_LEVEL_NOT_SUPPORTED(Severity.INFO),
    /**
     * Wait until ready completed successfully.
     */
    SUCCESS(Severity.DEBUG);

    private final Severity severity;

    Reason(final Severity severity) {
      this.severity = severity;
    }
  }

}
