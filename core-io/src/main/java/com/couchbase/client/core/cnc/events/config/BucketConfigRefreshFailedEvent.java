/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;
import java.util.Optional;

public class BucketConfigRefreshFailedEvent extends AbstractEvent {

  private final RefresherType type;
  private final Reason reason;
  private final Optional<Object> cause;

  public BucketConfigRefreshFailedEvent(Context context, RefresherType type, Reason reason, Optional<Object> cause) {
    super(reason.severity(), Category.CONFIG, Duration.ZERO, context);
    this.type = type;
    this.reason = reason;
    this.cause = cause;
  }

  @Override
  public String description() {
    return "Reason: " + reason + ", Type: " + type + ", Cause: " + cause.map(Object::toString).orElse("none");
  }

  /**
   * The reason with severity for the failure.
   */
  public enum Reason {
    /**
     * Usually because the bucket is not open/no config there at the given point in time.
     */
    NO_BUCKET_FOUND(Severity.DEBUG),
    /**
     * An individual request for a poll/stream failed.
     */
    INDIVIDUAL_REQUEST_FAILED(Severity.DEBUG),
    /**
     * A http config stream closed without error.
     */
    STREAM_CLOSED(Severity.DEBUG),
    /**
     * A http config stream closed with an error.
     */
    STREAM_FAILED(Severity.WARN);

    private final Severity severity;

    Reason(Severity severity) {
      this.severity = severity;
    }

    Severity severity() {
      return severity;
    }
  }

  /**
   * The type of refresher that causes the failure.
   */
  public enum RefresherType {
    /**
     * The KV refresher.
     */
    KV,
    /**
     * The cluster manager refresher.
     */
    MANAGER
  }

}
