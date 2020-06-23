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

package com.couchbase.client.core.cnc.events.request;

import com.couchbase.client.core.cnc.AbstractEvent;

import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * @deprecated Scheduled for removal in SDK 3.1.
 */
@Deprecated
public class SuspiciousExpiryDurationEvent extends AbstractEvent {
  private final Duration duration;
  private final Throwable cause;

  public SuspiciousExpiryDurationEvent(Duration duration) {
    super(Severity.WARN, Category.REQUEST, Duration.ZERO, null);
    // ^-- pass null Context because the context isn't useful in this case

    this.duration = requireNonNull(duration);

    // Create a synthetic exception whose stack trace includes the
    // application method that used the suspicious duration.
    this.cause = new Throwable("Deprecated expiry duration usage (not a failure, just informative)");
  }

  @Override
  public String description() {
    Instant effectiveExpiry = Instant.ofEpochSecond(duration.getSeconds());

    return "The specified expiry duration " + duration + " is longer than 50 years." +
        " For bug-compatibility with previous versions of SDK 3.0.x," +
        " the number of seconds in the duration will be interpreted as the epoch second when the document should expire (" + effectiveExpiry + ")." +
        " Stuffing an epoch second into a Duration is deprecated and will no longer work in SDK 3.1.";
  }

  @Override
  public Throwable cause() {
    return cause;
  }
}
