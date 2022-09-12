/*
 * Copyright (c) 2022 Couchbase, Inc.
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

/**
 * This event is raised in case a DNS SRV refresh attempt failed (see description for more details).
 */
public class DnsSrvRefreshAttemptFailedEvent extends AbstractEvent {

  private final Throwable cause;
  private final Reason reason;

  public DnsSrvRefreshAttemptFailedEvent(final Duration duration, final Context context, final Reason reason,
                                         final Throwable cause) {
    super(reason.severity(), Category.CONFIG, duration, context);
    this.cause = cause;
    this.reason = reason;
  }

  @Override
  public String description() {
    return "DNS SRV Refresh attempt failed: " + reason;
  }

  @Override
  public Throwable cause() {
    return cause;
  }

  public enum Reason {
    NO_NEW_SEEDS_RETURNED(Severity.DEBUG),
    OTHER(Severity.WARN);

    private final Severity severity;
    Reason(final Severity severity) {
      this.severity = severity;
    }

    public Severity severity() {
      return severity;
    }
  }

}
