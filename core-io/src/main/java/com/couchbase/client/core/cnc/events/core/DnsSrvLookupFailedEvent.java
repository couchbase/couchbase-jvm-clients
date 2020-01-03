/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import java.time.Duration;

/**
 * The client attempted a DNS SRV lookup but it failed.
 */
public class DnsSrvLookupFailedEvent extends AbstractEvent {

  private final Throwable cause;
  private final Reason reason;

  public DnsSrvLookupFailedEvent(final Severity severity, final Duration duration, final Throwable cause, final Reason reason) {
    super(severity, Category.CORE, duration, null);
    this.cause = cause;
    this.reason = reason;
  }

  @Override
  public String description() {
    String cause = "";
    if (this.cause != null) {
      cause = " (" + this.cause.getMessage() + ")";
    } else if (reason != null) {
      cause = " (" + reason.identifier() + ")";
    }
    return "DNS SRV lookup failed"+ cause +", trying to bootstrap from given hostname directly.";
  }

  @Override
  public Throwable cause() {
    return cause;
  }

  public Reason reason() {
    return reason;
  }

  public enum Reason {
    NAME_NOT_FOUND("name not found"),
    OTHER("other");

    private final String identifier;

    Reason(String identifier) {
      this.identifier = identifier;
    }

    public String identifier() {
      return identifier;
    }
  }

}
