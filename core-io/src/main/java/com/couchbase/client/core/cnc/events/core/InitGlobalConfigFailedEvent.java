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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;

import java.time.Duration;

/**
 * Raised when a global config could not be loaded.
 */
public class InitGlobalConfigFailedEvent extends AbstractEvent {

  private final Reason reason;
  private final Throwable cause;

  public InitGlobalConfigFailedEvent(Severity severity, Duration duration, CoreContext context, Reason reason,
                                     Throwable cause) {
    super(severity, Category.CORE, duration, context);
    this.reason = reason;
    if (reason == Reason.UNSUPPORTED) {
      // We do not need a stack if it is simply unsupported, it just adds noise to the debug logs
      this.cause = null;
    } else {
      this.cause = cause;
    }
  }

  @Override
  public Throwable cause() {
    return cause;
  }

  @Override
  public String description() {
    return "Initializing the global config failed: " + reason;
  }

  /**
   * The reasons why the global config init failed.
   */
  public enum Reason {
    UNSUPPORTED(Severity.DEBUG),
    NO_CONFIG_FOUND(Severity.WARN),
    UNKNOWN(Severity.WARN),
    SHUTDOWN(Severity.DEBUG);

    private final Severity severity;

    Reason(Severity severity) {
      this.severity = severity;
    }

    public Severity severity() {
      return severity;
    }
  }

}
